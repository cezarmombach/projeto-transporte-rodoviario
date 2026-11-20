# DESIGN: Fatia 1 — Vendas de Passagens até o Dashboard de Receita

> Design técnico da fatia 1: Postgres OLTP → dlt → MinIO bronze → PySpark/Delta silver →
> dbt/DuckDB gold → Metabase, orquestrado por Airflow.

## Metadata

| Atributo | Valor |
|----------|-------|
| **Feature** | FATIA1_VENDAS_RECEITA |
| **Data** | 2026-11-20 |
| **Autor** | design-agent (Claude) + Cézar |
| **DEFINE** | [DEFINE_FATIA1_VENDAS_RECEITA.md](./DEFINE_FATIA1_VENDAS_RECEITA.md) |
| **Status** | Ready for Build |
| **Confiança** | 0.85 — padrões de KB encontrados (dbt, spark, airflow, sql-patterns) e agentes casados; premissas A-001/A-002/A-003/A-008 exigem spike no início do build |

> Títulos de seção em inglês por causa do `spec-lint`; conteúdo em português.

---

## Architecture Overview

```text
┌──────────────────────────────── docker compose (WSL2, ≤ 10 GB) ─────────────────────────────────┐
│                                                                                                  │
│  profile: core                         profile: airflow                         profile: bi      │
│                                                                                                  │
│  ┌──────────────┐   ┌───────────────────────────────────────────────────────┐                   │
│  │ Postgres 17  │   │ Airflow 3 (LocalExecutor) — imagem custom             │                   │
│  │  db oltp     │◄──┤  DAG transporte_diario / transporte_carga_historica   │                   │
│  │  db airflow  │   │                                                       │                   │
│  └──────┬───────┘   │  1 simular ──► 2 extrair ──► 3 silver ──► 4 dbt build │                   │
│         │           │     (Python)     (dlt)      (PySpark      (dbt-duckdb │                   │
│         │           │                             local[*])      venv)      │                   │
│         │           │                         ──► 5 publicar ──► 6 reconciliar                  │
│         │           └─────┬──────────────┬─────────────┬──────────┬─────────┘                   │
│         │ SQL incremental │              │             │          │                              │
│         └────────────────►│              │             │          │                              │
│                           ▼              ▼             │          ▼                              │
│  ┌───────────────────────────────────────────────┐    │   ┌──────────────────┐  ┌────────────┐  │
│  │ MinIO (S3)                                     │    │   │ volume ./data     │  │ Metabase   │  │
│  │  s3://bronze/oltp/<tabela>/ingest_date=D/*.pq  │────┘   │ warehouse/gold    │  │ + driver   │  │
│  │  s3://silver/<tabela>/  (Delta Lake)           │◄─delta_scan─ .duckdb (rw)  │  │ DuckDB     │  │
│  └───────────────────────────────────────────────┘        │ serving/gold      │─►│ (read-only)│  │
│                                                            │  .duckdb (cópia)  │  └────────────┘  │
│                                                            └──────────────────┘                  │
└──────────────────────────────────────────────────────────────────────────────────────────────────┘
          GitHub Actions (CI): ruff · sqlfluff · pytest · dbt build em amostra Delta local
```

**Princípio:** o Airflow só orquestra. Toda a lógica mora em `src/transporte` (Python testável),
`spark/jobs` (entrypoint fino) e `dbt/`. Cada task chama um comando CLI idempotente parametrizado
pela data lógica `D` (`data_interval_start`).

---

## Components

| Componente | Responsabilidade | Tecnologia |
|------------|------------------|------------|
| **Postgres** | OLTP (`db oltp`, schema `oltp`) e metadados do Airflow (`db airflow`) | Postgres 17 |
| **MinIO** | Object storage S3: buckets `bronze` e `silver` | MinIO (imagem fixada, ver A-008) |
| **Simulador** | Gera catálogo (cidades, empresas, rotas), vendas do dia D e cancelamentos, de forma determinística | Python + Faker + psycopg 3 |
| **Ingestão** | Extrai do OLTP a janela `updated_at ∈ [D, D+1)` e grava Parquet no bronze, sobrescrevendo a partição `ingest_date=D` | dlt (`sql_table` + destino `filesystem` S3) |
| **Silver** | Lê a partição bronze, tipa, deduplica, mascara CPF e faz MERGE na tabela Delta | PySpark 4 + delta-spark 4 (local mode, 3 GB) |
| **Gold** | Staging → marts: `fato_vendas` (incremental) e dimensões; testes | dbt-core + dbt-duckdb + DuckDB (extensões `httpfs`, `delta`) |
| **Publicação** | Copia `warehouse/gold.duckdb` → `serving/gold.duckdb` de forma atômica | Python (`shutil` + `os.replace`) |
| **Reconciliação** | Compara contagem e receita líquida OLTP × gold para a data D; falha a DAG se diferir | Python (psycopg + duckdb) |
| **Orquestração** | DAG diária (catchup) e DAG de carga histórica (manual) | Airflow 3, LocalExecutor, BashOperator |
| **BI** | Dashboard de receita | Metabase + `metabase_duckdb_driver` |
| **CI** | Lint, testes, `dbt build` em amostra | GitHub Actions + uv |

---

## Key Decisions

### Decision 1: DuckDB lê a silver Delta direto no MinIO (`delta_scan`)

| Atributo | Valor |
|----------|-------|
| **Status** | Accepted (sujeito ao spike A-001) |
| **Date** | 2026-11-20 |

**Context:** o gold (dbt + DuckDB) precisa ler a silver escrita pelo Spark em Delta Lake no MinIO.

**Choice:** sources do dbt apontam para `delta_scan('s3://silver/<tabela>')`; credenciais do MinIO
via `secrets` do perfil dbt-duckdb (`ENDPOINT`, `URL_STYLE 'path'`, `USE_SSL false`). Tabelas Delta
gravadas com `delta.enableDeletionVectors = false` e sem features de escrita avançadas.

**Rationale:** silver é a única fonte de verdade (padrão lakehouse), sem etapa de exportação e
sem dados duplicados.

**Alternatives Rejected:**
1. Spark exporta Parquet além do Delta — duplica dados e cria divergência.
2. Spark grava direto no `.duckdb` — lock frágil do arquivo, silver deixa de ser lakehouse.

**Consequences:**
- Aceitamos restringir features do Delta ao que o leitor do DuckDB suporta.
- Plano B (se o spike falhar): Spark grava também Parquet em `s3://silver-export/` (abordagem B).

---

### Decision 2: PySpark em local mode dentro da imagem do Airflow

| Atributo | Valor |
|----------|-------|
| **Status** | Accepted |
| **Date** | 2026-11-20 |

**Context:** a RAM é o recurso mais escasso (≤ 10 GB no WSL2) e o volume diário é pequeno (~3k linhas).

**Choice:** a imagem custom do Airflow inclui OpenJDK 17, PySpark e os jars `delta-spark`,
`hadoop-aws` e AWS SDK baixados no build (sem `--packages` em runtime). A task roda
`spark-submit --master local[4] --driver-memory 3g spark/jobs/silver_oltp.py --data D`.

**Rationale:** um cluster standalone (master + worker) consumiria 2–3 GB parados; local mode só
ocupa memória enquanto a task roda e continua sendo Spark + Delta real.

**Alternatives Rejected:**
1. Spark standalone em containers — RAM ociosa, mais serviços.
2. DockerOperator — exige montar o socket do Docker no Airflow (risco de segurança).

**Consequences:**
- Imagem do Airflow maior (~+700 MB).
- Trocar para cluster no futuro muda só o `--master` e a imagem.

---

### Decision 3: Publicação do gold por cópia atômica para o Metabase

| Atributo | Valor |
|----------|-------|
| **Status** | Accepted (sujeito a A-002/A-003) |
| **Date** | 2026-11-20 |

**Context:** o DuckDB permite um processo escritor **ou** vários leitores por arquivo. O dbt
escreve; o Metabase lê continuamente.

**Choice:** o dbt escreve em `data/warehouse/gold.duckdb`. A task `publicar` copia para
`data/serving/gold.duckdb.tmp` e faz `os.replace` para `data/serving/gold.duckdb`. O Metabase monta
`data/serving` como read-only.

**Rationale:** separa escrita e leitura (padrão blue/green simplificado) sem um banco extra.

**Alternatives Rejected:**
1. Metabase lendo o arquivo do dbt — conflito de lock durante o `dbt build`.
2. Publicar o gold em Postgres — mais robusto para BI, mas adiciona uma etapa de carga; fica como
   plano B se o driver DuckDB do Metabase falhar (A-002).

**Consequences:**
- O Metabase pode manter conexão aberta no arquivo antigo até reconectar; o build valida se o
  driver reabre o arquivo (senão, a task chama a API do Metabase para re-sincronizar).

---

### Decision 4: Idempotência por data lógica em todas as camadas

| Atributo | Valor |
|----------|-------|
| **Status** | Accepted |
| **Date** | 2026-11-20 |

**Context:** o DEFINE exige que 3 reexecuções da mesma data deixem totais idênticos (AT-002).

**Choice:**

| Camada | Mecanismo |
|--------|-----------|
| Simulador | `passagem_id` determinístico por (D, sequência) + `INSERT ... ON CONFLICT DO NOTHING`; cancelamentos escolhidos por seed(D) e aplicados só se `status = 'vendida'` |
| Bronze | Janela fechada `updated_at ∈ [D, D+1)` (dlt `incremental` com `end_value`, sem estado) + remoção do prefixo `ingest_date=D` antes de gravar |
| Silver | `MERGE` por `passagem_id`, atualiza só se `s.updated_at > t.updated_at` |
| Gold | `fato_vendas` incremental `delete+insert` por `passagem_id` com lookback de 3 dias |

**Rationale:** a data lógica do Airflow é a única fonte do intervalo; nada depende do relógio.

**Alternatives Rejected:**
1. Estado incremental do dlt (watermark salvo) — reexecutar uma data antiga não reprocessaria a janela.

**Consequences:** reprocessar D é sempre seguro; o backfill do Airflow funciona.

---

### Decision 5: Carga histórica em lote, separada da DAG diária

| Atributo | Valor |
|----------|-------|
| **Status** | Accepted |
| **Date** | 2026-11-20 |

**Context:** 730 execuções da DAG diária custariam ~40–60 s cada (startup do Spark e do dbt),
ou seja, 8–12 h — acima do critério de ≤ 3 h do DEFINE.

**Choice:** DAG `transporte_carga_historica` (gatilho manual): simula o intervalo inteiro, extrai
em uma única carga (`ingest_date = data_fim`), roda o Spark uma vez e o dbt com `--full-refresh`.
A DAG diária começa em `data_fim + 1` e o backfill do Airflow fica para intervalos curtos.

**Rationale:** mesmo código, parâmetros diferentes; cumpre o tempo sem atalhos.

**Consequences:** o critério "backfill de 730 dias ≤ 3 h" passa a ser medido na DAG de carga
histórica. **Revisor (janela 2): confirmar e ajustar o texto no DEFINE.**

---

### Decision 6: Um Postgres para OLTP e metadados do Airflow

| Atributo | Valor |
|----------|-------|
| **Status** | Accepted |
| **Date** | 2026-11-20 |

**Choice:** um container Postgres com dois bancos (`oltp`, `airflow`) e usuários distintos,
criados por `sql/init/00_databases.sql`.

**Rationale:** economiza ~300 MB de RAM; o isolamento por banco e por usuário é suficiente localmente.

**Alternatives Rejected:** dois containers Postgres — mais RAM sem ganho na fatia 1.

---

### Decision 7: dbt em virtualenv separado dentro da imagem

**Choice:** `/opt/dbt-venv` com dbt-core + dbt-duckdb; o projeto (`transporte`) e dlt no ambiente
do Airflow, instalados respeitando o arquivo de constraints do Airflow.

**Rationale:** dbt e Airflow têm conflitos de dependências conhecidos; venvs separados evitam
quebrar a imagem a cada upgrade.

---

## File Manifest

| # | Arquivo | Ação | Propósito | Agente | Dependências |
|---|---------|------|-----------|--------|--------------|
| 1 | `pyproject.toml` | Modify | Dependências por grupo (`pipeline`, `spark`, `dbt`, `dev`), script `transporte`, markers `spark`/`integration` | @python-developer | None |
| 2 | `.env.example` | Modify | Todas as variáveis da seção Configuration | @data-platform-engineer | None |
| 3 | `sql/init/00_databases.sql` | Create | Cria bancos `oltp`/`airflow` e usuários | @schema-designer | None |
| 4 | `sql/init/10_oltp_schema.sql` | Create | Tabelas `cidade`, `empresa`, `rota`, `passagem` + índices (`updated_at`) | @schema-designer | 3 |
| 5 | `docker/airflow/Dockerfile` | Create | Airflow 3 + Java 17 + PySpark + jars Delta/S3A + venv dbt + pacote `transporte` | @data-platform-engineer | 1 |
| 6 | `docker/metabase/Dockerfile` | Create | Metabase + jar `metabase_duckdb_driver` | @data-platform-engineer | None |
| 7 | `docker-compose.yml` | Create | Serviços, profiles `core`/`airflow`/`bi`, limites de memória, volumes, criação dos buckets | @data-platform-engineer | 2, 3, 5, 6 |
| 8 | `Makefile` | Create | `up`, `down`, `historico`, `test`, `lint`, `dbt-ci` | @shell-script-specialist | 7 |
| 9 | `src/transporte/common/config.py` | Create | Settings tipadas a partir de env (pydantic-settings) | @python-developer | 1 |
| 10 | `src/transporte/common/logging.py` | Create | Logging estruturado (key=value) com contagens por etapa | @python-developer | None |
| 11 | `src/transporte/simulator/catalogo.py` | Create | Catálogo determinístico: ~40 cidades, ~8 empresas, ~120 rotas | @python-developer | 9 |
| 12 | `src/transporte/simulator/gerador.py` | Create | Funções puras: vendas do dia D (sazonalidade) e cancelamentos | @python-developer | 11 |
| 13 | `src/transporte/simulator/repositorio.py` | Create | Escrita idempotente no Postgres (upsert de catálogo, insert/cancel) | @python-developer | 4, 12 |
| 14 | `src/transporte/ingestion/oltp.py` | Create | Source dlt com janela `[inicio, fim)` e pipeline para o bronze com overwrite da partição | @python-developer | 9 |
| 15 | `src/transporte/silver/__init__.py` | Create | Pacote silver | @spark-engineer | None |
| 16 | `src/transporte/silver/session.py` | Create | Factory de SparkSession (Delta + S3A/MinIO) | @spark-engineer | 9 |
| 17 | `src/transporte/silver/transforms.py` | Create | Funções puras de DataFrame: tipagem, dedup, máscara de CPF | @spark-engineer | None |
| 18 | `src/transporte/silver/merge.py` | Create | MERGE Delta de `passagem`; overwrite das dimensões | @spark-engineer | 16, 17 |
| 19 | `spark/jobs/silver_oltp.py` | Create | Entrypoint `spark-submit` (args `--data`/`--historico`) | @spark-engineer | 18 |
| 20 | `src/transporte/quality/__init__.py` | Create | Pacote de qualidade | @data-quality-analyst | None |
| 21 | `src/transporte/quality/reconciliacao.py` | Create | OLTP × gold por data: contagem e receita líquida | @data-quality-analyst | 9 |
| 22 | `src/transporte/serving/__init__.py` | Create | Pacote de serving | @python-developer | None |
| 23 | `src/transporte/serving/publicar.py` | Create | Cópia atômica warehouse → serving | @python-developer | 9 |
| 24 | `src/transporte/cli.py` | Create | CLI: `catalogo`, `simular`, `extrair`, `publicar`, `reconciliar` | @python-developer | 13, 14, 21, 23 |
| 25 | `dbt/dbt_project.yml` | Create | Projeto, materializações por pasta | @dbt-specialist | None |
| 26 | `dbt/profiles.yml` | Create | Targets `dev` (MinIO) e `ci` (Delta local), extensões e secrets via env | @dbt-specialist | 25 |
| 27 | `dbt/packages.yml` | Create | `dbt_utils` (versão fixada) | @dbt-specialist | 25 |
| 28 | `dbt/models/staging/oltp/_oltp__sources.yml` | Create | Sources silver via `delta_scan`, freshness | @dbt-specialist | 26 |
| 29 | `dbt/models/staging/oltp/_oltp__models.yml` | Create | Docs e testes de staging | @dbt-specialist | 28 |
| 30 | `dbt/models/staging/oltp/stg_oltp__passagem.sql` | Create | Renomeia/tipa; calcula `valor_liquido` | @dbt-specialist | 28 |
| 31 | `dbt/models/staging/oltp/stg_oltp__rota.sql` | Create | Staging de rota | @dbt-specialist | 28 |
| 32 | `dbt/models/staging/oltp/stg_oltp__empresa.sql` | Create | Staging de empresa | @dbt-specialist | 28 |
| 33 | `dbt/models/staging/oltp/stg_oltp__cidade.sql` | Create | Staging de cidade | @dbt-specialist | 28 |
| 34 | `dbt/models/marts/core/_core__models.yml` | Create | Docs, PKs, relationships, accepted_values | @dbt-specialist | 35–39 |
| 35 | `dbt/models/marts/core/dim_tempo.sql` | Create | Calendário (`generate_series`), `data_sk` = yyyymmdd | @dbt-specialist | 25 |
| 36 | `dbt/models/marts/core/dim_cidade.sql` | Create | Dimensão cidade (sem população na fatia 1) | @dbt-specialist | 33 |
| 37 | `dbt/models/marts/core/dim_empresa.sql` | Create | Dimensão empresa | @dbt-specialist | 32 |
| 38 | `dbt/models/marts/core/dim_rota.sql` | Create | Rota com origem/destino/empresa/distância | @dbt-specialist | 31 |
| 39 | `dbt/models/marts/core/fato_vendas.sql` | Create | Fato incremental, grão = 1 passagem | @dbt-specialist | 30, 35–38 |
| 40 | `dbt/tests/assert_viagem_nao_antes_da_compra.sql` | Create | Teste singular de regra de negócio | @data-quality-analyst | 39 |
| 41 | `.sqlfluff` | Create | Dialeto DuckDB, templater dbt (target `ci`) | @dbt-specialist | 26 |
| 42 | `airflow/dags/_defaults.py` | Create | `default_args` (retries, backoff), comandos base | @airflow-specialist | None |
| 43 | `airflow/dags/transporte_diario.py` | Create | DAG diária: simular → extrair → silver → dbt → publicar → reconciliar | @airflow-specialist | 19, 24, 42 |
| 44 | `airflow/dags/transporte_carga_historica.py` | Create | DAG manual da carga histórica | @airflow-specialist | 19, 24, 42 |
| 45 | `scripts/gerar_amostra_ci.py` | Create | Gera amostra Delta local (deltalake/delta-rs) a partir do simulador para o CI | @python-developer | 12 |
| 46 | `.github/workflows/ci.yml` | Create | Jobs `lint`, `test`, `dbt` | @ci-cd-specialist | 41, 45 |
| 47 | `.pre-commit-config.yaml` | Modify | Adiciona sqlfluff | @ci-cd-specialist | 41 |
| 48 | `tests/conftest.py` | Create | Fixtures: settings de teste, SparkSession local | @test-generator | 9, 16 |
| 49 | `tests/unit/test_gerador.py` | Create | Determinismo, volume, cancelamentos, dia sem vendas | @test-generator | 12 |
| 50 | `tests/unit/test_transforms_silver.py` | Create | Dedup, tipagem, máscara (marker `spark`) | @test-generator | 17 |
| 51 | `tests/unit/test_reconciliacao.py` | Create | Lógica de comparação com dados em memória | @test-generator | 21 |
| 52 | `tests/unit/test_publicar.py` | Create | Cópia atômica (arquivo tmp + replace) | @test-generator | 23 |
| 53 | `tests/integration/test_dags.py` | Create | DagBag sem erros de import, sem ciclos (roda no container) | @airflow-specialist | 43, 44 |
| 54 | `tests/integration/test_pipeline_e2e.py` | Create | AT-001/002/003/004/007/009 contra o compose no ar | @test-generator | 7, 43 |
| 55 | `docs/adr/0001-duckdb-le-delta-no-minio.md` | Create | ADR da Decision 1 | @code-documenter | None |
| 56 | `docs/adr/0002-spark-local-mode-no-airflow.md` | Create | ADR da Decision 2 | @code-documenter | None |
| 57 | `docs/adr/0003-publicacao-gold-metabase.md` | Create | ADR da Decision 3 | @code-documenter | None |
| 58 | `docs/adr/0004-idempotencia-por-data-logica.md` | Create | ADR das Decisions 4 e 5 | @code-documenter | None |
| 59 | `docs/metabase/dashboard-receita.md` | Create | Passo a passo do dashboard (3 perguntas + SQL) | @code-documenter | 39 |
| 60 | `README.md` | Modify | Como subir, rodar a carga histórica, testar | @code-documenter | 8 |

**Total de arquivos:** 60 (55 novos, 5 modificados).

---

## Agent Assignment Rationale

| Agente | Arquivos | Por quê |
|--------|----------|---------|
| @python-developer | 1, 9–14, 22–24, 45 | Pacote Python, CLI, simulador, integração dlt |
| @schema-designer | 3, 4 | DDL do OLTP, chaves e índices |
| @data-platform-engineer | 2, 5–7 | Compose, imagens, limites de recursos |
| @spark-engineer | 15–19 | PySpark, Delta MERGE, S3A |
| @dbt-specialist | 25–39, 41 | Modelagem dbt, incremental, perfis DuckDB |
| @data-quality-analyst | 20, 21, 40 | Reconciliação e regras de negócio |
| @airflow-specialist | 42–44, 53 | Design de DAG, retries, backfill |
| @ci-cd-specialist | 46, 47 | GitHub Actions e pre-commit |
| @test-generator | 48–52, 54 | Fixtures e testes pytest |
| @shell-script-specialist | 8 | Makefile |
| @code-documenter | 55–60 | ADRs e documentação |

**Descoberta de agentes:** varredura de `agents/**/*.md` do plugin AgentSpec 3.6.1; casamento por
tipo de arquivo, caminho e domínio de KB.

---

## Code Patterns

### Pattern 1: Simulador determinístico e idempotente

```python
# src/transporte/simulator/gerador.py
from __future__ import annotations

import hashlib
import random
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from decimal import Decimal

IDS_POR_DIA = 100_000  # passagem_id = ordinal(D) * IDS_POR_DIA + seq


@dataclass(frozen=True)
class Passagem:
    passagem_id: int
    rota_id: int
    data_compra: datetime
    data_viagem: date
    valor: Decimal
    passageiro_cpf: str


def rng_do_dia(seed_base: int, dia: date) -> random.Random:
    """Gerador isolado por data: rodar D de novo produz exatamente as mesmas vendas."""
    digest = hashlib.sha256(f"{seed_base}:{dia.isoformat()}".encode()).digest()
    return random.Random(int.from_bytes(digest[:8], "big"))


def passagem_id(dia: date, seq: int) -> int:
    if seq >= IDS_POR_DIA:
        raise ValueError("sequência diária excedeu IDS_POR_DIA")
    return dia.toordinal() * IDS_POR_DIA + seq


def instante_no_dia(rng: random.Random, dia: date) -> datetime:
    return datetime.combine(dia, time.min) + timedelta(seconds=rng.randrange(86_400))
```

```sql
-- src/transporte/simulator/repositorio.py (SQL usado)
INSERT INTO oltp.passagem (passagem_id, rota_id, data_compra, data_viagem, valor, status,
                           passageiro_cpf, created_at, updated_at)
VALUES (%s, %s, %s, %s, %s, 'vendida', %s, %s, %s)
ON CONFLICT (passagem_id) DO NOTHING;

-- cancelamento: só altera quem ainda está vendida → reexecução não muda nada
UPDATE oltp.passagem
   SET status = 'cancelada', updated_at = %(instante)s
 WHERE passagem_id = ANY(%(ids)s) AND status = 'vendida';
```

### Pattern 2: Extração dlt com janela fechada e overwrite da partição

```python
# src/transporte/ingestion/oltp.py
from datetime import date, datetime, time, timedelta

import dlt
import s3fs
from dlt.sources.sql_database import sql_table

from transporte.common.config import Settings

LAYOUT = "{table_name}/ingest_date={ingest_date}/{load_id}.{file_id}.{ext}"


def extrair_dia(cfg: Settings, dia: date) -> None:
    inicio = datetime.combine(dia, time.min)
    fim = inicio + timedelta(days=1)

    # 1. Idempotência: remove a partição antes de gravar de novo
    fs = s3fs.S3FileSystem(
        key=cfg.minio_user,
        secret=cfg.minio_password,
        client_kwargs={"endpoint_url": cfg.minio_endpoint},
    )
    for tabela in ("passagem", "rota", "empresa", "cidade"):
        prefixo = f"bronze/oltp/{tabela}/ingest_date={dia.isoformat()}"
        if fs.exists(prefixo):
            fs.rm(prefixo, recursive=True)

    # 2. Janela [inicio, fim): end_value faz o dlt rodar sem salvar estado
    passagem = sql_table(
        credentials=cfg.oltp_url,
        schema="oltp",
        table="passagem",
        backend="pyarrow",
        incremental=dlt.sources.incremental(
            "updated_at",
            initial_value=inicio,
            end_value=fim,
            range_end="open",
        ),
    )
    dimensoes = [
        sql_table(credentials=cfg.oltp_url, schema="oltp", table=t, backend="pyarrow")
        for t in ("rota", "empresa", "cidade")
    ]

    destino = dlt.destinations.filesystem(
        bucket_url="s3://bronze/oltp",
        layout=LAYOUT,
        extra_placeholders={"ingest_date": dia.isoformat()},
    )
    pipeline = dlt.pipeline(pipeline_name="oltp_bronze", destination=destino, dataset_name="oltp")
    info = pipeline.run(
        [passagem, *dimensoes], loader_file_format="parquet", write_disposition="append"
    )
    info.raise_on_failed_jobs()
```

> Validar no build: nomes exatos de `range_end` e `extra_placeholders` na versão fixada do dlt.

### Pattern 3: Silver — dedup + MERGE Delta

```python
# src/transporte/silver/transforms.py
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F


def deduplicar(df: DataFrame, chave: str = "passagem_id", ordem: str = "updated_at") -> DataFrame:
    """Mantém só a versão mais recente de cada chave (AT-004)."""
    w = Window.partitionBy(chave).orderBy(F.col(ordem).desc())
    return df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")


def mascarar_cpf(df: DataFrame, salt: str) -> DataFrame:
    return df.withColumn(
        "passageiro_hash", F.sha2(F.concat(F.lit(salt), F.col("passageiro_cpf")), 256)
    ).drop("passageiro_cpf")
```

```python
# src/transporte/silver/merge.py
from delta.tables import DeltaTable

PROPS = {"delta.enableDeletionVectors": "false"}  # compatível com delta_scan do DuckDB


def merge_passagem(spark, origem, caminho: str) -> None:
    if not DeltaTable.isDeltaTable(spark, caminho):
        (
            origem.write.format("delta")
            .partitionBy("ano_mes_compra")
            .options(**PROPS)  # opções "delta.*" viram table properties na criação
            .save(caminho)
        )
        return
    (
        DeltaTable.forPath(spark, caminho)
        .alias("t")
        .merge(origem.alias("s"), "t.passagem_id = s.passagem_id")
        .whenMatchedUpdateAll(condition="s.updated_at > t.updated_at")
        .whenNotMatchedInsertAll()
        .execute()
    )
```

### Pattern 4: Perfil dbt-duckdb lendo Delta no MinIO

```yaml
# dbt/profiles.yml
transporte:
  target: "{{ env_var('DBT_TARGET', 'dev') }}"
  outputs:
    dev:
      type: duckdb
      path: "{{ env_var('GOLD_DUCKDB_PATH', '/data/warehouse/gold.duckdb') }}"
      threads: 4
      extensions: [httpfs, delta]
      secrets:
        - type: s3
          key_id: "{{ env_var('MINIO_ROOT_USER') }}"
          secret: "{{ env_var('MINIO_ROOT_PASSWORD') }}"
          endpoint: "{{ env_var('MINIO_ENDPOINT_HOST', 'minio:9000') }}"
          url_style: path
          use_ssl: false
          region: us-east-1
    ci:
      type: duckdb
      path: "target/ci.duckdb"
      threads: 2
      extensions: [delta]
```

```yaml
# dbt/models/staging/oltp/_oltp__sources.yml
version: 2
sources:
  - name: silver
    meta:
      external_location: "delta_scan('{{ env_var('SILVER_ROOT', 's3://silver') }}/{name}')"
    tables:
      - name: passagem
        loaded_at_field: updated_at
        freshness:
          warn_after: {count: 1, period: day}
          error_after: {count: 2, period: day}
      - name: rota
      - name: empresa
      - name: cidade
```

### Pattern 5: Fato incremental com lookback

```sql
-- dbt/models/marts/core/fato_vendas.sql
{{
    config(
        materialized='incremental',
        unique_key='passagem_id',
        incremental_strategy='delete+insert',
        on_schema_change='fail'
    )
}}

with passagens as (
    select * from {{ ref('stg_oltp__passagem') }}
    {% if is_incremental() %}
    where updated_at > (select max(updated_at) - interval 3 day from {{ this }})
    {% endif %}
)

select
    p.passagem_id,
    r.rota_sk,
    r.empresa_sk,
    r.cidade_origem_sk,
    r.cidade_destino_sk,
    cast(strftime(p.data_compra, '%Y%m%d') as integer) as data_compra_sk,
    cast(strftime(p.data_viagem, '%Y%m%d') as integer) as data_viagem_sk,
    p.status,
    p.valor,
    p.valor_liquido,          -- valor se vendida, 0 se cancelada
    p.data_compra,
    p.updated_at
from passagens p
inner join {{ ref('dim_rota') }} r on r.rota_id = p.rota_id
```

> O `inner join` com `dim_rota` + teste `relationships` na staging garante o AT-006 (rota órfã falha
> no teste antes de sumir silenciosamente do fato).

### Pattern 6: DAG diária

```python
# airflow/dags/transporte_diario.py
from datetime import datetime

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG

from _defaults import DEFAULT_ARGS, DATA, DBT, SPARK_SUBMIT

with DAG(
    dag_id="transporte_diario",
    start_date=datetime(2026, 11, 20),  # = data_fim da carga histórica + 1
    schedule="@daily",
    catchup=True,
    max_active_runs=1,  # MERGE/dbt nunca concorrentes
    default_args=DEFAULT_ARGS,
    tags=["fatia1", "vendas"],
) as dag:
    simular = BashOperator(task_id="simular", bash_command=f"transporte simular --data {DATA}")
    extrair = BashOperator(task_id="extrair", bash_command=f"transporte extrair --data {DATA}")
    silver = BashOperator(
        task_id="silver",
        bash_command=f"{SPARK_SUBMIT} /opt/transporte/spark/jobs/silver_oltp.py --data {DATA}",
    )
    dbt_build = BashOperator(task_id="dbt_build", bash_command=f"{DBT} build")
    publicar = BashOperator(task_id="publicar", bash_command="transporte publicar")
    reconciliar = BashOperator(
        task_id="reconciliar",
        bash_command=f"transporte reconciliar --data {DATA}",
        retries=0,
    )

    simular >> extrair >> silver >> dbt_build >> publicar >> reconciliar
```

```python
# airflow/dags/_defaults.py
from datetime import timedelta

DATA = "{{ data_interval_start | ds }}"
DBT = "cd /opt/transporte/dbt && /opt/dbt-venv/bin/dbt"
SPARK_SUBMIT = "spark-submit --master local[4] --driver-memory 3g"

DEFAULT_ARGS = {
    "owner": "dados",
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=10),
    "execution_timeout": timedelta(minutes=20),
}
```

### Pattern 7: Publicação atômica

```python
# src/transporte/serving/publicar.py
import os
import shutil
from pathlib import Path


def publicar(origem: Path, destino: Path) -> None:
    destino.parent.mkdir(parents=True, exist_ok=True)
    tmp = destino.with_suffix(".duckdb.tmp")
    shutil.copy2(origem, tmp)
    os.replace(tmp, destino)  # atômico no mesmo filesystem
```

---

## Data Flow

```text
1. simular D       → INSERT vendas de D (~1,4k–7k) + UPDATE ~5% de cancelamentos de vendas
   │                 recentes com viagem futura. updated_at ∈ [D, D+1).
   ▼
2. extrair D       → dlt lê passagem com updated_at ∈ [D, D+1) + dimensões completas
   │                 → s3://bronze/oltp/<tabela>/ingest_date=D/*.parquet (partição sobrescrita)
   ▼
3. silver D        → Spark lê a partição D → tipa → dedup → hash do CPF
   │                 → MERGE em s3://silver/passagem (Delta); overwrite de rota/empresa/cidade
   ▼
4. dbt build       → staging (views sobre delta_scan) → dims (table) → fato_vendas (incremental)
   │                 → testes (falha = DAG falha, nada é publicado)
   ▼
5. publicar        → warehouse/gold.duckdb → serving/gold.duckdb (atômico)
   ▼
6. reconciliar D   → OLTP vs serving: count e sum(valor_liquido) com data_compra = D → iguais?
```

---

## Integration Points

| Sistema | Integração | Autenticação |
|---------|------------|--------------|
| Postgres (OLTP) | psycopg 3 (simulador, reconciliação), SQLAlchemy (dlt) | Usuário/senha via `.env` |
| MinIO | S3 API: s3fs (dlt), S3A (Spark), httpfs (DuckDB) | Access/secret key via `.env` |
| DuckDB (arquivo) | dbt-duckdb (escrita), `duckdb` Python (reconciliação), driver JDBC (Metabase) | Arquivo local; serving montado read-only |
| Metabase | UI; API opcional para re-sync após publicar | Admin local |
| GitHub Actions | Workflow em push/PR | `GITHUB_TOKEN` padrão; sem secrets externos |

---

## Testing Strategy

| Tipo | Escopo | Arquivos | Ferramentas | Meta |
|------|--------|----------|-------------|------|
| Unit | Simulador, reconciliação, publicação | `tests/unit/test_gerador.py`, `test_reconciliacao.py`, `test_publicar.py` | pytest | ≥ 80% de cobertura em `src/transporte` (exceto I/O) |
| Unit (Spark) | Transforms silver | `tests/unit/test_transforms_silver.py` | pytest + SparkSession local (marker `spark`) | Todas as funções de `transforms.py` |
| dbt | Modelos gold | `_oltp__models.yml`, `_core__models.yml`, `dbt/tests/*.sql` | dbt test | 100% dos modelos com PK testada |
| Integração | DAGs | `tests/integration/test_dags.py` | pytest + DagBag (no container) | 0 erros de import |
| E2E | Pipeline completo | `tests/integration/test_pipeline_e2e.py` | pytest + compose no ar (marker `integration`) | ATs abaixo |
| Manual | Falha transitória e dashboard | Roteiro no README | `docker compose stop minio` | AT-005, AT-001 (dashboard) |

**Cobertura dos testes de aceite:**

| AT | Onde é verificado |
|----|-------------------|
| AT-001 caminho feliz | E2E: roda D, confere linhas em bronze/silver/fato; dashboard manual |
| AT-002 idempotência | E2E: roda D 3x, compara contagens e soma por camada |
| AT-003 cancelamento tardio | Unit (gerador) + E2E: cancela passagem de D em D+3, confere silver e receita de D |
| AT-004 duplicata | Unit Spark: `deduplicar` com 2 versões da mesma chave |
| AT-005 falha transitória | Manual: parar MinIO durante `extrair`, conferir retry e ausência de duplicatas |
| AT-006 qualidade bloqueia | dbt: teste `relationships` + E2E inserindo rota órfã no OLTP |
| AT-007 reconciliação | Task `reconciliar` em toda execução + unit da lógica |
| AT-008 CI | Workflow `ci.yml` em PR (branch protection no `main`) |
| AT-009 dia sem vendas | Unit (gerador com volume 0) + E2E com `SIMULADOR_VOLUME_DIA=0` |

**CI (`.github/workflows/ci.yml`):**

| Job | Passos |
|-----|--------|
| `lint` | `uv sync` → `ruff check` → `ruff format --check` → `sqlfluff lint dbt/models` |
| `test` | `uv sync` → `pytest -m "not integration"` (Java do runner para o marker `spark`) |
| `dbt` | `uv run scripts/gerar_amostra_ci.py` (Delta local via delta-rs) → `dbt deps` → `dbt build --target ci` |

---

## Error Handling

| Erro | Estratégia | Retry? |
|------|------------|--------|
| Postgres/MinIO indisponível | Retry com backoff exponencial (2x, até 10 min); tasks idempotentes tornam o retry seguro | Sim |
| Spark OOM | Falha a task; log com memória configurada; ajustar `--driver-memory`/partições | Não (automático) |
| Teste dbt falha | DAG falha em `dbt_build`; `publicar` não roda → Metabase mantém a última versão boa | Não |
| Reconciliação diverge | Task falha com contagens/somas dos dois lados no log | Não (`retries=0`) |
| Schema inesperado no bronze | Spark aplica schema explícito; coluna nova falha o job (ver Schema Evolution) | Não |
| Timeout | `execution_timeout` de 20 min por task | Sim (via retries) |

---

## Configuration

| Chave (env) | Tipo | Padrão | Descrição |
|-------------|------|--------|-----------|
| `POSTGRES_USER` / `POSTGRES_PASSWORD` | string | — | Superusuário do container Postgres |
| `OLTP_URL` | string | `postgresql+psycopg://oltp:...@postgres:5432/oltp` | Conexão do simulador/dlt |
| `MINIO_ROOT_USER` / `MINIO_ROOT_PASSWORD` | string | — | Credenciais do MinIO |
| `MINIO_ENDPOINT` | string | `http://minio:9000` | Endpoint S3 |
| `SIMULADOR_SEED` | int | `42` | Seed base do simulador |
| `SIMULADOR_VOLUME_DIA` | int | `2700` | Média de vendas por dia (sazonalidade ±50%) |
| `SIMULADOR_TAXA_CANCELAMENTO` | float | `0.05` | Fração de vendas canceladas |
| `HISTORICO_INICIO` / `HISTORICO_FIM` | date | `2024-11-20` / `2026-11-19` | Intervalo da carga histórica (730 dias) |
| `CPF_HASH_SALT` | string | — | Salt do hash do CPF na silver |
| `GOLD_DUCKDB_PATH` | path | `/data/warehouse/gold.duckdb` | Arquivo do dbt |
| `SERVING_DUCKDB_PATH` | path | `/data/serving/gold.duckdb` | Arquivo lido pelo Metabase |
| `SPARK_DRIVER_MEMORY` | string | `3g` | Memória do Spark local |

Limites de memória do compose (soma ≤ 10 GB): Postgres 1 GB · MinIO 1 GB · Airflow
(api-server + scheduler + dag-processor) 4,5 GB incluindo Spark 3 GB durante a task · Metabase
1,5 GB (profile `bi`, pode subir depois da carga).

---

## Security Considerations

- Credenciais só em `.env` (gitignored); `.env.example` sem valores reais; `detect-private-key` no pre-commit.
- CPF (fictício) nunca sai do bronze em claro: a silver guarda só `sha256(salt || cpf)`; o gold não tem a coluna.
- Portas publicadas apenas em `127.0.0.1`; usuários distintos para `oltp` e `airflow` no Postgres.
- Metabase monta o serving como read-only; o Airflow não recebe o socket do Docker.
- Imagens com tags fixadas (nada de `latest`).

---

## Observability

| Aspecto | Implementação |
|---------|---------------|
| Logging | Logging estruturado `key=value` por etapa (`etapa=extrair data=D linhas=N`) nos logs do Airflow |
| Métricas | Contagens por camada logadas e retornadas por task; `run_results.json` do dbt; `DESCRIBE HISTORY` do Delta |
| Linhagem | dbt docs (source → fato); metadados `_dlt_load_id` no bronze |
| Frescor | `dbt source freshness` em `silver.passagem` |
| Tracing | N/A na fatia 1 |

---

## Pipeline Architecture

### DAG Diagram

```text
transporte_diario (@daily, catchup, max_active_runs=1)
  simular ─► extrair ─► silver ─► dbt_build ─► publicar ─► reconciliar
                                     │ falha
                                     └──► publicar não roda (Metabase fica na versão anterior)

transporte_carga_historica (manual)
  catalogo ─► simular_intervalo ─► extrair_intervalo ─► silver --historico ─► dbt build --full-refresh ─► publicar ─► reconciliar_amostra
```

### Partition Strategy

| Tabela | Chave de partição | Granularidade | Motivo |
|--------|-------------------|---------------|--------|
| bronze `oltp/*` | `ingest_date` | Diária | Reprocessamento por data; overwrite simples |
| silver `passagem` | `ano_mes_compra` | Mensal | ~2M linhas: partição diária geraria arquivos pequenos demais |
| silver `rota`/`empresa`/`cidade` | — | — | Tabelas pequenas, overwrite completo |
| gold `fato_vendas` | — | — | DuckDB local; volume não justifica |

### Incremental Strategy

| Modelo | Estratégia | Chave | Lookback |
|--------|------------|-------|----------|
| silver `passagem` | MERGE (upsert, última versão vence) | `passagem_id` / `updated_at` | Partição D do bronze |
| `fato_vendas` | `delete+insert` | `passagem_id` | 3 dias de `updated_at` |
| dimensões gold | `table` (full refresh) | — | — |
| `dim_tempo` | `table` | `data_sk` | 2024-01-01 a 2027-12-31 |

### Schema Evolution Plan

| Mudança | Tratamento | Rollback |
|---------|------------|----------|
| Coluna nova no OLTP | Bronze aceita (Parquet); silver tem schema explícito → adicionar no schema + `mergeSchema` controlado; dbt `on_schema_change='fail'` força atualização consciente | Delta time travel (`VERSION AS OF`) |
| Mudança de tipo | Nova coluna com o tipo novo; migrar consumidores; remover a antiga depois | Time travel |
| Coluna removida | Manter nula na silver por um ciclo; remover do contrato dbt depois | Re-adicionar coluna |

### Data Quality Gates

| Gate | Ferramenta | Limite | Ação na falha |
|------|------------|--------|---------------|
| PK não nula e única (silver e gold) | dbt `not_null` + `unique` | 0 violações | Bloqueia publicação |
| FKs do fato | dbt `relationships` | 0 órfãos | Bloqueia publicação |
| `status` válido | dbt `accepted_values` (`vendida`, `cancelada`) | 0 violações | Bloqueia publicação |
| `valor > 0` e viagem ≥ compra | dbt teste singular / `dbt_utils.expression_is_true` | 0 violações | Bloqueia publicação |
| Reconciliação OLTP × gold | Task `reconciliar` | Diferença 0 | DAG falha |
| Frescor da silver | `dbt source freshness` | warn 1 dia / error 2 dias | Warn no log / falha |

---

## Riscos e Spikes do Build (fazer primeiro)

| ID | Spike | Critério de saída | Plano B |
|----|-------|-------------------|---------|
| A-001 | Spark grava Delta no MinIO → DuckDB `delta_scan` lê | `select count(*)` bate | Spark exporta Parquet (abordagem B) |
| A-002 | Metabase + `metabase_duckdb_driver` abre o serving | Pergunta simples retorna dados | Publicar gold em Postgres |
| A-003 | Metabase enxerga o arquivo novo após `os.replace` | Dado novo aparece sem reiniciar | Chamar API de re-sync / reconectar |
| A-008 | Imagem do MinIO disponível e fixável | `docker pull` da tag escolhida | Usar a última imagem community publicada ou alternativa S3 (SeaweedFS/Garage) |

> A-008 é nova: a MinIO deixou de publicar imagens da edição community no fim de 2025 — o build
> deve confirmar qual tag usar. **Revisor (janela 2): adicionar A-008 ao DEFINE.**

---

## Revision History

| Versão | Data | Autor | Mudanças |
|--------|------|-------|----------|
| 1.0 | 2026-11-20 | design-agent | Versão inicial a partir do DEFINE da fatia 1 |

---

## Next Step

**Pronto para:** revisão na janela 2 → depois `/build .claude/sdd/features/DESIGN_FATIA1_VENDAS_RECEITA.md`
