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
| **Confiança** | 0.88 — padrões de KB encontrados (dbt, spark, airflow, sql-patterns) e agentes casados; revisado na janela 2 ([REVIEW_FATIA1_JANELA2.md](./REVIEW_FATIA1_JANELA2.md)); premissas A-001/A-002/A-003/A-008 exigem spike no marco M0 |

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
│  │  db metabase │   │                                                       │                   │
│  └──────┬───────┘   │  1 simular ──► 2 extrair ──► 3 silver ──► 4 dbt build │                   │
│         │           │     (Python)     (dlt)      (PySpark      (dbt-duckdb │                   │
│         │           │                             local[*])      venv)      │                   │
│         │           │                         ──► 5 reconciliar ──► 6 publicar                  │
│         │           └─────┬──────────────┬─────────────┬──────────┬─────────┘                   │
│         │ SQL incremental │              │             │          │                              │
│         └────────────────►│              │             │          │                              │
│                           ▼              ▼             │          ▼                              │
│  ┌───────────────────────────────────────────────┐    │   ┌──────────────────┐  ┌────────────┐  │
│  │ MinIO (S3)                                     │    │   │ volume ./data     │  │ Metabase   │  │
│  │  s3://bronze/oltp/<tabela>/ingest_date=D/*.pq  │────┘   │ warehouse/gold    │  │ + driver   │  │
│  │  s3://silver/<tabela>/  (Delta Lake)           │◄─delta_scan─ .duckdb (rw)  │  │ DuckDB     │  │
│  └───────────────────────────────────────────────┘        │ serving/gold      │─►│ (read-only)│  │
│                                                            │  .duckdb (marts)  │  └────────────┘  │
│                                                            └──────────────────┘                  │
└──────────────────────────────────────────────────────────────────────────────────────────────────┘
          GitHub Actions (CI): ruff · sqlfluff · pytest · dbt build em amostra Delta local
```

**Princípio:** o Airflow só orquestra. Toda a lógica mora em `src/transporte` (Python testável),
`spark/jobs` (entrypoint fino) e `dbt/`. Cada task chama um comando CLI idempotente parametrizado
pela data lógica `D` (`data_interval_start`, em UTC). O simulador fica **fora** do pipeline: ele
faz o papel dos clientes e do sistema de vendas, alimentando o OLTP; o pipeline começa no Postgres.

---

## Components

| Componente | Responsabilidade | Tecnologia |
|------------|------------------|------------|
| **Postgres** | OLTP (`db oltp`: schemas `oltp` e `simulador`), metadados do Airflow (`db airflow`) e app DB do Metabase (`db metabase`) | Postgres 17 |
| **MinIO** | Object storage S3: buckets `bronze` e `silver` | MinIO (imagem fixada por tag + digest, ver A-008) |
| **Simulador** | Gera catálogo (cidades, empresas, rotas), vendas do dia D e aplica os cancelamentos **agendados** para D (agenda em `simulador.agenda_cancelamento`, decidida no nascimento da venda) | Python + Faker + psycopg 3 (`COPY` na carga em massa) |
| **Ingestão** | Extrai do OLTP a janela `updated_at ∈ [D, D+1)` e grava Parquet no bronze, sobrescrevendo a partição `ingest_date=D`; cada execução começa sem pacotes pendentes | dlt (`sql_table` + destino `filesystem` S3) |
| **Silver** | Lê a partição bronze (ausente = 0 linhas, pula o MERGE), tipa, deduplica, mascara CPF e faz MERGE na tabela Delta | PySpark 4 + delta-spark 4 (local mode, 3 GB, `session.timeZone=UTC`) |
| **Gold** | Staging → marts: `fato_vendas` (incremental guiado pela data D) e dimensões com SK por hash; testes | dbt-core + dbt-duckdb + DuckDB (extensões `httpfs`, `delta`) |
| **Reconciliação** | Compara contagem e receita líquida OLTP × gold de **todas** as datas, no warehouse, **antes** de publicar; falha a DAG listando as divergências | Python (psycopg + duckdb) |
| **Publicação** | Cria um arquivo novo só com os marts (tabelas) e troca `serving/gold.duckdb` de forma atômica | Python (`duckdb` ATTACH + CTAS, `os.replace`) |
| **Orquestração** | DAG diária (catchup, `depends_on_past`) e DAG de carga histórica (manual); pool `transporte_escrita` (1 slot) entre as duas | Airflow 3, LocalExecutor, BashOperator |
| **BI** | Dashboard de receita | Metabase sobre base Debian + `metabase_duckdb_driver`; app DB no Postgres |
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

**Choice:** a imagem custom do Airflow inclui OpenJDK 17, PySpark (no `/opt/pipeline-venv`, ver
Decision 7) e os jars `delta-spark`, `hadoop-aws` e AWS SDK baixados no build (sem `--packages` em
runtime). A task roda `spark-submit --master local[4] --driver-memory $SPARK_DRIVER_MEMORY
spark/jobs/silver_oltp.py --data D`.

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
| **Status** | Accepted (sujeito a A-002/A-003) — revisado na v1.1 |
| **Date** | 2026-11-20 |

**Context:** o DuckDB permite um processo escritor **ou** vários leitores por arquivo. O dbt
escreve; o Metabase lê continuamente.

**Choice:** o dbt escreve em `data/warehouse/gold.duckdb`. A task `publicar` (que só roda depois
da reconciliação) cria `data/serving/gold.duckdb.tmp` **do zero**, com `ATTACH` do warehouse em
modo leitura e `CREATE TABLE AS` apenas dos marts (`fato_vendas`, `dim_*`), e faz `os.replace`
para `data/serving/gold.duckdb`. O Metabase monta `data/serving` como read-only.

**Rationale:** separa escrita e leitura (padrão blue/green simplificado) sem um banco extra. Expor
só o produto final evita que as views de staging (que leem o MinIO via `delta_scan`) quebrem o
sync do Metabase, e o arquivo novo fechado já sai sem alterações pendentes no WAL.

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
| Simulador | `passagem_id` determinístico por (D, sequência) + `INSERT ... ON CONFLICT DO NOTHING`; o cancelamento de cada venda é **agendado no nascimento** (função pura de seed + `passagem_id`) em `simulador.agenda_cancelamento`; `simular D` aplica só os agendados para D e ainda `vendida` — nunca consulta o estado do OLTP para escolher |
| Bronze | Janela fechada `updated_at ∈ [D, D+1)` (dlt `incremental` com `end_value`, sem estado) + remoção do prefixo `ingest_date=D` antes de gravar + pacotes pendentes descartados (`pipelines_dir` temporário) |
| Silver | `MERGE` por `passagem_id`, atualiza só se `s.updated_at > t.updated_at`; partição ausente = 0 linhas |
| Gold | `fato_vendas` incremental `delete+insert` por `passagem_id`, filtrando `updated_at ∈ [D, D+1)` com D recebido do Airflow (`--vars`) — nunca pelo estado da tabela de destino |

**Rationale:** a data lógica do Airflow é a única fonte do intervalo; nada depende do relógio nem do
estado atual das tabelas (OLTP ou destino).

**Alternatives Rejected:**
1. Estado incremental do dlt (watermark salvo) — reexecutar uma data antiga não reprocessaria a janela.
2. Escolher cancelamentos consultando o OLTP — reprocessar D cancelaria passagens diferentes.
3. Lookback sobre `max(updated_at)` do fato — se D-5 falha e D-4..D seguem, a reexecução de D-5 é
   filtrada e suas vendas nunca entram no fato (buraco silencioso).

**Consequences:** reprocessar D é sempre seguro; o backfill do Airflow funciona. A DAG diária usa
`depends_on_past=True` (os dias rodam em fila: uma falha para o trem até ser corrigida) — ver
Decision 9.

---

### Decision 5: Carga histórica em lote, separada da DAG diária

| Atributo | Valor |
|----------|-------|
| **Status** | Accepted |
| **Date** | 2026-11-20 |

**Context:** 730 execuções da DAG diária custariam ~40–60 s cada (startup do Spark e do dbt),
ou seja, 8–12 h — acima do critério de ≤ 3 h do DEFINE.

**Choice:** DAG `transporte_carga_historica` (gatilho manual): simula o intervalo inteiro (inserção
em massa via `COPY`), **extrai dia a dia** num único processo — chamando a mesma `extrair_dia(D)`
da diária, o que gera as mesmas partições `ingest_date=D` —, roda o Spark **uma vez** lendo o
intervalo e o dbt **uma vez** com `--full-refresh`. A DAG diária começa em `HISTORICO_FIM + 1` e o
backfill do Airflow fica para intervalos curtos.

**Rationale:** mesmo código, parâmetros diferentes; o custo de startup do Spark e do dbt é pago uma
vez só (o dlt é leve: estimativa de 15–30 min para as 730 extrações).

**Alternatives Rejected:**
1. Extração única em `ingest_date = data_fim` — reprocessar essa data pela diária apagaria os 2 anos
   de bronze, e a partição violaria o contrato "1 partição = 1 dia".
2. 730 execuções da DAG diária — 8–12 h.

**Consequences:** os critérios do DEFINE v1.1 separam "carga histórica ≤ 3 h" (medida nesta DAG) e
"backfill curto ≤ 15 min por data" (DAG diária). As tasks desta DAG têm `execution_timeout`
próprio (maior que os 20 min da diária).

---

### Decision 6: Um Postgres para OLTP, Airflow e Metabase

| Atributo | Valor |
|----------|-------|
| **Status** | Accepted — revisado na v1.1 |
| **Date** | 2026-11-20 |

**Choice:** um container Postgres com três bancos (`oltp`, `airflow`, `metabase`) e usuários
distintos, criados por `sql/init/00_databases.sql`. O banco `oltp` tem também o schema
`simulador` (agenda de cancelamentos), que o pipeline não extrai.

**Rationale:** economiza ~300 MB de RAM por container evitado; o isolamento por banco e por usuário
é suficiente localmente. O app DB do Metabase no Postgres (em vez do H2 dentro do container) evita
perder os dashboards quando o container é recriado — é o que a documentação do Metabase recomenda.

**Alternatives Rejected:** dois containers Postgres — mais RAM sem ganho na fatia 1; H2 do Metabase
em volume — desaconselhado pelo próprio Metabase fora de testes.

---

### Decision 7: Venvs separados para dbt e pipeline dentro da imagem

| Atributo | Valor |
|----------|-------|
| **Status** | Accepted — revisado na v1.1 |
| **Date** | 2026-11-20 |

**Choice:** `/opt/dbt-venv` com dbt-core + dbt-duckdb; `/opt/pipeline-venv` com o projeto
(`transporte`) + dlt + pyspark + delta-spark (`docker/airflow/requirements-pipeline.txt`). O
ambiente do Airflow fica só com Airflow e providers. O BashOperator chama os binários dos venvs.

**Rationale:** dbt, dlt e PySpark trazem pyarrow, pandas e sqlalchemy, exatamente onde as
constraints do Airflow são rígidas; venvs separados evitam quebrar a imagem a cada upgrade.

---

### Decision 8: UTC e decimal em todas as camadas

| Atributo | Valor |
|----------|-------|
| **Status** | Accepted |
| **Date** | 2026-11-20 |

**Context:** o `TIMESTAMP` do Postgres não tem fuso, e o dado passa por cinco ferramentas
(simulador, Postgres, dlt, Spark, DuckDB). Se uma delas supuser outro fuso, vendas noturnas mudam
de dia e a reconciliação ao centavo quebra.

**Choice:** `TZ=UTC` em todos os containers; `spark.sql.session.timeZone=UTC`; DuckDB com
`TimeZone='UTC'` (settings do perfil dbt); o simulador gera instantes em UTC; o Airflow já usa UTC.
`valor` é DECIMAL(10,2) no Postgres → Arrow → Spark `DecimalType(10,2)` → DuckDB, e as somas da
reconciliação usam `Decimal` em Python. Conversão para horário local só na exibição (Metabase).

**Rationale:** padrão de mercado: guardar e processar em UTC, converter só na borda.

**Alternatives Rejected:** horário de Brasília em todas as ferramentas — contra o padrão do Airflow
e sujeito a mudanças de regra de fuso; deixar o padrão de cada ferramenta — é o cenário do erro.

**Consequences:** teste unitário com venda às 23:30 UTC de D, que deve permanecer em D até o gold.
ADR 0006.

---

### Decision 9: Ordem, concorrência e portão de publicação

| Atributo | Valor |
|----------|-------|
| **Status** | Accepted |
| **Date** | 2026-11-20 |

**Choice:**
- Ordem da diária: `simular → extrair → silver → dbt_build → reconciliar → publicar`. A
  reconciliação compara **todas** as datas no warehouse; divergência bloqueia a publicação.
- `depends_on_past=True` na DAG diária: o dia D só roda depois que D-1 terminou com sucesso.
- Pool `transporte_escrita` com 1 slot (`airflow/pools.json`) nas tasks `silver`, `dbt_build`,
  `reconciliar` e `publicar` das **duas** DAGs (o `max_active_runs=1` só vale dentro de cada DAG).

**Rationale:** checagens de qualidade são um portão, não um alarme atrasado; e o DuckDB aceita um
só escritor por arquivo, enquanto o Delta rejeita MERGEs simultâneos.

**Consequences:** uma falha para a fila até ser corrigida (comportamento desejado para dados); a
carga histórica e a diária nunca escrevem ao mesmo tempo.

---

## File Manifest

| # | Arquivo | Ação | Propósito | Agente | Dependências |
|---|---------|------|-----------|--------|--------------|
| 1 | `pyproject.toml` | Modify | Dependências por grupo (`pipeline`, `spark`, `dbt`, `dev`), script `transporte`, markers `spark`/`integration` | @python-developer | None |
| 2 | `.env.example` | Modify | Todas as variáveis da seção Configuration | @data-platform-engineer | None |
| 3 | `sql/init/00_databases.sql` | Create | Cria bancos `oltp`/`airflow`/`metabase` e usuários | @schema-designer | None |
| 4 | `sql/init/10_oltp_schema.sql` | Create | Tabelas `cidade`, `empresa`, `rota`, `passagem` + índices (`updated_at`) | @schema-designer | 3 |
| 5 | `docker/airflow/Dockerfile` | Create | Airflow 3 + Java 17 + jars Delta/S3A + `/opt/dbt-venv` + `/opt/pipeline-venv` (transporte, dlt, pyspark) | @data-platform-engineer | 1, 63 |
| 6 | `docker/metabase/Dockerfile` | Create | Base Debian (eclipse-temurin JRE) + `metabase.jar` + `metabase_duckdb_driver` (mesma versão do DuckDB do dbt) | @data-platform-engineer | None |
| 7 | `docker-compose.yml` | Create | Serviços, profiles `core`/`airflow`/`bi`, limites de memória, volumes, criação dos buckets, `TZ=UTC`, import do pool, app DB do Metabase | @data-platform-engineer | 2, 3, 5, 6, 61, 65 |
| 8 | `Makefile` | Create | `up`, `down`, `historico`, `test`, `lint`, `dbt-ci` | @shell-script-specialist | 7 |
| 9 | `src/transporte/common/config.py` | Create | Settings tipadas a partir de env (pydantic-settings) | @python-developer | 1 |
| 10 | `src/transporte/common/logging.py` | Create | Logging estruturado (key=value) com contagens por etapa | @python-developer | None |
| 11 | `src/transporte/simulator/catalogo.py` | Create | Catálogo determinístico: ~40 cidades, ~8 empresas, ~120 rotas | @python-developer | 9 |
| 12 | `src/transporte/simulator/gerador.py` | Create | Funções puras: vendas do dia D (sazonalidade, instantes UTC) e agendamento do cancelamento no nascimento da venda | @python-developer | 11 |
| 13 | `src/transporte/simulator/repositorio.py` | Create | Escrita idempotente no Postgres (upsert de catálogo, insert de vendas + agenda, cancelamentos agendados para D; `COPY` na carga em massa) | @python-developer | 4, 12, 62 |
| 14 | `src/transporte/ingestion/oltp.py` | Create | Source dlt com janela `[inicio, fim)` e pipeline para o bronze com overwrite da partição, sem pacotes pendentes, com `_dlt_load_id` | @python-developer | 9 |
| 15 | `src/transporte/silver/__init__.py` | Create | Pacote silver | @spark-engineer | None |
| 16 | `src/transporte/silver/session.py` | Create | Factory de SparkSession (Delta + S3A/MinIO, `session.timeZone=UTC`) e helper de existência de caminho | @spark-engineer | 9 |
| 17 | `src/transporte/silver/transforms.py` | Create | Funções puras de DataFrame: tipagem, dedup, máscara de CPF | @spark-engineer | None |
| 18 | `src/transporte/silver/merge.py` | Create | MERGE Delta de `passagem`; overwrite das dimensões | @spark-engineer | 16, 17 |
| 19 | `spark/jobs/silver_oltp.py` | Create | Entrypoint `spark-submit` (args `--data`/`--historico`); partição ausente = 0 linhas, pula o MERGE | @spark-engineer | 18 |
| 20 | `src/transporte/quality/__init__.py` | Create | Pacote de qualidade | @data-quality-analyst | None |
| 21 | `src/transporte/quality/reconciliacao.py` | Create | OLTP × gold (warehouse) de todas as datas: contagem e receita líquida (`Decimal`), full outer join, lista divergências | @data-quality-analyst | 9 |
| 22 | `src/transporte/serving/__init__.py` | Create | Pacote de serving | @python-developer | None |
| 23 | `src/transporte/serving/publicar.py` | Create | Arquivo serving novo só com os marts (ATTACH + CTAS) + troca atômica | @python-developer | 9 |
| 24 | `src/transporte/cli.py` | Create | CLI: `catalogo`, `simular`, `extrair`, `publicar`, `reconciliar` | @python-developer | 13, 14, 21, 23 |
| 25 | `dbt/dbt_project.yml` | Create | Projeto, materializações por pasta | @dbt-specialist | None |
| 26 | `dbt/profiles.yml` | Create | Targets `dev` (MinIO) e `ci` (Delta local), extensões, secrets via env, `TimeZone=UTC` | @dbt-specialist | 25 |
| 27 | `dbt/packages.yml` | Create | `dbt_utils` (versão fixada) | @dbt-specialist | 25 |
| 28 | `dbt/models/staging/oltp/_oltp__sources.yml` | Create | Sources silver via `delta_scan`, testes de PK na source, freshness só com `warn_after` | @dbt-specialist | 26 |
| 29 | `dbt/models/staging/oltp/_oltp__models.yml` | Create | Docs e testes de staging, incluindo `relationships` de `rota_id` (portão do AT-006) | @dbt-specialist | 28 |
| 30 | `dbt/models/staging/oltp/stg_oltp__passagem.sql` | Create | Renomeia/tipa; calcula `valor_liquido` | @dbt-specialist | 28 |
| 31 | `dbt/models/staging/oltp/stg_oltp__rota.sql` | Create | Staging de rota | @dbt-specialist | 28 |
| 32 | `dbt/models/staging/oltp/stg_oltp__empresa.sql` | Create | Staging de empresa | @dbt-specialist | 28 |
| 33 | `dbt/models/staging/oltp/stg_oltp__cidade.sql` | Create | Staging de cidade | @dbt-specialist | 28 |
| 34 | `dbt/models/marts/core/_core__models.yml` | Create | Docs, PKs, SKs por hash, relationships, accepted_values | @dbt-specialist | 35–39 |
| 35 | `dbt/models/marts/core/dim_tempo.sql` | Create | Calendário (`generate_series`) até `max(data_viagem)` + 1 ano, `data_sk` = yyyymmdd | @dbt-specialist | 30 |
| 36 | `dbt/models/marts/core/dim_cidade.sql` | Create | Dimensão cidade (sem população na fatia 1), `cidade_sk` por hash | @dbt-specialist | 33 |
| 37 | `dbt/models/marts/core/dim_empresa.sql` | Create | Dimensão empresa, `empresa_sk` por hash | @dbt-specialist | 32 |
| 38 | `dbt/models/marts/core/dim_rota.sql` | Create | Rota com origem/destino/empresa/distância, SKs por hash | @dbt-specialist | 31 |
| 39 | `dbt/models/marts/core/fato_vendas.sql` | Create | Fato incremental guiado por `var('data')`, grão = 1 passagem | @dbt-specialist | 30, 35–38 |
| 40 | `dbt/tests/assert_viagem_nao_antes_da_compra.sql` | Create | Teste singular de regra de negócio | @data-quality-analyst | 39 |
| 41 | `.sqlfluff` | Create | Dialeto DuckDB, templater dbt (target `ci`) | @dbt-specialist | 26 |
| 42 | `airflow/dags/_defaults.py` | Create | `default_args` (retries, backoff, `depends_on_past`), comandos base, pool | @airflow-specialist | None |
| 43 | `airflow/dags/transporte_diario.py` | Create | DAG diária: simular → extrair → silver → (dbt_freshness ∥ dbt_build) → reconciliar → publicar | @airflow-specialist | 19, 24, 42 |
| 44 | `airflow/dags/transporte_carga_historica.py` | Create | DAG manual da carga histórica (extração dia a dia, Spark e dbt 1x, timeouts próprios) | @airflow-specialist | 19, 24, 42 |
| 45 | `scripts/gerar_amostra_ci.py` | Create | Gera amostra Delta local (deltalake/delta-rs) a partir do simulador para o CI | @python-developer | 12 |
| 46 | `.github/workflows/ci.yml` | Create | Jobs `lint` (ruff), `test` (pytest), `dbt` (amostra → deps → sqlfluff → build) | @ci-cd-specialist | 41, 45 |
| 47 | `.pre-commit-config.yaml` | Modify | Adiciona sqlfluff | @ci-cd-specialist | 41 |
| 48 | `tests/conftest.py` | Create | Fixtures: settings de teste, SparkSession local | @test-generator | 9, 16 |
| 49 | `tests/unit/test_gerador.py` | Create | Determinismo, volume, agenda de cancelamento determinística e independente da ordem, dia sem vendas | @test-generator | 12 |
| 50 | `tests/unit/test_transforms_silver.py` | Create | Dedup, tipagem (decimal, venda 23:30 UTC permanece em D), máscara sem coluna `*cpf*` (marker `spark`) | @test-generator | 17 |
| 51 | `tests/unit/test_reconciliacao.py` | Create | Lógica de comparação de todas as datas com dados em memória | @test-generator | 21 |
| 52 | `tests/unit/test_publicar.py` | Create | Serving só com marts (sem views) + troca atômica | @test-generator | 23 |
| 53 | `tests/integration/test_dags.py` | Create | DagBag sem erros de import, sem ciclos (marker `integration`, roda no container) | @airflow-specialist | 43, 44 |
| 54 | `tests/integration/test_pipeline_e2e.py` | Create | AT-001/002/003/004/006/007/009 contra o compose no ar (AT-006: órfão na silver + `RESTORE`) | @test-generator | 7, 43, 68 |
| 55 | `docs/adr/0001-duckdb-le-delta-no-minio.md` | Create | ADR da Decision 1 | @code-documenter | None |
| 56 | `docs/adr/0002-spark-local-mode-no-airflow.md` | Create | ADR da Decision 2 | @code-documenter | None |
| 57 | `docs/adr/0003-publicacao-gold-metabase.md` | Create | ADR da Decision 3 | @code-documenter | None |
| 58 | `docs/adr/0004-idempotencia-por-data-logica.md` | Create | ADR das Decisions 4, 5 e 9 | @code-documenter | None |
| 59 | `docs/metabase/dashboard-receita.md` | Create | Passo a passo do dashboard (3 perguntas + SQL) | @code-documenter | 39 |
| 60 | `README.md` | Modify | Como subir, rodar a carga histórica, testar; branch protection do `main`; "Limitações conhecidas" | @code-documenter | 8 |
| 61 | `.dockerignore` | Create | Exclui `.venv/`, `data/`, caches e `.git` do contexto de build | @data-platform-engineer | None |
| 62 | `sql/init/20_simulador_schema.sql` | Create | Schema `simulador` + tabela `agenda_cancelamento(passagem_id PK, data_cancelamento)` | @schema-designer | 3, 4 |
| 63 | `docker/airflow/requirements-pipeline.txt` | Create | Dependências fixadas do `/opt/pipeline-venv` | @data-platform-engineer | 1 |
| 64 | `airflow/.airflowignore` | Create | Ignora `_defaults.py` no parse de DAGs | @airflow-specialist | None |
| 65 | `airflow/pools.json` | Create | Pool `transporte_escrita` (1 slot) | @airflow-specialist | None |
| 66 | `tests/unit/test_merge_silver.py` | Create | MERGE: insert, cancelamento atualiza, versão antiga não sobrescreve, 2x idempotente (marker `spark`) | @test-generator | 18 |
| 67 | `tests/unit/test_ingestao_janela.py` | Create | Bordas da janela `[D, D+1)` em UTC | @test-generator | 14 |
| 68 | `tests/integration/conftest.py` | Create | Fixtures do E2E: compose no ar, data D, injeção de órfão + `RESTORE` do Delta | @test-generator | 7 |
| 69 | `docs/adr/0005-object-storage.md` | Create | ADR da escolha da imagem do MinIO e plano B (A-008) | @code-documenter | None |
| 70 | `docs/adr/0006-fuso-utc-e-decimal.md` | Create | ADR da Decision 8 | @code-documenter | None |
| 71 | `.github/dependabot.yml` | Create | Atualização semanal de dependências (uv/pip, docker, github-actions) | @ci-cd-specialist | None |
| 72 | `docker/spark/` | Delete | Pasta sem uso (o Spark roda na imagem do Airflow) | @data-platform-engineer | None |

**Total de arquivos:** 72 (67 novos, 4 modificados, 1 removido).

---

## Agent Assignment Rationale

| Agente | Arquivos | Por quê |
|--------|----------|---------|
| @python-developer | 1, 9–14, 22–24, 45 | Pacote Python, CLI, simulador, integração dlt |
| @schema-designer | 3, 4, 62 | DDL do OLTP e do schema do simulador, chaves e índices |
| @data-platform-engineer | 2, 5–7, 61, 63, 72 | Compose, imagens, venvs, limites de recursos |
| @spark-engineer | 15–19 | PySpark, Delta MERGE, S3A |
| @dbt-specialist | 25–39, 41 | Modelagem dbt, incremental, perfis DuckDB |
| @data-quality-analyst | 20, 21, 40 | Reconciliação e regras de negócio |
| @airflow-specialist | 42–44, 53, 64, 65 | Design de DAG, retries, backfill, pools |
| @ci-cd-specialist | 46, 47, 71 | GitHub Actions, pre-commit, Dependabot |
| @test-generator | 48–52, 54, 66–68 | Fixtures e testes pytest |
| @shell-script-specialist | 8 | Makefile |
| @code-documenter | 55–60, 69, 70 | ADRs e documentação |

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
    """Instante naive em UTC (Decision 8)."""
    return datetime.combine(dia, time.min) + timedelta(seconds=rng.randrange(86_400))


def agendar_cancelamento(seed_base: int, p: Passagem, taxa: float) -> date | None:
    """Decide, no nascimento da venda, se e quando ela será cancelada.

    Função pura de seed + passagem_id: não depende do estado do OLTP nem da ordem
    em que os dias são processados (reprocessar D cancela sempre as mesmas passagens).
    """
    rng = random.Random(f"{seed_base}:cancelamento:{p.passagem_id}")
    dias_ate_viagem = (p.data_viagem - p.data_compra.date()).days
    if dias_ate_viagem < 1 or rng.random() >= taxa:
        return None
    return p.data_compra.date() + timedelta(days=rng.randint(1, dias_ate_viagem))
```

```sql
-- src/transporte/simulator/repositorio.py (SQL usado)
INSERT INTO oltp.passagem (passagem_id, rota_id, data_compra, data_viagem, valor, status,
                           passageiro_cpf, created_at, updated_at)
VALUES (%s, %s, %s, %s, %s, 'vendida', %s, %s, %s)
ON CONFLICT (passagem_id) DO NOTHING;

-- agenda (schema simulador, nunca extraído pelo pipeline)
INSERT INTO simulador.agenda_cancelamento (passagem_id, data_cancelamento)
VALUES (%s, %s)
ON CONFLICT (passagem_id) DO NOTHING;

-- cancelamentos de D: só os agendados para D e ainda vendidos → reexecução não muda nada
UPDATE oltp.passagem p
   SET status = 'cancelada', updated_at = %(instante)s
  FROM simulador.agenda_cancelamento a
 WHERE a.passagem_id = p.passagem_id
   AND a.data_cancelamento = %(dia)s
   AND p.status = 'vendida';
```

> Na carga histórica, vendas e agenda entram por `COPY` (psycopg `cursor.copy`) em lote, numa
> tabela temporária, seguido de `INSERT ... SELECT ... ON CONFLICT DO NOTHING`.

### Pattern 2: Extração dlt com janela fechada e overwrite da partição

```python
# src/transporte/ingestion/oltp.py
import tempfile
from datetime import date, datetime, time, timedelta

import dlt
import s3fs
from dlt.sources.sql_database import sql_table

from transporte.common.config import Settings

LAYOUT = "{table_name}/ingest_date={ingest_date}/{load_id}.{file_id}.{ext}"


def extrair_dia(cfg: Settings, dia: date) -> None:
    inicio = datetime.combine(dia, time.min)  # naive UTC (Decision 8)
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
    # 3. Cada execução começa do zero: sem pacotes pendentes de uma tentativa anterior (AT-005)
    with tempfile.TemporaryDirectory() as workdir:
        pipeline = dlt.pipeline(
            pipeline_name="oltp_bronze",
            destination=destino,
            dataset_name="oltp",
            pipelines_dir=workdir,
        )
        pipeline.drop_pending_packages()
        info = pipeline.run(
            [passagem, *dimensoes], loader_file_format="parquet", write_disposition="append"
        )
        info.raise_on_failed_jobs()
```

> Validar no build: nomes exatos de `range_end` e `extra_placeholders` na versão fixada do dlt;
> `restore_from_destination=False` (env `RESTORE_FROM_DESTINATION=false`) e
> `add_dlt_load_id=true` (env `NORMALIZE__PARQUET_NORMALIZER__ADD_DLT_LOAD_ID=true`, necessário
> com `backend="pyarrow"` para o requisito de lineage). Com 0 linhas no dia, o dlt não cria a
> partição de `passagem` — a silver trata isso (Pattern 3).

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

```python
# spark/jobs/silver_oltp.py (trecho) — dia sem vendas (AT-009)
caminho = f"s3a://bronze/oltp/passagem/ingest_date={dia.isoformat()}"
if not caminho_existe(spark, caminho):  # helper em silver/session.py (Hadoop FileSystem)
    log.info("etapa=silver data=%s linhas=0 motivo=particao_ausente", dia)
else:
    merge_passagem(spark, preparar(spark.read.parquet(caminho)), SILVER_PASSAGEM)
sobrescrever_dimensoes(spark, dia)  # rota/empresa/cidade seguem normalmente
```

> A `SparkSession` (`silver/session.py`) define `spark.sql.session.timeZone=UTC`, e o schema
> explícito da silver usa `DecimalType(10, 2)` para `valor` (Decision 8).

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
      settings:
        TimeZone: UTC
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
      settings:
        TimeZone: UTC
```

> No CI, `SILVER_ROOT` aponta para a pasta local da amostra Delta gerada por
> `scripts/gerar_amostra_ci.py`.

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
        freshness:  # só aviso: no backfill o tempo simulado dispara warn, o que é esperado
          warn_after: {count: 1, period: day}
        columns:
          - name: passagem_id
            tests: [not_null, unique]  # PK da silver (gate)
      - name: rota
      - name: empresa
      - name: cidade
```

### Pattern 5: Fato incremental guiado pela data lógica

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
    -- D vem do Airflow (--vars); nunca do estado do próprio fato (Decision 4)
    where updated_at >= cast('{{ var("data") }}' as timestamp)
      and updated_at < cast('{{ var("data") }}' as timestamp) + interval 1 day
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

> O portão do AT-006 é o teste `relationships` de `stg_oltp__passagem.rota_id → stg_oltp__rota`:
> ele roda antes do fato e, ao falhar, pula os modelos seguintes. Um teste de FK no fato não serve
> de portão, porque o `inner join` já descarta órfãos.

```sql
-- dbt/models/marts/core/dim_rota.sql (trecho) — SKs estáveis por hash da chave natural
select
    {{ dbt_utils.generate_surrogate_key(['r.rota_id']) }} as rota_sk,
    r.rota_id,
    {{ dbt_utils.generate_surrogate_key(['r.empresa_id']) }} as empresa_sk,
    {{ dbt_utils.generate_surrogate_key(['r.cidade_origem_id']) }} as cidade_origem_sk,
    {{ dbt_utils.generate_surrogate_key(['r.cidade_destino_id']) }} as cidade_destino_sk,
    r.distancia_km
from {{ ref('stg_oltp__rota') }} r
```

> Numerar linhas (`row_number`) para gerar SK é proibido: as dimensões são recriadas a cada build
> e o fato é incremental, então a numeração mudaria e as vendas antigas apontariam para a
> dimensão errada.

### Pattern 6: DAG diária

```python
# airflow/dags/transporte_diario.py
from datetime import datetime

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG

from _defaults import DATA, DBT, DBT_VARS, DEFAULT_ARGS, POOL_ESCRITA, SPARK_SUBMIT, TRANSPORTE

with DAG(
    dag_id="transporte_diario",
    start_date=datetime(2026, 11, 20),  # = HISTORICO_FIM + 1
    schedule="@daily",
    catchup=True,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,  # depends_on_past=True: os dias rodam em fila (Decision 9)
    tags=["fatia1", "vendas"],
) as dag:
    simular = BashOperator(
        task_id="simular",
        bash_command=f"{TRANSPORTE} simular --data {DATA}",
    )
    extrair = BashOperator(
        task_id="extrair",
        bash_command=f"{TRANSPORTE} extrair --data {DATA}",
    )
    silver = BashOperator(
        task_id="silver",
        bash_command=f"{SPARK_SUBMIT} /opt/transporte/spark/jobs/silver_oltp.py --data {DATA}",
        pool=POOL_ESCRITA,
    )
    dbt_freshness = BashOperator(
        task_id="dbt_freshness",
        bash_command=f"{DBT} source freshness",  # só warn_after: avisa, nunca falha
    )
    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command=f"{DBT} build {DBT_VARS}",
        pool=POOL_ESCRITA,
    )
    reconciliar = BashOperator(
        task_id="reconciliar",
        bash_command=f"{TRANSPORTE} reconciliar --todas-as-datas",  # lê o warehouse
        retries=0,
        pool=POOL_ESCRITA,
    )
    publicar = BashOperator(
        task_id="publicar",
        bash_command=f"{TRANSPORTE} publicar",
        pool=POOL_ESCRITA,
    )

    simular >> extrair >> silver >> [dbt_freshness, dbt_build]
    dbt_build >> reconciliar >> publicar  # portão: nada divergente chega ao Metabase
```

```python
# airflow/dags/_defaults.py  (ignorado no parse via airflow/.airflowignore)
from datetime import timedelta

DATA = "{{ data_interval_start | ds }}"
TRANSPORTE = "/opt/pipeline-venv/bin/transporte"
DBT = "cd /opt/transporte/dbt && /opt/dbt-venv/bin/dbt"
DBT_VARS = """--vars '{"data": "{{ data_interval_start | ds }}"}'"""
SPARK_SUBMIT = (
    "/opt/pipeline-venv/bin/spark-submit --master local[4] "
    "--driver-memory ${SPARK_DRIVER_MEMORY:-3g}"
)
POOL_ESCRITA = "transporte_escrita"  # 1 slot (airflow/pools.json), compartilhado pelas 2 DAGs

DEFAULT_ARGS = {
    "owner": "dados",
    "depends_on_past": True,
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=10),
    "execution_timeout": timedelta(minutes=20),
}
```

### Pattern 7: Publicação atômica só dos marts

```python
# src/transporte/serving/publicar.py
import os
from pathlib import Path

import duckdb

MARTS = ("dim_tempo", "dim_cidade", "dim_empresa", "dim_rota", "fato_vendas")


def publicar(origem: Path, destino: Path, schema: str = "main") -> None:
    """Cria um arquivo novo só com os marts (tabelas) e troca o serving de forma atômica."""
    destino.parent.mkdir(parents=True, exist_ok=True)
    tmp = destino.with_suffix(".duckdb.tmp")
    tmp.unlink(missing_ok=True)
    with duckdb.connect(str(tmp)) as con:
        con.execute(f"ATTACH '{origem}' AS wh (READ_ONLY)")
        for tabela in MARTS:
            con.execute(f"CREATE TABLE {tabela} AS SELECT * FROM wh.{schema}.{tabela}")
        con.execute("DETACH wh")
    os.replace(tmp, destino)  # atômico no mesmo filesystem; arquivo fechado = sem WAL pendente
```

---

## Data Flow

```text
1. simular D       → INSERT vendas de D (~1,4k–7k) + agenda de cancelamento de cada uma
   │                 + UPDATE das passagens agendadas para cancelar em D. updated_at ∈ [D, D+1) UTC.
   ▼
2. extrair D       → dlt lê passagem com updated_at ∈ [D, D+1) + dimensões completas
   │                 → s3://bronze/oltp/<tabela>/ingest_date=D/*.parquet (partição sobrescrita;
   │                   sem partição de passagem se o dia tiver 0 linhas)
   ▼
3. silver D        → Spark lê a partição D (ausente = 0 linhas) → tipa → dedup → hash do CPF
   │                 → MERGE em s3://silver/passagem (Delta); overwrite de rota/empresa/cidade
   ▼
4. dbt build       → staging (views sobre delta_scan) → dims (table, SK por hash)
   │  (--vars D)      → fato_vendas (incremental, updated_at ∈ [D, D+1)) → testes
   │                 (falha = DAG falha, nada é publicado). Em paralelo: source freshness (aviso).
   ▼
5. reconciliar     → OLTP vs warehouse: count e sum(valor_liquido) por data_compra, TODAS as
   │                 datas (full outer join) → divergência = DAG falha, nada é publicado
   ▼
6. publicar        → arquivo novo só com marts → serving/gold.duckdb (os.replace atômico)
```

---

## Integration Points

| Sistema | Integração | Autenticação |
|---------|------------|--------------|
| Postgres (OLTP) | psycopg 3 (simulador, reconciliação), SQLAlchemy (dlt) | Usuário/senha via `.env` |
| MinIO | S3 API: s3fs (dlt), S3A (Spark), httpfs (DuckDB) | Access/secret key via `.env` |
| DuckDB (arquivo) | dbt-duckdb (escrita no warehouse), `duckdb` Python (reconciliação no warehouse, publicação), driver JDBC (Metabase, no serving) — mesma versão do DuckDB nos três | Arquivo local; serving montado read-only |
| Metabase | UI; API opcional para re-sync após publicar; app DB no Postgres (`db metabase`) | Admin local; usuário próprio no Postgres |
| GitHub Actions | Workflow em push/PR | `GITHUB_TOKEN` padrão; sem secrets externos |

---

## Testing Strategy

| Tipo | Escopo | Arquivos | Ferramentas | Meta |
|------|--------|----------|-------------|------|
| Unit | Simulador, reconciliação, publicação | `tests/unit/test_gerador.py`, `test_reconciliacao.py`, `test_publicar.py` | pytest | ≥ 80% de cobertura em `src/transporte` (exceto I/O) |
| Unit (Spark) | Transforms e MERGE silver | `tests/unit/test_transforms_silver.py`, `test_merge_silver.py` | pytest + SparkSession local + Delta em pasta temporária (marker `spark`) | Todas as funções de `transforms.py` e `merge.py` |
| Unit | Janela de extração | `tests/unit/test_ingestao_janela.py` | pytest | Bordas de `[D, D+1)` em UTC |
| dbt | Modelos gold | `_oltp__models.yml`, `_core__models.yml`, `dbt/tests/*.sql` | dbt test | 100% dos modelos com PK testada |
| Integração | DAGs | `tests/integration/test_dags.py` | pytest + DagBag (no container, marker `integration`) | 0 erros de import |
| E2E | Pipeline completo | `tests/integration/test_pipeline_e2e.py` | pytest + compose no ar (marker `integration`) | ATs abaixo |
| Manual | Falha transitória e dashboard | Roteiro no README | `docker compose stop minio` | AT-005, AT-001 (dashboard) |

**Cobertura dos testes de aceite:**

| AT | Onde é verificado |
|----|-------------------|
| AT-001 caminho feliz | E2E: roda D, confere linhas em bronze/silver/fato; dashboard manual |
| AT-002 idempotência | E2E: roda D 3x, compara contagens e soma por camada |
| AT-003 cancelamento tardio | Unit (agenda do gerador + `test_merge_silver`) + E2E: escolhe uma passagem de D agendada para D+3, roda até D+3, confere silver e receita de D |
| AT-004 duplicata | Unit Spark: `deduplicar` com 2 versões da mesma chave + `test_merge_silver` (versão antiga não sobrescreve) |
| AT-005 falha transitória | Manual: parar MinIO durante `extrair`, conferir retry e ausência de duplicatas no bronze (sem pacotes pendentes) |
| AT-006 qualidade bloqueia | E2E: grava passagem órfã direto na silver Delta, `dbt build` deve falhar no `relationships` da staging, depois `RESTORE` da versão anterior |
| AT-007 reconciliação | Task `reconciliar` (todas as datas, antes de publicar) em toda execução + unit da lógica |
| AT-008 CI | Workflow `ci.yml` em PR (branch protection no `main`) |
| AT-009 dia sem vendas | Unit (gerador com volume 0) + E2E com `SIMULADOR_VOLUME_DIA=0` (partição ausente → silver pula o MERGE, DAG verde) |

**CI (`.github/workflows/ci.yml`):**

| Job | Passos |
|-----|--------|
| `lint` | `uv sync` → `ruff check` → `ruff format --check` |
| `test` | `uv sync` → `pytest -m "not integration"` (Java do runner para o marker `spark`) |
| `dbt` | `uv run scripts/gerar_amostra_ci.py` (Delta local via delta-rs) → `dbt deps` → `sqlfluff lint dbt/models` (templater dbt, target `ci`) → `dbt build --target ci` |

> A amostra do CI é escrita pelo delta-rs, não pelo Spark: a compatibilidade Spark → `delta_scan`
> é validada pelo spike A-001 e pelo E2E, não pelo CI.

---

## Error Handling

| Erro | Estratégia | Retry? |
|------|------------|--------|
| Postgres/MinIO indisponível | Retry com backoff exponencial (2x, até 10 min); tasks idempotentes tornam o retry seguro | Sim |
| Spark OOM | Falha a task; log com memória configurada; ajustar `--driver-memory`/partições | Não (automático) |
| Retry da extração após falha parcial | `pipelines_dir` temporário + `drop_pending_packages()`: nenhum pacote antigo é reenviado; a partição de D é apagada e regravada | Sim |
| Teste dbt falha | DAG falha em `dbt_build`; `reconciliar` e `publicar` não rodam → Metabase mantém a última versão boa | Não |
| Reconciliação diverge | Task falha listando as datas divergentes com contagens/somas dos dois lados; `publicar` não roda | Não (`retries=0`) |
| Dia sem vendas (partição ausente) | Silver registra `linhas=0` e pula o MERGE; dimensões seguem; DAG verde | — |
| Dia anterior falhou | `depends_on_past=True`: o dia seguinte espera; corrigir e limpar a task destrava a fila | — |
| Escrita concorrente (histórica × diária) | Pool `transporte_escrita` (1 slot) serializa silver, dbt, reconciliação e publicação | — |
| Schema inesperado no bronze | Spark aplica schema explícito; coluna nova falha o job (ver Schema Evolution) | Não |
| Timeout | `execution_timeout` de 20 min por task na diária; timeouts próprios (maiores) na histórica | Sim (via retries) |

---

## Configuration

| Chave (env) | Tipo | Padrão | Descrição |
|-------------|------|--------|-----------|
| `POSTGRES_USER` / `POSTGRES_PASSWORD` | string | — | Superusuário do container Postgres |
| `OLTP_URL` | string | `postgresql+psycopg://oltp:...@postgres:5432/oltp` | Conexão do simulador/dlt |
| `MINIO_ROOT_USER` / `MINIO_ROOT_PASSWORD` | string | — | Credenciais do MinIO |
| `MINIO_ENDPOINT` | string | `http://minio:9000` | Endpoint S3 |
| `DESTINATION__FILESYSTEM__CREDENTIALS__AWS_ACCESS_KEY_ID` / `..._AWS_SECRET_ACCESS_KEY` / `..._ENDPOINT_URL` | string | — / — / `http://minio:9000` | Credenciais e endpoint do destino filesystem do dlt |
| `RESTORE_FROM_DESTINATION` | bool | `false` | dlt não restaura estado do bucket |
| `NORMALIZE__PARQUET_NORMALIZER__ADD_DLT_LOAD_ID` | bool | `true` | Carimba `_dlt_load_id` em cada linha do bronze (lineage) |
| `TZ` | string | `UTC` | Fuso de todos os containers (Decision 8) |
| `SIMULADOR_SEED` | int | `42` | Seed base do simulador |
| `SIMULADOR_VOLUME_DIA` | int | `2700` | Média de vendas por dia (sazonalidade ±50%) |
| `SIMULADOR_TAXA_CANCELAMENTO` | float | `0.05` | Fração das vendas que nascem com cancelamento agendado |
| `HISTORICO_INICIO` / `HISTORICO_FIM` | date | `2024-11-20` / `2026-11-19` | Intervalo da carga histórica (730 dias) |
| `CPF_HASH_SALT` | string | — | Salt secreto do hash do CPF na silver (só no `.env`, nunca no Git) |
| `SILVER_ROOT` | string | `s3://silver` | Raiz das tabelas Delta lidas pelo dbt (no CI: pasta local da amostra) |
| `DBT_TARGET` / `DBT_PROFILES_DIR` | string / path | `dev` / `/opt/transporte/dbt` | Target e local do `profiles.yml` |
| `GOLD_DUCKDB_PATH` | path | `/data/warehouse/gold.duckdb` | Arquivo do dbt |
| `SERVING_DUCKDB_PATH` | path | `/data/serving/gold.duckdb` | Arquivo lido pelo Metabase |
| `SPARK_DRIVER_MEMORY` | string | `3g` | Memória do Spark local (lida pelo `SPARK_SUBMIT`) |
| `MB_DB_TYPE` / `MB_DB_HOST` / `MB_DB_DBNAME` / `MB_DB_USER` / `MB_DB_PASS` | string | `postgres` / `postgres` / `metabase` / `metabase` / — | App DB do Metabase no Postgres |

Limites de memória do compose (soma ≤ 10 GB): Postgres 1 GB · MinIO 1 GB · Airflow
(api-server + scheduler + dag-processor) 4,5 GB incluindo Spark 3 GB durante a task · Metabase
1,5 GB (profile `bi`, pode subir depois da carga). Medição: `docker stats` amostrado durante a
carga histórica; disco com `docker system df` + `du -sh data/`.

---

## Security Considerations

- Credenciais só em `.env` (gitignored); `.env.example` sem valores reais; `detect-private-key` no pre-commit.
- CPF (fictício) nunca sai do bronze em claro: a silver guarda só `sha256(salt || cpf)`; o gold não tem a coluna. O hash só protege enquanto `CPF_HASH_SALT` ficar fora do Git (só no `.env`); teste garante que nenhuma coluna `*cpf*` existe em silver e gold.
- Portas publicadas apenas em `127.0.0.1`; usuários distintos para `oltp`, `airflow` e `metabase` no Postgres.
- Metabase monta o serving como read-only; o Airflow não recebe o socket do Docker.
- Imagens com tags fixadas (nada de `latest`); MinIO fixado também por digest. Dependabot (`.github/dependabot.yml`) abre PRs semanais de atualização, validados pelo CI.
- Branch protection no `main` (CI verde obrigatório), configurada manualmente no GitHub (passo no README).

---

## Observability

| Aspecto | Implementação |
|---------|---------------|
| Logging | Logging estruturado `key=value` por etapa (`etapa=extrair data=D linhas=N`) nos logs do Airflow |
| Métricas | Contagens por camada logadas e retornadas por task; `run_results.json` do dbt; `DESCRIBE HISTORY` do Delta |
| Linhagem | dbt docs (source → fato); metadados `_dlt_load_id` no bronze |
| Frescor | Task `dbt_freshness` (`dbt source freshness` em `silver.passagem`, só `warn_after`): avisa, nunca bloqueia |
| Tracing | N/A na fatia 1 |

---

## Pipeline Architecture

### DAG Diagram

```text
transporte_diario (@daily, catchup, max_active_runs=1, depends_on_past)
                                 ┌─► dbt_freshness (só aviso)
  simular ─► extrair ─► silver ──┤
                                 └─► dbt_build ─► reconciliar ─► publicar
                                        │ falha       │ diverge
                                        └─────────────┴──► publicar não roda
                                                           (Metabase fica na versão anterior)
  [pool transporte_escrita: silver, dbt_build, reconciliar, publicar]

transporte_carga_historica (manual, timeouts próprios)
  catalogo ─► simular_intervalo (COPY) ─► extrair_intervalo (loop de extrair_dia, 1 partição/dia)
           ─► silver --historico (1x) ─► dbt build --full-refresh (1x) ─► reconciliar ─► publicar
  [mesmo pool transporte_escrita]
```

### Partition Strategy

| Tabela | Chave de partição | Granularidade | Motivo |
|--------|-------------------|---------------|--------|
| bronze `oltp/*` | `ingest_date` | Diária (também na carga histórica) | Reprocessamento por data; overwrite simples; contrato "1 partição = 1 dia" sem exceção |
| silver `passagem` | `ano_mes_compra` | Mensal | ~2M linhas: partição diária geraria arquivos pequenos demais |
| silver `rota`/`empresa`/`cidade` | — | — | Tabelas pequenas, overwrite completo |
| gold `fato_vendas` | — | — | DuckDB local; volume não justifica |

### Incremental Strategy

| Modelo | Estratégia | Chave | Lookback |
|--------|------------|-------|----------|
| silver `passagem` | MERGE (upsert, última versão vence) | `passagem_id` / `updated_at` | Partição D do bronze |
| `fato_vendas` | `delete+insert` | `passagem_id` | Janela `updated_at ∈ [D, D+1)` com D via `--vars` (sem lookback sobre o destino); full refresh na carga histórica |
| dimensões gold | `table` (full refresh) | SK = hash da chave natural | — |
| `dim_tempo` | `table` | `data_sk` | 2024-01-01 até `max(data_viagem)` + 1 ano (calculado) |

### Schema Evolution Plan

| Mudança | Tratamento | Rollback |
|---------|------------|----------|
| Coluna nova no OLTP | Bronze aceita (Parquet); silver tem schema explícito → adicionar no schema + `mergeSchema` controlado; dbt `on_schema_change='fail'` força atualização consciente | Delta time travel (`VERSION AS OF`) |
| Mudança de tipo | Nova coluna com o tipo novo; migrar consumidores; remover a antiga depois | Time travel |
| Coluna removida | Manter nula na silver por um ciclo; remover do contrato dbt depois | Re-adicionar coluna |

### Data Quality Gates

| Gate | Ferramenta | Limite | Ação na falha |
|------|------------|--------|---------------|
| PK não nula e única (silver e gold) | dbt `not_null` + `unique` (na source silver e nos marts) | 0 violações | Bloqueia publicação |
| Rota órfã | dbt `relationships` em `stg_oltp__passagem.rota_id` (antes do join do fato) | 0 órfãos | Bloqueia publicação |
| `status` válido | dbt `accepted_values` (`vendida`, `cancelada`) | 0 violações | Bloqueia publicação |
| `valor > 0` e viagem ≥ compra | dbt teste singular / `dbt_utils.expression_is_true` | 0 violações | Bloqueia publicação |
| Reconciliação OLTP × gold (todas as datas) | Task `reconciliar`, antes de `publicar` | Diferença 0 (centavos inclusos) | DAG falha; bloqueia publicação |
| Frescor da silver | Task `dbt_freshness` | warn 1 dia | Só aviso no log (não bloqueia) |

---

## Riscos e Spikes do Build (fazer primeiro)

| ID | Spike | Critério de saída | Plano B |
|----|-------|-------------------|---------|
| A-001 | Spark grava Delta no MinIO → DuckDB `delta_scan` lê | `select count(*)` bate | Spark exporta Parquet (abordagem B) |
| A-002 | Metabase (base Debian) + `metabase_duckdb_driver` abre o serving; mesma versão do DuckDB no dbt, no driver e no Python | Pergunta simples retorna dados; sync sem erros | Publicar gold em Postgres |
| A-003 | Metabase enxerga o arquivo novo após `os.replace` | Dado novo aparece sem reiniciar | Chamar API de re-sync / reconectar |
| A-008 | Imagem do MinIO disponível e fixável | `docker pull` da tag + digest escolhidos | Usar a última imagem community publicada ou alternativa S3 (SeaweedFS/Garage); ADR 0005 |

> A-008: a MinIO deixou de publicar imagens da edição community no fim de 2025 — o build deve
> confirmar qual tag usar (premissa registrada no DEFINE v1.1). Conferir o mesmo risco na imagem
> auxiliar `mc` usada para criar os buckets.

---

## Build Milestones

O `/build` segue marcos com critério de saída; cada marco só começa quando o anterior fecha.

| Marco | Entrega | Critério de saída |
|-------|---------|-------------------|
| M0 | Spikes A-001, A-002, A-003, A-008 | As 4 premissas confirmadas ou com plano B escolhido |
| M1 | Walking skeleton por CLI/Makefile, **sem Airflow**: simulador → bronze → silver → gold → reconciliação | 1 dia processado ponta a ponta, com a reconciliação batendo |
| M2 | DAGs diária e histórica (pool, `depends_on_past`) | Carga histórica + 3 dias seguidos rodando no Airflow |
| M3 | Metabase + dashboard | 3 perguntas respondidas (receita/mês, top 10 rotas, receita por empresa) |
| M4 | CI, ADRs, README (incluindo "Limitações conhecidas") | CI verde no `main` |

---

## Revision History

| Versão | Data | Autor | Mudanças |
|--------|------|-------|----------|
| 1.0 | 2026-11-20 | design-agent | Versão inicial a partir do DEFINE da fatia 1 |
| 1.1 | 2026-11-20 | iterate-agent | Cascata do DEFINE v1.1 — revisão janela 2 ([REVIEW_FATIA1_JANELA2.md](./REVIEW_FATIA1_JANELA2.md)): agenda de cancelamento (R1); histórica dia a dia (R2); fato incremental por `--vars` + `depends_on_past` (R3); reconciliar todas as datas antes de publicar (R4); Decision 8 UTC/decimal (R5); Decision 9 ordem/pool; Decisions 3, 6 e 7 revisadas; AT-006 executável, SKs por hash, Metabase Debian + app DB Postgres, serving só marts, dlt sem pacotes pendentes, dia sem vendas, testes de MERGE e janela (M6–M15); Build Milestones (M16); itens B17–B29; manifesto 60 → 72 |

---

## Next Step

**Pronto para:** janela 3 → `/build .claude/sdd/features/DESIGN_FATIA1_VENDAS_RECEITA.md`,
começando pelo marco M0 (spikes).
