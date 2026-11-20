# DEFINE: Fatia 1 — Vendas de Passagens até o Dashboard de Receita

> Pipeline diário, idempotente e testado que leva as vendas de passagens do Postgres OLTP até um
> dashboard de receita no Metabase, atravessando bronze, silver e gold.

## Metadata

| Atributo | Valor |
|----------|-------|
| **Feature** | FATIA1_VENDAS_RECEITA |
| **Data** | 2026-11-20 |
| **Autor** | define-agent (Claude) + Cézar |
| **Status** | ✅ Complete (Designed) |
| **Clarity Score** | 13/15 |
| **Origem** | [BRAINSTORM_TRANSPORTE_RODOVIARIO.md](BRAINSTORM_TRANSPORTE_RODOVIARIO.md) (fatia 1 do plano) |

> Títulos de seção ficam em inglês porque o `spec-lint` do AgentSpec valida por nome de seção;
> o conteúdo é em português.

---

## Problem Statement

As vendas de passagens existem apenas no banco transacional (OLTP), onde consultas analíticas
competem com a operação e não há histórico confiável de alterações (ex.: cancelamentos). Analistas
não têm uma fonte única, testada e reprocessável para responder "quanto vendemos, em quais rotas e
empresas, em cada período".

---

## Target Users

| Usuário | Papel | Dor |
|---------|-------|-----|
| Analista comercial | Acompanha receita e volume | Precisa de receita por rota/empresa/período sem consultar o OLTP nem montar planilhas |
| Engenheiro de dados (mantenedor — Cézar) | Opera e evolui a plataforma | Precisa de um pipeline que possa ser reexecutado sem duplicar dados, com testes e CI que acusem regressões |

---

## Goals

| Prioridade | Objetivo |
|------------|----------|
| **MUST** | Simulador gera o histórico (~2 anos) e as vendas de cada dia no Postgres, de forma determinística (seed) |
| **MUST** | Vendas podem ser **canceladas** depois da compra (UPDATE no OLTP), para que a extração incremental e o MERGE tenham alterações reais para tratar |
| **MUST** | Extração incremental do Postgres para o bronze (MinIO, Parquet) com dlt, por watermark de `updated_at` |
| **MUST** | Bronze → silver com PySpark + Delta Lake: tipagem, deduplicação e MERGE por chave da passagem (última versão vence) |
| **MUST** | Gold com dbt + DuckDB lendo a silver Delta direto no MinIO: `fato_vendas`, `dim_rota`, `dim_empresa`, `dim_cidade`, `dim_tempo` |
| **MUST** | Uma DAG diária no Airflow encadeia simulação → extração → silver → dbt build, com retries |
| **MUST** | Reexecutar qualquer data não duplica nem perde dados (idempotência em todas as camadas) |
| **MUST** | Testes do dbt (not_null, unique, relationships, accepted_values) e testes unitários (pytest) passando |
| **MUST** | CI no GitHub Actions: ruff, sqlfluff, pytest (unitários) e `dbt build` em amostra pequena |
| **MUST** | Dashboard de receita no Metabase: receita e passagens por mês, por rota e por empresa |
| **SHOULD** | Backfill de um intervalo de datas pelo próprio Airflow (catchup/backfill da DAG) |
| **SHOULD** | Teste de reconciliação OLTP × gold (contagem e receita por data) automatizado |
| **SHOULD** | dbt docs gerado com lineage de source (silver) até `fato_vendas` |
| **SHOULD** | ADRs registrados para as decisões estruturais (ver Constraints) |
| **COULD** | Build das imagens Docker customizadas no CI |
| **COULD** | dbt-expectations para regras de negócio (ex.: valor dentro de faixa por distância) |

---

## Success Criteria

- [ ] Carga histórica gera entre **1M e 5M** passagens cobrindo **730 dias** (2 anos), com seed fixa produzindo os mesmos totais a cada execução.
- [ ] A DAG diária completa uma data em **≤ 15 min** na máquina local (WSL2).
- [ ] O backfill dos 730 dias completa em **≤ 3 h**.
- [ ] Pico de memória do WSL2 **≤ 10 GB** durante a DAG (limite do `.wslconfig`).
- [ ] Imagens Docker + dados da fatia 1 ocupam **≤ 15 GB** de disco.
- [ ] Reconciliação: para **100%** das datas, contagem de passagens e receita líquida no OLTP == em `fato_vendas` (diferença 0, centavos inclusos).
- [ ] Reexecutar a mesma data **3 vezes seguidas** deixa contagem e receita idênticas em bronze (por partição), silver e gold.
- [ ] **0** chaves primárias nulas ou duplicadas em silver e em todos os modelos gold.
- [ ] **100%** dos testes dbt e pytest passando; CI verde no `main`.
- [ ] Dashboard com **≥ 3** perguntas: receita/mês, top 10 rotas por receita, receita por empresa.

---

## Acceptance Tests

| ID | Cenário | Dado | Quando | Então |
|----|---------|------|--------|-------|
| AT-001 | Caminho feliz diário | Histórico carregado e serviços no ar | A DAG roda para a data D | Vendas de D aparecem em bronze, silver, `fato_vendas` e no dashboard |
| AT-002 | Idempotência | A data D já foi processada | A DAG roda de novo para D | Contagens e receita de D ficam iguais em todas as camadas |
| AT-003 | Cancelamento tardio | Passagem vendida em D foi cancelada em D+3 | A DAG roda para D+3 | Silver mantém 1 linha da passagem com status `cancelada`; receita líquida de D no gold cai pelo valor dela |
| AT-004 | Duplicata na extração | O bronze recebeu a mesma versão de uma passagem duas vezes (janela sobreposta) | A silver é processada | Silver tem 1 linha por `passagem_id` |
| AT-005 | Falha transitória | MinIO indisponível durante a extração | A task falha | Airflow faz retry; quando o serviço volta, a execução termina sem duplicatas |
| AT-006 | Qualidade bloqueia | Uma venda com `rota_id` inexistente chega à silver | `dbt build` roda | O teste `relationships` falha e a DAG fica marcada como falha |
| AT-007 | Reconciliação | Uma data qualquer processada | O teste de reconciliação roda | Contagem e receita líquida OLTP == gold |
| AT-008 | CI | Um PR altera um modelo dbt | O CI roda | ruff, sqlfluff, pytest e `dbt build` (amostra) executam; qualquer falha bloqueia o merge |
| AT-009 | Dia sem vendas | O simulador gera 0 vendas para D | A DAG roda | Execução termina com sucesso, sem erro e sem linhas novas |

---

## Out of Scope

- CSVs da ANTT (empresas, linhas, terminais reais) → fatia 2. Na fatia 1, empresas, cidades e rotas vêm do **seed do simulador**.
- Snapshots SCD2 → fatia 2.
- IBGE (população), Open-Meteo (clima) e o dashboard de demanda externa → fatia 3 (`dim_cidade` sai sem população).
- Alertas externos (e-mail/Slack) e Ollama → fatia 4. Na fatia 1, falha = status e log no Airflow.
- FastAPI (removido do projeto na v1).
- Métricas de ocupação e de comportamento de compra.
- Streaming / micro-lotes; cloud; Kubernetes; hardening de segurança dos serviços (credenciais locais via `.env`).

---

## Constraints

| Tipo | Restrição | Impacto |
|------|-----------|---------|
| Técnica | WSL2 com 15 GB de RAM (limite ~10 GB) e 37 GB de disco livre | Compose por profiles; Spark com 3–4 GB; imagens enxutas |
| Técnica | Stack 100% gratuita e local (sem Snowflake/Databricks) | DuckDB como warehouse; PySpark local |
| Técnica | Delta gravado sem features avançadas (ex.: deletion vectors) | Garante leitura pela extensão `delta` do DuckDB (vira ADR) |
| Técnica | Airflow só orquestra; lógica fica em `src/transporte`, `spark/jobs`, `dbt` | Lógica testável sem Airflow |
| Técnica | Python 3.12, uv, ruff, pre-commit já configurados | Dependências novas entram via `uv add` |
| Processo | Ciclo de 3 janelas: specs → revisão → build | DESIGN sai desta janela; revisão em contexto novo |
| Recurso | Nenhum dado versionado no Git; e-mail do Git = noreply | Amostra do CI é gerada pelo simulador, não commitada |

---

## Technical Context

| Aspecto | Valor | Notas |
|---------|-------|-------|
| **Local de deploy** | `src/transporte/{simulator,ingestion,common}`, `spark/jobs/`, `dbt/`, `airflow/dags/`, `sql/init/`, `docker/`, `.github/workflows/` | Estrutura já criada no repo |
| **KB Domains** | `dbt` (incremental-model, generic-tests, model-types), `spark` (delta-integration, read-write-patterns), `airflow` (dag-design, error-handling), `sql-patterns` (deduplication) | Consultar no /design |
| **IaC Impact** | Novos recursos: `docker-compose.yml` com Postgres, MinIO, Airflow (LocalExecutor), Spark, Metabase | Profiles para caber na RAM |

---

## Data Contract

### Source Inventory

| Fonte | Tipo | Volume | Frescor | Dono |
|-------|------|--------|---------|------|
| `passagem` (vendas) | Postgres OLTP (simulado) | ~1–5M histórico; ~1,4k–7k/dia | Diário (D-1 disponível na execução de D) | Simulador |
| `empresa`, `cidade`, `rota` | Postgres OLTP (seed do simulador) | Dezenas / centenas | Estático na fatia 1 | Simulador |

### Schema Contract (`passagem`, nível lógico — tipos finais no /design)

| Coluna | Tipo | Restrições | PII? |
|--------|------|------------|------|
| `passagem_id` | BIGINT | PK, NOT NULL, UNIQUE | Não |
| `rota_id` | INT | NOT NULL, FK → `rota` | Não |
| `data_compra` | TIMESTAMP | NOT NULL | Não |
| `data_viagem` | DATE | NOT NULL, ≥ data da compra | Não |
| `valor` | DECIMAL(10,2) | NOT NULL, > 0 | Não |
| `status` | VARCHAR | NOT NULL, IN (`vendida`, `cancelada`) | Não |
| `passageiro_cpf` | VARCHAR | NOT NULL; mascarado/hasheado a partir da silver | **Sim** (fictício, Faker) |
| `created_at` | TIMESTAMP | NOT NULL | Não |
| `updated_at` | TIMESTAMP | NOT NULL; watermark da extração | Não |

**Receita líquida** = soma de `valor` com `status = 'vendida'`. Receita é atribuída à
**data da compra** (`data_compra`).

### Freshness SLAs

| Camada | Alvo | Medição |
|--------|------|---------|
| Bronze | Partição de D escrita na execução de D | Existência da partição `ingest_date=D` |
| Silver | Atualizada na mesma execução | Versão nova da tabela Delta |
| Gold / dashboard | Atualizado ao fim da DAG de D (≤ 15 min) | Status da DAG + max(`data_compra`) em `fato_vendas` |

### Completeness Metrics

- 100% das passagens do OLTP presentes em silver e gold para cada data processada.
- 0 PKs nulas ou duplicadas em silver e gold.
- 100% das FKs de `fato_vendas` resolvem para uma dimensão.

### Lineage Requirements

- Lineage de modelos dbt da source (silver Delta) até `fato_vendas`, visível no dbt docs.
- Cada linha de bronze carrega metadados de carga (id da carga, data de ingestão) do dlt.

---

## Assumptions

| ID | Premissa | Se estiver errada | Validada? |
|----|----------|-------------------|-----------|
| A-001 | A extensão `delta` do DuckDB lê tabelas Delta no MinIO (S3 path-style) gravadas pelo Spark | Volta à abordagem B do brainstorm (Spark exporta Parquet) | [ ] — spike no início do build |
| A-002 | O Metabase conecta ao DuckDB via driver da comunidade (`metabase_duckdb_driver`) | Gold é publicado em Postgres para o Metabase, ou outro BI | [ ] |
| A-003 | DuckDB permite o Metabase ler enquanto o dbt não escreve (1 escritor, leitores read-only) | Metabase lê um arquivo publicado/copiado ao fim da DAG | [ ] — definir no /design |
| A-004 | dlt faz extração incremental por `updated_at` e grava Parquet no MinIO (destino filesystem/S3) | Extração com SQL + PyArrow manual | [ ] |
| A-005 | Spark + delta-spark rodam em container com 3–4 GB sem OOM para o volume diário e o backfill | Ajustar partições/memória ou reduzir volume para ~1M | [ ] |
| A-006 | Todos os serviços da fatia 1 cabem em ≤ 10 GB de RAM usando profiles | Subir serviços em etapas (ingestão+Spark, depois dbt+Metabase) | [ ] |
| A-007 | Cancelamentos ocorrem até a data da viagem; não há outras alterações de venda | Ampliar o modelo de status e o MERGE | [x] decisão de escopo |

---

## Clarity Score Breakdown

| Elemento | Nota (0-3) | Notas |
|----------|-----------|-------|
| Problem | 3 | Problema e impacto claros, derivados do brainstorm |
| Users | 2 | Personas definidas, mas são hipotéticas (projeto de portfólio) |
| Goals | 3 | MoSCoW explícito |
| Success | 3 | Critérios numéricos e testáveis |
| Scope | 2 | Fronteiras claras; regras de cancelamento e atribuição de receita são premissas a revisar |
| **Total** | **13/15** | Acima do gate de 12 |

---

## Open Questions

Nenhuma bloqueia o /design. Para o revisor (janela 2) confirmar:

1. Receita atribuída à **data da compra** (e não à data da viagem) está ok?
2. Faixas de desempenho (≤ 15 min/dia, ≤ 3 h de backfill) são realistas para a máquina?
3. Incluir CPF fictício (PII) para praticar mascaramento, ou remover o campo?

---

## Revision History

| Versão | Data | Autor | Mudanças |
|--------|------|-------|----------|
| 1.0 | 2026-11-20 | define-agent | Versão inicial a partir do BRAINSTORM (fatia 1) |

---

## Next Step

**Design:** [DESIGN_FATIA1_VENDAS_RECEITA.md](DESIGN_FATIA1_VENDAS_RECEITA.md) → próximo: `/build`
