# REVIEW: Fatia 1 — Revisão da janela 2

> Revisão crítica do BRAINSTORM, DEFINE e DESIGN da fatia 1, feita em contexto novo (janela 2).
> Todas as decisões abaixo foram APROVADAS pelo Cézar em 2026-11-20 e devem ser aplicadas com
> `/iterate`.

## Metadata

| Atributo | Valor |
|----------|-------|
| **Feature** | FATIA1_VENDAS_RECEITA |
| **Data** | 2026-11-20 |
| **Autor** | revisor (Claude, janela 2) + Cézar |
| **Documentos revisados** | [BRAINSTORM_TRANSPORTE_RODOVIARIO.md](BRAINSTORM_TRANSPORTE_RODOVIARIO.md), [DEFINE_FATIA1_VENDAS_RECEITA.md](DEFINE_FATIA1_VENDAS_RECEITA.md), [DESIGN_FATIA1_VENDAS_RECEITA.md](DESIGN_FATIA1_VENDAS_RECEITA.md) |
| **Status** | ✅ Aplicado — DEFINE v1.1 e DESIGN v1.1 (2026-11-20) |

## Instruções para o /iterate

Aplicar em `DEFINE_FATIA1_VENDAS_RECEITA.md` e `DESIGN_FATIA1_VENDAS_RECEITA.md`. Ambos → revisão
1.1 (Revision History: "Revisão janela 2 — ver REVIEW_FATIA1_JANELA2.md"). Remover do DESIGN as
notas "Revisor (janela 2)". Manter títulos de seção em inglês (spec-lint) e rodar o spec-lint ao
final. O BRAINSTORM não muda.

---

## ALTA

**R1 — Cancelamentos determinísticos (agenda).**
- O gerador decide, no nascimento da venda, se e quando ela será cancelada (função pura de
  seed + passagem_id; `data_cancelamento` entre `data_compra` e `data_viagem`, ou nula).
- A agenda fica persistida em `simulador.agenda_cancelamento(passagem_id PK, data_cancelamento)`,
  num schema `simulador` que o pipeline NÃO extrai.
- `simular D` = INSERT das vendas de D + `UPDATE ... SET status='cancelada', updated_at=<instante em D>`
  para as passagens com `data_cancelamento = D AND status = 'vendida'`.
- O simulador nunca consulta o estado do OLTP para escolher quem cancelar.
- DESIGN: Decision 4 (linha Simulador), Pattern 1, Data Flow passo 1, `SIMULADOR_TAXA_CANCELAMENTO`
  (= fração das vendas que terão cancelamento agendado).
- **Motivo:** escolher os cancelamentos consultando o estado atual do OLTP faz com que reprocessar
  D cancele passagens diferentes, o que quebra o AT-002 e a reprodutibilidade da fonte.

**R2 — Carga histórica particionada por dia.**
- `extrair_intervalo` = loop de `extrair_dia(D)` para cada dia, num único processo
  (partições `ingest_date=D` idênticas às da diária).
- Spark roda 1x lendo o intervalo; dbt roda 1x com `--full-refresh`.
- Remover "ingest_date = data_fim".
- DESIGN: Decision 5, DAG Diagram. DEFINE: ver "Edições no DEFINE".
- **Motivo:** com 2 anos numa partição `ingest_date=data_fim`, reprocessar essa data pela DAG
  diária apagaria todo o bronze histórico; além disso, a partição violaria o contrato "1 partição = 1 dia".

**R3 — `fato_vendas` incremental guiado pela data lógica.**
- `dbt build --vars '{data: "<D>"}'`.
- Filtro `updated_at >= D AND updated_at < D + 1 day` (UTC) no lugar de
  `max(updated_at) - interval 3 day`. `delete+insert` por `passagem_id`. Full refresh na histórica.
- `depends_on_past=True` no `default_args` da DAG diária.
- DESIGN: Decision 4 (linha Gold), Pattern 5, Pattern 6, Incremental Strategy (lookback → "dia D").
- **Motivo:** se D-5 falha e D-4..D seguem, a reexecução de D-5 é filtrada pelo lookback e as
  vendas de D-5 nunca entram no fato (buraco silencioso).

**R4 — Reconciliar todas as datas ANTES de publicar.**
- Ordem: `simular → extrair → silver → dbt_build → reconciliar → publicar`.
- `reconciliar` lê o warehouse (não o serving) e compara TODAS as datas: OLTP × gold
  `GROUP BY data_compra::date` com count e `sum(valor_liquido)`, via full outer join.
- Divergência → falha listando as datas e os valores dos dois lados.
- DESIGN: Components, Data Flow 5/6, Pattern 6, DAG Diagram (diária e histórica), Data Quality
  Gates, cobertura do AT-007.
- **Motivo:** com a ordem atual, dado divergente já chega ao Metabase antes da checagem; e
  conferir só D não cumpre o critério "100% das datas".

**R5 — UTC e decimal em todas as camadas.**
- `TZ=UTC` em todos os containers; `spark.sql.session.timeZone=UTC`; DuckDB `SET TimeZone='UTC'`
  (settings no profiles.yml); o simulador gera instantes em UTC; o Airflow em UTC (padrão).
- `valor` como DECIMAL(10,2) no Postgres → Arrow → Spark `DecimalType(10,2)` → DuckDB;
  somas da reconciliação em `Decimal`.
- Teste: uma venda às 23:30 UTC de D permanece em D até o gold.
- DEFINE Data Contract: nota "timestamps em UTC". Novo ADR 0006.
- **Motivo:** `TIMESTAMP` sem fuso interpretado com fusos diferentes entre ferramentas muda o dia
  das vendas noturnas e quebra a reconciliação ao centavo.

## MÉDIA

**M6 — AT-006 executável.**
- O E2E grava uma passagem órfã direto na silver Delta, espera falha do `dbt build` e depois faz
  `RESTORE` para a versão anterior. A FK no OLTP é mantida.
- `relationships` em `stg_oltp__passagem.rota_id → stg_oltp__rota` é o portão. O teste de FK no
  fato não conta como gate (o inner join descarta órfãos). Ajustar Pattern 5, a nota do Pattern 5,
  Quality Gates e a cobertura dos ATs.
- **Motivo:** a FK do OLTP impede inserir órfãos lá, então o teste planejado era impossível.

**M7 — Surrogate keys estáveis.** Todas as dims com
`_sk = dbt_utils.generate_surrogate_key([chave natural])`. Documentar em `_core__models.yml`.
**Motivo:** dims recriadas (`table`) + fato incremental; uma SK por numeração de linhas mudaria
entre builds e apontaria vendas para a dimensão errada.

**M8 — Imagem do Metabase.**
- Base Debian (eclipse-temurin JRE) + `metabase.jar` + `metabase_duckdb_driver`.
- Mesma versão do DuckDB (fixada) no dbt-duckdb, no driver e no `duckdb` Python.
- O spike A-002 valida.
- **Motivo:** a imagem oficial (Alpine/musl) provavelmente não carrega o JDBC do DuckDB; uma
  versão mais antiga do DuckDB não abre um arquivo gravado por uma mais nova.

**M9 — Serving só com marts.**
- `publicar` cria um arquivo novo contendo somente `fato_vendas` e `dim_*` como TABELAS
  (ATTACH + CREATE TABLE AS), em `.tmp` + `os.replace`. Sem views de staging no serving.
- Atualizar Pattern 7 e Decision 3.
- **Motivo:** as views de staging usam `delta_scan` no MinIO e quebrariam o sync do Metabase; o
  arquivo novo também evita perder alterações pendentes no WAL.

**M10 — dlt sem reenvio de pacotes.** Antes do run: `drop_pending_packages()`, `pipelines_dir`
temporário por execução, `restore_from_destination=False`. Pattern 2.
**Motivo:** no retry, o dlt reenviaria o pacote pendente da tentativa anterior, duplicando o bronze.

**M11 — Dia sem vendas.**
- O job da silver verifica a existência de `passagem/ingest_date=D`. Se não existir: log
  `linhas=0` e pula o MERGE (as dimensões seguem normalmente).
- DEFINE Freshness SLA (bronze): "load do dlt registrado para D, inclusive com 0 linhas".
- **Motivo:** com 0 linhas, o dlt não cria a partição e o Spark falha com "path does not exist" (AT-009).

**M12 — Pool de escrita.** Pool `transporte_escrita` (1 slot) nas tasks `silver`, `dbt_build` e
`publicar` das DUAS DAGs; `airflow/pools.json` importado na inicialização do compose.
**Motivo:** `max_active_runs=1` não impede a DAG histórica e a diária de escreverem ao mesmo tempo
(MERGE concorrente no Delta; dois escritores no `.duckdb`).

**M13 — Venv do pipeline.**
- `/opt/pipeline-venv` com `transporte` + dlt + pyspark + delta-spark; `/opt/dbt-venv` mantido;
  o ambiente do Airflow só com Airflow e providers. O BashOperator chama os binários dos venvs.
- Ampliar a Decision 7.
- **Motivo:** pyarrow, pandas e sqlalchemy conflitam com as constraints do Airflow.

**M14 — Testes adicionais.**
- `tests/unit/test_merge_silver.py`: insert; cancelamento atualiza; versão antiga não
  sobrescreve; rodar 2x é idempotente.
- `tests/unit/test_ingestao_janela.py`: bordas de `[D, D+1)`.
- Mais: agenda determinística e independente da ordem (`test_gerador`); venda às 23:30 UTC;
  nenhuma coluna `*cpf*` em silver e gold.
- **Motivo:** as peças que sustentam a idempotência (MERGE, janela de extração) não tinham teste.

**M15 — Metabase persistente.** App DB num banco `metabase` (usuário próprio) no Postgres
existente via `00_databases.sql`; env `MB_DB_*`. Manter `docs/metabase/dashboard-receita.md`.
**Motivo:** o H2 padrão dentro do container perde os dashboards quando o container é recriado.

**M16 — Build em marcos.** Adicionar ao DESIGN:

| Marco | Entrega | Critério de saída |
|---|---|---|
| M0 | Spikes A-001, A-002, A-003, A-008 | Confirmadas ou plano B escolhido |
| M1 | Walking skeleton por CLI, sem Airflow: simulador → bronze → silver → gold → reconciliação | 1 dia ponta a ponta com reconciliação OK |
| M2 | DAGs diária e histórica | Histórica + 3 dias seguidos no Airflow |
| M3 | Metabase + dashboard | 3 perguntas respondidas |
| M4 | CI, ADRs, README | CI verde no main |

Remover os 2 COULD do DEFINE (build de imagens no CI; dbt-expectations).
**Motivo:** com 60+ arquivos, construir tudo de uma vez adiaria até o fim a descoberta de
incompatibilidades básicas.

## BAIXA

- **B17:** `dim_tempo` termina em `max(data_viagem) + 1 ano` (nada fixo em 2027).
- **B18:** task `dbt_freshness` (`dbt source freshness`) só com aviso: apenas `warn_after`, sem
  `error_after`; roda após `silver`, em paralelo ao `dbt_build`, sem bloquear. Atualizar Quality Gates.
- **B19:** dlt com `add_dlt_load_id = true` (normalizer parquet/arrow), para cumprir o requisito de
  lineage do bronze.
- **B20:** completar a Configuration: `DESTINATION__FILESYSTEM__CREDENTIALS__{AWS_ACCESS_KEY_ID,
  AWS_SECRET_ACCESS_KEY,ENDPOINT_URL}`, `SILVER_ROOT`, `DBT_TARGET`, `DBT_PROFILES_DIR`, `TZ`,
  `MB_DB_*`; o `SPARK_SUBMIT` passa a usar `$SPARK_DRIVER_MEMORY`.
- **B21:** DAG histórica com `execution_timeout` próprio; inserção em massa do simulador via `COPY` (psycopg).
- **B22:** `airflow/.airflowignore` ignorando `_defaults.py`.
- **B23:** `tests/integration/test_dags.py` com o marker `integration`.
- **B24:** sqlfluff com templater `dbt` (target `ci`), rodando no job `dbt` do CI após `dbt deps`;
  o job `lint` fica com ruff.
- **B25:** método de medição: `docker stats` amostrado durante a carga histórica; `docker system df`
  + `du -sh data/`. Escrever junto aos critérios do DEFINE.
- **B26:** documentar `CPF_HASH_SALT` fora do Git; hash = `sha256(salt || cpf)`.
- **B27:** corrigir a contagem do manifesto; remover `docker/spark/`.
- **B28:** README com o passo de branch protection do `main`.
- **B29:** `.github/dependabot.yml` (ecossistemas uv/pip, docker, github-actions; semanal) —
  longevidade das versões fixadas.
- **README:** seção "Limitações conhecidas" (segredos em .env vs cofre; medição manual vs
  monitoramento; alertas só na fatia 4; sem backup do MinIO — dados reconstruíveis pela seed).

## Manifesto — adicionar / alterar

| Arquivo | Ação | Origem |
|---|---|---|
| `.dockerignore` | Create | revisão |
| `sql/init/20_simulador_schema.sql` | Create (`simulador.agenda_cancelamento`) | R1 |
| `sql/init/00_databases.sql` | (existente) + banco/usuário `metabase` | M15 |
| `docker/airflow/requirements-pipeline.txt` | Create | M13 |
| `airflow/.airflowignore` | Create | B22 |
| `airflow/pools.json` | Create | M12 |
| `tests/unit/test_merge_silver.py` | Create | M14 |
| `tests/unit/test_ingestao_janela.py` | Create | M14 |
| `tests/integration/conftest.py` | Create (compose, data D, RESTORE Delta) | M6, M14 |
| `docs/adr/0005-object-storage.md` | Create | A-008 |
| `docs/adr/0006-fuso-utc-e-decimal.md` | Create | R5 |
| `.github/dependabot.yml` | Create | B29 |
| `docker/spark/` | Delete | B27 |

Atualizar Agent Assignment e o total de arquivos.

## Edições no DEFINE

- **Success Criteria:** substituir "backfill dos 730 dias ≤ 3 h" por (a) "Carga histórica de 730 dias
  (DAG `transporte_carga_historica`) ≤ 3 h" e (b) "Backfill curto (até 7 dias) pela DAG diária ≤ 15 min
  por data". Acrescentar o método de medição de memória e disco (B25).
- **Goals:** SHOULD de backfill → "Backfill de intervalos curtos pela DAG diária; a carga inicial usa a
  DAG de carga histórica". Remover os 2 COULD.
- **Freshness SLAs (bronze):** texto do M11.
- **Data Contract:** timestamps em UTC (R5).
- **Assumptions:** A-003 → "resolvida pela Decision 3 (cópia atômica; serving só com marts)". Adicionar:
  `| A-008 | Existe imagem do MinIO (community) fixável por tag + digest | Usar SeaweedFS ou Garage (S3-compatível); muda só o compose e o endpoint | [ ] — spike |`
- **Open Questions → respostas:**
  1. Receita pela data da compra: confirmado. O fato mantém `data_viagem_sk`; documentar que a receita
     de períodos passados muda com cancelamentos tardios (posição atual, não fechamento).
  2. Limites de tempo realistas (estimativa: 2–4 min/dia); o insert em massa via COPY é a condição da
     carga histórica.
  3. CPF mantido: em claro só no bronze, hash com salt secreto na silver, ausente no gold.
- **Status/Clarity:** reavaliar após as edições.

---

## Next Step

No terminal WSL:
`/agentspec:workflow:iterate .claude/sdd/features/DEFINE_FATIA1_VENDAS_RECEITA.md "Aplicar as decisões de .claude/sdd/features/REVIEW_FATIA1_JANELA2.md"`
e aceitar a cascata para o DESIGN. Depois: janela 3 → `/build` começando pelo marco M0.
