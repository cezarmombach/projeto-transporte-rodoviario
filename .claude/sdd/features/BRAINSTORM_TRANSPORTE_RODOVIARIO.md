# BRAINSTORM: Plataforma de Dados de Transporte Rodoviário

> Sessão exploratória para clarificar intenção e abordagem antes da captura de requisitos.

## Metadata

| Atributo | Valor |
|----------|-------|
| **Feature** | TRANSPORTE_RODOVIARIO |
| **Data** | 2026-11-17 (iniciado em 2026-11-16) |
| **Autor** | brainstorm-agent (Claude) + Cézar |
| **Status** | Ready for Define |

---

## Ideia Inicial

**Entrada original:** plataforma de dados ponta a ponta sobre venda de passagens de ônibus, stack
100% gratuita e local, arquitetura medalhão, para aprender Engenharia de Dados nos padrões de
mercado e publicar no GitHub. Foco principal: "fazer bem feito"; apresentar em entrevista é
secundário.

**Contexto levantado:**
- Repositório já estruturado: `src/transporte/{ingestion,simulator,common}`, `spark/jobs`, `dbt`,
  `airflow/{dags,plugins}`, `docker`, `sql/init`, `tests/{unit,integration}`, `docs/adr`.
- Tooling configurado: uv + Python 3.12, ruff, pytest (marker `integration`), pre-commit.
- Nenhum código de negócio, serviço Docker ou modelo dbt ainda existe.

**Contexto técnico observado (para o Define):**

| Aspecto | Observação | Implicação |
|---------|------------|------------|
| Localização provável | `src/transporte/` (Python), `spark/jobs/`, `dbt/`, `airflow/dags/`, `docker/` | Airflow só orquestra; lógica vive em pacotes testáveis |
| Domínios de conhecimento | dlt, Airflow, PySpark/Delta, dbt-duckdb, MinIO/S3, Metabase | Consultar padrões de cada ferramenta no Design |
| IaC | Docker Compose com profiles | Sem cloud; tudo local no WSL2 |

---

## Perguntas de Descoberta & Respostas

| # | Pergunta | Resposta | Impacto |
|---|----------|----------|---------|
| 1 | Quais perguntas de negócio os dashboards respondem? | Receita/volume (rota, empresa, período) + demanda externa (vendas per capita IBGE, clima Open-Meteo) | Define o star schema; dispensa modelar viagens/capacidade/canal |
| 2 | Volume da simulação? | Médio: ~1–5M vendas (~2 anos de histórico) | Dá peso real a MERGE e incremental; cabe em 15 GB RAM / 37 GB disco |
| 3 | Frescor dos dados? | Lote diário | DAG `@daily`, extração incremental por watermark, backfill possível |
| 4 | Estratégia de entrega? | Fatia vertical | Uma fonte atravessa todas as camadas antes de adicionar as demais |
| 5 | Como o gold lê a silver Delta? | DuckDB lê o Delta direto do MinIO | Silver é a única fonte de verdade; sem cópia de dados |
| 6 | O que fica fora da v1? | Só o FastAPI | Ollama, SCD2 e alertas externos permanecem (em fatias posteriores) |
| 7 | Critério de pronto da fatia 1? | Pipeline + testes + CI | Qualidade entra desde a primeira fatia, não no final |

---

## Inventário de Amostras

| Tipo | Local | Qtde | Notas |
|------|-------|------|-------|
| Arquivos de entrada | N/A (gerados pelo simulador Faker) | — | Seed fixa para reprodutibilidade |
| CSVs ANTT | Dados abertos ANTT (baixar na fatia 2) | ~3 | Linhas, empresas, terminais; encoding/separador a verificar |
| APIs | IBGE (municípios/população), Open-Meteo (clima histórico) | 2 | Sem chave; respeitar rate limit |
| Ground truth | N/A | — | Totais do simulador servem de reconciliação (bronze × gold) |

**Como as amostras serão usadas:**
- Amostra pequena e determinística do simulador como fixture do CI (`dbt build` em amostra).
- Contagens geradas pelo simulador como teste de reconciliação entre camadas.

---

## Abordagens Exploradas (leitura silver → gold)

### Abordagem A: DuckDB lê o Delta direto no MinIO ⭐ Recomendada

**Descrição:** `dbt-duckdb` com a extensão `delta` (`delta_scan()`) e um secret S3 apontando para o
MinIO (endpoint local, `url_style = path`). As tabelas silver são declaradas como sources do dbt.

**Prós:**
- Silver é a única fonte de verdade (padrão lakehouse).
- Nenhuma etapa extra de exportação; menos disco e menos pontos de falha.

**Contras:**
- A extensão `delta` do DuckDB não suporta todos os recursos de escrita do Delta (ex.: deletion
  vectors) → o Spark precisa gravar com features compatíveis.

**Por que recomendada:** mantém a arquitetura limpa e é exatamente o padrão que o mercado usa
(engine de consulta lendo a tabela aberta no object storage).

### Abordagem B: Spark exporta Parquet além do Delta

**Prós:** compatibilidade total com qualquer leitor.
**Contras:** duplica dados, risco de divergência entre Delta e Parquet, mais disco.

### Abordagem C: Spark grava direto no arquivo DuckDB

**Prós:** gold lê tabelas nativas.
**Contras:** acoplamento frágil (lock do arquivo `.duckdb`), silver deixa de ser lakehouse.

---

## Contexto de Engenharia de Dados

### Sistemas de origem

| Fonte | Tipo | Volume estimado | Frescor |
|-------|------|-----------------|---------|
| Vendas de passagens (Faker) | Postgres OLTP | ~1–5M linhas histórico; ~2–7k/dia | Diário (incremental) |
| ANTT: linhas, empresas, terminais | CSV (dados abertos) | Milhares de linhas | Esporádico (snapshot) |
| IBGE: municípios e população | API REST | ~5.570 municípios | Anual |
| Open-Meteo: clima diário | API REST | cidades × dias (~centenas de milhares) | Diário |

### Esboço do fluxo

```text
[Postgres OLTP] ─┐
[CSVs ANTT]  ────┼─► dlt (Airflow) ─► MinIO bronze (Parquet, particionado por data)
[IBGE / Meteo] ──┘                         │
                                           ▼
                          PySpark + Delta silver (tipado, dedup, MERGE)
                                           │  delta_scan()
                                           ▼
                          dbt + DuckDB gold (staging → marts, star schema)
                                           │
                                           ▼
                                    Metabase (dashboards)
```

### Modelo gold esperado

- `fato_vendas` — grão: 1 passagem vendida
- `fato_clima_diario` — grão: cidade × dia
- `dim_rota`, `dim_empresa`, `dim_cidade` (com população), `dim_tempo`

---

## Abordagem Selecionada

| Atributo | Valor |
|----------|-------|
| **Escolhida** | Abordagem A (DuckDB lê Delta no MinIO) |
| **Confirmação** | 2026-11-17 |
| **Justificativa** | Fonte única de verdade, padrão lakehouse, menos disco |

---

## Decisões-Chave

| # | Decisão | Justificativa | Alternativa rejeitada |
|---|---------|---------------|-----------------------|
| 1 | Entrega em fatias verticais | Valida integração entre todas as camadas cedo | Camada a camada |
| 2 | Lote diário com watermark | Padrão batch de mercado; permite backfill e idempotência | Micro-lotes por hora / carga única |
| 3 | DuckDB lê Delta via `delta_scan` | Silver como fonte única de verdade | Export Parquet; Spark → DuckDB |
| 4 | Spark só na camada silver | Volume não exige Spark; uso focado em MERGE/Delta | Spark em todo o pipeline |
| 5 | Gold focado em receita + demanda externa | YAGNI: dispensa viagens, capacidade e canal | Ocupação; comportamento de compra |
| 6 | Delta gravado sem features avançadas (ex.: deletion vectors) | Compatibilidade com o leitor do DuckDB | — (vira ADR) |
| 7 | Qualidade e CI desde a fatia 1 | Evita dívida técnica acumulada | Testes no final |

---

## Features Removidas (YAGNI)

| Feature | Motivo | Pode entrar depois? |
|---------|--------|---------------------|
| FastAPI sobre o gold | Metabase já atende o consumo | Sim |
| Métricas de ocupação | Exige modelar viagens e capacidade de ônibus | Sim |
| Comportamento de compra (antecedência, canal, perfil) | Fora das perguntas de negócio escolhidas | Sim |

---

## Validações Incrementais

| Seção | Apresentada | Feedback | Ajustado? |
|-------|-------------|----------|-----------|
| Ideia e pipeline | ✅ | Entendido | Não |
| Escopo, gold e fatias de entrega | ✅ | Aprovado ("Sim, pode gerar") | Não |

---

## Requisitos Sugeridos para o /define

### Problema (rascunho)
Não existe uma plataforma que consolide vendas de passagens com dados públicos (ANTT, IBGE, clima)
para responder perguntas de receita e demanda de forma confiável, testada e reprocessável.

### Usuários-alvo (rascunho)

| Usuário | Dor |
|---------|-----|
| Analista comercial | Precisa de receita/volume por rota, empresa e período sem consultar o OLTP |
| Analista de planejamento | Quer entender demanda per capita e efeito do clima nas vendas |
| Engenheiro de dados (mantenedor) | Precisa de pipeline idempotente, testado e observável |

### Plano de fatias

1. **Fatia 1** — Postgres vendas → bronze → silver → gold → dashboard de receita.
2. **Fatia 2** — CSVs ANTT (empresas, linhas, terminais) + snapshots SCD2.
3. **Fatia 3** — IBGE + Open-Meteo + dashboard de demanda externa.
4. **Fatia 4** — Alertas externos de falha + Ollama (text-to-SQL/classificação).

### Critérios de sucesso da fatia 1 (rascunho)
- [ ] DAG diária leva vendas do Postgres até o dashboard de receita no Metabase.
- [ ] Reexecutar a mesma data não duplica dados (idempotência em bronze, silver e gold).
- [ ] Testes dbt (not_null, unique, relationships, accepted_values) passando.
- [ ] Reconciliação: contagem/receita do OLTP = contagem/receita em `fato_vendas` para a data.
- [ ] CI verde: ruff, sqlfluff, pytest e `dbt build` em amostra pequena.

### Restrições
- WSL2 com 15 GB de RAM (limite ~10 GB via `.wslconfig`) e 37 GB de disco livre.
- Stack 100% gratuita e local; sem Snowflake/Databricks.
- Compose por profiles; Spark limitado a 3–4 GB; Airflow com LocalExecutor.
- Nenhum dado versionado no Git; e-mail do Git = noreply do GitHub.

### Fora de escopo (confirmado)
- FastAPI.
- Métricas de ocupação e comportamento de compra.

---

## Resumo da Sessão

| Métrica | Valor |
|---------|-------|
| Perguntas feitas | 7 |
| Abordagens exploradas | 3 |
| Features removidas (YAGNI) | 3 |
| Validações | 2 |

---

## Próximo Passo

**Pronto para:** `/define .claude/sdd/features/BRAINSTORM_TRANSPORTE_RODOVIARIO.md` (escopo: fatia 1)
