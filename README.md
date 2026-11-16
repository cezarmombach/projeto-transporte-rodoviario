# Plataforma de Dados de Transporte Rodoviário

Plataforma de dados ponta a ponta (arquitetura medalhão) sobre venda de passagens de ônibus,
construída com stack 100% open source e local.

> 🚧 Em construção.

## Arquitetura

Fontes (Postgres OLTP, CSVs ANTT, APIs IBGE/Open-Meteo)
→ **dlt** (orquestrado por **Airflow**)
→ **MinIO** bronze (Parquet)
→ **PySpark + Delta Lake** silver
→ **dbt + DuckDB** gold (star schema)
→ **Metabase**

## Estrutura

| Pasta | Conteúdo |
|---|---|
| `src/transporte/` | Pacote Python: ingestão, simulador OLTP, utilitários |
| `spark/jobs/` | Jobs PySpark bronze → silver |
| `dbt/` | Projeto dbt (staging → marts) |
| `airflow/` | DAGs e plugins (apenas orquestração) |
| `docker/` | Dockerfiles customizados |
| `sql/init/` | Schema inicial do Postgres OLTP |
| `tests/` | Testes unitários e de integração |
| `docs/adr/` | Architecture Decision Records |
| `scripts/` | Scripts utilitários |
