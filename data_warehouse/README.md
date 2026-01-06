# Data Warehouse

This folder contains the SQL schema and analytical objects used by the SyStock data warehouse. The implementation follows a classic star schema with a focus on fast analytical queries and a set of materialized analytical views to support reporting.

## Overview
- Schema: `analytics` (dimensions, facts, views)
- DBMS: PostgreSQL — tested against modern Postgres releases (12+). Use of SERIAL, numeric types and standard SQL window functions.
- Design: Star schema where business facts reference conformed dimensions.

## Structure
- `dims/` — dimension table DDL (customer, product, store, time). These tables hold descriptive attributes used to slice and filter measures.
- `facts/` — fact table DDL (sales, inventory snapshots, internal distributions). Facts contain measures and foreign keys to dimensions.
- `views/` — analytical views that aggregate facts into business-oriented metrics (weekly sales, top products, store performance, inventory value, etc.).

## Star Schema (concept)
- Dimensions: `dim_cliente`, `dim_produto`, `dim_loja`, `dim_tempo`.
- Facts: `fato_vendas`, `fato_estoque`, `fato_distribuicoes`.
- Grain: Facts are stored at a row-level appropriate for analytics (e.g., `fato_vendas` is one row per sold item). Use surrogate keys (`sk_*`) in joins for performance and stability.

## Database (PostgreSQL)
- Default schema: `analytics`.
- Indexes: Each table declares targeted indexes for common join/filter columns (time, product, store, client). Consider additional partitioning or composite indexes for large production volumes.
- Data types: numeric precision is chosen for currency (`NUMERIC(10,2)`). Adjust precision if your business requires different scale.

## Views — Emphasis and Usage
The views are the primary analytical interface. They encapsulate common business logic, window functions, and aggregations so analysts and BI tools work with ready-to-query datasets.

- `vw_vendas_semanais` — Weekly KPIs: total sales, quantity, cost, profit, average margin, plus previous-week comparisons. Use for dashboards showing trends and recent performance.
- `vw_evolucao_vendas` — Week-over-week evolution and percentage growth. Uses `LAG()` and percentage calculations; handle NULL/zero denominators in queries.
- `vw_performance_lojas` — Store-level rollups: total sales, profit, average margin. Useful for leaderboards and store comparisons.
- `vw_categorias_mais_vendidas` — Aggregated sales by product category (volume, value, profit). Use for assortment and category analysis.
- `vw_produtos_mais_vendidos` — Top-selling products by quantity/value/profit. Useful for merchandising and promotions.
- `vw_valor_estoque_atual` — Current stock value and potential sale value per product/store. This view selects latest inventory snapshots per product/store for an up-to-date perspective.

Notes on Views:
- Views join facts to `dim_tempo` for consistent date attributes — use `dim_tempo` as the canonical time dimension.
- Views rely on aggregates and window functions; if query performance becomes an issue, consider materialized views or scheduled refreshes.
- Always validate business rules (cost vs price calculations, margins) against source systems after ETL changes.

## Recommended queries
- Quick sample to get last 8 weeks of weekly sales:

```sql
SELECT * 
FROM analytics.vw_vendas_semanais
ORDER BY inicio_semana DESC
LIMIT 8;
```

- Top 10 products by units sold (last 30 days):

```sql
SELECT 
    p.sk_produto, 
    p.nome_produto, 
    SUM(fv.quantidade) AS units
FROM analytics.fato_vendas fv
JOIN analytics.dim_produto p 
    ON fv.id_produto = p.sk_produto
JOIN analytics.dim_tempo t 
    ON fv.id_tempo = t.id_tempo
WHERE t.data_completa >= current_date - INTERVAL '30 days'
GROUP BY p.sk_produto, p.nome_produto
ORDER BY units DESC
LIMIT 10;
```

## Operational notes
- ETL load timestamps (`data_carga`) exist on all tables — use for auditing incremental loads.
- Conformed dimensions: keep `id_*_api` fields to allow reconciliation with the source API.
- When adding columns or changing semantics, update both DDL and the views that derive metrics from them.

