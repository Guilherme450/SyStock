# Data Warehouse Schema Documentation

This document describes dimension tables (`dim_*`), fact tables (`fato_*`) and analytical views (`vw_*`) defined under the `analytics` schema. Entries follow a consistent convention: Purpose, Grain, Primary/Business keys, Columns (name, type, short description), Indexes, and Notes.

---

**Dimension: analytics.dim_cliente**
- **Purpose:** Master customer information for reporting and joins with sales.
- **Grain:** One row per customer (as provided by source API).
- **Primary Key:** `sk_cliente` (surrogate, SERIAL)
- **Business Key / Natural Key:** `id_cliente_api` (source id), `cpf_cnpj` (unique document)
- **Columns:**
  - `sk_cliente` : SERIAL — surrogate primary key
  - `id_cliente_api` : INTEGER — source/customer id (unique)
  - `nome_cliente` : VARCHAR(255) — customer full name
  - `cpf_cnpj` : VARCHAR(14) — document identifier (CPF or CNPJ)
  - `email` : VARCHAR(255) — contact email
  - `telefone` : VARCHAR(20) — contact phone
  - `endereco` : TEXT — free-form address
  - `tipo_cliente` : VARCHAR(20) — customer type (e.g., individual/company)
  - `data_carga` : TIMESTAMP — ETL load timestamp
- **Indexes:** `idx_dim_cliente_tipo` on `tipo_cliente`
- **Notes:** `id_cliente_api` and `cpf_cnpj` are enforced as unique. Use `sk_cliente` in fact tables for referential integrity.

---

**Dimension: analytics.dim_loja**
- **Purpose:** Store information about physical stores / locations.
- **Grain:** One row per store.
- **Primary Key:** `sk_loja` (surrogate, SERIAL)
- **Business Key:** `id_loja_api` (source id)
- **Columns:**
  - `sk_loja` : SERIAL — surrogate primary key
  - `id_loja_api` : INTEGER — source/store id (unique)
  - `nome_loja` : VARCHAR(255) — store name
  - `endereco_loja` : TEXT — store address
  - `data_carga` : TIMESTAMP — ETL load timestamp
- **Indexes:** `idx_dim_loja_nome_loja` on `nome_loja`
- **Notes:** Use `sk_loja` as FK in facts. Keep `id_loja_api` for reconciliation with source systems.

---

**Dimension: analytics.dim_produto**
- **Purpose:** Product master and category attributes for product-level analytics.
- **Grain:** One row per product.
- **Primary Key:** `sk_produto` (surrogate, SERIAL)
- **Business Key:** `id_produto_api` (source id)
- **Columns:**
  - `sk_produto` : SERIAL — surrogate primary key
  - `id_produto_api` : INTEGER — source/product id (unique)
  - `nome_produto` : VARCHAR(255) — product name
  - `nome_categoria` : VARCHAR(255) — product category name
  - `descricao_categoria` : TEXT — category description
  - `preco_venda` : NUMERIC(10,2) — retail price
  - `custo_fornecedor` : NUMERIC(10,2) — supplier cost
  - `ativo` : BOOLEAN — active flag
  - `data_carga` : TIMESTAMP — ETL load timestamp
- **Indexes:** `idx_dim_produto_categoria` on `nome_categoria`, `idx_dim_produto_ativo` on `ativo`
- **Notes:** Facts should reference `sk_produto` where possible. `id_produto_api` preserves source mapping.

---

**Dimension: analytics.dim_tempo**
- **Purpose:** Date/time dimension for time-based aggregations.
- **Grain:** One row per calendar date (`data_completa`).
- **Primary Key / Business Key:** `id_tempo` (SERIAL) and `data_completa` (unique)
- **Columns:**
  - `id_tempo` : SERIAL — surrogate primary key
  - `data_completa` : DATE — calendar date (unique)
  - `ano` : INTEGER — year
  - `trimestre` : INTEGER — quarter
  - `mes` : INTEGER — month
  - `semana` : INTEGER — ISO week number
  - `dia` : INTEGER — day of month
  - `dia_semana` : VARCHAR(20) — weekday name
  - `eh_fim_semana` : BOOLEAN — weekend flag
- **Indexes:** `idx_dim_tempo_data`, `idx_dim_tempo_ano_mes`, `idx_dim_tempo_ano_semana`
- **Notes:** This table is used by all time-based facts. Ensure full coverage of dates loaded by pipeline.

---

**Fact: analytics.fato_vendas**
- **Purpose:** Transactional sales fact that records sold items.
- **Grain:** One row per sold item (unique by `id_venda_api` + `id_produto`).
- **Primary Key:** `sk_venda` (surrogate, SERIAL)
- **Natural/Business Keys & FKs:**
  - `id_venda_api` : source sale id
  - `id_tempo` REFERENCES `analytics.dim_tempo(id_tempo)`
  - `id_produto` REFERENCES `analytics.dim_produto(id_produto_api)` (note: some references use `sk_produto` elsewhere)
  - `id_loja` REFERENCES `analytics.dim_loja(sk_loja)`
  - `id_cliente` REFERENCES `analytics.dim_cliente(sk_cliente)`
- **Measures / Columns:**
  - `quantidade` : INTEGER — units sold
  - `valor_unitario` : NUMERIC(10,2) — sale unit price
  - `valor_total` : NUMERIC(10,2) — sale line total
  - `custo_unitario` : NUMERIC(10,2) — unit cost
  - `custo_total` : NUMERIC(10,2) — cost for the line
  - `lucro` : NUMERIC(10,2) — profit (valor_total - custo_total)
  - `margem_lucro` : NUMERIC(5,2) — profit margin percentage (nullable)
  - `data_carga` : TIMESTAMP — ETL load timestamp
- **Indexes:** idx_fato_vendas_tempo, idx_fato_vendas_produto, idx_fato_vendas_loja, idx_fato_vendas_cliente
- **Constraints:** Unique constraint `uq_item_vendas (id_venda_api, id_produto)` prevents duplicate sale-item rows.
- **Notes:** Use `id_tempo` to join with `dim_tempo` for date attributes. Verify `id_produto` FK consistency (some places reference `id_produto_api` vs `sk_produto`).

---

**Fact: analytics.fato_estoque**
- **Purpose:** Periodic inventory snapshot per product and store.
- **Grain:** One row per product-store-date (represented by `id_tempo`).
- **Primary Key:** `sk_estoque` (surrogate, SERIAL)
- **Business Key / FKs:**
  - `id_estoque_api` : source inventory id (unique)
  - `id_tempo` REFERENCES `analytics.dim_tempo(id_tempo)`
  - `id_produto` REFERENCES `analytics.dim_produto(sk_produto)`
  - `id_loja` REFERENCES `analytics.dim_loja(sk_loja)`
- **Measures / Columns:**
  - `quantidade_inicial` : INTEGER — opening quantity
  - `quantidade_final` : INTEGER — closing quantity
  - `entradas` : INTEGER — positive movements in period
  - `saidas` : INTEGER — negative movements in period
  - `valor_estoque_inicial` : NUMERIC(10,2) — opening value
  - `valor_estoque_final` : NUMERIC(10,2) — closing value
  - `data_carga` : TIMESTAMP — ETL load timestamp
- **Indexes:** idx_fato_estoque_tempo, idx_fato_estoque_produto, idx_fato_estoque_loja
- **Notes:** Views rely on the latest `id_tempo` per product/store to compute current stock value.

---

**Fact: analytics.fato_distribuicoes**
- **Purpose:** Records internal product transfers between stores.
- **Grain:** One row per distribution item (unique by `id_distribuicao_api` + product).
- **Primary Key:** `sk_distribuicao` (surrogate, SERIAL)
- **Business Key / FKs:**
  - `id_distribuicao_api` : source distribution id
  - `id_tempo` REFERENCES `analytics.dim_tempo(id_tempo)`
  - `id_produto` REFERENCES `analytics.dim_produto(id_produto_api)`
  - `id_loja_origem` REFERENCES `analytics.dim_loja(sk_loja)`
  - `id_loja_destino` REFERENCES `analytics.dim_loja(sk_loja)`
- **Measures / Columns:**
  - `quantidade` : INTEGER — units transferred
  - `status` : VARCHAR(50) — distribution status
  - `data_carga` : TIMESTAMP — ETL load timestamp
- **Indexes:** idx_fato_dist_tempo, idx_fato_dist_produto, idx_fato_dist_origem, idx_fato_dist_destino
- **Constraints:** Unique constraint `uq_item_distribuicao (id_distribuicao_api, id_produto)`.

---

## Views (analytics)
Each view exposes aggregated or derived metrics built on the facts/dims. Columns listed are significant output fields; consult the SQL view definitions for exact column types and derivation logic.

**View: analytics.vw_evolucao_vendas**
- **Purpose:** Week-over-week sales evolution and percentage growth.
- **Key output columns:** `ano`, `semana`, `inicio_semana`, `valor_vendas`, `lucro`, `valor_vendas_semana_anterior`, `crescimento_percentual`
- **Sources:** `analytics.fato_vendas` joined with `analytics.dim_tempo`.
- **Notes:** Uses window functions (`LAG`) and group by week to calculate growth percentage; be careful with NULL or zero denominators.

**View: analytics.vw_vendas_semanais**
- **Purpose:** Weekly sales KPIs (total sales, quantity, cost, profit, average margin).
- **Key output columns:** `ano`, `semana`, `inicio_semana`, `total_vendas`, `total_quantidade`, `valor_total_vendas`, `custo_total`, `lucro_total`, `margem_lucro_media`, `valor_vendas_semana_anterior`
- **Sources:** `analytics.fato_vendas` joined with `analytics.dim_tempo`.

**View: analytics.vw_performance_lojas**
- **Purpose:** Store-level performance metrics (total sales, profit, average margin).
- **Key output columns:** `sk_loja`, `nome_loja`, `total_vendas`, `valor_total_vendas`, `lucro_total`, `margem_media`
- **Sources:** `analytics.fato_vendas` joined with `analytics.dim_loja`.

**View: analytics.vw_categorias_mais_vendidas**
- **Purpose:** Sales aggregated by product category (volume, value, profit).
- **Key output columns:** `nome_categoria`, `total_vendas`, `quantidade_vendida`, `valor_total`, `lucro_total`
- **Sources:** `analytics.fato_vendas` joined with `analytics.dim_produto`.

**View: analytics.vw_produtos_mais_vendidos**
- **Purpose:** Top-selling products by units sold, value and profit.
- **Key output columns:** `sk_produto`, `nome_produto`, `nome_categoria`, `quantidade_vendida`, `valor_total`, `lucro_total`, `numero_vendas`
- **Sources:** `analytics.fato_vendas` joined with `analytics.dim_produto`.

**View: analytics.vw_valor_estoque_atual**
- **Purpose:** Current inventory value per store/product and potential sale value + potential profit.
- **Key output columns:** `sk_loja`, `nome_loja`, `sk_produto`, `nome_produto`, `nome_categoria`, `quantidade_estoque`, `valor_estoque_final`, `valor_potencial_venda`, `lucro_potencial`
- **Sources:** `analytics.fato_estoque`, `analytics.dim_produto`, `analytics.dim_loja`.
- **Notes:** The view selects the latest `id_tempo` per product/store to represent current inventory.


