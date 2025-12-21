DROP TABLE IF EXISTS analytics.dim_produto CASCADE;

CREATE TABLE analytics.dim_produto (
    id_produto INTEGER PRIMARY KEY,
    nome_produto VARCHAR(255) NOT NULL,
    descricao_produto TEXT,
    --id_categoria INTEGER,
    nome_categoria VARCHAR(255),
    descricao_categoria TEXT,
    preco_venda NUMERIC(10,2),
    custo_fornecedor NUMERIC(10,2),
    ativo BOOLEAN DEFAULT TRUE,
    --margem_bruta NUMERIC(10,2),
    --percentual_margem NUMERIC(5,2),
    data_carga TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_dim_produto_categoria ON analytics.dim_produto(nome_categoria);
CREATE INDEX idx_dim_produto_ativo ON analytics.dim_produto(ativo);

-- Popular dimensão produto
-- INSERT INTO analytics.dim_produto (
--     id_produto, nome_produto, descricao_produto,
--     id_categoria, nome_categoria, descricao_categoria,
--     preco_venda, custo_fornecedor, ativo,
--     margem_bruta, percentual_margem
-- )
-- SELECT 
--     p.id,
--     p.name,
--     p.description,
--     p.category_id,
--     c.name AS categoria_nome,
--     c.description AS categoria_descricao,
--     p.sale_price,
--     p.cost_price,
--     p.active,
--     p.sale_price - p.cost_price AS margem_bruta,
--     ROUND(((p.sale_price - p.cost_price) / NULLIF(p.sale_price, 0)) * 100, 2) AS percentual_margem
-- FROM products p
-- LEFT JOIN categories c ON p.category_id = c.id;
