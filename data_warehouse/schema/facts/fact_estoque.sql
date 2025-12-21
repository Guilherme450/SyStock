DROP TABLE IF EXISTS analytics.fato_estoque CASCADE;

CREATE TABLE analytics.fato_estoque (
    id_estoque SERIAL PRIMARY KEY,
    id_tempo INTEGER NOT NULL REFERENCES analytics.dim_tempo(id_tempo),
    id_produto INTEGER NOT NULL REFERENCES analytics.dim_produto(id_produto),
    id_loja INTEGER NOT NULL REFERENCES analytics.dim_loja(id_loja),
    quantidade_inicial INTEGER DEFAULT 0,
    quantidade_final INTEGER NOT NULL,
    entradas INTEGER DEFAULT 0,
    saidas INTEGER DEFAULT 0,
    valor_estoque_inicial NUMERIC(10,2) DEFAULT 0,
    valor_estoque_final NUMERIC(10,2) NOT NULL,
    custo_medio NUMERIC(10,2) NOT NULL,
    data_snapshot TIMESTAMP NOT NULL,
    data_carga TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(id_tempo, id_produto, id_loja)
);

CREATE INDEX idx_fato_estoque_tempo ON analytics.fato_estoque(id_tempo);
CREATE INDEX idx_fato_estoque_produto ON analytics.fato_estoque(id_produto);
CREATE INDEX idx_fato_estoque_loja ON analytics.fato_estoque(id_loja);
CREATE INDEX idx_fato_estoque_data ON analytics.fato_estoque(data_snapshot);

-- Popular fato estoque com window functions
-- INSERT INTO analytics.fato_estoque (
--     id_tempo, id_produto, id_loja,
--     quantidade_inicial, quantidade_final,
--     entradas, saidas,
--     valor_estoque_inicial, valor_estoque_final,
--     custo_medio, data_snapshot
-- )
-- SELECT 
--     t.id_tempo,
--     ps.product_id,
--     ps.store_id,
--     COALESCE(
--         LAG(ps.quantity) OVER (
--             PARTITION BY ps.product_id, ps.store_id 
--             ORDER BY ps.updated_at
--         ), 
--         0
--     ) AS quantidade_inicial,
--     ps.quantity AS quantidade_final,
--     CASE 
--         WHEN ps.quantity > COALESCE(
--             LAG(ps.quantity) OVER (
--                 PARTITION BY ps.product_id, ps.store_id 
--                 ORDER BY ps.updated_at
--             ), 
--             0
--         ) 
--         THEN ps.quantity - COALESCE(
--             LAG(ps.quantity) OVER (
--                 PARTITION BY ps.product_id, ps.store_id 
--                 ORDER BY ps.updated_at
--             ), 
--             0
--         )
--         ELSE 0
--     END AS entradas,
--     CASE 
--         WHEN ps.quantity < COALESCE(
--             LAG(ps.quantity) OVER (
--                 PARTITION BY ps.product_id, ps.store_id 
--                 ORDER BY ps.updated_at
--             ), 
--             0
--         )
--         THEN COALESCE(
--             LAG(ps.quantity) OVER (
--                 PARTITION BY ps.product_id, ps.store_id 
--                 ORDER BY ps.updated_at
--             ), 
--             0
--         ) - ps.quantity
--         ELSE 0
--     END AS saidas,
--     COALESCE(
--         LAG(ps.quantity) OVER (
--             PARTITION BY ps.product_id, ps.store_id 
--             ORDER BY ps.updated_at
--         ), 
--         0
--     ) * p.custo_fornecedor AS valor_estoque_inicial,
--     ps.quantity * p.custo_fornecedor AS valor_estoque_final,
--     p.custo_fornecedor AS custo_medio,
--     ps.updated_at AS data_snapshot
-- FROM product_store ps
-- INNER JOIN analytics.dim_produto p ON ps.product_id = p.id_produto
-- INNER JOIN analytics.dim_tempo t ON DATE(ps.updated_at) = t.data_completa;