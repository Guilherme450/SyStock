DROP TABLE IF EXISTS analytics.fato_vendas CASCADE;

CREATE TABLE analytics.fato_vendas (
    id_venda SERIAL PRIMARY KEY,
    id_tempo INTEGER NOT NULL REFERENCES analytics.dim_tempo(id_tempo),
    id_produto INTEGER NOT NULL REFERENCES analytics.dim_produto(id_produto),
    id_loja INTEGER NOT NULL REFERENCES analytics.dim_loja(id_loja),
    id_cliente INTEGER NOT NULL REFERENCES analytics.dim_cliente(id_cliente),
    quantidade INTEGER NOT NULL,
    valor_unitario NUMERIC(10,2) NOT NULL,
    valor_total NUMERIC(10,2) NOT NULL,
    custo_unitario NUMERIC(10,2) NOT NULL,
    custo_total NUMERIC(10,2) NOT NULL,
    lucro NUMERIC(10,2) NOT NULL,
    margem_lucro NUMERIC(5,2),
    data_venda TIMESTAMP NOT NULL,
    status_venda VARCHAR(50),
    data_carga TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Criar índices para otimização
CREATE INDEX idx_fato_vendas_tempo ON analytics.fato_vendas(id_tempo);
CREATE INDEX idx_fato_vendas_produto ON analytics.fato_vendas(id_produto);
CREATE INDEX idx_fato_vendas_loja ON analytics.fato_vendas(id_loja);
CREATE INDEX idx_fato_vendas_cliente ON analytics.fato_vendas(id_cliente);
CREATE INDEX idx_fato_vendas_data ON analytics.fato_vendas(data_venda);
CREATE INDEX idx_fato_vendas_status ON analytics.fato_vendas(status_venda);

-- Popular fato vendas
-- INSERT INTO analytics.fato_vendas (
--     id_tempo, id_produto, id_loja, id_cliente,
--     quantidade, valor_unitario, valor_total,
--     custo_unitario, custo_total, lucro, margem_lucro,
--     data_venda, status_venda
-- )
-- SELECT 
--     t.id_tempo,
--     si.product_id,
--     s.store_id,
--     s.client_id,
--     si.quantity,
--     si.unit_price,
--     si.total_price,
--     p.custo_fornecedor,
--     p.custo_fornecedor * si.quantity AS custo_total,
--     si.total_price - (p.custo_fornecedor * si.quantity) AS lucro,
--     ROUND(
--         ((si.total_price - (p.custo_fornecedor * si.quantity)) / NULLIF(si.total_price, 0)) * 100, 
--         2
--     ) AS margem_lucro,
--     s.sale_data,
--     s.status
-- FROM sale_items si
-- INNER JOIN sales s ON si.sale_id = s.id
-- INNER JOIN analytics.dim_produto p ON si.product_id = p.id_produto
-- INNER JOIN analytics.dim_tempo t ON DATE(s.sale_data) = t.data_completa
-- WHERE si.removed_at IS NULL
--   AND s.status IS NOT NULL;