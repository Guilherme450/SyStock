DROP TABLE IF EXISTS analytics.fato_vendas CASCADE;

CREATE TABLE analytics.fato_vendas (
    sk_venda SERIAL PRIMARY KEY,
    id_venda_api INTEGER NOT NULL UNIQUE,
    id_tempo INTEGER NOT NULL REFERENCES analytics.dim_tempo(id_tempo),
    id_produto INTEGER NOT NULL REFERENCES analytics.dim_produto(sk_produto),
    id_loja INTEGER NOT NULL REFERENCES analytics.dim_loja(sk_loja),
    id_cliente INTEGER NOT NULL REFERENCES analytics.dim_cliente(sk_cliente),
    quantidade INTEGER NOT NULL, -- quantidade vendida de um produto
    valor_unitario NUMERIC(10,2) NOT NULL,
    valor_total NUMERIC(10,2) NOT NULL,
    custo_unitario NUMERIC(10,2) NOT NULL,
    custo_total NUMERIC(10,2) NOT NULL,
    lucro NUMERIC(10,2) NOT NULL,
    margem_lucro NUMERIC(5,2),
    --status_venda VARCHAR(50),
    data_carga TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Criar índices para otimização
CREATE INDEX idx_fato_vendas_tempo ON analytics.fato_vendas(id_tempo);
CREATE INDEX idx_fato_vendas_produto ON analytics.fato_vendas(id_produto);
CREATE INDEX idx_fato_vendas_loja ON analytics.fato_vendas(id_loja);
CREATE INDEX idx_fato_vendas_cliente ON analytics.fato_vendas(id_cliente);
--CREATE INDEX idx_fato_vendas_status ON analytics.fato_vendas(status_venda);
