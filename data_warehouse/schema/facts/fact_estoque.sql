DROP TABLE IF EXISTS analytics.fato_estoque CASCADE;

CREATE TABLE analytics.fato_estoque (
    sk_estoque SERIAL PRIMARY KEY,
    id_estoque_api INTEGER NOT NULL UNIQUE,
    id_tempo INTEGER NOT NULL REFERENCES analytics.dim_tempo(id_tempo),
    id_produto INTEGER NOT NULL REFERENCES analytics.dim_produto(sk_produto),
    id_loja INTEGER NOT NULL REFERENCES analytics.dim_loja(sk_loja),
    --quantidade_total INTEGER DEFAULT 0,
    quantidade_inicial INTEGER DEFAULT 0,
    quantidade_final INTEGER NOT NULL,
    entradas INTEGER DEFAULT 0,
    saidas INTEGER DEFAULT 0,
    valor_estoque_inicial NUMERIC(10,2) DEFAULT 0,
    valor_estoque_final NUMERIC(10,2) NOT NULL,
    data_carga TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_fato_estoque_tempo ON analytics.fato_estoque(id_tempo);
CREATE INDEX idx_fato_estoque_produto ON analytics.fato_estoque(id_produto);
CREATE INDEX idx_fato_estoque_loja ON analytics.fato_estoque(id_loja);
