DROP TABLE IF EXISTS analytics.fato_distribuicoes CASCADE;

CREATE TABLE analytics.fato_distribuicoes (
    sk_distribuicao SERIAL PRIMARY KEY,
    id_distribuicao_api INTEGER NOT NULL UNIQUE,
    id_tempo INTEGER NOT NULL REFERENCES analytics.dim_tempo(id_tempo),
    id_produto INTEGER NOT NULL REFERENCES analytics.dim_produto(sk_produto),
    id_loja_origem INTEGER NOT NULL REFERENCES analytics.dim_loja(sk_loja),
    id_loja_destino INTEGER NOT NULL REFERENCES analytics.dim_loja(sk_loja),
    quantidade INTEGER NOT NULL,
    --data_distribuicao TIMESTAMP NOT NULL,
    --data_registro TIMESTAMP NOT NULL,
    status VARCHAR(50),
    data_carga TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_fato_dist_tempo ON analytics.fato_distribuicoes(id_tempo);
CREATE INDEX idx_fato_dist_produto ON analytics.fato_distribuicoes(id_produto);
CREATE INDEX idx_fato_dist_origem ON analytics.fato_distribuicoes(id_loja_origem);
CREATE INDEX idx_fato_dist_destino ON analytics.fato_distribuicoes(id_loja_destino);
CREATE INDEX idx_fato_dist_data ON analytics.fato_distribuicoes(sk_distribuicao);
