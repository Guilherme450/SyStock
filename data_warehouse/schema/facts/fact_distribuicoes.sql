DROP TABLE IF EXISTS analytics.fato_distribuicoes CASCADE;

CREATE TABLE analytics.fato_distribuicoes (
    id_distribuicao SERIAL PRIMARY KEY,
    id_tempo INTEGER NOT NULL REFERENCES analytics.dim_tempo(id_tempo),
    id_produto INTEGER NOT NULL REFERENCES analytics.dim_produto(id_produto),
    id_loja_origem INTEGER NOT NULL REFERENCES analytics.dim_loja(id_loja),
    id_loja_destino INTEGER NOT NULL REFERENCES analytics.dim_loja(id_loja),
    quantidade INTEGER NOT NULL,
    data_distribuicao TIMESTAMP NOT NULL,
    data_registro TIMESTAMP NOT NULL,
    status VARCHAR(50),
    data_carga TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_fato_dist_tempo ON analytics.fato_distribuicoes(id_tempo);
CREATE INDEX idx_fato_dist_produto ON analytics.fato_distribuicoes(id_produto);
CREATE INDEX idx_fato_dist_origem ON analytics.fato_distribuicoes(id_loja_origem);
CREATE INDEX idx_fato_dist_destino ON analytics.fato_distribuicoes(id_loja_destino);
CREATE INDEX idx_fato_dist_data ON analytics.fato_distribuicoes(data_distribuicao);

-- Popular fato distribuições
-- INSERT INTO analytics.fato_distribuicoes (
--     id_tempo, id_produto, id_loja_origem, id_loja_destino,
--     quantidade, data_distribuicao, data_registro, status
-- )
-- SELECT 
--     t.id_tempo,
--     idi.product_id,
--     id.from_store_id,
--     id.to_store_id,
--     idi.quantity,
--     id.distribution_date,
--     id.registered_at,
--     id.status
-- FROM internal_distributions id
-- INNER JOIN internal_distribution_items idi ON id.id = idi.internal_distribution_id
-- INNER JOIN analytics.dim_tempo t ON DATE(id.distribution_date) = t.data_completa;