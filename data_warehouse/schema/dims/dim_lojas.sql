DROP TABLE IF EXISTS analytics.dim_loja CASCADE;

CREATE TABLE analytics.dim_loja (
    id_loja INTEGER PRIMARY KEY,
    nome_loja VARCHAR(255) NOT NULL,
    endereco_loja TEXT,
    fornecedor_nome VARCHAR(255),
    fornecedor_cnpj VARCHAR(14),
    data_carga TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_dim_loja_nome_loja ON analytics.dim_loja(nome_loja);

-- -- Popular dimensão loja
-- INSERT INTO analytics.dim_loja (
--     id_loja, nome_loja, endereco_loja, fornecedor_nome, fornecedor_cnpj
-- )
-- SELECT 
--     st.id,
--     st.name,
--     st.address,
--     s.name AS fornecedor_nome,
--     s.cnpj
-- FROM stores st
-- LEFT JOIN suppliers s ON st.id = s.id;