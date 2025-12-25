DROP TABLE IF EXISTS analytics.dim_cliente CASCADE;

CREATE TABLE analytics.dim_cliente (
    sk_cliente SERIAL PRIMARY KEY,
    id_cliente_api INTEGER NOT NULL UNIQUE,
    nome_cliente VARCHAR(255) NOT NULL,
    cpf_cnpj VARCHAR(14) NOT NULL UNIQUE,
    email VARCHAR(255),
    telefone VARCHAR(20),
    endereco TEXT,
    tipo_cliente VARCHAR(20),
    data_carga TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_dim_cliente_tipo ON analytics.dim_cliente(tipo_cliente);

