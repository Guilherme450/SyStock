DROP TABLE IF EXISTS analytics.dim_cliente CASCADE;

CREATE TABLE analytics.dim_cliente (
    id_cliente INTEGER PRIMARY KEY,
    nome_cliente VARCHAR(255) NOT NULL,
    cpf_cnpj VARCHAR(14) NOT NULL UNIQUE,
    email VARCHAR(255),
    telefone VARCHAR(20),
    endereco TEXT,
    tipo_cliente VARCHAR(20),
    data_carga TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_dim_cliente_tipo ON analytics.dim_cliente(tipo_cliente);

--Popular dimensão cliente
-- INSERT INTO analytics.dim_cliente (
--      id_cliente, nome_cliente, cpf_cnpj, 
--      email, telefone, endereco, tipo_cliente
--  )
--  SELECT 
--      id,
--      name,
--      cpf_cnpj,
--      email,
--      phone,
--      address,
--      CASE 
--          WHEN LENGTH(cpf_cnpj) = 11 THEN 'Pessoa Física'
--          WHEN LENGTH(cpf_cnpj) = 14 THEN 'Pessoa Jurídica'
--          ELSE 'Não Classificado'
--      END AS tipo_cliente
--  FROM clients;