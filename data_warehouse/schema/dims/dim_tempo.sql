DROP TABLE IF EXISTS analytics.dim_tempo CASCADE;

CREATE TABLE analytics.dim_tempo (
    id_tempo SERIAL PRIMARY KEY,
    data_completa DATE NOT NULL UNIQUE,
    ano INTEGER NOT NULL,
    trimestre INTEGER NOT NULL,
    mes INTEGER NOT NULL,
    semana INTEGER NOT NULL,
    dia INTEGER NOT NULL,
    dia_semana VARCHAR(20) NOT NULL,
    --nome_mes VARCHAR(20) NOT NULL,
    eh_fim_semana BOOLEAN NOT NULL
    --inicio_semana DATE NOT NULL,
    --inicio_mes DATE NOT NULL,
    --inicio_trimestre DATE NOT NULL,
    --inicio_ano DATE NOT NULL
);

-- Criar índices
CREATE INDEX idx_dim_tempo_data ON analytics.dim_tempo(data_completa);
CREATE INDEX idx_dim_tempo_ano_mes ON analytics.dim_tempo(ano, mes);
CREATE INDEX idx_dim_tempo_ano_semana ON analytics.dim_tempo(ano, semana);

-- Popular dimensão tempo (2020 a 2030)
-- INSERT INTO analytics.dim_tempo (
--     data_completa, ano, trimestre, mes, semana, dia, 
--     dia_semana, nome_mes, eh_fim_semana,
--     inicio_semana, inicio_mes, inicio_trimestre, inicio_ano
-- )
-- SELECT 
--     datum AS data_completa,
--     EXTRACT(YEAR FROM datum) AS ano,
--     EXTRACT(QUARTER FROM datum) AS trimestre,
--     EXTRACT(MONTH FROM datum) AS mes,
--     EXTRACT(WEEK FROM datum) AS semana,
--     EXTRACT(DAY FROM datum) AS dia,
--     TO_CHAR(datum, 'Day') AS dia_semana,
--     TO_CHAR(datum, 'Month') AS nome_mes,
--     CASE WHEN EXTRACT(DOW FROM datum) IN (0, 6) THEN TRUE ELSE FALSE END AS eh_fim_semana,
--     DATE_TRUNC('week', datum)::DATE AS inicio_semana,
--     DATE_TRUNC('month', datum)::DATE AS inicio_mes,
--     DATE_TRUNC('quarter', datum)::DATE AS inicio_trimestre,
--     DATE_TRUNC('year', datum)::DATE AS inicio_ano
-- FROM (
--     SELECT '2020-01-01'::DATE + SEQUENCE.day AS datum
--     FROM GENERATE_SERIES(0, 3652) AS SEQUENCE(day)
-- ) dates;