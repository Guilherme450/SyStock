-- View: Evolução de Vendas (Crescimento)
CREATE OR REPLACE VIEW analytics.vw_evolucao_vendas AS
WITH agregados AS (
    SELECT
        t.ano,
        t.semana,
        DATE_TRUNC('week', t.data_completa)::date AS inicio_semana,
        SUM(fv.valor_total) AS valor_vendas,
        SUM(fv.lucro) AS lucro
    FROM analytics.fato_vendas fv
    INNER JOIN analytics.dim_tempo t ON fv.id_tempo = t.id_tempo
    GROUP BY t.ano, t.semana, DATE_TRUNC('week', t.data_completa)::date
)
SELECT
    a.ano,
    a.semana,
    a.inicio_semana,
    a.valor_vendas,
    a.lucro,
    LAG(a.valor_vendas) OVER (ORDER BY a.ano, a.semana) AS valor_vendas_semana_anterior,
    LAG(a.lucro) OVER (ORDER BY a.ano, a.semana) AS lucro_semana_anterior,
    ROUND(
        ((a.valor_vendas - LAG(a.valor_vendas) OVER (ORDER BY a.ano, a.semana)) /
         NULLIF(LAG(a.valor_vendas) OVER (ORDER BY a.ano, a.semana), 0)) * 100,
        2
    ) AS crescimento_percentual
FROM agregados a
ORDER BY a.ano DESC, a.semana DESC;