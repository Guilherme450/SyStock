-- View: Vendas por Semana
CREATE OR REPLACE VIEW analytics.vw_vendas_semanais AS
WITH weekly AS (
    SELECT
        t.ano,
        t.semana,
        DATE_TRUNC('week', t.data_completa)::date AS inicio_semana,
        COUNT(DISTINCT fv.sk_venda) AS total_vendas,
        SUM(fv.quantidade) AS total_quantidade,
        SUM(fv.valor_total) AS valor_total_vendas,
        SUM(fv.custo_total) AS custo_total,
        SUM(fv.lucro) AS lucro_total,
        ROUND(AVG(fv.margem_lucro), 2) AS margem_lucro_media
    FROM analytics.fato_vendas fv
    INNER JOIN analytics.dim_tempo t ON fv.id_tempo = t.id_tempo
    GROUP BY t.ano, t.semana, DATE_TRUNC('week', t.data_completa)::date
)
SELECT
    w.ano,
    w.semana,
    w.inicio_semana,
    w.total_vendas,
    w.total_quantidade,
    w.valor_total_vendas,
    w.custo_total,
    w.lucro_total,
    w.margem_lucro_media,
    LAG(w.valor_total_vendas) OVER (ORDER BY w.ano, w.semana) AS valor_vendas_semana_anterior,
    LAG(w.lucro_total) OVER (ORDER BY w.ano, w.semana) AS lucro_semana_anterior
FROM weekly w
ORDER BY w.ano DESC, w.semana DESC;