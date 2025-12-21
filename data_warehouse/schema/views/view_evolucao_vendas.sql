-- View: Evolução de Vendas (Crescimento)
CREATE OR REPLACE VIEW analytics.vw_evolucao_vendas AS
SELECT 
    t.ano,
    t.semana,
    t.inicio_semana,
    SUM(fv.valor_total) AS valor_vendas,
    SUM(fv.lucro) AS lucro,
    LAG(SUM(fv.valor_total)) OVER (ORDER BY t.ano, t.semana) AS valor_vendas_semana_anterior,
    LAG(SUM(fv.lucro)) OVER (ORDER BY t.ano, t.semana) AS lucro_semana_anterior,
    ROUND(
        ((SUM(fv.valor_total) - LAG(SUM(fv.valor_total)) OVER (ORDER BY t.ano, t.semana)) / 
        NULLIF(LAG(SUM(fv.valor_total)) OVER (ORDER BY t.ano, t.semana), 0)) * 100,
        2
    ) AS crescimento_percentual
FROM analytics.fato_vendas fv
INNER JOIN analytics.dim_tempo t ON fv.id_tempo = t.id_tempo
GROUP BY t.ano, t.semana, t.inicio_semana
ORDER BY t.ano DESC, t.semana DESC;