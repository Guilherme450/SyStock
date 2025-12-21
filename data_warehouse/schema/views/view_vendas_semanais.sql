-- View: Vendas por Semana
CREATE OR REPLACE VIEW analytics.vw_vendas_semanais AS
SELECT 
    t.ano,
    t.semana,
    t.inicio_semana,
    COUNT(DISTINCT fv.id_venda) AS total_vendas,
    SUM(fv.quantidade) AS total_quantidade,
    SUM(fv.valor_total) AS valor_total_vendas,
    SUM(fv.custo_total) AS custo_total,
    SUM(fv.lucro) AS lucro_total,
    ROUND(AVG(fv.margem_lucro), 2) AS margem_lucro_media
FROM analytics.fato_vendas fv
INNER JOIN analytics.dim_tempo t ON fv.id_tempo = t.id_tempo
GROUP BY t.ano, t.semana, t.inicio_semana
ORDER BY t.ano DESC, t.semana DESC;