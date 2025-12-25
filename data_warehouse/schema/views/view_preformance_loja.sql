-- View: Performance por Loja
CREATE OR REPLACE VIEW analytics.vw_performance_lojas AS
SELECT 
    l.sk_loja,
    l.nome_loja,
    COUNT(DISTINCT fv.sk_venda) AS total_vendas,
    SUM(fv.valor_total) AS valor_total_vendas,
    SUM(fv.lucro) AS lucro_total,
    ROUND(AVG(fv.margem_lucro), 2) AS margem_media
FROM analytics.fato_vendas fv
INNER JOIN analytics.dim_loja l ON fv.id_loja = l.sk_loja
GROUP BY l.sk_loja, l.nome_loja
ORDER BY valor_total_vendas DESC;