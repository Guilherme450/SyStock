-- View: Categorias Mais Vendidas
CREATE OR REPLACE VIEW analytics.vw_categorias_mais_vendidas AS
SELECT 
    p.nome_categoria,
    COUNT(DISTINCT fv.id_venda) AS total_vendas,
    SUM(fv.quantidade) AS quantidade_vendida,
    SUM(fv.valor_total) AS valor_total,
    SUM(fv.lucro) AS lucro_total
FROM analytics.fato_vendas fv
INNER JOIN analytics.dim_produto p ON fv.id_produto = p.id_produto
GROUP BY p.nome_categoria
ORDER BY quantidade_vendida DESC;