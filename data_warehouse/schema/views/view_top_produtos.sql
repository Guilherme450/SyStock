-- View: Top Produtos Mais Vendidos
CREATE OR REPLACE VIEW analytics.vw_produtos_mais_vendidos AS
SELECT 
    p.sk_produto,
    p.nome_produto,
    p.nome_categoria,
    SUM(fv.quantidade) AS quantidade_vendida,
    SUM(fv.valor_total) AS valor_total,
    SUM(fv.lucro) AS lucro_total,
    COUNT(DISTINCT fv.sk_venda) AS numero_vendas
FROM analytics.fato_vendas fv
INNER JOIN analytics.dim_produto p ON fv.id_produto = p.sk_produto
GROUP BY p.sk_produto, p.nome_produto, p.nome_categoria
ORDER BY quantidade_vendida DESC;