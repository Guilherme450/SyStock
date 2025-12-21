-- View: Valor em Estoque Atual
CREATE OR REPLACE VIEW analytics.vw_valor_estoque_atual AS
SELECT 
    l.id_loja,
    l.nome_loja,
    p.id_produto,
    p.nome_produto,
    p.nome_categoria,
    fe.quantidade_final AS quantidade_estoque,
    fe.valor_estoque_final,
    p.preco_venda * fe.quantidade_final AS valor_potencial_venda,
    (p.preco_venda * fe.quantidade_final) - fe.valor_estoque_final AS lucro_potencial
FROM analytics.fato_estoque fe
INNER JOIN analytics.dim_produto p ON fe.id_produto = p.id_produto
INNER JOIN analytics.dim_loja l ON fe.id_loja = l.id_loja
INNER JOIN (
    SELECT id_produto, id_loja, MAX(data_snapshot) AS ultima_data
    FROM analytics.fato_estoque
    GROUP BY id_produto, id_loja
) ultimos ON fe.id_produto = ultimos.id_produto 
           AND fe.id_loja = ultimos.id_loja 
           AND fe.data_snapshot = ultimos.ultima_data
WHERE fe.quantidade_final > 0;