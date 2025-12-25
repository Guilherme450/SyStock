-- View: Valor em Estoque Atual
CREATE OR REPLACE VIEW analytics.vw_valor_estoque_atual AS
SELECT 
    l.sk_loja,
    l.nome_loja,
    p.sk_produto,
    p.nome_produto,
    p.nome_categoria,
    fe.quantidade_final AS quantidade_estoque,
    fe.valor_estoque_final,
    p.preco_venda * fe.quantidade_final AS valor_potencial_venda,
    (p.preco_venda * fe.quantidade_final) - fe.valor_estoque_final AS lucro_potencial
FROM analytics.fato_estoque fe
INNER JOIN analytics.dim_produto p ON fe.id_produto = p.sk_produto
INNER JOIN analytics.dim_loja l ON fe.id_loja = l.sk_loja
INNER JOIN (
    -- usar o último id_tempo por produto/loja para garantir consistência com dim_tempo
    SELECT id_produto, id_loja, MAX(id_tempo) AS ultimo_id_tempo
    FROM analytics.fato_estoque
    GROUP BY id_produto, id_loja
) ultimos ON fe.id_produto = ultimos.id_produto 
           AND fe.id_loja = ultimos.id_loja 
           AND fe.id_tempo = ultimos.ultimo_id_tempo
WHERE fe.quantidade_final > 0;