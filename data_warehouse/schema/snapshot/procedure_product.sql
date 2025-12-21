-- ==================================================
-- PROCEDURE 1: Atualizar Dimensão Produto (SCD Type 1)
-- ==================================================
-- Propósito: Sincronizar mudanças nas tabelas de produtos e categorias
-- Quando usar: Diariamente ou quando houver alterações de preços/categorias
-- Tipo: SCD Type 1 (sobrescreve dados anteriores)

CREATE OR REPLACE FUNCTION analytics.sp_atualizar_dim_produto()
RETURNS void AS $
BEGIN
    -- UPSERT: Insert ou Update se já existir
    INSERT INTO analytics.dim_produto (
        id_produto, nome_produto, descricao_produto,
        id_categoria, nome_categoria, descricao_categoria,
        preco_venda, custo_fornecedor, ativo,
        margem_bruta, percentual_margem
    )
    SELECT 
        p.id,
        p.name,
        p.description,
        p.category_id,
        c.name AS categoria_nome,
        c.description AS categoria_descricao,
        p.sale_price,
        p.cost_price,
        p.active,
        p.sale_price - p.cost_price AS margem_bruta,
        ROUND(((p.sale_price - p.cost_price) / NULLIF(p.sale_price, 0)) * 100, 2) AS percentual_margem
    FROM products p
    LEFT JOIN categories c ON p.category_id = c.id
    
    -- ON CONFLICT: Se produto já existe, atualiza os dados
    ON CONFLICT (id_produto) 
    DO UPDATE SET
        nome_produto = EXCLUDED.nome_produto,
        descricao_produto = EXCLUDED.descricao_produto,
        id_categoria = EXCLUDED.id_categoria,
        nome_categoria = EXCLUDED.nome_categoria,
        descricao_categoria = EXCLUDED.descricao_categoria,
        preco_venda = EXCLUDED.preco_venda,
        custo_fornecedor = EXCLUDED.custo_fornecedor,
        ativo = EXCLUDED.ativo,
        margem_bruta = EXCLUDED.margem_bruta,
        percentual_margem = EXCLUDED.percentual_margem,
        data_carga = CURRENT_TIMESTAMP;
    
    -- Log de execução
    RAISE NOTICE 'Dimensão Produto atualizada com sucesso em %', CURRENT_TIMESTAMP;
END;
$ LANGUAGE plpgsql;

-- Exemplo de uso:
-- SELECT analytics.sp_atualizar_dim_produto();