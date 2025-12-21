-- ==================================================
-- PROCEDURE 3: Snapshot Diário de Estoque
-- ==================================================
-- Propósito: Criar snapshot do estoque atual para análise histórica
-- Quando usar: Todo dia no fechamento (ex: 23:59)

CREATE OR REPLACE FUNCTION analytics.sp_snapshot_estoque_diario()
RETURNS void AS $
DECLARE
    v_data_hoje DATE := CURRENT_DATE;
BEGIN
    -- Verifica se já existe snapshot para hoje
    IF EXISTS (
        SELECT 1 
        FROM analytics.fato_estoque 
        WHERE DATE(data_snapshot) = v_data_hoje
    ) THEN
        RAISE NOTICE 'Snapshot já existe para %', v_data_hoje;
        RETURN;
    END IF;
    
    -- Cria snapshot do estoque atual
    INSERT INTO analytics.fato_estoque (
        id_tempo, id_produto, id_loja,
        quantidade_inicial, quantidade_final,
        entradas, saidas,
        valor_estoque_inicial, valor_estoque_final,
        custo_medio, data_snapshot
    )
    SELECT 
        t.id_tempo,
        ps.product_id,
        ps.store_id,
        -- Quantidade do dia anterior
        COALESCE(
            (SELECT quantidade_final 
             FROM analytics.fato_estoque fe 
             WHERE fe.id_produto = ps.product_id 
               AND fe.id_loja = ps.store_id 
             ORDER BY data_snapshot DESC 
             LIMIT 1),
            0
        ) AS quantidade_inicial,
        ps.quantity AS quantidade_final,
        -- Calcula entradas do dia
        GREATEST(
            ps.quantity - COALESCE(
                (SELECT quantidade_final 
                 FROM analytics.fato_estoque fe 
                 WHERE fe.id_produto = ps.product_id 
                   AND fe.id_loja = ps.store_id 
                 ORDER BY data_snapshot DESC 
                 LIMIT 1),
                0
            ),
            0
        ) AS entradas,
        -- Calcula saídas do dia
        GREATEST(
            COALESCE(
                (SELECT quantidade_final 
                 FROM analytics.fato_estoque fe 
                 WHERE fe.id_produto = ps.product_id 
                   AND fe.id_loja = ps.store_id 
                 ORDER BY data_snapshot DESC 
                 LIMIT 1),
                0
            ) - ps.quantity,
            0
        ) AS saidas,
        -- Valores
        COALESCE(
            (SELECT valor_estoque_final 
             FROM analytics.fato_estoque fe 
             WHERE fe.id_produto = ps.product_id 
               AND fe.id_loja = ps.store_id 
             ORDER BY data_snapshot DESC 
             LIMIT 1),
            0
        ) AS valor_estoque_inicial,
        ps.quantity * p.custo_fornecedor AS valor_estoque_final,
        p.custo_fornecedor AS custo_medio,
        CURRENT_TIMESTAMP AS data_snapshot
    FROM product_store ps
    INNER JOIN analytics.dim_produto p ON ps.product_id = p.id_produto
    INNER JOIN analytics.dim_tempo t ON t.data_completa = v_data_hoje;
    
    RAISE NOTICE 'Snapshot de estoque criado para %', v_data_hoje;
END;
$ LANGUAGE plpgsql;

-- Exemplo de uso:
-- SELECT analytics.sp_snapshot_estoque_diario();