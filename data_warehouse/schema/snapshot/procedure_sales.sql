-- ==================================================
-- PROCEDURE 2: Carregar Vendas Incrementais
-- ==================================================
-- Propósito: Adicionar apenas vendas novas desde a última carga
-- Quando usar: Diariamente, após o fechamento do dia
-- Estratégia: Carrega apenas vendas com data > última data carregada

CREATE OR REPLACE FUNCTION analytics.sp_carregar_vendas_incrementais()
RETURNS TABLE(
    vendas_inseridas INTEGER,
    data_inicial TIMESTAMP,
    data_final TIMESTAMP
) AS $
DECLARE
    v_count INTEGER;
    v_data_inicial TIMESTAMP;
    v_data_final TIMESTAMP;
BEGIN
    -- Busca a última data de venda já carregada
    SELECT COALESCE(MAX(data_venda), '1900-01-01') 
    INTO v_data_inicial
    FROM analytics.fato_vendas;
    
    -- Insere apenas vendas NOVAS (incrementais)
    INSERT INTO analytics.fato_vendas (
        id_tempo, id_produto, id_loja, id_cliente,
        quantidade, valor_unitario, valor_total,
        custo_unitario, custo_total, lucro, margem_lucro,
        data_venda, status_venda
    )
    SELECT 
        t.id_tempo,
        si.product_id,
        s.store_id,
        s.client_id,
        si.quantity,
        si.unit_price,
        si.total_price,
        p.custo_fornecedor,
        p.custo_fornecedor * si.quantity AS custo_total,
        si.total_price - (p.custo_fornecedor * si.quantity) AS lucro,
        ROUND(
            ((si.total_price - (p.custo_fornecedor * si.quantity)) / NULLIF(si.total_price, 0)) * 100, 
            2
        ) AS margem_lucro,
        s.sale_data,
        s.status
    FROM sale_items si
    INNER JOIN sales s ON si.sale_id = s.id
    INNER JOIN analytics.dim_produto p ON si.product_id = p.id_produto
    INNER JOIN analytics.dim_tempo t ON DATE(s.sale_data) = t.data_completa
    WHERE si.removed_at IS NULL
      AND s.status IS NOT NULL
      AND s.sale_data > v_data_inicial;  -- FILTRO INCREMENTAL
    
    -- Conta quantas vendas foram inseridas
    GET DIAGNOSTICS v_count = ROW_COUNT;
    
    -- Busca a data final carregada
    SELECT MAX(data_venda) 
    INTO v_data_final
    FROM analytics.fato_vendas;
    
    -- Retorna estatísticas da carga
    RETURN QUERY SELECT v_count, v_data_inicial, v_data_final;
    
    RAISE NOTICE '% vendas carregadas de % até %', v_count, v_data_inicial, v_data_final;
END;
$ LANGUAGE plpgsql;

-- Exemplo de uso:
-- SELECT * FROM analytics.sp_carregar_vendas_incrementais();