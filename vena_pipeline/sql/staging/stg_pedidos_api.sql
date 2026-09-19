-- stg_pedidos_api.sql — pedidos da API, deduplicados e tipados.
--
-- ACHADOS NOS 48.000 REGISTROS:
--  * cliente_id e quantidade são NULL nos mesmos 245 registros (0,51%);
--  * valor_unitario: 47.756 números + 244 strings COM SUFIXO ("462.99 BRL");
--  * data_pedido: 47.765 ISO ("2025-04-25T00:54:38") + 235 "dd/mm/yyyy";
--  * pedido_id é único (o dedup por updated_at é defesa p/ pedido reenviado).
--
-- IDEMPOTENTE: MERGE por pedido_id; só atualiza se updated_at for mais novo.
-- Rodar 2x com o mesmo snapshot não altera nenhuma linha.
CREATE TABLE IF NOT EXISTS `{project}.{dataset}.stg_pedidos_api` (
  pedido_id INT64,
  cliente_id INT64,
  produto_id INT64,
  quantidade INT64,
  valor_unitario FLOAT64,
  data_pedido TIMESTAMP,
  updated_at TIMESTAMP,
  status STRING,
  dq_flag_cliente_nulo BOOL,
  dq_flag_quantidade_nula BOOL,
  dq_flag_data_invalida BOOL,
  dq_flag_valor_invalido BOOL
)
CLUSTER BY pedido_id;

MERGE `{project}.{dataset}.stg_pedidos_api` AS alvo
USING (
  WITH tipado AS (
    SELECT
      pedido_id, cliente_id, produto_id, quantidade,
      -- "462.99 BRL" / "151.3" -> mantém só dígitos, '.' e '-'
      SAFE_CAST(REGEXP_REPLACE(TRIM(valor_unitario), r'[^0-9.\-]', '') AS FLOAT64) AS valor_unitario,
      COALESCE(SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%S', data_pedido),
               SAFE.PARSE_TIMESTAMP('%d/%m/%Y', data_pedido),
               SAFE.PARSE_TIMESTAMP('%Y-%m-%d', data_pedido)) AS data_pedido,
      COALESCE(SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%S', updated_at),
               SAFE.PARSE_TIMESTAMP('%d/%m/%Y', updated_at)) AS updated_at,
      status
    FROM `{project}.{dataset}.raw_pedidos_api`
  )
  SELECT *,
    (cliente_id IS NULL) AS dq_flag_cliente_nulo,
    (quantidade IS NULL) AS dq_flag_quantidade_nula,
    (data_pedido IS NULL) AS dq_flag_data_invalida,   -- sobre o valor JÁ parseado
    (valor_unitario IS NULL) AS dq_flag_valor_invalido
  FROM tipado
  QUALIFY ROW_NUMBER() OVER (PARTITION BY pedido_id ORDER BY updated_at DESC) = 1
) AS origem
ON alvo.pedido_id = origem.pedido_id
WHEN MATCHED AND origem.updated_at > alvo.updated_at THEN UPDATE SET
  cliente_id = origem.cliente_id, produto_id = origem.produto_id, quantidade = origem.quantidade,
  valor_unitario = origem.valor_unitario, data_pedido = origem.data_pedido,
  updated_at = origem.updated_at, status = origem.status,
  dq_flag_cliente_nulo = origem.dq_flag_cliente_nulo,
  dq_flag_quantidade_nula = origem.dq_flag_quantidade_nula,
  dq_flag_data_invalida = origem.dq_flag_data_invalida,
  dq_flag_valor_invalido = origem.dq_flag_valor_invalido
WHEN NOT MATCHED THEN INSERT (
  pedido_id, cliente_id, produto_id, quantidade, valor_unitario, data_pedido, updated_at, status,
  dq_flag_cliente_nulo, dq_flag_quantidade_nula, dq_flag_data_invalida, dq_flag_valor_invalido
) VALUES (
  origem.pedido_id, origem.cliente_id, origem.produto_id, origem.quantidade, origem.valor_unitario,
  origem.data_pedido, origem.updated_at, origem.status,
  origem.dq_flag_cliente_nulo, origem.dq_flag_quantidade_nula,
  origem.dq_flag_data_invalida, origem.dq_flag_valor_invalido
);
