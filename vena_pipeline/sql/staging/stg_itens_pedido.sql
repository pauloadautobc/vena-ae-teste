-- stg_itens_pedido.sql — 5.000.000 itens tipados, com flags de integridade referencial.
--
-- ACHADOS NOS DADOS: item_id é único; produto_id órfão em 74.639 linhas (1,49%);
-- cliente_id órfão em 75.201 (1,50%); quantidade NULL em 50.169 (1,0%); o mesmo
-- pedido_id aparece com cliente_id diferente em itens diferentes.
-- Também: o `pedido_id` desta tabela NÃO é o mesmo namespace do `pedido_id` da API
-- (pedido_id=1 aponta para clientes/produtos diferentes) — por isso o mart faz UNION
-- das duas fontes e nunca JOIN por pedido_id.
--
-- Não descartamos linha suspeita: marcamos com dq_flag_* e o consumidor decide.
-- IDEMPOTENTE: item_id é imutável, então o MERGE só insere o que ainda não existe.
-- Todo o trabalho é feito DENTRO do BigQuery (nada volta para pandas).
CREATE TABLE IF NOT EXISTS `{project}.{dataset}.stg_itens_pedido` (
  item_id INT64,
  pedido_id INT64,
  cliente_id INT64,
  produto_id INT64,
  data_item TIMESTAMP,
  quantidade INT64,
  valor_unitario FLOAT64,
  dq_flag_produto_orfao BOOL,
  dq_flag_cliente_orfao BOOL,
  dq_flag_quantidade_nula BOOL,
  dq_flag_pedido_com_clientes_inconsistentes BOOL
)
PARTITION BY DATE(data_item)
CLUSTER BY pedido_id;

MERGE `{project}.{dataset}.stg_itens_pedido` AS alvo
USING (
  SELECT
    i.item_id, i.pedido_id, i.cliente_id, i.produto_id,
    SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', i.data_item) AS data_item,
    i.quantidade, i.valor_unitario,
    (p.produto_id IS NULL) AS dq_flag_produto_orfao,
    (c.cliente_id IS NULL) AS dq_flag_cliente_orfao,
    (i.quantidade IS NULL) AS dq_flag_quantidade_nula,
    COUNT(DISTINCT i.cliente_id) OVER (PARTITION BY i.pedido_id) > 1
      AS dq_flag_pedido_com_clientes_inconsistentes
  FROM `{project}.{dataset}.raw_itens_pedido` i
  LEFT JOIN `{project}.{dataset}.stg_produtos` p ON p.produto_id = i.produto_id
  LEFT JOIN (
    SELECT DISTINCT cliente_id FROM `{project}.{dataset}.stg_clientes_scd2` WHERE is_current
  ) c ON c.cliente_id = i.cliente_id
) AS origem
ON alvo.item_id = origem.item_id
WHEN NOT MATCHED THEN INSERT (
  item_id, pedido_id, cliente_id, produto_id, data_item, quantidade, valor_unitario,
  dq_flag_produto_orfao, dq_flag_cliente_orfao, dq_flag_quantidade_nula,
  dq_flag_pedido_com_clientes_inconsistentes
) VALUES (
  origem.item_id, origem.pedido_id, origem.cliente_id, origem.produto_id, origem.data_item,
  origem.quantidade, origem.valor_unitario, origem.dq_flag_produto_orfao, origem.dq_flag_cliente_orfao,
  origem.dq_flag_quantidade_nula, origem.dq_flag_pedido_com_clientes_inconsistentes
);
