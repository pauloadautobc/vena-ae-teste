-- mart_vendas_diarias.sql — tabela final para o dashboard de "saúde comercial".
-- Grão: (data, origem, status).
--
-- DECISÃO: as duas fontes de pedidos são UNIDAS (UNION ALL), nunca joinadas por
-- pedido_id — os dois `pedido_id` são namespaces diferentes (pedido_id=1 na API e
-- em itens_pedido são clientes/produtos diferentes). Um JOIN rodaria sem erro e
-- estaria silenciosamente errado. Cada fonte vira uma `origem`.
-- `itens_pedido` não tem status: usamos 'nao_aplicavel' (ausência estrutural, não dado faltante).
-- `status` da API inclui cancelado/reembolsado: por isso fica no grão, e o BI decide o que somar.
-- Mart 100% derivado da staging: CREATE OR REPLACE é idempotente.
CREATE OR REPLACE TABLE `{project}.{dataset}.mart_vendas_diarias` AS
WITH vendas_api AS (
  SELECT
    DATE(data_pedido) AS data,
    'pedidos_api' AS origem,
    COALESCE(status, 'nao_informado') AS status,
    COUNT(*) AS qtd_registros,
    SUM(quantidade) AS quantidade_total,
    SUM(quantidade * valor_unitario) AS receita_total,
    SAFE_DIVIDE(SUM(quantidade * valor_unitario), COUNT(*)) AS ticket_medio
  FROM `{project}.{dataset}.stg_pedidos_api`
  WHERE data_pedido IS NOT NULL  -- sem data válida não há a que dia atribuir
  GROUP BY 1, 2, 3
),
vendas_itens AS (
  SELECT
    DATE(data_item) AS data,
    'itens_pedido' AS origem,
    'nao_aplicavel' AS status,
    COUNT(*) AS qtd_registros,
    SUM(quantidade) AS quantidade_total,
    SUM(quantidade * valor_unitario) AS receita_total,
    SAFE_DIVIDE(SUM(quantidade * valor_unitario), COUNT(*)) AS ticket_medio
  FROM `{project}.{dataset}.stg_itens_pedido`
  WHERE data_item IS NOT NULL
  GROUP BY 1, 2, 3
)
SELECT * FROM vendas_api
UNION ALL
SELECT * FROM vendas_itens;
