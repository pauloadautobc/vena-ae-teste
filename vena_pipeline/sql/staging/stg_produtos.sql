-- stg_produtos.sql — catálogo tipado (SCD1: o teste não pede histórico de preço de tabela).
--
-- ACHADOS NOS DADOS: `ativo` mistura {'0','1','S','N',NULL} (161 NULL de 800);
-- `preco_tabela` mistura "R$ 533.46" e "533.46"; `categoria` é NULL em 165;
-- produto_id é único.
-- Derivado 100% do raw a cada execução: CREATE OR REPLACE é idempotente.
CREATE OR REPLACE TABLE `{project}.{dataset}.stg_produtos` AS
SELECT
  produto_id,
  TRIM(nome_produto) AS nome_produto,
  LOWER(TRIM(nome_produto)) AS nome_produto_chave,
  NULLIF(TRIM(categoria), '') AS categoria,
  SAFE_CAST(REGEXP_REPLACE(TRIM(preco_tabela), r'[^0-9.\-]', '') AS FLOAT64) AS preco_tabela,
  CASE
    WHEN UPPER(TRIM(ativo)) IN ('1', 'S', 'SIM', 'TRUE') THEN TRUE
    WHEN UPPER(TRIM(ativo)) IN ('0', 'N', 'NAO', 'NÃO', 'FALSE') THEN FALSE
    ELSE NULL  -- desconhecido != inativo: não assumimos default
  END AS ativo,
  (ativo IS NULL) AS dq_flag_ativo_desconhecido,
  (SAFE_CAST(REGEXP_REPLACE(TRIM(preco_tabela), r'[^0-9.\-]', '') AS FLOAT64) IS NULL) AS dq_flag_preco_invalido
FROM `{project}.{dataset}.raw_produtos`;
