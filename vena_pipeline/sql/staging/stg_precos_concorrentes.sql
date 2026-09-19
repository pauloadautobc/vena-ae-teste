-- stg_precos_concorrentes.sql — série temporal de preços de concorrentes.
--
-- A raw guarda TODA coleta (log append-only). Aqui o grão é 1 linha por
-- (produto, concorrente, DIA de coleta), mantendo a coleta mais recente do dia.
-- Por que o dia e não o timestamp: com o timestamp na chave, cada re-execução do
-- pipeline acrescentaria mais linhas (violando a idempotência). Com o dia:
--   * 2ª execução no mesmo dia -> mesmas linhas (a coleta mais nova prevalece);
--   * dia seguinte -> linhas novas (histórico diário preservado).
CREATE TABLE IF NOT EXISTS `{project}.{dataset}.stg_precos_concorrentes` (
  produto_chave STRING,
  produto_bruto STRING,
  categoria STRING,
  preco FLOAT64,
  concorrente STRING,
  disponivel BOOL,
  estoque_status STRING,
  parse_strategy STRING,
  coletado_em TIMESTAMP,
  data_coleta DATE
)
PARTITION BY data_coleta
CLUSTER BY produto_chave;

MERGE `{project}.{dataset}.stg_precos_concorrentes` AS alvo
USING (
  SELECT
    LOWER(TRIM(produto)) AS produto_chave, produto AS produto_bruto, categoria, preco,
    concorrente, disponivel, estoque_status, parse_strategy, coletado_em,
    DATE(coletado_em) AS data_coleta
  FROM `{project}.{dataset}.raw_precos_concorrentes`
  WHERE preco IS NOT NULL  -- preço ilegível não serve para benchmarking
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY LOWER(TRIM(produto)), concorrente, DATE(coletado_em) ORDER BY coletado_em DESC
  ) = 1
) AS origem
ON alvo.produto_chave = origem.produto_chave
   AND alvo.concorrente = origem.concorrente
   AND alvo.data_coleta = origem.data_coleta
WHEN MATCHED AND origem.coletado_em > alvo.coletado_em THEN UPDATE SET
  produto_bruto = origem.produto_bruto, categoria = origem.categoria, preco = origem.preco,
  disponivel = origem.disponivel, estoque_status = origem.estoque_status,
  parse_strategy = origem.parse_strategy, coletado_em = origem.coletado_em
WHEN NOT MATCHED THEN INSERT (
  produto_chave, produto_bruto, categoria, preco, concorrente, disponivel,
  estoque_status, parse_strategy, coletado_em, data_coleta
) VALUES (
  origem.produto_chave, origem.produto_bruto, origem.categoria, origem.preco, origem.concorrente,
  origem.disponivel, origem.estoque_status, origem.parse_strategy, origem.coletado_em, origem.data_coleta
);
