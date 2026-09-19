-- mart_posicionamento_competitivo.sql — posição de preço de cada concorrente, por produto e dia.
-- Grão: (data_coleta, produto, concorrente).
--
-- LIMITAÇÃO VERIFICADA NOS DADOS: o scraping usa nomes reais ("Tênis Runner Pro") e o
-- catálogo interno usa texto placeholder ("Repudiandae Repellendus"): 0 dos 9 produtos do
-- scraping existem no catálogo, e não há SKU/EAN em comum. Comparar médias por categoria
-- seria comparar cestas de produtos diferentes (métrica enganosa), então NÃO fazemos isso.
--
-- O mart entrega o que é verdadeiro sem o catálogo (preço do concorrente vs. mínimo/média/
-- máximo do mercado, ranking, disponibilidade) e mantém o LEFT JOIN com o catálogo: se um dia
-- existir chave de ligação, `preco_interno`/`gap_percentual_vs_interno` passam a ser preenchidos.
-- Hoje ficam NULL com dq_flag_produto_nao_mapeado = TRUE — e o asset check de cobertura
-- torna isso visível no painel do Dagster.
CREATE OR REPLACE TABLE `{project}.{dataset}.mart_posicionamento_competitivo` AS
WITH mercado AS (
  SELECT
    data_coleta, produto_chave, produto_bruto AS produto, categoria, concorrente,
    preco AS preco_concorrente, disponivel AS concorrente_em_estoque, estoque_status, coletado_em,
    MIN(preco) OVER w AS preco_minimo_mercado,
    AVG(preco) OVER w AS preco_medio_mercado,
    MAX(preco) OVER w AS preco_maximo_mercado,
    COUNT(*) OVER w AS qtd_concorrentes,
    RANK() OVER (PARTITION BY data_coleta, produto_chave ORDER BY preco) AS ranking_preco
  FROM `{project}.{dataset}.stg_precos_concorrentes`
  WINDOW w AS (PARTITION BY data_coleta, produto_chave)
)
SELECT
  m.data_coleta, p.produto_id, m.produto, m.categoria, m.concorrente,
  m.preco_concorrente, m.preco_minimo_mercado, ROUND(m.preco_medio_mercado, 2) AS preco_medio_mercado,
  m.preco_maximo_mercado, m.ranking_preco, m.qtd_concorrentes,
  SAFE_DIVIDE(m.preco_concorrente - m.preco_minimo_mercado, m.preco_minimo_mercado) AS gap_percentual_vs_menor_preco,
  m.concorrente_em_estoque, m.estoque_status,
  p.preco_tabela AS preco_interno,
  SAFE_DIVIDE(p.preco_tabela - m.preco_concorrente, m.preco_concorrente) AS gap_percentual_vs_interno,
  m.coletado_em,
  (p.produto_id IS NULL) AS dq_flag_produto_nao_mapeado
FROM mercado m
LEFT JOIN `{project}.{dataset}.stg_produtos` p ON p.nome_produto_chave = m.produto_chave;
