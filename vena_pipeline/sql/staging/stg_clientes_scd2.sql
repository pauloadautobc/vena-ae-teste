-- stg_clientes_scd2.sql — clientes com histórico Type-2 (SCD2) e quarentena de conflitos.
--
-- ACHADO NOS DADOS: 12 `cliente_id` aparecem 2x com CPF/nome/cidade DIFERENTES.
-- Não é mudança de atributo no tempo (o que SCD2 resolve): é colisão de chave
-- entre duas pessoas diferentes. Escolher uma das linhas corromperia o
-- histórico de compras de alguém com dados de outra, então TODA a família em
-- conflito vai para `stg_clientes_quarentena` (revisão manual) e fica fora do SCD2.
--
-- SCD2: se o hash dos atributos mudou, a versão atual é fechada (valid_to,
-- is_current=FALSE) e uma nova é inserida. IDEMPOTENTE: com o mesmo raw, os
-- hashes batem, nada fecha e nada é inserido.
--
-- (BigQuery scripting exige o DECLARE antes de qualquer outro statement.)
DECLARE run_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP();

CREATE TABLE IF NOT EXISTS `{project}.{dataset}.stg_clientes_scd2` (
  cliente_id INT64,
  nome STRING,
  cpf STRING,
  email STRING,
  cidade STRING,
  estado STRING,
  data_cadastro DATE,
  segmento STRING,
  attrs_hash STRING,
  valid_from TIMESTAMP,
  valid_to TIMESTAMP,
  is_current BOOL
)
CLUSTER BY cliente_id;

-- 1) cliente_id em conflito de identidade (mesmo id, CPFs diferentes)
CREATE TEMP TABLE conflitos AS (
  SELECT cliente_id
  FROM `{project}.{dataset}.raw_clientes`
  GROUP BY cliente_id
  HAVING COUNT(DISTINCT NULLIF(TRIM(cpf), '')) > 1
);

-- 2) quarentena: reflete o raw atual, então CREATE OR REPLACE é idempotente
CREATE OR REPLACE TABLE `{project}.{dataset}.stg_clientes_quarentena` AS
SELECT r.*, run_ts AS detectado_em, 'cliente_id duplicado com CPF divergente' AS motivo
FROM `{project}.{dataset}.raw_clientes` r
JOIN conflitos USING (cliente_id);

-- 3) entrada limpa: sem conflitos, tipada, normalizada, 1 linha por cliente_id
CREATE TEMP TABLE entrada AS (
  SELECT
    cliente_id,
    NULLIF(TRIM(nome), '') AS nome,
    NULLIF(TRIM(cpf), '') AS cpf,
    LOWER(NULLIF(TRIM(email), '')) AS email,
    NULLIF(TRIM(cidade), '') AS cidade,
    UPPER(NULLIF(TRIM(estado), '')) AS estado,
    SAFE_CAST(data_cadastro AS DATE) AS data_cadastro,
    NULLIF(TRIM(segmento), '') AS segmento,
    TO_HEX(SHA256(TO_JSON_STRING(STRUCT(
      NULLIF(TRIM(nome), ''), NULLIF(TRIM(cpf), ''), LOWER(NULLIF(TRIM(email), '')),
      NULLIF(TRIM(cidade), ''), UPPER(NULLIF(TRIM(estado), '')),
      SAFE_CAST(data_cadastro AS DATE), NULLIF(TRIM(segmento), '')
    )))) AS attrs_hash
  FROM `{project}.{dataset}.raw_clientes`
  WHERE cliente_id IS NOT NULL
    AND cliente_id NOT IN (SELECT cliente_id FROM conflitos)
  QUALIFY ROW_NUMBER() OVER (PARTITION BY cliente_id ORDER BY attrs_hash) = 1  -- determinístico
);

-- 4) fecha a versão atual de quem mudou
UPDATE `{project}.{dataset}.stg_clientes_scd2` AS alvo
SET valid_to = run_ts, is_current = FALSE
WHERE alvo.is_current
  AND EXISTS (
    SELECT 1 FROM entrada e
    WHERE e.cliente_id = alvo.cliente_id AND e.attrs_hash != alvo.attrs_hash
  );

-- 5) insere versão atual para cliente novo ou que acabou de ser fechado
INSERT INTO `{project}.{dataset}.stg_clientes_scd2`
SELECT e.cliente_id, e.nome, e.cpf, e.email, e.cidade, e.estado, e.data_cadastro, e.segmento,
       e.attrs_hash, run_ts, NULL, TRUE
FROM entrada e
WHERE NOT EXISTS (
  SELECT 1 FROM `{project}.{dataset}.stg_clientes_scd2` c
  WHERE c.cliente_id = e.cliente_id AND c.is_current AND c.attrs_hash = e.attrs_hash
);
