"""
Qualidade de dados = Dagster Asset Checks (rodam contra o BigQuery real e
aparecem no mesmo painel do pipeline). Cada um devolve métricas (totais,
contagens, taxa) como metadata.

Severidade: ERROR para invariantes que, se quebrarem, são bug (unicidade após
MERGE, 1 versão atual no SCD2). WARN para taxas de sujeira "esperadas" de um
sistema legado (nulos, órfãos): limiares calibrados nas taxas REAIS medidas
(ex.: ~1,5% de órfãos em itens_pedido -> alerta acima de 5%), não números redondos.

(A taxa de erro do parse do scraping é emitida pelo próprio asset
`raw_precos_concorrentes`, pois as linhas descartadas só existem em memória.)
"""
from dagster import AssetCheckResult, AssetCheckSeverity, asset_check

from vena_pipeline.assets.transform import (
    stg_clientes_scd2, stg_itens_pedido, stg_pedidos_api, stg_precos_concorrentes,
)
from vena_pipeline.resources import BigQueryResource

ERROR, WARN = AssetCheckSeverity.ERROR, AssetCheckSeverity.WARN


def _rate(part: int, total: int) -> float:
    return round(part / total, 4) if total else 0.0


# ------------------------------------------------------------- unicidade
@asset_check(asset=stg_pedidos_api, description="pedido_id único após o MERGE.")
def pedido_id_unico(bq: BigQueryResource) -> AssetCheckResult:
    dup = bq.row(f"SELECT COUNT(*) AS n FROM (SELECT pedido_id FROM `{bq.table('stg_pedidos_api')}` "
                 f"GROUP BY 1 HAVING COUNT(*) > 1)")["n"]
    return AssetCheckResult(passed=dup == 0, severity=ERROR, metadata={"pedido_id_duplicados": dup})


@asset_check(asset=stg_itens_pedido, description="item_id único (5M linhas).")
def item_id_unico(bq: BigQueryResource) -> AssetCheckResult:
    dup = bq.row(f"SELECT COUNT(*) AS n FROM (SELECT item_id FROM `{bq.table('stg_itens_pedido')}` "
                 f"GROUP BY 1 HAVING COUNT(*) > 1)")["n"]
    return AssetCheckResult(passed=dup == 0, severity=ERROR, metadata={"item_id_duplicados": dup})


@asset_check(asset=stg_clientes_scd2, description="No máximo UMA versão is_current por cliente_id (invariante do SCD2).")
def scd2_uma_versao_atual(bq: BigQueryResource) -> AssetCheckResult:
    v = bq.row(f"SELECT COUNT(*) AS n FROM (SELECT cliente_id FROM `{bq.table('stg_clientes_scd2')}` "
               f"WHERE is_current GROUP BY 1 HAVING COUNT(*) > 1)")["n"]
    return AssetCheckResult(passed=v == 0, severity=ERROR, metadata={"cliente_id_com_mais_de_1_atual": v})


@asset_check(asset=stg_clientes_scd2, description="CPF em mais de um cliente_id atual (cadastro duplicado) — só reporta.")
def cpf_duplicado(bq: BigQueryResource) -> AssetCheckResult:
    r = bq.row(f"SELECT COUNT(*) AS cpfs, IFNULL(SUM(n), 0) AS clientes FROM ("
               f"SELECT cpf, COUNT(DISTINCT cliente_id) AS n FROM `{bq.table('stg_clientes_scd2')}` "
               f"WHERE is_current GROUP BY cpf HAVING COUNT(DISTINCT cliente_id) > 1)")
    return AssetCheckResult(passed=r["cpfs"] == 0, severity=WARN,
                            metadata={"cpfs_em_mais_de_um_cliente_id": r["cpfs"], "clientes_envolvidos": r["clientes"]})


# ------------------------------------------------------------ nulos críticos
@asset_check(asset=stg_pedidos_api, description="cliente_id/quantidade nulos em pedidos: aceitável até 2% (real: 0,51%).")
def pedidos_nulos_criticos(bq: BigQueryResource) -> AssetCheckResult:
    r = bq.row(f"SELECT COUNT(*) AS total, COUNTIF(dq_flag_cliente_nulo) AS cliente_nulo, "
               f"COUNTIF(dq_flag_quantidade_nula) AS quantidade_nula FROM `{bq.table('stg_pedidos_api')}`")
    taxa = _rate(max(r["cliente_nulo"], r["quantidade_nula"]), r["total"])
    return AssetCheckResult(passed=taxa <= 0.02, severity=WARN, metadata={**r, "taxa_nulos": taxa})


@asset_check(asset=stg_pedidos_api, description="Erro de parse: valor ou data virou NULL na staging (WARN > 2%).")
def pedidos_erro_parse(bq: BigQueryResource) -> AssetCheckResult:
    r = bq.row(f"SELECT COUNT(*) AS total, COUNTIF(dq_flag_valor_invalido) AS valor_invalido, "
               f"COUNTIF(dq_flag_data_invalida) AS data_invalida FROM `{bq.table('stg_pedidos_api')}`")
    taxa = _rate(r["valor_invalido"] + r["data_invalida"], r["total"])
    return AssetCheckResult(passed=taxa <= 0.02, severity=WARN, metadata={**r, "taxa_erro_parse": taxa})


# ----------------------------------------------------- integridade referencial
@asset_check(asset=stg_itens_pedido, description="produto_id de itens_pedido existe no catálogo (real: 1,49% órfãos; WARN > 5%).")
def itens_fk_produto(bq: BigQueryResource) -> AssetCheckResult:
    r = bq.row(f"SELECT COUNT(*) AS total, COUNTIF(dq_flag_produto_orfao) AS orfaos "
               f"FROM `{bq.table('stg_itens_pedido')}`")
    taxa = _rate(r["orfaos"], r["total"])
    return AssetCheckResult(passed=taxa <= 0.05, severity=WARN, metadata={**r, "taxa_orfaos": taxa})


@asset_check(asset=stg_itens_pedido, description="cliente_id de itens_pedido existe no cadastro (real: 1,50% órfãos; WARN > 5%).")
def itens_fk_cliente(bq: BigQueryResource) -> AssetCheckResult:
    r = bq.row(f"SELECT COUNT(*) AS total, COUNTIF(dq_flag_cliente_orfao) AS orfaos "
               f"FROM `{bq.table('stg_itens_pedido')}`")
    taxa = _rate(r["orfaos"], r["total"])
    return AssetCheckResult(passed=taxa <= 0.05, severity=WARN, metadata={**r, "taxa_orfaos": taxa})


@asset_check(asset=stg_pedidos_api, description="produto_id/cliente_id dos pedidos da API existem nos cadastros (WARN > 5%).")
def pedidos_fk(bq: BigQueryResource) -> AssetCheckResult:
    r = bq.row(
        f"SELECT COUNT(*) AS total, COUNTIF(p.produto_id IS NULL) AS produto_orfao, "
        f"COUNTIF(o.cliente_id IS NOT NULL AND c.cliente_id IS NULL) AS cliente_orfao "
        f"FROM `{bq.table('stg_pedidos_api')}` o "
        f"LEFT JOIN `{bq.table('stg_produtos')}` p ON p.produto_id = o.produto_id "
        f"LEFT JOIN (SELECT DISTINCT cliente_id FROM `{bq.table('stg_clientes_scd2')}` WHERE is_current) c "
        f"ON c.cliente_id = o.cliente_id")
    tp, tc = _rate(r["produto_orfao"], r["total"]), _rate(r["cliente_orfao"], r["total"])
    return AssetCheckResult(passed=tp <= 0.05 and tc <= 0.05, severity=WARN,
                            metadata={**r, "taxa_produto_orfao": tp, "taxa_cliente_orfao": tc})


@asset_check(
    asset=stg_precos_concorrentes,
    description="Cobertura do catálogo: % de observações cujo produto casa com stg_produtos por nome. "
                "Hoje 0% (catálogo placeholder x nomes reais) — WARN de propósito, para ficar visível.",
)
def precos_cobertura_catalogo(bq: BigQueryResource) -> AssetCheckResult:
    r = bq.row(f"SELECT COUNT(*) AS total, COUNTIF(p.produto_id IS NOT NULL) AS mapeadas "
               f"FROM `{bq.table('stg_precos_concorrentes')}` c "
               f"LEFT JOIN `{bq.table('stg_produtos')}` p ON p.nome_produto_chave = c.produto_chave")
    taxa = _rate(r["mapeadas"], r["total"])
    return AssetCheckResult(passed=r["total"] > 0 and taxa >= 0.5, severity=WARN,
                            metadata={**r, "taxa_cobertura": taxa})


ALL_CHECKS = [
    pedido_id_unico, item_id_unico, scd2_uma_versao_atual, cpf_duplicado, pedidos_nulos_criticos,
    pedidos_erro_parse, itens_fk_produto, itens_fk_cliente, pedidos_fk, precos_cobertura_catalogo,
]
