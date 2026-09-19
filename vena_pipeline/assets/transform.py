"""
STAGING e MART: a transformação (o "T" do ELT) roda DENTRO do BigQuery, em SQL
versionado em `vena_pipeline/sql/`. Os assets só orquestram na ordem certa e
expõem métricas; nada volta para pandas.

`deps=[...]` (e não parâmetros) porque o que importa é que o upstream já tenha
sido materializado: a dependência de dados real acontece no SQL.
"""
from dagster import AssetExecutionContext, MaterializeResult, asset

from vena_pipeline.assets.raw import (
    raw_clientes, raw_itens_pedido, raw_pedidos_api, raw_precos_concorrentes, raw_produtos,
)
from vena_pipeline.resources import BigQueryResource


def _run(bq: BigQueryResource, sql_file: str, table: str, **extra) -> MaterializeResult:
    bq.run_sql_file(sql_file)
    return MaterializeResult(metadata={"linhas": bq.count(table), **extra})


# ------------------------------------------------------------------ staging
@asset(group_name="staging", deps=[raw_clientes],
       description="Clientes: SCD2 (histórico por hash de atributos) + quarentena de conflitos de identidade.")
def stg_clientes_scd2(context: AssetExecutionContext, bq: BigQueryResource) -> MaterializeResult:
    bq.run_sql_file("staging/stg_clientes_scd2.sql")
    atual = bq.row(f"SELECT COUNTIF(is_current) AS atuais FROM `{bq.table('stg_clientes_scd2')}`")["atuais"]
    return MaterializeResult(metadata={
        "linhas_total": bq.count("stg_clientes_scd2"), "versoes_atuais": atual,
        "linhas_quarentena": bq.count("stg_clientes_quarentena"),
    })


@asset(group_name="staging", deps=[raw_produtos], description="Catálogo tipado (preço, ativo).")
def stg_produtos(context: AssetExecutionContext, bq: BigQueryResource) -> MaterializeResult:
    return _run(bq, "staging/stg_produtos.sql", "stg_produtos")


@asset(group_name="staging", deps=[raw_pedidos_api],
       description="Pedidos da API: dedup por pedido_id, datas e valores tipados, flags de qualidade.")
def stg_pedidos_api(context: AssetExecutionContext, bq: BigQueryResource) -> MaterializeResult:
    return _run(bq, "staging/stg_pedidos_api.sql", "stg_pedidos_api")


@asset(group_name="staging", deps=[raw_itens_pedido, stg_produtos, stg_clientes_scd2],
       description="5M itens: tipagem + flags de integridade referencial (produto/cliente órfão).")
def stg_itens_pedido(context: AssetExecutionContext, bq: BigQueryResource) -> MaterializeResult:
    return _run(bq, "staging/stg_itens_pedido.sql", "stg_itens_pedido")


@asset(group_name="staging", deps=[raw_precos_concorrentes],
       description="Preços de concorrentes: 1 linha por produto/concorrente/dia (idempotente).")
def stg_precos_concorrentes(context: AssetExecutionContext, bq: BigQueryResource) -> MaterializeResult:
    return _run(bq, "staging/stg_precos_concorrentes.sql", "stg_precos_concorrentes")


# --------------------------------------------------------------------- mart
@asset(group_name="mart", deps=[stg_pedidos_api, stg_itens_pedido],
       description="Vendas por dia/origem/status: receita, ticket médio, volume (dashboard de saúde comercial).")
def mart_vendas_diarias(context: AssetExecutionContext, bq: BigQueryResource) -> MaterializeResult:
    bq.run_sql_file("mart/mart_vendas_diarias.sql")
    r = bq.row(f"SELECT COUNT(*) AS n, ROUND(SUM(receita_total), 2) AS receita FROM `{bq.table('mart_vendas_diarias')}`")
    return MaterializeResult(metadata={"linhas": r["n"], "receita_total_todas_origens": r["receita"]})


@asset(group_name="mart", deps=[stg_produtos, stg_precos_concorrentes],
       description="Posição de preço de cada concorrente por produto e dia (+ comparação interna quando houver chave).")
def mart_posicionamento_competitivo(context: AssetExecutionContext, bq: BigQueryResource) -> MaterializeResult:
    bq.run_sql_file("mart/mart_posicionamento_competitivo.sql")
    r = bq.row(f"SELECT COUNT(*) AS n, COUNTIF(dq_flag_produto_nao_mapeado) AS nao_mapeados "
               f"FROM `{bq.table('mart_posicionamento_competitivo')}`")
    return MaterializeResult(metadata={"linhas": r["n"], "linhas_sem_produto_no_catalogo": r["nao_mapeados"]})
