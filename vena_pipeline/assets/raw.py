"""
Camada RAW: espelho fiel de cada fonte, sem regra de negócio.

Fluxo de cada asset: fonte -> Parquet -> GCS (landing) -> load job no BigQuery.
Passar pelo GCS desacopla extração de carga: se o load falhar, o arquivo já
está persistido e não precisamos reconsultar a API (que tem rate limit).
Tipagem e limpeza acontecem na STAGING (SQL), nunca aqui.
"""
import os
import tempfile
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import requests
from dagster import (
    AssetCheckResult,
    AssetCheckSeverity,
    AssetCheckSpec,
    AssetExecutionContext,
    MaterializeResult,
    RetryPolicy,
    asset,
)
from tenacity import retry, stop_after_attempt, wait_exponential_jitter

from vena_pipeline import settings
from vena_pipeline.gcp import clear_prefix, load_parquet, upload_file, write_df_parquet
from vena_pipeline.ingest.scraper import evaluate_quality, parse_precos_concorrentes
from vena_pipeline.ingest.sqlite_reader import extract_large_table_in_parts, extract_small_table
from vena_pipeline.resources import BigQueryResource, GcsResource, SalesApiResource, SqliteResource


def _today() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


# ------------------------------------------------------------ Fonte 1: API
@asset(
    group_name="raw",
    retry_policy=RetryPolicy(max_retries=2, delay=10),
    description="Ingestão paginada e resiliente de /api/pedidos (retry, rate limit, recuperação de páginas).",
)
def raw_pedidos_api(
    context: AssetExecutionContext, sales_api: SalesApiResource, gcs: GcsResource, bq: BigQueryResource
) -> MaterializeResult:
    client = sales_api.build_client()
    # Faltou página mesmo após a recuperação -> IncompleteIngestionError -> o asset FALHA
    # (o RetryPolicy re-executa; seguro porque a raw é WRITE_TRUNCATE).
    rows = list(client.iter_all_pedidos(page_size=sales_api.page_size))
    stats = client.last_stats
    if stats["total_records"] is not None and len(rows) != stats["total_records"]:
        raise ValueError(f"Ingestão incompleta: {len(rows)} linhas, a API declarou {stats['total_records']}.")

    df = pd.DataFrame(rows)
    # `valor_unitario` vem como float (47.756) OU string "462.99 BRL" (244): guardamos como
    # STRING (NULL de verdade, não o texto "None"); a conversão é da staging.
    df["valor_unitario"] = df["valor_unitario"].map(lambda v: None if pd.isna(v) else str(v))
    # colunas inteiras com NULL virariam float64 (NaN): Int64 preserva inteiro + NULL.
    for col in ("pedido_id", "cliente_id", "produto_id", "quantidade"):
        df[col] = df[col].astype("Int64")

    prefix = f"raw/pedidos_api/dt={_today()}"
    with tempfile.TemporaryDirectory() as tmp:
        path = str(Path(tmp) / "pedidos_api.parquet")
        write_df_parquet(df, path)
        client_gcs = gcs.client()
        clear_prefix(client_gcs, gcs.bucket, prefix)
        uri = upload_file(client_gcs, gcs.bucket, path, f"{prefix}/pedidos_api.parquet")
    loaded = load_parquet(bq.client(), bq.table("raw_pedidos_api"), [uri])
    return MaterializeResult(metadata={
        "linhas_carregadas": loaded,
        "api_total_records": stats["total_records"],
        "api_total_pages": stats["total_pages"],
        "paginas_com_falha_1a_passada": stats["pages_failed_first_pass"],
        "paginas_recuperadas": stats["pages_recovered"],
    })


# ------------------------------------------------------- Fonte 2: scraping
SCRAPE_CHECK = "taxa_erro_parse_scraping"


def _simulate_failure(context: AssetExecutionContext) -> None:
    """FALHA PROPOSITAL exigida pelo teste: na 1ª tentativa do run o asset falha,
    e o RetryPolicy do Dagster o re-executa com sucesso. Ligada por padrão;
    `SIMULATE_SCRAPING_FAILURE=0` desliga. Nunca afeta a 2ª tentativa."""
    if os.getenv("SIMULATE_SCRAPING_FAILURE", "1") != "0" and context.retry_number == 0:
        context.log.warning("Falha PROPOSITAL simulada (SIMULATE_SCRAPING_FAILURE): o retry deve recuperar.")
        raise RuntimeError("[falha proposital] scraping indisponível na 1ª tentativa")


class ScrapingSchemaDriftError(Exception):
    """Nenhuma estratégia de parsing reconheceu o HTML."""


@asset(
    group_name="raw",
    retry_policy=RetryPolicy(max_retries=3, delay=10),
    check_specs=[AssetCheckSpec(
        name=SCRAPE_CHECK, asset="raw_precos_concorrentes",
        description="Taxa de erro do parse (linhas descartadas + preço ilegível) e linhas processadas; WARN > 2%.",
    )],
    description="Scraping resiliente de preços de concorrentes (a estrutura do HTML muda entre requisições).",
)
def raw_precos_concorrentes(
    context: AssetExecutionContext, gcs: GcsResource, bq: BigQueryResource
) -> MaterializeResult:
    _simulate_failure(context)

    @retry(stop=stop_after_attempt(3), wait=wait_exponential_jitter(initial=1, max=10), reraise=True)
    def fetch() -> str:
        response = requests.get(settings.SCRAPING_URL, timeout=20)
        response.raise_for_status()
        return response.text

    result = parse_precos_concorrentes(fetch())
    if result.strategy_used == "none":
        raise ScrapingSchemaDriftError("Nenhuma estratégia reconheceu o HTML (ver log scraping_schema_unrecognized).")

    coletado_em = datetime.now(timezone.utc)
    df = pd.DataFrame([
        {"produto": r.produto, "categoria": r.categoria, "preco": r.preco, "concorrente": r.concorrente,
         "disponivel": r.disponivel, "estoque_status": r.estoque_status,
         "parse_strategy": r.parse_strategy, "coletado_em": coletado_em}
        for r in result.records
    ])
    with tempfile.TemporaryDirectory() as tmp:
        path = str(Path(tmp) / "precos.parquet")
        write_df_parquet(df, path)
        stamp = coletado_em.strftime("%Y-%m-%dT%H%M%S")
        uri = upload_file(gcs.client(), gcs.bucket, path,
                          f"raw/precos_concorrentes/dt={coletado_em:%Y-%m-%d}/run={stamp}/precos.parquet")
    # APPEND: a raw é o LOG de coletas (cada execução é uma observação). A idempotência
    # dessa fonte está na staging (1 linha por produto/concorrente/dia).
    loaded = load_parquet(bq.client(), bq.table("raw_precos_concorrentes"), [uri], "WRITE_APPEND")

    q = evaluate_quality(result)
    return MaterializeResult(
        metadata={"linhas_na_tabela": loaded, "estrategia": result.strategy_used,
                  "linhas_processadas": result.raw_row_count, "linhas_descartadas": result.parse_error_count},
        check_results=[AssetCheckResult(
            check_name=SCRAPE_CHECK, passed=q.passed, metadata=q.metadata,
            severity=AssetCheckSeverity.ERROR if q.is_error else AssetCheckSeverity.WARN,
        )],
    )


# ----------------------------------------------------- Fonte 3: SQLite
def _small_table_asset(table: str, description: str):
    @asset(name=f"raw_{table}", group_name="raw", description=description)
    def _asset(context: AssetExecutionContext, sqlite: SqliteResource, gcs: GcsResource, bq: BigQueryResource):
        prefix = f"raw/{table}/dt={_today()}"
        with tempfile.TemporaryDirectory() as tmp:
            path = str(Path(tmp) / f"{table}.parquet")
            n = extract_small_table(sqlite.path, table, path)
            client_gcs = gcs.client()
            clear_prefix(client_gcs, gcs.bucket, prefix)
            uri = upload_file(client_gcs, gcs.bucket, path, f"{prefix}/{table}.parquet")
        loaded = load_parquet(bq.client(), bq.table(f"raw_{table}"), [uri])
        if loaded != n:
            raise ValueError(f"raw_{table}: extraídas {n}, no BigQuery {loaded}.")
        return MaterializeResult(metadata={"linhas_extraidas": n, "linhas_carregadas": loaded})

    return _asset


raw_clientes = _small_table_asset("clientes", "clientes (~6 mil linhas): leitura única.")
raw_produtos = _small_table_asset("produtos", "produtos (~800 linhas): leitura única.")


@asset(group_name="raw", description="itens_pedido (5M linhas): STREAMING em partes, nunca tudo em memória.")
def raw_itens_pedido(
    context: AssetExecutionContext, sqlite: SqliteResource, gcs: GcsResource, bq: BigQueryResource
) -> MaterializeResult:
    prefix = f"raw/itens_pedido/dt={_today()}"
    client_gcs = gcs.client()
    clear_prefix(client_gcs, gcs.bucket, prefix)
    uris, total = [], 0
    with tempfile.TemporaryDirectory() as tmp:
        for part in extract_large_table_in_parts(sqlite.path, "itens_pedido", tmp, sqlite.chunk_size):
            total = part["rows_total"]
            name = Path(part["path"]).name
            # sobe SÓ esta parte e a apaga do disco antes de gerar a próxima
            uris.append(upload_file(client_gcs, gcs.bucket, part["path"], f"{prefix}/{name}"))
            Path(part["path"]).unlink()
            context.log.info(f"itens_pedido: parte {part['part']} ({total:,} linhas acumuladas)")
    loaded = load_parquet(bq.client(), bq.table("raw_itens_pedido"), uris)
    if loaded != total:
        raise ValueError(f"raw_itens_pedido: extraídas {total}, no BigQuery {loaded}.")
    return MaterializeResult(metadata={"linhas_extraidas": total, "linhas_carregadas": loaded, "partes": len(uris)})
