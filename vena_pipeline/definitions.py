"""Montagem do projeto Dagster: assets, checks, job, schedule, sensor de alerta e resources."""
import logging

from dagster import (
    AssetSelection, Definitions, EnvVar, RunFailureSensorContext, ScheduleDefinition,
    define_asset_job, run_failure_sensor,
)

from vena_pipeline import settings
from vena_pipeline.assets import raw, transform
from vena_pipeline.checks import ALL_CHECKS
from vena_pipeline.logging_utils import log_event
from vena_pipeline.resources import BigQueryResource, GcsResource, SalesApiResource, SqliteResource

logger = logging.getLogger("vena_pipeline.definitions")

ALL_ASSETS = [
    raw.raw_pedidos_api, raw.raw_precos_concorrentes, raw.raw_clientes, raw.raw_produtos, raw.raw_itens_pedido,
    transform.stg_clientes_scd2, transform.stg_produtos, transform.stg_pedidos_api,
    transform.stg_itens_pedido, transform.stg_precos_concorrentes,
    transform.mart_vendas_diarias, transform.mart_posicionamento_competitivo,
]

# Um job para o DAG inteiro (12 assets): simples de operar e de demonstrar.
pipeline_job = define_asset_job("vena_pipeline_completo", selection=AssetSelection.all())

# Diário, de madrugada: a diretoria quer um dashboard "diário" e fora do horário
# comercial há menos disputa pelo rate limit da API.
daily_schedule = ScheduleDefinition(job=pipeline_job, cron_schedule="0 5 * * *")  # 05:00 UTC = 02:00 BRT


@run_failure_sensor(monitored_jobs=[pipeline_job])
def alerta_falha_pipeline(context: RunFailureSensorContext):
    """Alerta quando o job falha (depois de esgotados os retries dos assets).

    Aqui o destino é um log estruturado ERROR com o necessário para triagem;
    em produção seria Slack/PagerDuty — trocar o destino não muda a lógica.
    """
    log_event(
        logger, logging.ERROR, "ALERTA: pipeline_falhou",
        run_id=context.dagster_run.run_id,
        job_name=context.dagster_run.job_name,
        mensagem=context.failure_event.message,  # "... Steps failed: ['<asset>']"
    )


defs = Definitions(
    assets=ALL_ASSETS,
    asset_checks=ALL_CHECKS,
    jobs=[pipeline_job],
    schedules=[daily_schedule],
    sensors=[alerta_falha_pipeline],
    resources={
        "bq": BigQueryResource(project=settings.GCP_PROJECT, dataset=settings.BQ_DATASET),
        "gcs": GcsResource(bucket=settings.GCS_BUCKET),
        "sales_api": SalesApiResource(
            base_url=settings.API_BASE_URL,
            token=EnvVar("API_TOKEN"),  # lido só na execução; sem default no código
            page_size=settings.API_PAGE_SIZE,
            max_requests_per_window=settings.API_MAX_REQUESTS_PER_WINDOW,
            window_seconds=settings.API_WINDOW_SECONDS,
        ),
        "sqlite": SqliteResource(path=settings.SQLITE_PATH, chunk_size=settings.SQLITE_CHUNK_SIZE),
    },
)
