"""Montagem do pipeline: SQL, parquet p/ BigQuery, DAG, retry, falha proposital e sensor de alerta."""
import json
import logging

import pandas as pd
import pyarrow.parquet as pq
import pytest
from dagster import DagsterInstance, asset, build_run_status_sensor_context, materialize

from vena_pipeline.assets import raw
from vena_pipeline.definitions import alerta_falha_pipeline, defs, pipeline_job
from vena_pipeline.gcp import render_sql, write_df_parquet
from vena_pipeline.resources import SQL_DIR

SQL_FILES = sorted(SQL_DIR.rglob("*.sql"))


# ------------------------------------------------------------------- SQL
def test_all_seven_sql_files_exist():
    assert len(SQL_FILES) == 7


@pytest.mark.parametrize("path", SQL_FILES, ids=lambda p: p.name)
def test_sql_renders_without_leftover_placeholders(path):
    out = render_sql(path.read_text(encoding="utf-8"), "proj", "ds")
    assert "{project}" not in out and "{dataset}" not in out and "`proj.ds." in out


def test_render_sql_tolerates_literal_braces():
    assert render_sql("-- {'0','1'} {project}.{dataset}", "p", "d") == "-- {'0','1'} p.d"


@pytest.mark.parametrize("path", SQL_FILES, ids=lambda p: p.name)
def test_declare_precedes_every_other_statement(path):
    """BigQuery scripting: DECLARE só é válido no início do script."""
    body = " ".join(l.strip() for l in path.read_text(encoding="utf-8").splitlines()
                    if l.strip() and not l.strip().startswith("--"))
    seen_other = False
    for stmt in filter(None, (s.strip() for s in body.split(";"))):
        if stmt.upper().startswith("DECLARE"):
            assert not seen_other
        else:
            seen_other = True


# --------------------------------------------------------------- parquet
def test_parquet_timestamps_are_microseconds_and_nullable_ints_stay_int64(tmp_path):
    df = pd.DataFrame({"t": [pd.Timestamp("2026-09-19 12:34:56.123456", tz="UTC")] * 3, "cliente_id": [1, None, 3]})
    df["cliente_id"] = df["cliente_id"].astype("Int64")
    path = str(tmp_path / "a.parquet")
    write_df_parquet(df, path)
    schema = pq.read_schema(path)
    assert str(schema.field("t").type) == "timestamp[us, tz=UTC]"  # em ns o BigQuery carregaria INT64
    assert str(schema.field("cliente_id").type) == "int64"


# ------------------------------------------------------------------- DAG
def test_dag_has_real_dependencies_between_layers():
    g = defs.get_repository_def().asset_graph
    parents = lambda name: {k.to_user_string() for k in g.get(__import__("dagster").AssetKey(name)).parent_keys}
    assert parents("stg_itens_pedido") == {"raw_itens_pedido", "stg_produtos", "stg_clientes_scd2"}
    assert parents("mart_vendas_diarias") == {"stg_pedidos_api", "stg_itens_pedido"}
    assert parents("stg_pedidos_api") == {"raw_pedidos_api"}
    assert len(list(g.get_all_asset_keys())) == 12 and len(list(g.asset_check_keys)) == 11


def test_schedule_and_sensor_registered():
    repo = defs.get_repository_def()
    assert [s.cron_schedule for s in repo.schedule_defs] == ["0 5 * * *"]
    assert [s.name for s in repo.sensor_defs] == ["alerta_falha_pipeline"]
    assert pipeline_job.name == "vena_pipeline_completo"


def test_scraping_and_api_assets_have_retry_policies():
    assert raw.raw_precos_concorrentes.op.retry_policy.max_retries == 3
    assert raw.raw_pedidos_api.op.retry_policy.max_retries == 2


# ------------------------------------------------------ falha proposital
class FakeContext:
    def __init__(self, retry_number):
        self.retry_number = retry_number
        self.log = logging.getLogger("fake")


def test_simulated_failure_only_on_first_attempt(monkeypatch):
    monkeypatch.setenv("SIMULATE_SCRAPING_FAILURE", "1")
    with pytest.raises(RuntimeError, match="falha proposital"):
        raw._simulate_failure(FakeContext(retry_number=0))
    raw._simulate_failure(FakeContext(retry_number=1))  # a retentativa passa


def test_simulated_failure_can_be_switched_off(monkeypatch):
    monkeypatch.setenv("SIMULATE_SCRAPING_FAILURE", "0")
    raw._simulate_failure(FakeContext(retry_number=0))


def test_simulated_failure_is_on_by_default(monkeypatch):
    monkeypatch.delenv("SIMULATE_SCRAPING_FAILURE", raising=False)
    with pytest.raises(RuntimeError):
        raw._simulate_failure(FakeContext(retry_number=0))


# ---------------------------------------------------------------- sensor
@asset
def ativo_que_falha():
    raise RuntimeError("falha de teste")


def test_sensor_emits_structured_alert_for_a_real_failed_run(caplog):
    """Provoca uma falha REAL de run do Dagster e entrega o evento verdadeiro ao sensor."""
    instance = DagsterInstance.ephemeral()
    result = materialize([ativo_que_falha], instance=instance, raise_on_error=False)
    assert not result.success
    run = instance.get_run_by_id(result.run_id)
    ctx = build_run_status_sensor_context(
        sensor_name="alerta_falha_pipeline", dagster_instance=instance,
        dagster_run=run, dagster_event=result.get_job_failure_event(),
    )
    with caplog.at_level(logging.ERROR, logger="vena_pipeline.definitions"):
        alerta_falha_pipeline(ctx)
    alerts = [json.loads(r.getMessage()) for r in caplog.records if "ALERTA" in r.getMessage()]
    assert len(alerts) == 1
    assert alerts[0]["run_id"] == run.run_id and "ativo_que_falha" in alerts[0]["mensagem"]
