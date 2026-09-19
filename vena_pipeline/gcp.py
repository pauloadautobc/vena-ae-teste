"""Helpers de GCS/BigQuery: landing zone em Parquet no GCS -> load job no BigQuery."""
from __future__ import annotations

import logging
from typing import Sequence

from google.cloud import bigquery, storage

from vena_pipeline.logging_utils import log_event

logger = logging.getLogger("vena_pipeline.gcp")


def write_df_parquet(df, path: str) -> None:
    """DataFrame -> Parquet que o BigQuery carrega com os tipos certos.

    pandas grava timestamp em NANOssegundos e o BigQuery não tem essa
    precisão: sem coagir para microssegundos a coluna é carregada como INT64.
    """
    df.to_parquet(path, index=False, coerce_timestamps="us", allow_truncated_timestamps=True)


def upload_file(client: storage.Client, bucket: str, local_path: str, blob_name: str) -> str:
    client.bucket(bucket).blob(blob_name).upload_from_filename(local_path)
    uri = f"gs://{bucket}/{blob_name}"
    log_event(logger, logging.INFO, "gcs_upload", uri=uri)
    return uri


def clear_prefix(client: storage.Client, bucket: str, prefix: str) -> int:
    """Esvazia `prefix/` (uma re-execução começa limpa, sem sobras)."""
    blobs = list(client.list_blobs(bucket, prefix=prefix.rstrip("/") + "/"))
    for blob in blobs:
        blob.delete()
    return len(blobs)


def load_parquet(
    client: bigquery.Client, table_id: str, uris: Sequence[str], write_disposition: str = "WRITE_TRUNCATE"
) -> int:
    """Load job GCS -> BigQuery; devolve o nº de linhas da tabela."""
    job = client.load_table_from_uri(
        list(uris),
        table_id,
        job_config=bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.PARQUET,
            write_disposition=write_disposition,
            autodetect=True,
        ),
    )
    job.result()
    rows = client.get_table(table_id).num_rows
    log_event(logger, logging.INFO, "bq_load_complete", table=table_id, rows=rows, files=len(uris))
    return rows


def render_sql(template: str, project: str, dataset: str) -> str:
    # `.replace` (e não `str.format`): chaves literais em comentários/regex do SQL não podem quebrar.
    return template.replace("{project}", project).replace("{dataset}", dataset)
