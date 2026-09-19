"""Resources do Dagster: a fronteira com o mundo externo (BigQuery, GCS, API, SQLite).

Isolar aqui permite testar a lógica dos assets sem credencial e trocar
ambiente sem tocar nos assets. Segredos vêm de `EnvVar` (lidos só na execução).
"""
from pathlib import Path

from dagster import ConfigurableResource
from google.cloud import bigquery, storage

from vena_pipeline.gcp import render_sql
from vena_pipeline.ingest.api_client import ResilientApiClient

SQL_DIR = Path(__file__).parent / "sql"


class BigQueryResource(ConfigurableResource):
    project: str
    dataset: str

    def client(self) -> bigquery.Client:
        return bigquery.Client(project=self.project)

    def table(self, name: str) -> str:
        return f"{self.project}.{self.dataset}.{name}"

    def run_sql_file(self, relative_path: str) -> None:
        """Executa um script .sql de `vena_pipeline/sql/` (vários statements)."""
        sql = render_sql((SQL_DIR / relative_path).read_text(encoding="utf-8"), self.project, self.dataset)
        self.client().query(sql).result()

    def row(self, sql: str) -> dict:
        """Primeira linha de uma query, como dict."""
        return dict(list(self.client().query(sql).result())[0])

    def count(self, table_name: str) -> int:
        return self.row(f"SELECT COUNT(*) AS n FROM `{self.table(table_name)}`")["n"]


class GcsResource(ConfigurableResource):
    bucket: str

    def client(self) -> storage.Client:
        return storage.Client()


class SalesApiResource(ConfigurableResource):
    base_url: str
    token: str
    page_size: int = 500
    max_requests_per_window: int = 28
    window_seconds: float = 60.0

    def build_client(self) -> ResilientApiClient:
        return ResilientApiClient(
            self.base_url, self.token, self.max_requests_per_window, self.window_seconds
        )


class SqliteResource(ConfigurableResource):
    path: str
    chunk_size: int = 100_000
