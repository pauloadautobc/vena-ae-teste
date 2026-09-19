"""Configuração não sensível (defaults = ambiente fornecido pela Vena).

Segredos (API_TOKEN, chave da service account) NÃO têm default aqui: vêm do
ambiente em tempo de execução (ver `resources.py` e `.env.example`).
"""
import os

GCP_PROJECT = os.getenv("GCP_PROJECT", "vena-teste")
BQ_DATASET = os.getenv("BQ_DATASET", "teste_tecnico_ae_paulo_adauto_pazuch_barbosa")
GCS_BUCKET = os.getenv("GCS_BUCKET", "vena-teste-candidato-ae-paulo-adauto-pazuch-barbosa")

API_BASE_URL = os.getenv("API_BASE_URL", "https://api-vendas-teste-vyi7ppqsoq-rj.a.run.app")
SCRAPING_URL = os.getenv(
    "SCRAPING_URL", "https://scraping-precos-teste-vyi7ppqsoq-rj.a.run.app/concorrentes/precos"
)
SQLITE_PATH = os.getenv("SQLITE_PATH", "banco_transacional.sqlite")

# A API aplica 30 req/60 s (medido). Ficamos um pouco abaixo para ter margem.
API_MAX_REQUESTS_PER_WINDOW = 28
API_WINDOW_SECONDS = 60.0
API_PAGE_SIZE = 500  # máximo aceito pela API

# 100k linhas por parte: memória O(chunk), não O(5M).
SQLITE_CHUNK_SIZE = 100_000
