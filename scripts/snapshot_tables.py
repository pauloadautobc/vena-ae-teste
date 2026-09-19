"""Snapshot de todas as tabelas do dataset: nº de linhas + fingerprint independente de ordem.

Uso (prova de idempotência):
    python scripts/snapshot_tables.py antes.json
    <rodar o pipeline de novo>
    python scripts/snapshot_tables.py depois.json
    python scripts/snapshot_tables.py --compare antes.json depois.json

Colunas que mudam por definição a cada execução ficam fora do fingerprint:
  * stg_clientes_quarentena.detectado_em (carimbo do run);
  * preços/carimbos do scraping (o serviço devolve preços diferentes a cada request).
`raw_precos_concorrentes` é um log append-only: cresce por design e é ignorada na comparação.
"""
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from google.cloud import bigquery  # noqa: E402

from vena_pipeline import settings  # noqa: E402

EXCLUDE = {
    "stg_clientes_quarentena": "detectado_em",
    "stg_precos_concorrentes": "preco, coletado_em, disponivel, estoque_status, parse_strategy",
    "mart_posicionamento_competitivo": (
        "preco_concorrente, preco_minimo_mercado, preco_medio_mercado, preco_maximo_mercado, ranking_preco, "
        "gap_percentual_vs_menor_preco, concorrente_em_estoque, estoque_status, coletado_em"
    ),
}
APPEND_ONLY = {"raw_precos_concorrentes"}


def snapshot(path: str) -> None:
    client = bigquery.Client(project=settings.GCP_PROJECT)
    out = {}
    for t in sorted(client.list_tables(f"{settings.GCP_PROJECT}.{settings.BQ_DATASET}"), key=lambda x: x.table_id):
        exc = f"EXCEPT ({EXCLUDE[t.table_id]})" if t.table_id in EXCLUDE else ""
        sql = (f"SELECT COUNT(*) AS n, BIT_XOR(FARM_FINGERPRINT(TO_JSON_STRING(t))) AS fp "
               f"FROM (SELECT * {exc} FROM `{settings.GCP_PROJECT}.{settings.BQ_DATASET}.{t.table_id}`) t")
        row = list(client.query(sql).result())[0]
        out[t.table_id] = {"linhas": row["n"], "fingerprint": row["fp"]}
        print(f"{t.table_id:34}{row['n']:>12,} linhas  fp={row['fp']}")
    json.dump(out, open(path, "w"), indent=1)


def compare(a_path: str, b_path: str) -> int:
    a, b = json.load(open(a_path)), json.load(open(b_path))
    diverge = []
    for t in sorted(a):
        same = a[t] == b.get(t)
        note = "idêntico" if same else ("cresce por design (log)" if t in APPEND_ONLY else "DIFERENTE")
        print(f"{t:34}{a[t]['linhas']:>12,} -> {b[t]['linhas']:>12,}  {note}")
        if not same and t not in APPEND_ONLY:
            diverge.append(t)
    print("\nIDEMPOTENTE" if not diverge else f"\nDIVERGÊNCIA em: {diverge}")
    return 1 if diverge else 0


if __name__ == "__main__":
    if sys.argv[1] == "--compare":
        sys.exit(compare(sys.argv[2], sys.argv[3]))
    snapshot(sys.argv[1])
