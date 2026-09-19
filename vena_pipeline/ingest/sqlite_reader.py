"""
Leitura do SQLite sem carregar tudo em memória.

`itens_pedido` tem 5.000.000 linhas: o ponto do teste é NÃO usar `fetchall()`
nem `pandas.read_sql` sem `chunksize`. Aqui lemos com um único cursor e
`fetchmany(chunk_size)`, escrevemos cada chunk como uma parte Parquet e o
descartamos antes do próximo -> memória O(chunk_size), não O(5M).

`ORDER BY rowid` (e não `item_id`): `itens_pedido` não tem índice em item_id,
então `ORDER BY item_id` obrigaria o SQLite a ordenar 5M linhas em B-tree
temporário; rowid é a ordem física da tabela e sai sem custo. Sem OFFSET
(que reescanearia a tabela a cada chunk e viraria O(n²)).
"""
from __future__ import annotations

import logging
import sqlite3
from pathlib import Path
from typing import Iterator

import pyarrow as pa
import pyarrow.parquet as pq

from vena_pipeline.logging_utils import log_event

logger = logging.getLogger("vena_pipeline.sqlite")


def _connect(path: str) -> sqlite3.Connection:
    # somente leitura: o extrator nunca altera o banco de origem
    return sqlite3.connect(f"file:{Path(path).as_posix()}?mode=ro", uri=True)


def _to_table(columns: list[str], rows: list[tuple]) -> pa.Table:
    return pa.table({col: [row[i] for row in rows] for i, col in enumerate(columns)})


def extract_small_table(sqlite_path: str, table: str, output_path: str) -> int:
    """clientes (6k) e produtos (800): leitura única — a técnica certa para o volume."""
    con = _connect(sqlite_path)
    try:
        cur = con.execute(f"SELECT * FROM {table}")
        columns = [d[0] for d in cur.description]
        rows = cur.fetchall()
    finally:
        con.close()
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(_to_table(columns, rows), output_path)
    log_event(logger, logging.INFO, "sqlite_table_extracted", table=table, rows=len(rows))
    return len(rows)


def extract_large_table_in_parts(
    sqlite_path: str, table: str, output_dir: str, chunk_size: int = 100_000, order_by: str = "rowid"
) -> Iterator[dict]:
    """Generator: escreve `part-00001.parquet`, ... e devolve metadados de cada parte."""
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    con = _connect(sqlite_path)
    try:
        cur = con.execute(f"SELECT * FROM {table} ORDER BY {order_by}")
        columns = [d[0] for d in cur.description]
        part, total = 0, 0
        while True:
            chunk = cur.fetchmany(chunk_size)
            if not chunk:
                break
            part += 1
            total += len(chunk)
            path = str(Path(output_dir) / f"part-{part:05d}.parquet")
            pq.write_table(_to_table(columns, chunk), path)
            log_event(logger, logging.INFO, "sqlite_chunk_extracted", table=table, part=part,
                      rows_in_chunk=len(chunk), rows_total=total)
            yield {"part": part, "rows_in_chunk": len(chunk), "rows_total": total, "path": path}
    finally:
        con.close()
