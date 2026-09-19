"""Leitura em partes (requisito das 5M linhas): nunca acima de chunk_size, sem perder linha."""
import sqlite3

import pyarrow.parquet as pq
import pytest

from vena_pipeline.ingest.sqlite_reader import extract_large_table_in_parts, extract_small_table


@pytest.fixture
def db(tmp_path):
    path = str(tmp_path / "fake.sqlite")
    con = sqlite3.connect(path)
    con.execute("CREATE TABLE itens_pedido (item_id INTEGER, pedido_id INTEGER, valor REAL)")
    con.executemany("INSERT INTO itens_pedido VALUES (?,?,?)", [(i, i // 3, i * 1.5) for i in range(1, 10_001)])
    con.execute("CREATE TABLE produtos (produto_id INTEGER, nome TEXT)")
    con.executemany("INSERT INTO produtos VALUES (?,?)", [(1, "A"), (2, None)])
    con.commit()
    con.close()
    return path


def test_parts_never_exceed_chunk_size_and_cover_all_rows(db, tmp_path):
    out = tmp_path / "out"
    parts = list(extract_large_table_in_parts(db, "itens_pedido", str(out), chunk_size=1_000))
    assert len(parts) == 10 and all(p["rows_in_chunk"] <= 1_000 for p in parts)
    assert parts[-1]["rows_total"] == 10_000
    assert sum(pq.read_table(p["path"]).num_rows for p in parts) == 10_000


def test_last_partial_chunk(db, tmp_path):
    parts = list(extract_large_table_in_parts(db, "itens_pedido", str(tmp_path / "o"), chunk_size=3_333))
    assert parts[-1]["rows_in_chunk"] == 10_000 % 3_333 and parts[-1]["rows_total"] == 10_000


def test_no_row_is_duplicated_or_lost_across_parts(db, tmp_path):
    ids = []
    for p in extract_large_table_in_parts(db, "itens_pedido", str(tmp_path / "o"), chunk_size=999):
        ids += pq.read_table(p["path"]).column("item_id").to_pylist()
    assert ids == list(range(1, 10_001))  # ordem física (rowid), sem duplicar nem perder


def test_small_table_keeps_nulls(db, tmp_path):
    out = str(tmp_path / "p.parquet")
    assert extract_small_table(db, "produtos", out) == 2
    assert pq.read_table(out).column("nome").to_pylist() == ["A", None]


def test_source_database_is_opened_read_only(db):
    from vena_pipeline.ingest.sqlite_reader import _connect
    con = _connect(db)
    with pytest.raises(sqlite3.OperationalError):
        con.execute("INSERT INTO produtos VALUES (3, 'X')")
    con.close()
