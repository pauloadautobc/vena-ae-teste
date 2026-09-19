"""Scraping com schema drift. Fixtures A/B/C são HTML REAL capturado do serviço;
D é sintética (identificada como tal) e E é uma página de manutenção."""
from pathlib import Path

import pytest

from vena_pipeline.ingest.scraper import (
    ParseResult, PrecoConcorrente, estoque_to_bool, evaluate_quality, parse_currency,
    parse_precos_concorrentes, split_nome_categoria,
)

FX = Path(__file__).parent / "fixtures"


def load(name):
    return parse_precos_concorrentes((FX / name).read_text(encoding="utf-8"))


@pytest.mark.parametrize("fixture,strategy", [
    ("scraping_A_tabela.html", "table_tabela_precos"),
    ("scraping_B_cards.html", "cards_price_grid"),
    ("scraping_C_comparativo.html", "table_comparativo"),
])
def test_each_real_variant_is_recognized_with_12_rows_and_no_errors(fixture, strategy):
    r = load(fixture)
    assert r.strategy_used == strategy
    assert len(r.records) == 12 and r.parse_error_count == 0
    assert all(rec.preco is not None and rec.concorrente and rec.produto for rec in r.records)


def test_all_variants_yield_the_same_normalized_products_and_no_category_in_name():
    products = {}
    for fx in ("scraping_A_tabela.html", "scraping_B_cards.html", "scraping_C_comparativo.html"):
        products[fx] = {rec.produto for rec in load(fx).records}
        assert not any("(Calçados)" in p or "(Acessórios)" in p for p in products[fx])
    assert products["scraping_A_tabela.html"] == products["scraping_B_cards.html"] == products["scraping_C_comparativo.html"]
    assert "Meia Performance (3un)" in products["scraping_C_comparativo.html"]


def test_variant_c_splits_category_from_product():
    rec = load("scraping_C_comparativo.html").records[0]
    assert rec.categoria in ("Calçados", "Acessórios") and "(" not in rec.produto.replace("(3un)", "")


def test_renamed_css_classes_are_handled_by_generic_strategy():
    r = load("scraping_D_classes_renomeadas.html")
    assert r.strategy_used == "generic_table_headers" and len(r.records) == 2
    assert r.records[0].preco == 1234.50 and r.records[0].disponivel is False
    assert r.records[1].estoque_status == "Últimas unidades"


def test_unrecognized_page_never_raises_and_returns_none_strategy():
    r = load("scraping_E_desconhecida.html")
    assert r.strategy_used == "none" and r.records == []


def test_malformed_row_is_dropped_and_counted_not_fatal():
    html = """<table id="tabela-precos">
      <tr class="produto-row"><td class="nome">OK</td><td class="categoria">C</td><td class="preco">R$ 10.00</td>
          <td class="concorrente">A</td><td class="estoque">Em estoque</td></tr>
      <tr class="produto-row"><td class="nome">Quebrado</td></tr></table>"""
    r = parse_precos_concorrentes(html)
    assert [x.produto for x in r.records] == ["OK"] and r.parse_error_count == 1 and r.raw_row_count == 2


@pytest.mark.parametrize("raw,expected", [
    ("R$ 340.04", 340.04), ("354.69", 354.69), ("462.99 BRL", 462.99), ("1.234,56", 1234.56),
    (12, 12.0), (None, None), ("", None), ("indisponível", None),
])
def test_parse_currency(raw, expected):
    assert parse_currency(raw) == expected


def test_three_stock_states():
    assert estoque_to_bool("Em estoque") is True
    assert estoque_to_bool("Últimas unidades") is True  # ainda comprável
    assert estoque_to_bool("Indisponível") is False
    assert estoque_to_bool("???") is None


def test_split_nome_categoria_uses_last_parentheses():
    assert split_nome_categoria("Meia Performance (3un) (Acessórios)") == ("Meia Performance (3un)", "Acessórios")
    assert split_nome_categoria("Sem categoria") == ("Sem categoria", None)


# ------------------------------------------------------ taxa de erro (check)
def _rec(preco):
    return PrecoConcorrente(produto="p", categoria=None, preco=preco, concorrente="c", disponivel=True)


def test_quality_clean_page_passes():
    q = evaluate_quality(load("scraping_C_comparativo.html"))
    assert q.passed and not q.is_error and q.metadata["linhas_processadas"] == 12 and q.metadata["taxa_erro"] == 0.0


def test_quality_counts_dropped_rows_and_unreadable_prices_as_errors():
    r = ParseResult([_rec(None), _rec(1.0), _rec(2.0)], "x", raw_row_count=4, parse_error_count=1)
    q = evaluate_quality(r)
    assert not q.passed and not q.is_error
    assert q.metadata["linhas_descartadas"] == 1 and q.metadata["linhas_preco_ilegivel"] == 1
    assert q.metadata["taxa_erro"] == 0.5


def test_quality_zero_rows_is_a_hard_error():
    q = evaluate_quality(ParseResult([], "none", 0, 0))
    assert not q.passed and q.is_error


def test_quality_threshold_is_inclusive():
    assert evaluate_quality(ParseResult([_rec(1.0)] * 49, "x", 50, 1)).passed  # 2,0%
