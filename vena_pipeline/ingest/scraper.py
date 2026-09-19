"""
Scraping resiliente da página de preços de concorrentes.

Fato medido (30 requisições seguidas): o serviço ROTACIONA entre 3 estruturas
de HTML (10/10/10), com o mesmo conteúdo lógico:

  A  <table id="tabela-precos"> ... <td class="nome|categoria|preco|concorrente|estoque">
     preço "R$ 340.04"
  B  <div id="price-grid"> <div class="price-card" data-product data-store> <span class="pc-*">
     preço "354.69"
  C  <table class="comparativo"> <td class="col-loja|col-item|col-valor|col-disp">
     produto e categoria fundidos: "Tênis Runner Pro (Calçados)"

E 3 estados de estoque: "Em estoque", "Últimas unidades", "Indisponível".

Estratégia: cadeia de responsabilidade. Cada estratégia reconhece e lê só a
sua variante; a última (genérica) lê qualquer <table> pelo texto dos <th>,
sem depender de id/classe CSS — uma 4ª variante com classes renomeadas
continua funcionando. Se NADA reconhece a página, o parser NÃO levanta: devolve
`strategy_used="none"` e quem decide (o asset) é o retry do Dagster.
Uma linha malformada é descartada e contada; nunca derruba a página inteira.
"""
from __future__ import annotations

import logging
import re
import unicodedata
from dataclasses import dataclass, field
from typing import Optional

from bs4 import BeautifulSoup

from vena_pipeline.logging_utils import log_event

logger = logging.getLogger("vena_pipeline.scraper")


@dataclass
class PrecoConcorrente:
    produto: str
    categoria: Optional[str]
    preco: Optional[float]
    concorrente: str
    disponivel: Optional[bool]
    estoque_status: Optional[str] = None
    parse_strategy: str = field(default="", repr=False)


@dataclass
class ParseResult:
    records: list[PrecoConcorrente]
    strategy_used: str
    raw_row_count: int  # linhas lidas + descartadas
    parse_error_count: int  # linhas descartadas por malformação


# ---------------------------------------------------------------- helpers
_TOKEN_MOEDA = re.compile(r"(?i)r\$|\bbrl\b")
_BR_MILHAR = re.compile(r"^\d{1,3}(\.\d{3})+,\d{2}$")


def parse_currency(value) -> Optional[float]:
    """"R$ 340.04" | "354.69" | "462.99 BRL" | "1.234,56" -> float; ilegível -> None."""
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    text = _TOKEN_MOEDA.sub("", str(value)).strip()
    if not text:
        return None
    text = text.replace(".", "").replace(",", ".") if _BR_MILHAR.match(text) else text.replace(",", "")
    try:
        return float(text)
    except ValueError:
        return None


def estoque_to_bool(text: Optional[str]) -> Optional[bool]:
    if not text:
        return None
    t = text.strip().lower()
    if "indispon" in t or "esgotad" in t:
        return False
    if "estoque" in t or "unidade" in t:  # "Em estoque" e "Últimas unidades" = comprável
        return True
    return None


_NOME_CATEGORIA = re.compile(r"^(?P<nome>.+?)\s*\((?P<categoria>[^()]+)\)\s*$")


def split_nome_categoria(texto: str) -> tuple[str, Optional[str]]:
    """"Meia Performance (3un) (Acessórios)" -> ("Meia Performance (3un)", "Acessórios")."""
    m = _NOME_CATEGORIA.match(texto.strip())
    return (m.group("nome").strip(), m.group("categoria").strip()) if m else (texto.strip(), None)


def _text(node) -> Optional[str]:
    return node.get_text(strip=True) if node is not None else None


def _record(strategy, produto, categoria, preco_raw, concorrente, estoque) -> PrecoConcorrente:
    return PrecoConcorrente(
        produto=produto,
        categoria=categoria,
        preco=parse_currency(preco_raw),
        concorrente=concorrente,
        disponivel=estoque_to_bool(estoque),
        estoque_status=estoque.strip() if estoque and estoque.strip() else None,
        parse_strategy=strategy,
    )


# ------------------------------------------------------------- estratégias
class _StrategyA:
    name = "table_tabela_precos"

    def matches(self, soup):
        return soup.select_one("table#tabela-precos") is not None

    def parse(self, soup):
        out, errors = [], 0
        for row in soup.select("table#tabela-precos tr.produto-row"):
            try:
                out.append(_record(
                    self.name, _text(row.select_one("td.nome")).strip(), _text(row.select_one("td.categoria")),
                    _text(row.select_one("td.preco")), _text(row.select_one("td.concorrente")).strip(),
                    _text(row.select_one("td.estoque")),
                ))
            except (AttributeError, TypeError):
                errors += 1
                log_event(logger, logging.WARNING, "scraping_row_malformed", strategy=self.name)
        return out, errors


class _StrategyB:
    name = "cards_price_grid"

    def matches(self, soup):
        return soup.select_one("div#price-grid") is not None

    def parse(self, soup):
        out, errors = [], 0
        for card in soup.select("div#price-grid .price-card"):
            produto, loja = card.get("data-product"), card.get("data-store")
            price = card.select_one(".pc-price")
            if not produto or not loja or price is None:
                errors += 1
                log_event(logger, logging.WARNING, "scraping_row_malformed", strategy=self.name)
                continue
            out.append(_record(self.name, produto.strip(), _text(card.select_one(".pc-cat")),
                               _text(price), loja.strip(), _text(card.select_one(".pc-stock"))))
        return out, errors


class _StrategyC:
    name = "table_comparativo"

    def matches(self, soup):
        return soup.select_one("table.comparativo") is not None

    def parse(self, soup):
        out, errors = [], 0
        for row in soup.select("table.comparativo tbody tr"):
            try:
                nome, categoria = split_nome_categoria(_text(row.select_one("td.col-item")))
                out.append(_record(self.name, nome, categoria, _text(row.select_one("td.col-valor")),
                                   _text(row.select_one("td.col-loja")).strip(), _text(row.select_one("td.col-disp"))))
            except (AttributeError, TypeError):
                errors += 1
                log_event(logger, logging.WARNING, "scraping_row_malformed", strategy=self.name)
        return out, errors


def _norm(text: str) -> str:
    d = unicodedata.normalize("NFKD", text.lower())
    return "".join(c for c in d if not unicodedata.combining(c)).strip()


_HEADERS = [
    ("produto", ("produto", "item", "nome")),
    ("categoria", ("categoria",)),
    ("preco", ("preco", "valor")),
    ("concorrente", ("loja", "concorrente")),
    ("estoque", ("estoque", "disponib")),
]


class _StrategyGeneric:
    """Última rede: qualquer <table> cujos <th> pareçam produto + preço + loja."""

    name = "generic_table_headers"

    @staticmethod
    def _columns(table):
        mapping = {}
        for idx, th in enumerate(table.select("th")):
            h = _norm(th.get_text())
            for field_name, keys in _HEADERS:
                if field_name not in mapping and any(k in h for k in keys):
                    mapping[field_name] = idx
                    break
        return mapping if {"produto", "preco", "concorrente"} <= mapping.keys() else None

    def matches(self, soup):
        return any(self._columns(t) for t in soup.select("table"))

    def parse(self, soup):
        out, errors = [], 0
        for table in soup.select("table"):
            cols = self._columns(table)
            if not cols:
                continue
            for row in table.select("tr"):
                cells = row.select("td")
                if not cells:
                    continue
                try:
                    produto = cells[cols["produto"]].get_text(strip=True)
                    categoria = cells[cols["categoria"]].get_text(strip=True) if "categoria" in cols else None
                    if categoria is None:
                        produto, categoria = split_nome_categoria(produto)
                    estoque = cells[cols["estoque"]].get_text(strip=True) if "estoque" in cols else None
                    out.append(_record(self.name, produto, categoria, cells[cols["preco"]].get_text(strip=True),
                                       cells[cols["concorrente"]].get_text(strip=True), estoque))
                except IndexError:
                    errors += 1
                    log_event(logger, logging.WARNING, "scraping_row_malformed", strategy=self.name)
        return out, errors


# da mais específica para a mais genérica; a primeira que reconhecer vence
STRATEGIES = [_StrategyA(), _StrategyB(), _StrategyC(), _StrategyGeneric()]


def parse_precos_concorrentes(html: str) -> ParseResult:
    soup = BeautifulSoup(html, "lxml")
    for strategy in STRATEGIES:
        if strategy.matches(soup):
            records, errors = strategy.parse(soup)
            return ParseResult(records, strategy.name, len(records) + errors, errors)
    log_event(logger, logging.ERROR, "scraping_schema_unrecognized", html_snippet=html[:300])
    return ParseResult([], "none", 0, 0)


# ------------------------------------------------- métrica de qualidade
MAX_ERROR_RATE = 0.02


@dataclass(frozen=True)
class ScrapeQuality:
    passed: bool
    is_error: bool  # True = nenhuma linha lida (falha dura); False = só alerta de taxa
    metadata: dict


def evaluate_quality(result: ParseResult, max_error_rate: float = MAX_ERROR_RATE) -> ScrapeQuality:
    """Erro = linha descartada (malformada) + linha lida com preço ilegível."""
    total = result.raw_row_count
    preco_ilegivel = sum(1 for r in result.records if r.preco is None)
    erros = result.parse_error_count + preco_ilegivel
    taxa = (erros / total) if total else 1.0
    return ScrapeQuality(
        passed=total > 0 and taxa <= max_error_rate,
        is_error=total == 0,
        metadata={
            "linhas_processadas": total,
            "linhas_descartadas": result.parse_error_count,
            "linhas_preco_ilegivel": preco_ilegivel,
            "taxa_erro": round(taxa, 4),
            "limiar": max_error_rate,
            "estrategia": result.strategy_used,
        },
    )
