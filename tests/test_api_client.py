"""Ingestão da API: retry/backoff, rate limit (429 + Retry-After), 500 intermitente,
recuperação de páginas e falha ALTA (nunca entregar dado incompleto)."""
import pytest
import responses

from vena_pipeline.ingest.api_client import IncompleteIngestionError, RateLimiter, ResilientApiClient

BASE = "https://api.example.invalid"
URL = f"{BASE}/api/pedidos"
ERR_500 = {"error": "internal_error", "message": "Erro temporário, tente novamente"}


def page(n, total_pages, rows):
    return {"data": rows, "has_next": n < total_pages, "page": n, "page_size": 1,
            "total_pages": total_pages, "total_records": total_pages}


def client(**kw):
    return ResilientApiClient(BASE, "test-token", max_attempts=kw.pop("max_attempts", 3), **kw)


@responses.activate
def test_sends_bearer_token_and_pagination_params():
    responses.add(responses.GET, URL, json=page(1, 1, [{"pedido_id": 1}]))
    client().get_page(1, 500)
    req = responses.calls[0].request
    assert req.headers["Authorization"] == "Bearer test-token"
    assert "page=1" in req.url and "page_size=500" in req.url


@responses.activate
def test_429_waits_for_retry_after_header_then_succeeds(monkeypatch):
    slept = []
    monkeypatch.setattr("time.sleep", lambda s: slept.append(s))
    responses.add(responses.GET, URL, json={"error": "rate_limited"}, status=429, headers={"Retry-After": "7"})
    responses.add(responses.GET, URL, json=page(1, 1, [{"pedido_id": 1}]))
    client().get_page(1, 1)
    assert len(responses.calls) == 2
    assert 7.0 in slept  # esperou exatamente o que o servidor pediu


@responses.activate
def test_500_is_retried_with_backoff_then_succeeds():
    responses.add(responses.GET, URL, json=ERR_500, status=500)
    responses.add(responses.GET, URL, json=page(1, 1, [{"pedido_id": 1}]))
    assert client().get_page(1, 1)["total_records"] == 1
    assert len(responses.calls) == 2


@responses.activate
def test_gives_up_after_max_attempts():
    for _ in range(5):
        responses.add(responses.GET, URL, json=ERR_500, status=500)
    with pytest.raises(Exception, match="500"):
        client(max_attempts=3).get_page(1, 1)
    assert len(responses.calls) == 3


@responses.activate
def test_401_is_not_retried():
    responses.add(responses.GET, URL, json={"error": "unauthorized"}, status=401)
    with pytest.raises(Exception):
        client().get_page(1, 1)
    assert len(responses.calls) == 1  # erro de credencial não adianta retentar


@responses.activate
def test_page_failing_first_pass_is_recovered_in_recovery_pass():
    responses.add(responses.GET, URL, json=page(1, 2, [{"pedido_id": 1}]))
    responses.add(responses.GET, URL, json=ERR_500, status=500)
    responses.add(responses.GET, URL, json=ERR_500, status=500)
    responses.add(responses.GET, URL, json=page(2, 2, [{"pedido_id": 2}]))
    c = client(max_attempts=2)
    rows = list(c.iter_all_pedidos(page_size=1))
    assert sorted(r["pedido_id"] for r in rows) == [1, 2]
    assert c.last_stats["pages_failed_first_pass"] == 1 and c.last_stats["pages_recovered"] == 1


@responses.activate
def test_page_that_never_recovers_fails_loudly():
    responses.add(responses.GET, URL, json=page(1, 2, [{"pedido_id": 1}]))
    for _ in range(10):
        responses.add(responses.GET, URL, json=ERR_500, status=500)
    c, seen = client(max_attempts=2), []
    with pytest.raises(IncompleteIngestionError) as exc:
        for row in c.iter_all_pedidos(page_size=1):
            seen.append(row)
    assert seen == [{"pedido_id": 1}]          # a página boa foi entregue
    assert exc.value.failed_pages == [2]       # a faltante é reportada


@responses.activate
def test_first_page_failure_raises_instead_of_looping_forever():
    for _ in range(20):
        responses.add(responses.GET, URL, json=ERR_500, status=500)
    with pytest.raises(IncompleteIngestionError):
        list(client(max_attempts=2).iter_all_pedidos(page_size=1))
    assert len(responses.calls) == 2


def test_missing_token_fails_fast():
    with pytest.raises(RuntimeError, match="API_TOKEN"):
        ResilientApiClient(BASE, "")


def test_rate_limiter_blocks_when_window_is_full(monkeypatch):
    slept = []
    monkeypatch.setattr("time.sleep", lambda s: slept.append(s))
    limiter = RateLimiter(max_requests=3, window_seconds=60)
    for _ in range(3):
        limiter.acquire()
    assert slept == []
    limiter.acquire()  # a 4ª dentro da janela precisa esperar
    assert len(slept) == 1 and slept[0] > 0
