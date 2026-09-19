"""Tira os `sleep` reais do backoff/rate-limit dos testes: o que se verifica é o
comportamento (quantas tentativas, em que ordem), não os segundos de espera."""
import pytest


@pytest.fixture(autouse=True)
def _no_real_sleep(monkeypatch):
    monkeypatch.setattr("time.sleep", lambda *_a, **_k: None)
