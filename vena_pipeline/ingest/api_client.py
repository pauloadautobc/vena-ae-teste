"""
Cliente resiliente da API de vendas (`/api/pedidos`).

Comportamento REAL da API (medido, não suposto):
  * limite de 30 requisições / 60 s -> a 31ª recebe 429 com header `Retry-After`;
  * erros 500 intermitentes ({"error":"internal_error",...}) que somem no retry;
  * 48.000 pedidos em 96 páginas de 500.

Defesa em camadas:
  1. Rate limiter PROATIVO (janela deslizante no cliente): quase nunca leva 429.
  2. Retry REATIVO (tenacity): em 429 espera exatamente o `Retry-After`
     do servidor; em 5xx usa backoff exponencial com jitter.
  3. Uma página que esgota o retry não derruba as demais: vai para uma lista
     de pendentes e ganha novas rodadas de recuperação no final.
  4. Se ainda faltar página, levanta `IncompleteIngestionError` (falhar alto,
     nunca entregar dado incompleto com o asset "verde").
"""
from __future__ import annotations

import logging
import time
from collections import deque
from typing import Any, Iterator, Optional

import requests
from tenacity import (
    RetryCallState,
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential_jitter,
)

from vena_pipeline.logging_utils import log_event

logger = logging.getLogger("vena_pipeline.api")


class RateLimitedError(Exception):
    """429. Carrega o `Retry-After` (segundos) informado pelo servidor."""

    def __init__(self, retry_after: float, message: str = ""):
        self.retry_after = retry_after
        super().__init__(message or f"Rate limited, retry after {retry_after}s")


class TransientServerError(Exception):
    """5xx: erro transitório, elegível a retry."""


class IncompleteIngestionError(Exception):
    """A paginação terminou, mas faltaram páginas mesmo após a recuperação."""

    def __init__(self, message: str, failed_pages: list[int], rows_yielded: int, total_pages: Optional[int]):
        super().__init__(message)
        self.failed_pages = failed_pages
        self.rows_yielded = rows_yielded
        self.total_pages = total_pages


class RateLimiter:
    """Janela deslizante: no máximo `max_requests` a cada `window_seconds`."""

    def __init__(self, max_requests: int, window_seconds: float):
        self.max_requests = max_requests
        self.window_seconds = window_seconds
        self._timestamps: deque[float] = deque()

    def acquire(self) -> None:
        now = time.monotonic()
        while self._timestamps and now - self._timestamps[0] > self.window_seconds:
            self._timestamps.popleft()
        if len(self._timestamps) >= self.max_requests:
            sleep_for = self.window_seconds - (now - self._timestamps[0])
            if sleep_for > 0:
                log_event(logger, logging.INFO, "rate_limiter_throttle", sleep_seconds=round(sleep_for, 2))
                time.sleep(sleep_for)
        self._timestamps.append(time.monotonic())


def _wait_strategy(retry_state: RetryCallState) -> float:
    exc = retry_state.outcome.exception() if retry_state.outcome else None
    if isinstance(exc, RateLimitedError):
        return exc.retry_after  # o servidor diz quanto esperar
    return wait_exponential_jitter(initial=1, max=30)(retry_state)


def _log_retry(retry_state: RetryCallState) -> None:
    exc = retry_state.outcome.exception() if retry_state.outcome else None
    log_event(
        logger,
        logging.WARNING,
        "http_retry",
        attempt=retry_state.attempt_number,
        exception_type=type(exc).__name__ if exc else None,
        exception_message=str(exc)[:200] if exc else None,
    )


class ResilientApiClient:
    def __init__(
        self,
        base_url: str,
        token: str,
        max_requests_per_window: int = 28,
        window_seconds: float = 60.0,
        max_attempts: int = 6,
        timeout_seconds: float = 20.0,
        session: Optional[requests.Session] = None,
    ):
        if not token:
            raise RuntimeError("API_TOKEN não definido (ver .env.example).")
        self.base_url = base_url.rstrip("/")
        self.token = token
        self.max_attempts = max_attempts
        self.timeout_seconds = timeout_seconds
        self._limiter = RateLimiter(max_requests_per_window, window_seconds)
        self._session = session or requests.Session()
        self.last_stats: dict[str, Any] = {}

    def _request_once(self, page: int, page_size: int) -> dict[str, Any]:
        self._limiter.acquire()
        response = self._session.get(
            f"{self.base_url}/api/pedidos",
            params={"page": page, "page_size": page_size},
            headers={"Authorization": f"Bearer {self.token}"},
            timeout=self.timeout_seconds,
        )
        if response.status_code == 429:
            raise RateLimitedError(float(response.headers.get("Retry-After", 5)), response.text[:200])
        if response.status_code >= 500:
            raise TransientServerError(f"{response.status_code}: {response.text[:200]}")
        response.raise_for_status()  # 4xx (ex.: 401 token errado) NÃO é retentado
        return response.json()

    def get_page(self, page: int, page_size: int) -> dict[str, Any]:
        @retry(
            retry=retry_if_exception_type((RateLimitedError, TransientServerError, requests.ConnectionError, requests.Timeout)),
            wait=_wait_strategy,
            stop=stop_after_attempt(self.max_attempts),
            before_sleep=_log_retry,
            reraise=True,
        )
        def _do() -> dict[str, Any]:
            return self._request_once(page, page_size)

        return _do()

    def iter_all_pedidos(self, page_size: int = 500, recovery_passes: int = 1) -> Iterator[dict[str, Any]]:
        """Gera todos os pedidos, página a página (generator: nunca acumula tudo)."""
        _RETRYABLE = (RateLimitedError, TransientServerError, requests.ConnectionError, requests.Timeout)
        page, total_pages, total_records = 1, None, None
        rows, failed_pages = 0, []

        while total_pages is None or page <= total_pages:
            try:
                payload = self.get_page(page, page_size)
            except _RETRYABLE as exc:
                log_event(logger, logging.ERROR, "api_page_failed_after_retries", page=page, error=str(exc)[:200])
                if total_pages is None:
                    # Sem a 1ª página não há total_pages: seguir às cegas seria loop infinito.
                    raise IncompleteIngestionError(
                        f"Página 1 falhou após todas as tentativas: {exc}", [page], 0, None
                    ) from exc
                failed_pages.append(page)
                page += 1
                continue
            total_pages = payload["total_pages"]
            total_records = payload.get("total_records")
            for row in payload["data"]:
                rows += 1
                yield row
            page += 1

        first_pass_failures = len(failed_pages)
        for attempt in range(1, recovery_passes + 1):
            if not failed_pages:
                break
            log_event(logger, logging.WARNING, "api_recovery_pass", attempt=attempt, pending_pages=failed_pages)
            still_failing = []
            for pending in failed_pages:
                try:
                    payload = self.get_page(pending, page_size)
                except _RETRYABLE as exc:
                    log_event(logger, logging.ERROR, "api_page_still_failing", page=pending, error=str(exc)[:200])
                    still_failing.append(pending)
                    continue
                for row in payload["data"]:
                    rows += 1
                    yield row
            failed_pages = still_failing

        self.last_stats = {
            "rows": rows,
            "total_pages": total_pages,
            "total_records": total_records,
            "pages_failed_first_pass": first_pass_failures,
            "pages_recovered": first_pass_failures - len(failed_pages),
            "pages_unrecovered": len(failed_pages),
        }
        log_event(logger, logging.INFO, "api_ingestion_summary", **self.last_stats)
        if failed_pages:
            raise IncompleteIngestionError(
                f"{len(failed_pages)} página(s) não recuperada(s): {failed_pages}",
                failed_pages, rows, total_pages,
            )
