"""Logs estruturados: cada evento vira UMA linha JSON na própria mensagem.

`logger.info("evento", extra={...})` do stdlib não imprime o `extra` com o
formatter padrão (nem no log de run do Dagster). Serializar o evento na
mensagem faz o dado estruturado aparecer em qualquer destino e continuar
sendo pesquisável (grep/jq).
"""
import json
import logging
from typing import Any


def log_event(logger: logging.Logger, level: int, event: str, **fields: Any) -> None:
    logger.log(level, json.dumps({"event": event, **fields}, default=str, ensure_ascii=False))
