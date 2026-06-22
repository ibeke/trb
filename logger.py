from __future__ import annotations

import json
import logging
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

_LOG_DIR = Path(os.getenv("LOG_DIR", "logs"))
_LOG_DIR.mkdir(parents=True, exist_ok=True)


class JsonFormatter(logging.Formatter):
    """Minden log rekordot egysoros JSON-ként formáz."""

    def format(self, record: logging.LogRecord) -> str:  # noqa: A003
        payload: dict = {}
        # Az 'extra' mezők közül a 'event_data' dict közvetlenül kerül bele
        event_data: dict = getattr(record, "event_data", {})
        payload["timestamp"] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        payload["level"] = record.levelname
        payload["event"] = event_data.pop("event", record.getMessage())
        payload.update(event_data)
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        return json.dumps(payload, ensure_ascii=False, default=str)


def get_logger(name: str = "trading_bot") -> logging.Logger:
    logger = logging.getLogger(name)
    if logger.handlers:
        return logger
    logger.setLevel(logging.DEBUG)

    # Konzol handler
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.INFO)
    ch.setFormatter(JsonFormatter())

    # Fájl handler — napi rotáció helyett egyszerű dátum-prefix névvel
    today = datetime.now(timezone.utc).strftime("%Y%m%d")
    fh = logging.FileHandler(_LOG_DIR / f"{today}_bot.log", encoding="utf-8")
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(JsonFormatter())

    logger.addHandler(ch)
    logger.addHandler(fh)
    return logger


def log_event(logger: logging.Logger, event: str, level: str = "INFO", **kwargs) -> None:
    """Segédfüggvény strukturált esemény naplózáshoz."""
    kwargs["event"] = event
    lvl = getattr(logging, level.upper(), logging.INFO)
    logger.log(lvl, event, extra={"event_data": kwargs})
