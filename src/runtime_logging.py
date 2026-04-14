"""Minimal structured logging helpers for runtime workflows."""
from __future__ import annotations

import datetime
import json
from typing import Any


def emit_runtime_log(
    event: str,
    stage: str,
    status: str,
    **fields: Any,
) -> None:
    payload = {
        "ts": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "event": event,
        "stage": stage,
        "status": status,
    }
    for key, value in fields.items():
        if value is not None:
            payload[key] = value
    print(json.dumps(payload, default=str), flush=True)
