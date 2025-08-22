from __future__ import annotations

import time
from typing import Any, Dict, Tuple


_store: Dict[str, Tuple[float, Any]] = {}


def set_with_ttl(key: str, value: Any, ttl_seconds: int = 300) -> None:
    expires_at = time.time() + ttl_seconds
    _store[key] = (expires_at, value)


def get_if_fresh(key: str) -> Any | None:
    entry = _store.get(key)
    if not entry:
        return None
    expires_at, value = entry
    if time.time() > expires_at:
        _store.pop(key, None)
        return None
    return value


