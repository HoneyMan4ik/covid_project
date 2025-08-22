from __future__ import annotations

import os
from datetime import datetime
from typing import Dict, List

from tinydb import TinyDB, Query


def _db_path() -> str:
    base_dir = os.getenv("NOSQL_DIR", os.path.join(os.getcwd(), "data"))
    os.makedirs(base_dir, exist_ok=True)
    return os.path.join(base_dir, "annotations.json")


def get_db() -> TinyDB:
    return TinyDB(_db_path())


def add_annotation(geo: str, text: str, author: str | None = None) -> Dict[str, str]:
    db = get_db()
    payload = {
        "geo": geo,
        "text": text,
        "author": author or "anonymous",
        "created_at": datetime.utcnow().isoformat() + "Z",
    }
    db.insert(payload)
    return payload


def list_annotations(geo: str | None = None) -> List[Dict[str, str]]:
    db = get_db()
    if not geo:
        return list(db.all())
    Q = Query()
    return list(db.search(Q.geo == geo))


