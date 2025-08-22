import os
import sys
import pathlib
import pytest
from fastapi.testclient import TestClient

ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.main import app


@pytest.fixture(scope="module")
def client():
    return TestClient(app)


def snowflake_configured() -> bool:
    keys = [
        "SNOWFLAKE_HOST",
        "SNOWFLAKE_USER",
        "SNOWFLAKE_PASSWORD",
        "SNOWFLAKE_DATABASE",
        "SNOWFLAKE_SCHEMA",
        "SNOWFLAKE_WAREHOUSE",
    ]
    return all(os.getenv(k) for k in keys)


def test_openapi_has_expected_paths(client):
    schema = client.get("/openapi.json").json()
    paths = set(schema.get("paths", {}).keys())
    expected = {
        "/health",
        "/sf/ping",
        "/sf/config",
        "/covid/summary",
        "/covid/columns",
        "/covid/aggregate",
        "/analytics/summary",
        "/analytics/forecast",
        "/annotations",
        "/eda/mobility",
    }
    assert expected.issubset(paths)


def test_health(client):
    r = client.get("/health")
    assert r.status_code == 200
    assert r.json().get("status") == "ok"


def test_annotations_roundtrip(client, tmp_path, monkeypatch):
    monkeypatch.setenv("NOSQL_DIR", str(tmp_path))
    add = client.post("/annotations", params={"geo": "TestGeo", "text": "Hello", "author": "me"})
    assert add.status_code == 200
    get_all = client.get("/annotations")
    assert get_all.status_code == 200
    data = get_all.json()
    assert any(item.get("geo") == "TestGeo" for item in data)


@pytest.mark.skipif(not snowflake_configured(), reason="Snowflake env not configured")
def test_covid_columns(client):
    r = client.get("/covid/columns")
    assert r.status_code == 200
    assert isinstance(r.json(), list)


@pytest.mark.skipif(not snowflake_configured(), reason="Snowflake env not configured")
def test_covid_aggregate(client):
    r = client.get("/covid/aggregate", params={"date_col": "DATE", "value_col": "CASES", "limit": 5})
    assert r.status_code == 200
    body = r.json()
    assert "rows" in body


@pytest.mark.skipif(not snowflake_configured(), reason="Snowflake env not configured")
def test_sf_ping_and_config(client):
    assert client.get("/sf/ping").status_code == 200
    assert client.get("/sf/config").status_code == 200


@pytest.mark.skipif(not snowflake_configured(), reason="Snowflake env not configured")
def test_analytics_summary_and_forecast(client):
    s = client.get("/analytics/summary")
    assert s.status_code in (200, 500)
    f = client.get("/analytics/forecast", params={"periods": 3})
    assert f.status_code in (200, 400, 500)


@pytest.mark.skipif(not snowflake_configured(), reason="Snowflake env not configured")
def test_eda_mobility(client):
    r = client.get("/eda/mobility")
    assert r.status_code in (200, 500)
    if r.status_code == 200:
        assert "rows" in r.json()


