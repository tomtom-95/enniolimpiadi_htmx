import os
from pathlib import Path
import pytest
from fastapi.testclient import TestClient

from .main import app
from . import database

client = TestClient(app)

@pytest.fixture(autouse=True)
def setup_test_db():
    """Create a fresh test database for each test."""
    db_path = Path(os.environ["DATABASE_PATH"])
    schema_path = Path(os.environ["SCHEMA_PATH"])
    database.init_db(db_path, schema_path)

    yield

    db_path.unlink()

def test_get_health():
    res = client.get("/health")
    assert res.status_code == 200

@pytest.mark.parametrize("entity,expected_type,expected_item", [
    ("olympiads", "olimpiadi", "OlympiadA"),
    ("players", "giocatori", "PlayerA"),
    ("events", "eventi", "EventA"),
])
def test_get_entities(entity, expected_type, expected_item):
    res = client.get(f"/api/{entity}")
    assert res.status_code == 200
    assert expected_type in res.text
    assert expected_item in res.text
