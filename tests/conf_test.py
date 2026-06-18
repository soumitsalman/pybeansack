import os
import sys
from datetime import datetime
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[2]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

import pytest
from dotenv import load_dotenv

from pybeansack import create_client

load_dotenv()


@pytest.fixture
def pg_db():
    conn = os.getenv("PG_CONNECTION_STRING")
    if not conn:
        pytest.skip("PG_CONNECTION_STRING not set")
    db = create_client("pg", pg_connection_string=conn)
    yield db
    db.close()


@pytest.fixture
def duck_db():
    catalog = os.getenv("DUCKLAKE_CATALOG")
    storage = os.getenv("DUCKLAKE_STORAGE")
    if catalog and storage:
        db = create_client("dl", ducklake_catalog=catalog, ducklake_storage=storage)
    else:
        path = os.getenv("DUCKDB_STORAGE", "/tmp/beansack-test.duckdb")
        db = create_client("duck", duckdb_storage=path)
    yield db
    db.close()


@pytest.fixture
def lance_db():
    root = os.getenv("TEST_STORAGE", "/tmp/beansack-lance")
    path = f"{root}/{datetime.now().strftime('%Y-%m-%d')}-lancedb"
    db = create_client("lance", lancedb_storage=path)
    yield db
    db.close()


@pytest.fixture
def db(request):
    """Indirect parametrization: request.param is pg_db | duck_db | lance_db."""
    return request.getfixturevalue(request.param)
