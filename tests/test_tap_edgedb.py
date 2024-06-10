import pytest
from dotenv import load_dotenv
import os
from tap_saneedgedb.tap import TapSaneEdgedbTap

load_dotenv(os.path.join(os.path.dirname(__file__), '..', '.env'))

@pytest.fixture
def config():
    return {
        "edgedb_host": os.getenv("EDGEDB_HOST"),
        "edgedb_port": os.getenv("EDGEDB_PORT"),
        "edgedb_user": os.getenv("EDGEDB_USER"),
        "edgedb_password": os.getenv("EDGEDB_PASSWORD"),
        "edgedb_client_tls_security": os.getenv("EDGEDB_CLIENT_TLS_SECURITY", "strict")
    }

def test_sane_users_stream(config):
    tap = TapSaneEdgedbTap(config=config)
    stream = tap.streams["sane_users"]

    # Test first page
    records = list(stream.get_records(context={"limit": 5, "offset": 0}))
    assert len(records) == 5

    records = list(stream.get_records(context={"limit": 5, "offset": 5}))
    assert len(records) == 5

    records = list(stream.get_records(context={"limit": 5, "offset": 10}))
    assert len(records) == 5
