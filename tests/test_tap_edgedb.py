import pytest
from dotenv import load_dotenv
import os
from tap_saneedgedb.tap import TapSaneEdgedbTap
from datetime import datetime, timezone

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
    start_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    records = list(stream.get_records(context={"limit": 1, "offset": 0, "start_date": start_date }))
    assert len(records) == 1

    # records = list(stream.get_records(context={"limit": 2, "offset": 1}))
    # assert len(records) == 2

    # records = list(stream.get_records(context={"limit": 3, "offset": 3}))
    # assert len(records) == 3

# def test_sane_spaces_stream(config):
#     tap = TapSaneEdgedbTap(config=config)
#     stream = tap.streams["sane_spaces"]

#     # Test first page
#     records = list(stream.get_records(context={"limit": 1, "offset": 0}))
#     assert len(records) == 1

#     records = list(stream.get_records(context={"limit": 2, "offset": 1}))
#     assert len(records) == 2

#     records = list(stream.get_records(context={"limit": 3, "offset": 3}))
#     assert len(records) == 3

# def test_sane_nodes_stream(config):
#     tap = TapSaneEdgedbTap(config=config)
#     stream = tap.streams["sane_space_nodes"]

#     # Test first page
#     records = list(stream.get_records(context={"limit": 1, "offset": 0}))
#     assert len(records) == 1

#     records = list(stream.get_records(context={"limit": 2, "offset": 1}))
#     assert len(records) == 2

#     records = list(stream.get_records(context={"limit": 3, "offset": 3}))
#     assert len(records) == 3
