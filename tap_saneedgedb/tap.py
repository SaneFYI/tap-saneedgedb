"""SaneEdgedbTap tap class."""

from __future__ import annotations

from singer_sdk import Tap
from singer_sdk import typing as th  # JSON schema typing helpers

from tap_saneedgedb import streams


class TapSaneEdgedbTap(Tap):
    """SaneEdgedbTap tap class."""

    name = "tap-saneedgedb"

    config_jsonschema = th.PropertiesList(
        th.Property(
            "edgedb_host",
            th.StringType,
        ),
        th.Property(
            "edgedb_port",
            th.IntegerType,
        ),
        th.Property(
            "edgedb_user",
            th.StringType,
        ),
        th.Property(
            "edgedb_branch",
            th.StringType,
        ),
        th.Property(
            "edgedb_database",
            th.StringType,
        ),
        th.Property(
            "edgedb_password",
            th.StringType,
            secret=True,
        ),
        th.Property(
            "edgedb_secret_key",
            th.StringType,
            secret=True,
        ),
        th.Property(
            "edgedb_client_tls_security",
            th.StringType,
        ),
    ).to_dict()

    def discover_streams(self) -> list[streams.SaneEdgedbTapStream]:
        """Return a list of discovered streams.

        Returns:
            A list of discovered streams.
        """
        print("connecting")
        return [
            streams.UserModelStream(self),
            streams.SpaceModelStream(self),
            streams.SpaceNodeModelStream(self),
        ]


if __name__ == "__main__":
    TapSaneEdgedbTap.cli()
