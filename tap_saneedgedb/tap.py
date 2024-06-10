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
            required=True,
        ),
        th.Property(
            "edgedb_port",
            th.IntegerType,
            default=5656
        ),
        th.Property(
            "edgedb_port",
            th.StringType,
            default="edgedb"
        ),
        th.Property(
            "edgedb_password",
            th.StringType,
            required=True,
            secret=True,
        ),
        th.Property(
            "edgedb_client_tls_security",
            th.StringType,
            default="strict"
        ),
    ).to_dict()

    def discover_streams(self) -> list[streams.SaneEdgedbTapStream]:
        """Return a list of discovered streams.

        Returns:
            A list of discovered streams.
        """
        return [
            streams.UserModelStream(self),
        ]


if __name__ == "__main__":
    TapSaneEdgedbTap.cli()
