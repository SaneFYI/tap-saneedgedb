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
            "stream_name",
            th.StringType,
        ),
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
        stream_by_name = {
            "sane_users": streams.UserModelStream,
            "sane_spaces": streams.SpaceModelStream,
            "sane_space_nodes": streams.SpaceNodeModelStream,
        }
        if self.config.get("stream_name"):
            return [stream_by_name[self.config["stream_name"]](self)]

        return [
            streams.UserModelStream(self),
            streams.SpaceModelStream(self),
            streams.SpaceNodeModelStream(self),
        ]


if __name__ == "__main__":
    TapSaneEdgedbTap.cli()
