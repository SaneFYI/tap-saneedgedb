"""Stream type classes for tap-saneedgedb."""

from __future__ import annotations

import sys
import typing as t

from singer_sdk import typing as th  # JSON Schema typing helpers
from singer_sdk import Tap, Stream
import edgedb

from tap_saneedgedb.client import SaneEdgedbTapStream

if sys.version_info >= (3, 9):
    import importlib.resources as importlib_resources
else:
    import importlib_resources

SCHEMAS_DIR = importlib_resources.files(__package__) / "schemas"

class UserModelStream(Stream):
    name = "sane_users"
    primary_keys = ["id"]
    schema = th.PropertiesList(
        th.Property("id", th.StringType),
        th.Property("username", th.StringType),
        th.Property("bio", th.StringType),
        th.Property("created", th.DateTimeType),
        th.Property("deletion", th.BooleanType),
        th.Property("spaces", th.StringType),
        th.Property("following", th.StringType),
    ).to_dict()

    def __init__(self, tap: Tap):
        super().__init__(tap)
        self.client = edgedb.create_client()

    def get_records(self, context: Dict) -> Iterable[dict]:
        offset = context.get("offset", 0)
        limit = context.get("limit", 1000)
        query = '''
        with
            users := (
                select User {
                    id,
                    username,
                    bio,
                    account_created,
                    account_deletion: {
                        id
                    },
                    following: {
                        id
                    },
                    spaces: {
                        id
                    }
                }
                filter not exists .account_deletion
                order by .account_created
                offset <int64>$offset
                limit <int64>$limit
            )
        select users {
            user_id := users.id,
            username := users.username,
            bio := users.bio,
            created := users.account_created,
            deletion := exists(users.account_deletion),
            space_list := array_agg(users.spaces.id),
            following_list := array_agg(users.following.id)
        }
        '''
        results = self.client.query(query, offset=offset, limit=limit)
        for result in results:
            yield {
                "id": result.user_id,
                "username": result.username,
                "bio": result.bio,
                "created": result.created,
                "deletion": result.deletion,
                "spaces": result.space_list,
                "following": result.following_list,
            }
