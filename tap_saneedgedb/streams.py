"""Stream type classes for tap-saneedgedb."""

from __future__ import annotations

import sys
import typing as t

from singer_sdk import typing as th  # JSON Schema typing helpers
from singer_sdk import Tap, Stream
import edgedb
from datetime import datetime, timezone

from tap_saneedgedb.client import SaneEdgedbTapStream
from tap_saneedgedb.rich_text_serializer import convert_blocks_to_markdown

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

    replication_key = "account_created"
    is_sorted = True

    def __init__(self, tap: Tap):
        super().__init__(tap)
        self.client = edgedb.create_client()

    def get_last_updated(self, context: Dict) -> datetime:
        starting_timestamp: Optional[str] = self.get_starting_timestamp(context)
        default_timestamp = "2022-01-01T00:00:00Z"
        timestamp = starting_timestamp if starting_timestamp is not None else default_timestamp
        return datetime.strptime(timestamp, '%Y-%m-%dT%H:%M:%SZ').replace(tzinfo=timezone.utc)

    def get_records(self, context: Dict) -> Iterable[dict]:
        last_updated = self.get_last_updated(context)
        print(last_updated)
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
                and .account_created >= <datetime>$last_updated
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
        results = self.client.query(query, offset=offset, limit=limit, last_updated=last_updated)
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

class SpaceModelStream(Stream):
    name = "sane_spaces"
    primary_keys = ["id"]
    schema = th.PropertiesList(
        th.Property("id", th.StringType),
        th.Property("title", th.StringType),
        th.Property("description", th.StringType),
        th.Property("created", th.DateTimeType),
        th.Property("updated", th.DateTimeType),
        th.Property("deletion", th.BooleanType),
        th.Property("nodes", th.StringType),
        th.Property("owner", th.StringType),
        th.Property("followers", th.StringType),
        th.Property("is_public", th.BooleanType),
    ).to_dict()


    @property
    def replication_key(self):
        return "updated"

    def __init__(self, tap: Tap):
        super().__init__(tap)
        self.client = edgedb.create_client()

    def get_records(self, context: Dict) -> Iterable[dict]:
        print("This will be captured")
        print("This will be captured")
        print("This will be captured")
        state = self.get_stream_state()
        print("state", state)
        start_date = state.get('updated_at', '1970-01-01T00:00:00Z')
        print("date", start_date)
        offset = context.get("offset", 0)
        limit = context.get("limit", 1000)
        query = '''
        WITH
            spaces := (
                select Space {
                    id,
                    title,
                    description,
                    created,
                    updated,
                    is_public,
                    deletion: {
                        id
                    },
                    owner: {
                        id
                    },
                    nodes: {
                        id
                    },
                    followers: {
                        id
                    }
                }
                filter not exists .deletion
                and .is_public
                order by .created
                offset <int64>$offset
                limit <int64>$limit
            )
        SELECT spaces {
            space_id := spaces.id,
            title := spaces.title,
            description := spaces.description,
            created := spaces.created,
            updated := spaces.updated,
            deleted := exists(spaces.deletion),
            is_public := spaces.is_public,
            owner_id := spaces.owner.id,
            nodes_list := array_agg(spaces.nodes.id),
            followers_list := array_agg(spaces.followers.id)
        }
        '''
        results = self.client.query(query, offset=offset, limit=limit)
        for result in results:
            yield {
                "id": result.id,
                "title": result.title,
                "description": result.description,
                "created": result.created,
                "updated": result.updated,
                "is_public": result.is_public,
                "deletion": result.deleted,
                "owner": result.owner_id,
                "nodes": result.nodes_list,
                "followers": result.followers_list,
            }

class SpaceNodeModelStream(Stream):
    name = "sane_space_nodes"
    primary_keys = ["id"]
    schema = th.PropertiesList(
        th.Property("id", th.StringType),
        th.Property("space_id", th.StringType),
        th.Property("user_id", th.StringType),
        th.Property("title", th.StringType),
        th.Property("deletion", th.BooleanType),
        th.Property("created", th.DateTimeType),
        th.Property("updated", th.DateTimeType),
        th.Property("child_blocks", th.StringType),
        th.Property("node_content", th.StringType),
        th.Property("node_type", th.StringType),
        th.Property("node_url", th.StringType),
    ).to_dict()

    @property
    def replication_key(self):
        return "updated"

    def __init__(self, tap: Tap):
        super().__init__(tap)
        self.client = edgedb.create_client()

    def get_records(self, context: Dict) -> Iterable[dict]:
        offset = context.get("offset", 0)
        limit = context.get("limit", 1000)
        query = '''
        WITH
            nodes := (
                select Node {
                    id,
                    title,
                    updated,
                    created,
                    node_content,
                    node_url,
                    node_type,
                    deletion: {
                        id
                    },
                    creator: {
                        id
                    },
                    child_blocks: {
                        block_type,
                        block_data
                    },
                    owning_space: {
                        id,
                        owner: {
                            id
                        }
                    }
                }
                filter .owning_space.is_public
                and not exists .owning_space.deletion
                and not exists .deletion
                order by .created
                offset <int64>$offset
                limit <int64>$limit
            )
        SELECT nodes {
            node_id := nodes.id,
            space_id := nodes.owning_space.id,
            user_id := nodes.owning_space.owner.id,
            title := nodes.title,
            created := nodes.created,
            updated := nodes.updated,
            deleted := exists(nodes.deletion),
            node_content := nodes.node_content ?? "",
            node_type := nodes.node_type,
            node_url := nodes.node_url ?? "",
            child_blocks := nodes.child_blocks { block_type, block_data },
        }
        '''
        results = self.client.query(query, offset=offset, limit=limit)
        for result in results:
            yield {
                "id": result.id,
                "space_id": result.id,
                "user_id": result.id,
                "title":result.title, 
                "deletion": result.deleted,
                "created": result.created,
                "updated": result.updated,
                "child_blocks": convert_blocks_to_markdown(result.child_blocks) if result.node_type == "Text" else "",
                "node_content": result.node_content,
                "node_type": result.node_type,
                "node_url": result.node_url,
            }
