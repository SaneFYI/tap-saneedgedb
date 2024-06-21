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

class EdgeDbStream(Stream):
    primary_keys = ["id"]   
    is_sorted = True

    def __init__(self, tap: Tap):
        super().__init__(tap)
        self.logger.info("Connecting to Edgedb instance", self.config.get('edgedb_host'))
        self.client = edgedb.create_client(
            host = self.config.get('edgedb_host'),
            port = self.config.get('edgedb_port'),
            user = self.config.get('edgedb_user'),
            password = self.config.get('edgedb_password'),
            secret_key = self.config.get('edgedb_secret_key'),
            database = self.config.get('edgedb_database'),
            tls_security = self.config.get('edgedb_client_tls_security'),
        )

    def get_last_updated(self, context: Dict) -> datetime:
        starting_timestamp: Optional[datetime] = self.get_starting_timestamp(context)
        default_timestamp = datetime.strptime("2022-01-01T00:00:00Z", '%Y-%m-%dT%H:%M:%SZ')
        timestamp = starting_timestamp if starting_timestamp is not None else default_timestamp
        return timestamp.replace(tzinfo=timezone.utc)

class UserModelStream(EdgeDbStream):
    name = "sane_users"
    replication_key = "created"
    schema = th.PropertiesList(
        th.Property("id", th.StringType),
        th.Property("username", th.StringType),
        th.Property("bio", th.StringType),
        th.Property("created", th.DateTimeType),
        th.Property("deletion", th.BooleanType),
        th.Property("spaces", th.StringType),
        th.Property("following", th.StringType),
    ).to_dict()

    def get_records(self, context: Dict) -> Iterable[dict]:
        last_updated = self.get_last_updated(context)
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
                filter .account_created >= <datetime>$last_updated
                and not exists .account_deletion
                order by .account_created
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
        results = self.client.query(query, last_updated=last_updated)
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

class SpaceModelStream(EdgeDbStream):
    name = "sane_spaces"
    replication_key = "updated"
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
        th.Property("categories", th.StringType),
    ).to_dict()

    def get_records(self, context: Dict) -> Iterable[dict]:
        last_updated = self.get_last_updated(context)
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
                    },
                    categories,
                }
                filter .updated >= <datetime>$last_updated
                and not exists .deletion
                and .is_public
                order by .updated
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
            followers_list := array_agg(spaces.followers.id),
            categories := spaces.categories,
        }
        '''
        results = self.client.query(query, last_updated=last_updated)
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

class SpaceNodeModelStream(EdgeDbStream):
    name = "sane_space_nodes"
    replication_key = "updated"
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
        th.Property("categories", th.StringType),
    ).to_dict()

    def get_records(self, context: Dict) -> Iterable[dict]:
        last_updated = self.get_last_updated(context)
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
                    },
                    categories,
                }
                filter .updated >= <datetime>$last_updated
                and .owning_space.is_public
                and not exists .deletion
                and not exists .owning_space.deletion
                order by .updated
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
            categories := nodes.categories,
        }
        '''
        results = self.client.query(query, last_updated=last_updated)
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
