from dataclasses import dataclass
from typing import Dict, Any, List, Optional

from event.Event import Event
from event.PostEvents import PostUpsert, PostDelete
from .Ingestor import Ingestor


@dataclass
class PostIngestor(Ingestor):
    """
    Converts Jetstream events into internal Events (PostUpsert/PostDelete).

    Output:
      - [] for non-post events
      - [PostUpsert(...)] for create/update of app.bsky.feed.post
      - [PostDelete(...)] for delete of app.bsky.feed.post
    """

    async def handle_event(self, evt: Dict[str, Any]) -> List[Event]:
        if evt.get("kind") != "commit":
            return []

        commit = evt.get("commit") or {}
        if commit.get("collection") != "app.bsky.feed.post":
            return []

        did = evt.get("did")
        rkey = commit.get("rkey")
        if not did or not rkey:
            return []

        operation = commit.get("operation")
        cursor = str(evt.get("time_us")) if evt.get("time_us") is not None else None

        uri = f"at://{did}/app.bsky.feed.post/{rkey}"

        if operation == "delete":
            return [PostDelete(uri=uri, cursor=cursor)]

        if operation not in ("create", "update"):
            return []

        record = commit.get("record") or {}
        cid = commit.get("cid")
        if not cid:
            # For create/update commits, Jetstream normally includes cid. Be defensive.
            return []

        text = record.get("text") or ""
        created_at = record.get("createdAt") or ""

        # Replies: record["reply"] has {"root": {"uri","cid"}, "parent": {"uri","cid"}}
        reply = record.get("reply") or {}
        parent = reply.get("parent") or {}
        root = reply.get("root") or {}

        reply_parent_uri: Optional[str] = parent.get("uri")
        reply_root_uri: Optional[str] = root.get("uri")

        # Optional language(s). Posts may include "langs": ["en", ...]
        lang: Optional[str] = None
        langs = record.get("langs")
        if isinstance(langs, list) and langs:
            if isinstance(langs[0], str):
                lang = langs[0]

        return [
            PostUpsert(
                uri=uri,
                cid=str(cid),
                did=did,
                created_at=created_at,
                text=text,
                reply_parent_uri=reply_parent_uri,
                reply_root_uri=reply_root_uri,
                lang=lang,
                cursor=cursor,
                raw=None,  # or store record/commit if you want debug provenance
            )
        ]

    def wanted_collections(self) -> List[str]:
        return ["app.bsky.feed.post"]
