from dataclasses import dataclass
from datetime import datetime
from typing import Optional, Any, List, Dict

from .Event import Event


@dataclass(frozen=True)
class PostUpsert(Event):
    uri: str = ""
    cid: str = ""
    did: str = ""
    created_at: datetime = None
    text: str = ""
    reply_parent_uri: Optional[str] = None
    reply_root_uri: Optional[str] = None
    langs: Optional[List[str]] = None
    tags: Optional[List[str]] = None
    embed: Optional[dict[str, Any]] = None
    # optional extra metadata
    raw: Optional[dict[str, Any]] = None

    @classmethod
    def from_event(cls, evt: Dict[str, Any]) -> Optional["PostUpsert"]:
        if evt.get("kind") != "commit":
            return None

        commit = evt.get("commit") or {}
        if commit.get("collection") != "app.bsky.feed.post":
            return None

        operation = commit.get("operation")
        if operation not in ("create", "update"):
            return None

        did = evt.get("did") or commit.get("did")
        rkey = commit.get("rkey")
        if not did or not rkey:
            return None

        cid = commit.get("cid")
        if not cid:
            return None

        record = commit.get("record")
        if not isinstance(record, dict):
            return None

        created_at = cls._parse_created_at(record.get("createdAt"))
        if created_at is None:
            return None

        reply = record.get("reply") or {}
        parent = reply.get("parent") or {}
        root = reply.get("root") or {}

        record_langs = record.get("langs")
        langs: Optional[List[str]] = None
        if isinstance(record_langs, list):
            cleaned = [l for l in record_langs if isinstance(l, str)]
            if cleaned:
                langs = cleaned

        tags = cls._extract_tags(record)
        embed = cls._extract_embed(record.get("embed"))

        return cls(
            uri=f"at://{did}/app.bsky.feed.post/{rkey}",
            cid=str(cid),
            did=did,
            created_at=created_at,
            text=record.get("text") or "",
            reply_parent_uri=parent.get("uri"),
            reply_root_uri=root.get("uri"),
            langs=langs,
            tags=tags,
            embed=embed,
            cursor=str(evt.get("time_us")) if evt.get("time_us") is not None else None,
            raw=None,
        )

    @staticmethod
    def _parse_created_at(value: Any) -> Optional[datetime]:
        if not value or not isinstance(value, str):
            return None
        try:
            if value.endswith("Z"):
                value = value[:-1] + "+00:00"
            return datetime.fromisoformat(value)
        except ValueError:
            return None

    @staticmethod
    def _extract_tags(record: Dict[str, Any]) -> Optional[List[str]]:
        facets = record.get("facets")
        if not isinstance(facets, list):
            return None

        tags: List[str] = []
        seen = set()
        for facet in facets:
            if not isinstance(facet, dict):
                continue
            features = facet.get("features")
            if not isinstance(features, list):
                continue
            for feature in features:
                if not isinstance(feature, dict):
                    continue
                if feature.get("$type") != "app.bsky.richtext.facet#tag":
                    continue
                tag = feature.get("tag")
                if isinstance(tag, str) and tag not in seen:
                    seen.add(tag)
                    tags.append(tag)

        return tags or None

    @staticmethod
    def _extract_embed(embed: Any) -> Optional[dict[str, Any]]:
        if not isinstance(embed, dict):
            return None

        embed_type = embed.get("$type")
        if embed_type == "app.bsky.embed.images":
            images = embed.get("images")
            if not isinstance(images, list):
                images = []
            alts = [img.get("alt") for img in images if isinstance(img, dict) and isinstance(img.get("alt"), str)]
            return {
                "type": embed_type,
                "image_count": len(images),
                "alts": alts or None,
            }

        if embed_type == "app.bsky.embed.external":
            external = embed.get("external") or {}
            if not isinstance(external, dict):
                external = {}
            return {
                "type": embed_type,
                "uri": external.get("uri"),
                "title": external.get("title"),
                "description": external.get("description"),
                "has_thumb": bool(external.get("thumb")),
            }

        if embed_type == "app.bsky.embed.record":
            record = embed.get("record") or {}
            if not isinstance(record, dict):
                record = {}
            return {
                "type": embed_type,
                "uri": record.get("uri"),
                "cid": record.get("cid"),
            }

        if embed_type == "app.bsky.embed.recordWithMedia":
            record = embed.get("record") or {}
            if not isinstance(record, dict):
                record = {}
            media = embed.get("media")
            return {
                "type": embed_type,
                "record": {"uri": record.get("uri"), "cid": record.get("cid")},
                "media": PostUpsert._extract_embed(media),
            }

        return {"type": embed_type} if embed_type else None
