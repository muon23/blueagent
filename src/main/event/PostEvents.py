from dataclasses import dataclass
from datetime import datetime
from typing import Optional, Any, List

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
    # optional extra metadata
    raw: Optional[dict[str, Any]] = None


@dataclass(frozen=True)
class PostDelete(Event):
    uri: str = ""
