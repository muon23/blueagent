from dataclasses import dataclass
from typing import Optional, Dict, Any

from .Event import Event


@dataclass(frozen=True)
class PostDelete(Event):
    """Domain event for deletion of a Bluesky post record."""

    uri: str = ""

    @classmethod
    def from_event(cls, evt: Dict[str, Any]) -> Optional["PostDelete"]:
        """
        Build a `PostDelete` event from a Jetstream event payload.

        Args:
            evt: JSON-decoded Jetstream event dictionary.

        Returns:
            A `PostDelete` event when the payload represents a post deletion;
            otherwise `None`.

        Raises:
            None.
        """
        if evt.get("kind") != "commit":
            return None

        commit = evt.get("commit") or {}
        if commit.get("collection") != "app.bsky.feed.post":
            return None

        if commit.get("operation") != "delete":
            return None

        did = evt.get("did") or commit.get("did")
        rkey = commit.get("rkey")
        if not did or not rkey:
            return None

        return cls(
            uri=f"at://{did}/app.bsky.feed.post/{rkey}",
            cursor=str(evt.get("time_us")) if evt.get("time_us") is not None else None,
        )
