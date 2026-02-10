from dataclasses import dataclass
from typing import Dict, Any, List

from event.Event import Event
from event import PostUpsert, PostDelete
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
        delete_event = PostDelete.from_event(evt)
        if delete_event is not None:
            return self._apply_filters([delete_event])

        upsert_event = PostUpsert.from_event(evt)
        if upsert_event is not None:
            return self._apply_filters([upsert_event])

        return []

    def wanted_collections(self) -> List[str]:
        return ["app.bsky.feed.post"]

