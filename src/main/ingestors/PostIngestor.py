from dataclasses import dataclass
from typing import Dict, Any, List

from events.Event import Event
from events import PostUpsert, PostDelete
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
        """
        Parse one stream payload into post events.

        Args:
            evt: Jetstream event dictionary.

        Returns:
            Zero or more post events (`PostDelete` or `PostUpsert`) after
            applying configured filters.

        Raises:
            None.
        """
        delete_event = PostDelete.from_event(evt)
        if delete_event is not None:
            return self._apply_filters([delete_event])

        upsert_event = PostUpsert.from_event(evt)
        if upsert_event is not None:
            return self._apply_filters([upsert_event])

        return []

    def wanted_collections(self) -> List[str]:
        """
        Return the post collection route for stream subscription.

        Args:
            None.

        Returns:
            List containing only `app.bsky.feed.post`.

        Raises:
            None.
        """
        return ["app.bsky.feed.post"]

