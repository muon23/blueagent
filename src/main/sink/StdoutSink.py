import json
import sys
from dataclasses import asdict
from typing import Iterable

from event.Event import Event
from event.PostEvents import PostUpsert, PostDelete
from sink.Sink import Sink


class StdoutSink(Sink):
    """
    Debug sink that prints events as JSON lines.
    Best-effort: unknown events are printed generically.
    """
    def __init__(self, *, stream=None, include_raw: bool = False) -> None:
        super().__init__()
        self._stream = stream or sys.stdout
        self._include_raw = include_raw

        self.on(PostUpsert, self._handle_post_upsert)
        self.on(PostDelete, self._handle_post_delete)

    async def _handle_post_upsert(self, e: PostUpsert) -> None:
        d = asdict(e)
        if not self._include_raw:
            d.pop("raw", None)
        self._stream.write(json.dumps({"type": "PostUpsert", **d}, ensure_ascii=False) + "\n")
        self._stream.flush()

    async def _handle_post_delete(self, e: PostDelete) -> None:
        self._stream.write(json.dumps({"type": "PostDelete", "uri": e.uri, "cursor": e.cursor}) + "\n")
        self._stream.flush()

    async def write(self, events: Iterable[Event]) -> None:
        # best-effort for unknown types: print them too
        for ev in events:
            h = self._resolve_handler(ev)
            if h is not None:
                await h(ev)
            else:
                self._stream.write(json.dumps({"type": type(ev).__name__, **asdict(ev)}) + "\n")
                self._stream.flush()

