import asyncio
import json
import os
import random
import ssl
import time
import urllib.parse
from typing import Iterable, List, Dict, Any, Optional

import certifi
import websockets

from .Ingestor import Ingestor


JETSTREAM_INSTANCES = [
    "wss://jetstream1.us-east.bsky.network/subscribe",
    "wss://jetstream2.us-east.bsky.network/subscribe",
    "wss://jetstream1.us-west.bsky.network/subscribe",
    "wss://jetstream2.us-west.bsky.network/subscribe",
]


class StreamClient:
    def __init__(
        self,
        ingestors: List[Ingestor],
        instances: Optional[Iterable[str]] = None,
        cursor_file: Optional[str] = None,
        rewind_seconds: int = 3,
    ):
        self.ingestors = ingestors
        self.routes = set().union(*(i.wanted_collections() for i in ingestors))
        self.route_map: Dict[str, List[Ingestor]] = {}
        for ingestor in ingestors:
            for collection in ingestor.wanted_collections():
                self.route_map.setdefault(collection, []).append(ingestor)
        self.instances = list(instances) if instances is not None else JETSTREAM_INSTANCES
        self.rewind_seconds = rewind_seconds
        self.cursor_file = cursor_file or os.path.join(
            os.path.dirname(__file__),
            "jetstream_cursor.txt",
        )
        self.ssl_context = ssl.create_default_context(cafile=certifi.where())

    def load_cursor_us(self) -> Optional[int]:
        """Load last cursor (unix microseconds) from disk."""
        if not os.path.exists(self.cursor_file):
            return None
        try:
            with open(self.cursor_file, "r", encoding="utf-8") as f:
                val = f.read().strip()
            return int(val) if val else None
        except Exception:
            return None

    def save_cursor_us(self, cursor_us: int) -> None:
        """Persist cursor to disk (atomic-ish)."""
        tmp = self.cursor_file + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            f.write(str(cursor_us))
        os.replace(tmp, self.cursor_file)

    def build_url(self, base: str, cursor_us: Optional[int]) -> str:
        """
        Jetstream supports query params including:
          - wantedCollections (repeatable)
          - cursor (unix microseconds) to begin playback from
        """
        params = []
        for collection in sorted(self.routes):
            params.append(("wantedCollections", collection))
        if cursor_us is not None:
            params.append(("cursor", str(cursor_us)))
        qs = urllib.parse.urlencode(params, doseq=True)
        return f"{base}?{qs}"

    @staticmethod
    def now_us() -> int:
        return int(time.time() * 1_000_000)

    @classmethod
    def _extract_collection(cls, evt: Dict[str, Any]) -> Optional[str]:
        kind = evt.get("kind")
        if kind != "commit":
            return None
        commit = evt.get("commit", {})
        return commit.get("collection")

    async def _fanout(self, evt: Dict[str, Any], collection: Optional[str]) -> None:
        if collection is None:
            return
        for ingestor in self.route_map.get(collection, []):
            await ingestor.handle_event(evt)

    async def run_forever(self):
        cursor = self.load_cursor_us()
        if cursor is None:
            cursor = self.now_us()

        while True:
            base = random.choice(self.instances)
            effective_cursor = None
            if cursor is not None:
                effective_cursor = max(0, cursor - self.rewind_seconds * 1_000_000)
            url = self.build_url(base, effective_cursor)
            print(f"Connecting: {url}")

            try:
                async with websockets.connect(
                    url,
                    ssl=self.ssl_context,
                    max_size=None,
                    ping_interval=20,
                    ping_timeout=20,
                ) as ws:
                    print("Connected.")
                    async for msg in ws:
                        evt = json.loads(msg)
                        collection = self._extract_collection(evt)
                        await self._fanout(evt, collection)

                        time_us = evt.get("time_us")
                        if time_us is not None:
                            new_cursor = int(time_us)
                            if cursor is None or new_cursor > cursor:
                                cursor = new_cursor
                                self.save_cursor_us(cursor)
            except (websockets.ConnectionClosed, OSError, json.JSONDecodeError) as e:
                print(f"Disconnected/error: {type(e).__name__}: {e}")

            await asyncio.sleep(1.0)
