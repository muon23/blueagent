import io
import json
import os
import unittest
import urllib.parse
import uuid
from datetime import datetime, timezone

import asyncpg

from events.Event import Event
from events import PostUpsert, PostDelete
from sinks.MultiSink import MultiSink
from sinks.NullSink import NullSink
from sinks.PostgresSink import PostgresSink
from sinks.Sink import Sink
from sinks.StdoutSink import StdoutSink


class RecordingSink(Sink):
    def __init__(self) -> None:
        super().__init__()
        self.events = []

    async def write(self, events):
        self.events.extend(list(events))


class FailingSink(Sink):
    async def write(self, events):
        raise RuntimeError("boom")


class SinkTest(unittest.IsolatedAsyncioTestCase):
    async def test_null_sink_accepts_event(self):
        sink = NullSink()
        await sink.write([Event(cursor="123")])

    async def test_stdout_sink_prints_post_events(self):
        buf = io.StringIO()
        sink = StdoutSink(stream=buf, include_raw=False)

        upsert = PostUpsert(
            uri="at://did/app.bsky.feed.post/abc",
            cid="cid123",
            did="did:plc:xyz",
            created_at=datetime.fromisoformat("2025-01-01T00:00:00Z"),
            text="hello world",
            raw={"debug": True},
            cursor="111",
        )
        delete = PostDelete(uri="at://did/app.bsky.feed.post/def", cursor="222")

        await sink.write([upsert, delete])

        lines = [json.loads(line) for line in buf.getvalue().strip().splitlines()]
        print(json.dumps(lines, indent=4))
        self.assertEqual(2, len(lines))

        self.assertEqual("PostUpsert", lines[0]["type"])
        self.assertEqual(upsert.uri, lines[0]["uri"])
        self.assertNotIn("raw", lines[0])
        self.assertEqual("PostDelete", lines[1]["type"])
        self.assertEqual(delete.uri, lines[1]["uri"])
        self.assertEqual(delete.cursor, lines[1]["cursor"])

    async def test_multi_sink_fanout_and_optional_failure(self):
        critical = RecordingSink()
        optional_fail = FailingSink()
        sink = MultiSink(critical=[critical], optional=[optional_fail])

        ev = Event(cursor="c1")
        await sink.write([ev])

        self.assertEqual([ev], critical.events)

    async def test_multi_sink_critical_failure_bubbles(self):
        critical_fail = FailingSink()
        optional = RecordingSink()
        sink = MultiSink(critical=[critical_fail], optional=[optional])

        with self.assertRaises(RuntimeError):
            await sink.write([Event(cursor="c2")])

    async def test_postgres_sink_upsert_and_delete(self):
        dsn = os.getenv("POSTGRES_DSN") or os.getenv("DATABASE_URL")
        if not dsn:
            self.skipTest("Set POSTGRES_DSN to run PostgresSink test")

        # try:
        #     import asyncpg  # type: ignore
        # except Exception:
        #     self.skipTest("asyncpg not installed")

        schema = f"sink_test_{uuid.uuid4().hex}"
        dsn_with_schema = self._with_search_path(dsn, schema)

        conn = await asyncpg.connect(dsn)
        try:
            await conn.execute(f'CREATE SCHEMA "{schema}"')
        finally:
            await conn.close()

        sink = PostgresSink(dsn_with_schema, create_schema=True, batch_size=10)

        try:
            created_at = datetime(2025, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
            upsert = PostUpsert(
                uri="at://did/app.bsky.feed.post/abc",
                cid="cid123",
                did="did:plc:xyz",
                created_at=created_at,
                text="hello world",
                tags=["ai", "news"],
                embed={"type": "app.bsky.embed.external", "uri": "https://example.com"},
                cursor="111",
            )
            await sink.write([upsert])
            await sink.flush()

            conn = await asyncpg.connect(dsn_with_schema)
            try:
                row = await conn.fetchrow("SELECT uri, cid, text FROM posts WHERE uri = $1", upsert.uri)
                self.assertIsNotNone(row)
                self.assertEqual(upsert.cid, row["cid"])
                self.assertEqual(upsert.text, row["text"])

                upsert2 = PostUpsert(
                    uri=upsert.uri,
                    cid="cid124",
                    did=upsert.did,
                    created_at=created_at,
                    text="updated",
                    tags=["ai"],
                    embed={"type": "app.bsky.embed.images", "image_count": 1},
                    cursor="222",
                )
                await sink.write([upsert2])
                await sink.flush()

                row = await conn.fetchrow("SELECT uri, cid, text FROM posts WHERE uri = $1", upsert.uri)
                self.assertEqual(upsert2.cid, row["cid"])
                self.assertEqual(upsert2.text, row["text"])

                await sink.write([PostDelete(uri=upsert.uri, cursor="333")])
                await sink.flush()

                count = await conn.fetchval("SELECT COUNT(*) FROM posts WHERE uri = $1", upsert.uri)
                self.assertEqual(0, count)
            finally:
                await conn.close()
        finally:
            await sink.close()
            conn = await asyncpg.connect(dsn)
            try:
                await conn.execute(f'DROP SCHEMA "{schema}" CASCADE')
            finally:
                await conn.close()

    @staticmethod
    def _with_search_path(dsn: str, schema: str) -> str:
        options = urllib.parse.quote(f"-csearch_path={schema}")
        separator = "&" if "?" in dsn else "?"
        return f"{dsn}{separator}options={options}"


if __name__ == "__main__":
    unittest.main()
