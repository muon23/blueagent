import asyncio
import json
import os
import tempfile
import unittest
import urllib.parse

from ingestor.Ingestor import Ingestor
from ingestor.StreamClient import StreamClient


class RecordingIngestor(Ingestor):
    def __init__(self, collections):
        super().__init__()
        self._collections = collections
        self.events = []

    def wanted_collections(self):
        return self._collections

    async def handle_event(self, evt):
        self.events.append(evt)


class StreamClientTest(unittest.IsolatedAsyncioTestCase):
    async def test_fanout_routes_by_collection(self):
        ingestor_a = RecordingIngestor(["app.bsky.feed.post"])
        ingestor_b = RecordingIngestor(["app.bsky.actor.profile"])
        client = StreamClient([ingestor_a, ingestor_b])

        evt = {"kind": "commit", "commit": {"collection": "app.bsky.feed.post"}}
        await client._fanout(evt, "app.bsky.feed.post")

        self.assertEqual([evt], ingestor_a.events)
        self.assertEqual([], ingestor_b.events)

    def test_extract_collection(self):
        commit_evt = {"kind": "commit", "commit": {"collection": "app.bsky.feed.post"}}
        other_evt = {"kind": "identity", "commit": {"collection": "app.bsky.feed.post"}}

        self.assertEqual("app.bsky.feed.post", StreamClient._extract_collection(commit_evt))
        self.assertIsNone(StreamClient._extract_collection(other_evt))

    def test_build_url_includes_routes_and_cursor(self):
        ingestor_a = RecordingIngestor(["b", "a"])
        client = StreamClient([ingestor_a])

        url = client.build_url("wss://example", 123)
        parsed = urllib.parse.urlparse(url)
        qs = urllib.parse.parse_qs(parsed.query)

        self.assertEqual(["123"], qs["cursor"])
        self.assertEqual(["a", "b"], sorted(qs["wantedCollections"]))

    def test_cursor_file_default_is_cwd(self):
        client = StreamClient([RecordingIngestor(["app.bsky.feed.post"])])
        expected = os.path.join(os.getcwd(), "jetstream_cursor.txt")
        self.assertEqual(expected, client.cursor_file)

    async def test_live_ingestion_smoke(self):
        if not os.getenv("RUN_LIVE_JETSTREAM"):
            self.skipTest("Set RUN_LIVE_JETSTREAM=1 to run live smoke test")

        ingestor = RecordingIngestor(["app.bsky.feed.post"])
        with tempfile.TemporaryDirectory() as tmp:
            cursor_file = os.path.join(tmp, "jetstream_cursor.txt")
            client = StreamClient([ingestor], cursor_file=cursor_file)

            task = asyncio.create_task(client.run_forever())
            try:
                await asyncio.sleep(5)
            finally:
                task.cancel()
                with self.assertRaises(asyncio.CancelledError):
                    await task

        print(json.dumps(ingestor.events, indent=2))
        self.assertGreaterEqual(len(ingestor.events), 1)


if __name__ == "__main__":
    unittest.main()
