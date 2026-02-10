import unittest
from datetime import datetime, timezone

from event.Event import EventFilter
from event import PostUpsert, PostDelete
from ingestor.PostIngestor import PostIngestor


class DropAllFilter(EventFilter):
    def apply(self, events):
        return []


class PostIngestorTest(unittest.IsolatedAsyncioTestCase):
    async def test_create_post_event(self):
        ingestor = PostIngestor()
        evt = {
            "kind": "commit",
            "time_us": 123,
            "did": "did:plc:xyz",
            "commit": {
                "collection": "app.bsky.feed.post",
                "rkey": "abc",
                "operation": "create",
                "cid": "cid123",
                "record": {
                    "text": "hello world",
                    "createdAt": "2025-01-01T00:00:00Z",
                    "langs": ["en"],
                    "facets": [
                        {
                            "features": [
                                {"$type": "app.bsky.richtext.facet#tag", "tag": "ai"},
                                {"$type": "app.bsky.richtext.facet#tag", "tag": "news"},
                            ]
                        }
                    ],
                    "embed": {
                        "$type": "app.bsky.embed.external",
                        "external": {
                            "uri": "https://example.com",
                            "title": "Example",
                            "description": "Example desc",
                            "thumb": "thumb",
                        },
                    },
                    "reply": {
                        "parent": {"uri": "at://did/app.bsky.feed.post/p1", "cid": "c1"},
                        "root": {"uri": "at://did/app.bsky.feed.post/r1", "cid": "c2"},
                    },
                },
            },
        }

        events = await ingestor.handle_event(evt)
        self.assertEqual(1, len(events))
        self.assertIsInstance(events[0], PostUpsert)
        upsert: PostUpsert = events[0]  # type: ignore[assignment]
        self.assertEqual("at://did:plc:xyz/app.bsky.feed.post/abc", upsert.uri)
        self.assertEqual("cid123", upsert.cid)
        self.assertEqual("did:plc:xyz", upsert.did)
        self.assertEqual(datetime(2025, 1, 1, 0, 0, 0, tzinfo=timezone.utc), upsert.created_at)
        self.assertEqual("hello world", upsert.text)
        self.assertEqual("at://did/app.bsky.feed.post/p1", upsert.reply_parent_uri)
        self.assertEqual("at://did/app.bsky.feed.post/r1", upsert.reply_root_uri)
        self.assertEqual(["en"], upsert.langs)
        self.assertEqual(["ai", "news"], upsert.tags)
        self.assertEqual(
            {
                "type": "app.bsky.embed.external",
                "uri": "https://example.com",
                "title": "Example",
                "description": "Example desc",
                "has_thumb": True,
            },
            upsert.embed,
        )
        self.assertEqual("123", upsert.cursor)

    async def test_delete_post_event(self):
        ingestor = PostIngestor()
        evt = {
            "kind": "commit",
            "time_us": 456,
            "did": "did:plc:xyz",
            "commit": {
                "collection": "app.bsky.feed.post",
                "rkey": "def",
                "operation": "delete",
            },
        }

        events = await ingestor.handle_event(evt)
        self.assertEqual(1, len(events))
        self.assertIsInstance(events[0], PostDelete)
        delete: PostDelete = events[0]  # type: ignore[assignment]
        self.assertEqual("at://did:plc:xyz/app.bsky.feed.post/def", delete.uri)
        self.assertEqual("456", delete.cursor)

    async def test_non_post_event_is_ignored(self):
        ingestor = PostIngestor()
        evt = {
            "kind": "commit",
            "time_us": 789,
            "did": "did:plc:xyz",
            "commit": {
                "collection": "app.bsky.actor.profile",
                "rkey": "profile",
                "operation": "update",
            },
        }

        events = await ingestor.handle_event(evt)
        self.assertEqual([], events)

    async def test_filters_are_applied(self):
        ingestor = PostIngestor(filters=[DropAllFilter()])
        evt = {
            "kind": "commit",
            "time_us": 123,
            "did": "did:plc:xyz",
            "commit": {
                "collection": "app.bsky.feed.post",
                "rkey": "abc",
                "operation": "create",
                "cid": "cid123",
                "record": {"text": "hello world", "createdAt": "2025-01-01T00:00:00Z"},
            },
        }

        events = await ingestor.handle_event(evt)
        self.assertEqual([], events)


if __name__ == "__main__":
    unittest.main()
