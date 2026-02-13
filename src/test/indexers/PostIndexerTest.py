import unittest
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Any

from embeddings.TextEmbedding import TextEmbedding
from indexers.PostIndexer import PostIndexer
from sql_stores.SqlStore import SqlStore
from vector_stores.VectorStore import VectorStore, Match


class FakeEmbedding(TextEmbedding):
    def embed_documents(self, texts: list[str]) -> list[list[float]]:
        return [[float(len(t))] for t in texts]

    def embed_query(self, text: str) -> list[float]:
        return [float(len(text))]

    def get_model_name(self) -> str:
        return "fake"

    def get_dimension(self) -> int:
        return 1

    @classmethod
    def get_supported_models(cls) -> list[str]:
        return ["fake"]


class FakeSqlStore(SqlStore):
    def __init__(self, rows: list[dict[str, Any]]):
        self._rows = rows
        self.last_query = None
        self.last_params = None

    def execute(self, sql: str, params=None) -> int:
        return 0

    def execute_many(self, sql: str, params_list) -> int:
        return 0

    def query(self, sql: str, params=None):
        self.last_query = sql
        self.last_params = params
        return self._rows

    @contextmanager
    def transaction(self):
        yield self

    def close(self) -> None:
        return


class FakeVectorStore(VectorStore):
    def __init__(self):
        self.items: list[tuple[str, list[float], dict[str, Any] | None, str | None]] = []

    def upsert(self, record_id: str, vector: list[float], metadata=None, document=None) -> None:
        self.items.append((record_id, vector, metadata, document))

    def query(self, vector: list[float], top_k: int = 10, metadata_filter=None) -> list[Match]:
        return []

    def delete(self, record_id: str) -> bool:
        return False


class PostIndexerTest(unittest.TestCase):
    def test_index_once_embeds_and_upserts(self):
        rows = [
            {
                "uri": "at://did:plc:1/app.bsky.feed.post/1",
                "text": "science news",
                "created_at": datetime(2026, 2, 10, 0, 0, 0, tzinfo=timezone.utc),
                "langs": ["en"],
                "tags": ["science"],
                "embed": {"type": "app.bsky.embed.external"},
            },
            {
                "uri": "at://did:plc:2/app.bsky.feed.post/2",
                "text": "  ",
                "created_at": datetime(2026, 2, 10, 0, 1, 0, tzinfo=timezone.utc),
                "langs": ["en"],
                "tags": ["ignore-empty"],
                "embed": None,
            },
        ]
        sql_store = FakeSqlStore(rows)
        vector_store = FakeVectorStore()
        embedding = FakeEmbedding()
        indexer = PostIndexer(embedding, sql_store, vector_store)

        count = indexer.index_once()

        self.assertEqual(1, count)
        self.assertEqual(1, len(vector_store.items))
        record_id, vector, metadata, document = vector_store.items[0]
        self.assertEqual(rows[0]["uri"], record_id)
        self.assertEqual([12.0], vector)
        self.assertEqual("science news", document)
        self.assertEqual(rows[0]["tags"], metadata["tags"])
        self.assertEqual(rows[0]["langs"], metadata["langs"])
        self.assertEqual(rows[0]["embed"], metadata["embed"])


if __name__ == "__main__":
    unittest.main()
