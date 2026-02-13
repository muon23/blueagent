from __future__ import annotations

import json
import re
from datetime import datetime, timedelta, timezone
from typing import Any

from embeddings.TextEmbedding import TextEmbedding
from sql_stores.SqlStore import SqlStore
from vector_stores.VectorStore import VectorStore

_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


class PostIndexer:
    """
    Index posts into a vector store using pluggable embedding and storage backends.

    Design goals:
    - Embedding model is injected (TextEmbedding implementation from cjutil)
    - Source SQL store is injected (SqlStore implementation from cjutil)
    - Target vector store is injected (VectorStore implementation from cjutil)
    """

    def __init__(
            self,
            embedding: TextEmbedding,
            sql_store: SqlStore,
            vector_store: VectorStore,
            *,
            posts_table: str = "posts",
            lookback_days: int = 3,
            default_batch_size: int = 500,
    ) -> None:
        """
        Initialize post indexer with pluggable components.

        Args:
            embedding: Embedding provider for post text.
            sql_store: SQL source store used to read posts.
            vector_store: Vector store used to upsert embeddings.
            posts_table: Source posts table name.
            lookback_days: Default lookback window for indexing.
            default_batch_size: Default maximum posts per indexing run.

        Returns:
            None.

        Raises:
            ValueError: If `posts_table` is not a valid SQL identifier.
        """
        self.embedding = embedding
        self.sql_store = sql_store
        self.vector_store = vector_store
        self._validate_identifier(posts_table, "posts_table")
        self.posts_table = posts_table
        self.lookback_days = lookback_days
        self.default_batch_size = default_batch_size

    def index_once(
            self,
            *,
            since: datetime | None = None,
            limit: int | None = None,
    ) -> int:
        """
        Index one batch of posts from SQL into vector store.

        Args:
            since: Optional lower bound for `created_at`.
            limit: Optional maximum rows to process.

        Returns:
            Number of indexed posts.

        Raises:
            RuntimeError: When embedding provider returns mismatched vector count.
            Exception: Propagates SQL/vector store failures.
        """
        rows = self._fetch_posts(since=since, limit=limit)
        if not rows:
            return 0

        docs: list[str] = []
        prepared_rows: list[dict[str, Any]] = []
        for row in rows:
            text = (row.get("text") or "").strip()
            if not text:
                continue
            docs.append(text)
            prepared_rows.append(row)

        if not prepared_rows:
            return 0

        vectors = self.embedding.embed_documents(docs)
        if len(vectors) != len(prepared_rows):
            raise RuntimeError(
                f"Embedding count mismatch: got {len(vectors)}, expected {len(prepared_rows)}"
            )

        items: list[tuple[str, list[float], dict[str, Any] | None, str | None]] = []
        for row, vector, doc in zip(prepared_rows, vectors, docs):
            record_id = str(row["uri"])
            metadata = self._row_to_metadata(row)
            items.append((record_id, vector, metadata, doc))

        self.vector_store.upsert_many(items)
        return len(items)

    def _fetch_posts(self, *, since: datetime | None, limit: int | None) -> list[dict[str, Any]]:
        effective_limit = limit if limit is not None else self.default_batch_size
        effective_since = since if since is not None else (
                datetime.now(timezone.utc) - timedelta(days=self.lookback_days)
        )

        sql = f"""
        SELECT uri, text, created_at, langs, tags, embed
        FROM {self.posts_table}
        WHERE created_at >= %s
        ORDER BY created_at ASC
        LIMIT %s
        """
        return self.sql_store.query(sql, (effective_since, effective_limit))

    @staticmethod
    def _validate_identifier(identifier: str, name: str) -> None:
        if not _IDENTIFIER.match(identifier):
            raise ValueError(f"Invalid SQL identifier for {name}: {identifier}")

    @staticmethod
    def _row_to_metadata(row: dict[str, Any]) -> dict[str, Any]:
        created_at = row.get("created_at")
        if isinstance(created_at, datetime):
            created_at = created_at.isoformat()

        embed_value = row.get("embed")
        if isinstance(embed_value, str):
            try:
                embed_value = json.loads(embed_value)
            except json.JSONDecodeError:
                # Keep original string if it is not JSON.
                pass

        metadata: dict[str, Any] = {
            "uri": row.get("uri"),
            "created_at": created_at,
            "langs": row.get("langs"),
            "tags": row.get("tags"),
            "embed": embed_value,
        }
        return metadata
