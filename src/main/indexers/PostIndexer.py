from __future__ import annotations

import json
import re
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

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
            schema_name: str | None = None,
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
            schema_name: Optional SQL schema for the source posts table.
            posts_table: Source posts table name.
            lookback_days: Default lookback window for indexing.
            default_batch_size: Default maximum posts per indexing run.

        Returns:
            None.

        Raises:
            ValueError: If `schema_name`/`posts_table` are invalid SQL identifiers.
        """
        self.embedding = embedding
        self.sql_store = sql_store
        self.vector_store = vector_store
        if schema_name is not None:
            self._validate_identifier(schema_name, "schema_name")
        self.schema_name = schema_name
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
        count, _ = self.index_batch(since=since, limit=limit)
        return count

    def count_indexable_posts(self, *, since: datetime | None = None) -> int:
        """
        Count posts eligible for indexing from a given start timestamp.

        Args:
            since: Optional lower bound for `created_at`.

        Returns:
            Number of rows with non-empty text that satisfy the time filter.

        Raises:
            Exception: Propagates SQL store query errors.
        """
        effective_since = since if since is not None else (
                datetime.now(timezone.utc) - timedelta(days=self.lookback_days)
        )
        sql = f"""
        SELECT COUNT(*) AS cnt
        FROM {self._qualified_posts_table()}
        WHERE created_at > %s
          AND text IS NOT NULL
          AND btrim(text) <> ''
        """
        rows = self.sql_store.query(sql, (effective_since,))
        if not rows:
            return 0
        value = rows[0].get("cnt", 0)
        return int(value or 0)

    def index_batch(
            self,
            *,
            since: datetime | None = None,
            limit: int | None = None,
    ) -> tuple[int, Optional[datetime]]:
        """
        Index one batch and return both count and watermark timestamp.

        Args:
            since: Optional lower bound for `created_at`.
            limit: Optional maximum rows to process.

        Returns:
            A tuple of:
            - indexed row count
            - last `created_at` observed in fetched rows (or `None` if empty).

        Raises:
            RuntimeError: When embedding provider returns mismatched vector count.
            Exception: Propagates SQL/vector store failures.
        """
        rows = self._fetch_posts(since=since, limit=limit)
        if not rows:
            return 0, None

        last_created_at = self._normalize_created_at(rows[-1].get("created_at"))

        docs: list[str] = []
        prepared_rows: list[dict[str, Any]] = []
        for row in rows:
            text = (row.get("text") or "").strip()
            if not text:
                continue
            docs.append(text)
            prepared_rows.append(row)

        if not prepared_rows:
            return 0, last_created_at

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
        return len(items), last_created_at

    def _fetch_posts(self, *, since: datetime | None, limit: int | None) -> list[dict[str, Any]]:
        effective_limit = limit if limit is not None else self.default_batch_size
        effective_since = since if since is not None else (
                datetime.now(timezone.utc) - timedelta(days=self.lookback_days)
        )

        sql = f"""
        SELECT uri, text, created_at, langs, tags, embed
        FROM {self._qualified_posts_table()}
        WHERE created_at > %s
        ORDER BY created_at ASC
        LIMIT %s
        """
        return self.sql_store.query(sql, (effective_since, effective_limit))

    @staticmethod
    def _validate_identifier(identifier: str, name: str) -> None:
        if not _IDENTIFIER.match(identifier):
            raise ValueError(f"Invalid SQL identifier for {name}: {identifier}")

    def _qualified_posts_table(self) -> str:
        if self.schema_name is None:
            return self.posts_table
        return f"{self.schema_name}.{self.posts_table}"

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

    @staticmethod
    def _normalize_created_at(value: Any) -> Optional[datetime]:
        if value is None:
            return None
        if isinstance(value, datetime):
            if value.tzinfo is None:
                return value.replace(tzinfo=timezone.utc)
            return value
        if isinstance(value, str):
            try:
                if value.endswith("Z"):
                    value = value[:-1] + "+00:00"
                parsed = datetime.fromisoformat(value)
                if parsed.tzinfo is None:
                    return parsed.replace(tzinfo=timezone.utc)
                return parsed
            except ValueError:
                return None
        return None
