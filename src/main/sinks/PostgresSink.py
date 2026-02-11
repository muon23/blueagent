import json
import time
from typing import List, Iterable, Tuple, Any

from events.Event import Event
from events import PostUpsert, PostDelete
from sinks.Sink import Sink


class PostgresSink(Sink):
    """
    Postgres sink with batching + UPSERT.

    Requires: asyncpg
      pip install asyncpg

    Behavior:
    - PostUpsert: upserts on posts.uri
      * updates row if cid differs (still updates to latest text/fields)
    - PostDelete: deletes by uri

    You can tune:
    - batch_size: max rows per DB roundtrip
    - flush_interval_s: auto-flush every N seconds (call tick() from your runner)
    """
    def __init__(
            self,
            dsn: str,
            *,
            batch_size: int = 1000,
            flush_interval_s: float = 1.0,
            create_schema: bool = True,
    ) -> None:
        super().__init__()
        self._dsn = dsn
        self._batch_size = batch_size
        self._flush_interval_s = flush_interval_s
        self._create_schema = create_schema

        self._pool = None  # asyncpg.Pool
        self._pending_upserts: List[PostUpsert] = []
        self._pending_deletes: List[PostDelete] = []
        self._last_flush = time.monotonic()

        self.on(PostUpsert, self._enqueue_post_upsert)
        self.on(PostDelete, self._enqueue_post_delete)

    async def open(self) -> None:
        import asyncpg  # local import to keep optional dependency clean

        if self._pool is not None:
            return
        self._pool = await asyncpg.create_pool(dsn=self._dsn, min_size=1, max_size=10)

        if self._create_schema:
            async with self._pool.acquire() as conn:
                await conn.execute(self._POST_SCHEMA_SQL)

    async def _enqueue_post_upsert(self, e: PostUpsert) -> None:
        self._pending_upserts.append(e)
        if len(self._pending_upserts) >= self._batch_size:
            await self._flush_upserts()

    async def _enqueue_post_delete(self, e: PostDelete) -> None:
        self._pending_deletes.append(e)
        if len(self._pending_deletes) >= self._batch_size:
            await self._flush_deletes()

    async def tick(self) -> None:
        """
        Call periodically (e.g., in your main loop) to flush on time intervals.
        """
        now = time.monotonic()
        if now - self._last_flush >= self._flush_interval_s:
            await self.flush()

    async def write(self, events: Iterable[Event]) -> None:
        if self._pool is None:
            await self.open()
        await super().write(events)
        # optional: time-based flush is handled by tick()/flush() called by runner

    async def flush(self) -> None:
        if self._pool is None:
            return
        await self._flush_upserts()
        await self._flush_deletes()
        self._last_flush = time.monotonic()

    async def close(self) -> None:
        await self.flush()
        if self._pool is not None:
            await self._pool.close()
            self._pool = None

    async def _flush_upserts(self) -> None:
        if not self._pending_upserts:
            return
        assert self._pool is not None

        batch = self._pending_upserts
        self._pending_upserts = []

        # Prepare rows
        rows: List[Tuple[Any, ...]] = []
        for e in batch:
            rows.append(
                (
                    e.uri,
                    e.cid,
                    e.did,
                    e.created_at,
                    e.text,
                    e.reply_parent_uri,
                    e.reply_root_uri,
                    e.langs,
                    e.tags,
                    json.dumps(e.embed, ensure_ascii=False) if e.embed is not None else None,
                )
            )

        sql = """
        INSERT INTO posts (
          uri, cid, did, created_at, text, reply_parent_uri, reply_root_uri, langs, tags, embed
        )
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
        ON CONFLICT (uri) DO UPDATE
        SET
          cid = EXCLUDED.cid,
          did = EXCLUDED.did,
          created_at = EXCLUDED.created_at,
          text = EXCLUDED.text,
          reply_parent_uri = EXCLUDED.reply_parent_uri,
          reply_root_uri = EXCLUDED.reply_root_uri,
          langs = EXCLUDED.langs,
          tags = EXCLUDED.tags,
          embed = EXCLUDED.embed,
          updated_at = NOW()
        WHERE posts.cid IS DISTINCT FROM EXCLUDED.cid;
        """

        async with self._pool.acquire() as conn:
            # executemany is fine for MVP; for very high throughput you can use COPY.
            await conn.executemany(sql, rows)

    async def _flush_deletes(self) -> None:
        if not self._pending_deletes:
            return
        assert self._pool is not None

        batch = self._pending_deletes
        self._pending_deletes = []

        uris = [(e.uri,) for e in batch]
        sql = "DELETE FROM posts WHERE uri = $1;"

        async with self._pool.acquire() as conn:
            await conn.executemany(sql, uris)

    _POST_SCHEMA_SQL = """
    CREATE TABLE IF NOT EXISTS posts (
      uri TEXT PRIMARY KEY,
      cid TEXT NOT NULL,
      did TEXT NOT NULL,
      created_at TIMESTAMPTZ NOT NULL,
      text TEXT NOT NULL,
      reply_parent_uri TEXT NULL,
      reply_root_uri TEXT NULL,
      langs TEXT[] NULL,
      tags TEXT[] NULL,
      embed JSONB NULL,
      inserted_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );

    CREATE INDEX IF NOT EXISTS idx_posts_created_at ON posts (created_at DESC);
    CREATE INDEX IF NOT EXISTS idx_posts_did ON posts (did);
    CREATE INDEX IF NOT EXISTS idx_posts_reply_root ON posts (reply_root_uri);
    """

