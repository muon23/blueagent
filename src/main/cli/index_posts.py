import argparse
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

SRC_MAIN = Path(__file__).resolve().parents[1]
if str(SRC_MAIN) not in sys.path:
    # Keep stdlib precedence to avoid shadowing modules like `profile`.
    sys.path.append(str(SRC_MAIN))

from cli.bootstrap import PROJECT_ROOT
from cli.config import DEFAULT_ENV, load_pipeline_config
from embeddings import of as embedding_of
from indexers.PostIndexer import PostIndexer
from sql_stores.PostgresSqlStore import PostgresSqlStore
from vector_stores.PGVectorStore import PGVectorStore


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Index posts into vector store.")
    parser.add_argument("--env", default=DEFAULT_ENV, help="Deployment environment (default: dev).")
    parser.add_argument(
        "--config",
        default=None,
        help="Path to YAML config (default: deployment/<env>/pipeline.yaml).",
    )
    parser.add_argument("--dsn", default=None, help="Override Postgres DSN.")
    parser.add_argument("--model-name", default=None, help="Override embedding model name.")
    parser.add_argument("--lookback-days", type=int, default=None, help="Override lookback days.")
    parser.add_argument("--batch-size", type=int, default=None, help="Override indexing batch size.")
    parser.add_argument("--cursor-file", default=None, help="Override index cursor file path.")
    parser.add_argument(
        "--ignore-cursor",
        action="store_true",
        help="Ignore existing cursor file and start fresh from --since or lookback window.",
    )
    parser.add_argument(
        "--since",
        default=None,
        help="Start indexing from ISO-8601 time (for example: 2026-02-10T00:00:00Z).",
    )
    parser.add_argument("--vector-table", default=None, help="Override vector table name.")
    parser.add_argument("--vector-schema", default=None, help="Override vector schema.")
    parser.add_argument("--vector-distance", default=None, help="Override vector distance metric.")
    parser.add_argument(
        "--no-create-vector-table",
        action="store_true",
        help="Disable vector table auto-create.",
    )
    return parser.parse_args()


def _resolve_runtime_config(args: argparse.Namespace):
    pipeline_cfg = load_pipeline_config(PROJECT_ROOT, args.config, env=args.env)
    cfg = pipeline_cfg.index
    db_cfg = pipeline_cfg.database
    dsn = args.dsn or os.getenv("POSTGRES_DSN") or db_cfg.dsn
    schema_name = db_cfg.schema_name
    posts_table = db_cfg.posts_table
    model_name = args.model_name or cfg.model_name
    lookback_days = args.lookback_days if args.lookback_days is not None else cfg.lookback_days
    batch_size = args.batch_size if args.batch_size is not None else cfg.batch_size
    cursor_file = args.cursor_file or cfg.cursor_file
    ignore_cursor = args.ignore_cursor
    since = _parse_iso_datetime(args.since) if args.since else None
    vector_table = args.vector_table or db_cfg.vector_table
    vector_schema = args.vector_schema if args.vector_schema is not None else schema_name
    vector_distance = args.vector_distance or cfg.vector_distance
    create_vector = False if args.no_create_vector_table else cfg.create_vector_if_not_exist
    return (
        dsn,
        schema_name,
        posts_table,
        model_name,
        lookback_days,
        batch_size,
        cursor_file,
        ignore_cursor,
        since,
        vector_table,
        vector_schema,
        vector_distance,
        create_vector,
    )


def _parse_iso_datetime(value: str) -> datetime:
    if value.endswith("Z"):
        value = value[:-1] + "+00:00"
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed


def _load_cursor(path: Path) -> Optional[datetime]:
    if not path.exists():
        return None
    value = path.read_text(encoding="utf-8").strip()
    if not value:
        return None
    try:
        return _parse_iso_datetime(value)
    except ValueError:
        return None


def _save_cursor(path: Path, cursor: datetime) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(cursor.isoformat(), encoding="utf-8")


def _render_progress(current: int, total: int, width: int = 30) -> None:
    if total <= 0:
        return
    clamped = min(current, total)
    ratio = clamped / total
    filled = int(width * ratio)
    bar = "#" * filled + "-" * (width - filled)
    print(f"\rIndexing [{bar}] {clamped}/{total}", end="", flush=True)


def main() -> None:
    args = parse_args()
    (
        dsn,
        schema_name,
        posts_table,
        model_name,
        lookback_days,
        batch_size,
        cursor_file,
        ignore_cursor,
        since,
        vector_table,
        vector_schema,
        vector_distance,
        create_vector,
    ) = _resolve_runtime_config(args)

    embedding = embedding_of(model_name)
    sql_store = PostgresSqlStore(dsn)
    vector_store = PGVectorStore(
        dsn=dsn,
        table_name=vector_table,
        dimension=embedding.get_dimension(),
        schema_name=vector_schema,
        distance=vector_distance,
        create_if_not_exist=create_vector,
    )

    indexer = PostIndexer(
        embedding=embedding,
        sql_store=sql_store,
        vector_store=vector_store,
        schema_name=schema_name,
        posts_table=posts_table,
        lookback_days=lookback_days,
        default_batch_size=batch_size,
    )

    cursor_path = Path(cursor_file)
    start_since = since
    if start_since is None and not ignore_cursor:
        start_since = _load_cursor(cursor_path)
    if start_since is None:
        start_since = datetime.now(timezone.utc) - timedelta(days=lookback_days)

    total_target = indexer.count_indexable_posts(since=start_since)
    _render_progress(0, total_target)

    total = 0
    current_since = start_since
    while True:
        count, last_created_at = indexer.index_batch(since=current_since, limit=batch_size)
        if last_created_at is None:
            break
        _save_cursor(cursor_path, last_created_at)
        current_since = last_created_at
        total += count
        _render_progress(total, total_target)

    sql_store.close()
    if total_target > 0:
        print()

    print(
        f"Indexed {total} posts from "
        f"'{(schema_name + '.') if schema_name else ''}{posts_table}' "
        f"into vector table '{(vector_schema + '.') if vector_schema else ''}{vector_table}'. "
        f"Cursor file: {cursor_path}."
    )


if __name__ == "__main__":
    main()
