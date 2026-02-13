import argparse
import asyncio
import contextlib
import os
import sys
from pathlib import Path

SRC_MAIN = Path(__file__).resolve().parents[1]
if str(SRC_MAIN) not in sys.path:
    # Keep stdlib precedence to avoid shadowing modules like `profile`.
    sys.path.append(str(SRC_MAIN))

from cli.bootstrap import PROJECT_ROOT
from cli.config import DEFAULT_ENV, load_pipeline_config
from events.PostEventFilters import PostLanguageFilter
from ingestors.PostIngestor import PostIngestor
from ingestors.StreamClient import StreamClient
from sinks.PostgresSink import PostgresSink


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run Bluesky post ingestion into Postgres.")
    parser.add_argument("--env", default=DEFAULT_ENV, help="Deployment environment (default: dev).")
    parser.add_argument(
        "--config",
        default=None,
        help="Path to YAML config (default: deployment/<env>/pipeline.yaml).",
    )
    parser.add_argument("--minutes", type=float, default=None, help="Override ingestion runtime in minutes.")
    parser.add_argument("--dsn", default=None, help="Override Postgres DSN.")
    parser.add_argument("--cursor-file", default=None, help="Override cursor file path.")
    parser.add_argument("--batch-size", type=int, default=None, help="Override PostgresSink batch size.")
    parser.add_argument("--flush-interval-s", type=float, default=None, help="Override PostgresSink flush interval.")
    parser.add_argument(
        "--sink-flush-interval-s", type=float, default=None, help="Override StreamClient sink flush interval."
    )
    parser.add_argument("--langs", default=None, help="Override comma-separated language filters.")
    parser.add_argument("--no-create-schema", action="store_true", help="Disable posts schema auto-create.")
    return parser.parse_args()


def _resolve_runtime_config(args: argparse.Namespace):
    pipeline_cfg = load_pipeline_config(PROJECT_ROOT, args.config, env=args.env)
    cfg = pipeline_cfg.ingest
    db_cfg = pipeline_cfg.database
    minutes = args.minutes if args.minutes is not None else cfg.minutes
    dsn = args.dsn or os.getenv("POSTGRES_DSN") or db_cfg.dsn
    schema_name = db_cfg.schema_name
    posts_table = db_cfg.posts_table
    cursor_file = args.cursor_file or cfg.cursor_file
    batch_size = args.batch_size if args.batch_size is not None else cfg.batch_size
    flush_interval_s = args.flush_interval_s if args.flush_interval_s is not None else cfg.flush_interval_s
    sink_flush_interval_s = (
        args.sink_flush_interval_s if args.sink_flush_interval_s is not None else cfg.sink_flush_interval_s
    )
    langs = [lang.strip().lower() for lang in (args.langs.split(",") if args.langs else cfg.langs) if lang.strip()]
    if not langs:
        langs = ["en"]
    create_schema = False if args.no_create_schema else cfg.create_schema
    return (
        minutes,
        dsn,
        schema_name,
        posts_table,
        cursor_file,
        batch_size,
        flush_interval_s,
        sink_flush_interval_s,
        langs,
        create_schema,
    )


async def run_ingestion(args: argparse.Namespace) -> None:
    minutes, dsn, schema_name, posts_table, cursor_file, batch_size, flush_interval_s, sink_flush_interval_s, langs, create_schema = (
        _resolve_runtime_config(args)
    )

    cursor_path = Path(cursor_file)
    cursor_path.parent.mkdir(parents=True, exist_ok=True)

    sink = PostgresSink(
        dsn=dsn,
        schema_name=schema_name,
        table_name=posts_table,
        batch_size=batch_size,
        flush_interval_s=flush_interval_s,
        create_schema=create_schema,
    )
    client = StreamClient(
        [PostIngestor(filters=[PostLanguageFilter(langs=langs)])],
        sink=sink,
        cursor_file=str(cursor_path),
        sink_flush_interval_s=sink_flush_interval_s,
    )

    print(f"Starting ingestion for {minutes} minute(s).")
    print(f"Schema: {schema_name or '(default search_path)'}")
    print(f"Posts table: {posts_table}")
    print(f"Language filter: {langs}")
    print(f"Cursor file: {cursor_path}")
    print("Press Ctrl+C to stop early.")

    task = asyncio.create_task(client.run_forever())
    try:
        await asyncio.sleep(max(0.0, minutes) * 60.0)
    finally:
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task
        await sink.close()
        print("Ingestion stopped and sink closed.")


def main() -> None:
    args = parse_args()
    asyncio.run(run_ingestion(args))


if __name__ == "__main__":
    main()
