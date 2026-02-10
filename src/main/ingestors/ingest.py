import argparse
import asyncio
import contextlib
import os
import sys
from pathlib import Path


def _setup_paths() -> Path:
    current_file = Path(__file__).resolve()
    project_root = current_file.parents[3]
    src_main = project_root / "src" / "main"
    if str(src_main) not in sys.path:
        sys.path.insert(0, str(src_main))
    return project_root


PROJECT_ROOT = _setup_paths()

DEFAULT_N_MINUTES = 0.1
DEFAULT_POSTGRES_DSN = "postgresql://localhost/postgres"
DEFAULT_JETSTREAM_CURSOR_FILE = str(PROJECT_ROOT / "data" / "jetstream_cursor.txt")
DEFAULT_BATCH_SIZE = 500
DEFAULT_FLUSH_INTERVAL_S = 1.0
DEFAULT_SINK_FLUSH_INTERVAL_S = 1.0
DEFAULT_LANGS = ["en"]
DEFAULT_LANGS_CSV = ",".join(DEFAULT_LANGS)

# Adds anychat/src/main to sys.path for llms imports outside IntelliJ.
import bootstrap  # noqa: F401

from ingestors.PostIngestor import PostIngestor
from ingestors.StreamClient import StreamClient
from event.PostEventFilters import PostLanguageFilter
from sinks.PostgresSink import PostgresSink


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run Bluesky post ingestion into Postgres.")
    parser.add_argument(
        "--minutes",
        type=float,
        default=DEFAULT_N_MINUTES,
        help=f"How many minutes to run ingestion (default: {DEFAULT_N_MINUTES}).",
    )
    parser.add_argument(
        "--dsn",
        default=os.getenv("POSTGRES_DSN", DEFAULT_POSTGRES_DSN),
        help=f"Postgres DSN. Defaults to POSTGRES_DSN env var, then {DEFAULT_POSTGRES_DSN}.",
    )
    parser.add_argument(
        "--cursor-file",
        default=DEFAULT_JETSTREAM_CURSOR_FILE,
        help=f"Cursor file path (default: {DEFAULT_JETSTREAM_CURSOR_FILE}).",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=DEFAULT_BATCH_SIZE,
        help=f"PostgresSink batch size (default: {DEFAULT_BATCH_SIZE}).",
    )
    parser.add_argument(
        "--flush-interval-s",
        type=float,
        default=DEFAULT_FLUSH_INTERVAL_S,
        help=f"PostgresSink flush interval in seconds (default: {DEFAULT_FLUSH_INTERVAL_S}).",
    )
    parser.add_argument(
        "--sink-flush-interval-s",
        type=float,
        default=DEFAULT_SINK_FLUSH_INTERVAL_S,
        help=f"StreamClient sink flush interval in seconds (default: {DEFAULT_SINK_FLUSH_INTERVAL_S}).",
    )
    parser.add_argument(
        "--langs",
        default=DEFAULT_LANGS_CSV,
        help=f"Comma-separated language filters (default: {DEFAULT_LANGS_CSV}).",
    )
    parser.add_argument(
        "--no-create-schema",
        action="store_true",
        help="Disable auto-create schema for posts table/indexes.",
    )
    return parser.parse_args()


async def run_ingestion(args: argparse.Namespace) -> None:
    cursor_path = Path(args.cursor_file)
    cursor_path.parent.mkdir(parents=True, exist_ok=True)

    langs = [lang.strip().lower() for lang in args.langs.split(",") if lang.strip()]
    if not langs:
        langs = DEFAULT_LANGS

    sink = PostgresSink(
        dsn=args.dsn,
        batch_size=args.batch_size,
        flush_interval_s=args.flush_interval_s,
        create_schema=not args.no_create_schema,
    )
    client = StreamClient(
        [PostIngestor(filters=[PostLanguageFilter(langs=langs)])],
        sink=sink,
        cursor_file=str(cursor_path),
        sink_flush_interval_s=args.sink_flush_interval_s,
    )

    print(f"Starting ingestion for {args.minutes} minute(s).")
    print(f"Language filter: {langs}")
    print(f"Cursor file: {cursor_path}")
    print("Press Ctrl+C to stop early.")

    task = asyncio.create_task(client.run_forever())
    try:
        await asyncio.sleep(max(0.0, args.minutes) * 60.0)
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
