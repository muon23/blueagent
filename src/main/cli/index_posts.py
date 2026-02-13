import argparse
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

SRC_MAIN = Path(__file__).resolve().parents[1]
if str(SRC_MAIN) not in sys.path:
    sys.path.insert(0, str(SRC_MAIN))

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
    parser.add_argument("--vector-table", default=None, help="Override vector table name.")
    parser.add_argument("--vector-dimension", type=int, default=None, help="Override vector dimension.")
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
    dsn = args.dsn or os.getenv("POSTGRES_DSN") or pipeline_cfg.database.dsn
    posts_table = pipeline_cfg.database.posts_table
    model_name = args.model_name or cfg.model_name
    lookback_days = args.lookback_days if args.lookback_days is not None else cfg.lookback_days
    batch_size = args.batch_size if args.batch_size is not None else cfg.batch_size
    vector_table = args.vector_table or cfg.vector_table
    vector_dimension = args.vector_dimension if args.vector_dimension is not None else cfg.vector_dimension
    vector_schema = args.vector_schema if args.vector_schema is not None else cfg.vector_schema
    vector_distance = args.vector_distance or cfg.vector_distance
    create_vector = False if args.no_create_vector_table else cfg.create_vector_if_not_exist
    return (
        dsn,
        posts_table,
        model_name,
        lookback_days,
        batch_size,
        vector_table,
        vector_dimension,
        vector_schema,
        vector_distance,
        create_vector,
    )


def main() -> None:
    args = parse_args()
    (
        dsn,
        posts_table,
        model_name,
        lookback_days,
        batch_size,
        vector_table,
        vector_dimension,
        vector_schema,
        vector_distance,
        create_vector,
    ) = _resolve_runtime_config(args)

    embedding = embedding_of(model_name)
    sql_store = PostgresSqlStore(dsn)
    vector_store = PGVectorStore(
        dsn=dsn,
        table_name=vector_table,
        dimension=vector_dimension,
        schema_name=vector_schema,
        distance=vector_distance,
        create_if_not_exist=create_vector,
    )

    indexer = PostIndexer(
        embedding=embedding,
        sql_store=sql_store,
        vector_store=vector_store,
        posts_table=posts_table,
        lookback_days=lookback_days,
        default_batch_size=batch_size,
    )

    since = datetime.now(timezone.utc) - timedelta(days=lookback_days)
    count = indexer.index_once(since=since, limit=batch_size)
    sql_store.close()

    print(f"Indexed {count} posts from table '{posts_table}' into vector table '{vector_table}'.")


if __name__ == "__main__":
    main()
