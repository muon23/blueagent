from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any


DEFAULT_DEPLOYMENT_DIR = Path("deployment")
DEFAULT_ENV = "dev"


@dataclass
class IngestConfig:
    minutes: float = 5.0
    cursor_file: str = "data/jetstream_cursor.txt"
    batch_size: int = 500
    flush_interval_s: float = 1.0
    sink_flush_interval_s: float = 1.0
    langs: list[str] = None
    create_schema: bool = True

    def __post_init__(self) -> None:
        if self.langs is None:
            self.langs = ["en"]


@dataclass
class IndexConfig:
    model_name: str = "sentence-transformers/all-MiniLM-L6-v2"
    lookback_days: int = 3
    batch_size: int = 500
    vector_table: str = "post_embeddings"
    vector_dimension: int = 384
    vector_schema: str | None = None
    vector_distance: str = "cosine"
    create_vector_if_not_exist: bool = True


@dataclass
class PipelineConfig:
    database: "DatabaseConfig"
    ingest: IngestConfig
    index: IndexConfig


@dataclass
class DatabaseConfig:
    dsn: str = "postgresql://localhost/postgres"
    posts_table: str = "posts"


def _merge_dataclass_defaults(dc_cls, values: dict[str, Any] | None):
    values = values or {}
    return dc_cls(**values)


def _load_yaml(path: Path) -> dict[str, Any]:
    try:
        import yaml  # type: ignore
    except ImportError as e:
        raise RuntimeError(
            "PyYAML is required to read pipeline config. Install with `pip install pyyaml`."
        ) from e

    with path.open("r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    if not isinstance(data, dict):
        raise ValueError(f"Config root must be a mapping: {path}")
    return data


def _default_config_path_for_env(env: str) -> Path:
    return DEFAULT_DEPLOYMENT_DIR / env / "pipeline.yaml"


def load_pipeline_config(
        project_root: Path,
        config_path: str | None = None,
        env: str = DEFAULT_ENV,
) -> PipelineConfig:
    rel_path = Path(config_path) if config_path else _default_config_path_for_env(env)
    path = rel_path if rel_path.is_absolute() else project_root / rel_path
    data = _load_yaml(path)

    db_cfg = _merge_dataclass_defaults(DatabaseConfig, data.get("database"))
    ingest_cfg = _merge_dataclass_defaults(IngestConfig, data.get("ingest"))
    index_cfg = _merge_dataclass_defaults(IndexConfig, data.get("index"))

    if not Path(ingest_cfg.cursor_file).is_absolute():
        ingest_cfg.cursor_file = str(project_root / ingest_cfg.cursor_file)

    return PipelineConfig(database=db_cfg, ingest=ingest_cfg, index=index_cfg)
