"""Shared helpers for config loading and path resolution."""

from __future__ import annotations

import os
import re
from pathlib import Path
from typing import Any

import yaml

REPO_ROOT = Path(__file__).resolve().parents[2]
CONFIG_DIR = REPO_ROOT / "config"


def load_yaml(path: Path | str) -> Any:
    path = Path(path)
    if not path.is_absolute():
        path = REPO_ROOT / path
    with path.open() as f:
        return yaml.safe_load(f)


def render_template(text: str, mapping: dict[str, str]) -> str:
    """Replace ${var} placeholders."""

    def repl(match: re.Match[str]) -> str:
        key = match.group(1)
        return str(mapping.get(key, match.group(0)))

    return re.sub(r"\$\{([a-zA-Z0-9_]+)\}", repl, text)


def load_benchmark_config() -> dict[str, Any]:
    cfg = load_yaml(CONFIG_DIR / "benchmark.yaml")
    catalog = os.environ.get("CATALOG", cfg.get("catalog", "genie_tco"))
    schema = os.environ.get("SCHEMA", cfg.get("schema", "bench"))
    # Strip ${...} defaults if still templated
    if isinstance(catalog, str) and catalog.startswith("${"):
        catalog = os.environ.get("CATALOG", "genie_tco")
    if isinstance(schema, str) and schema.startswith("${"):
        schema = os.environ.get("SCHEMA", "bench")
    cfg["catalog"] = catalog
    cfg["schema"] = schema
    cfg["scale_profile"] = os.environ.get("SCALE_PROFILE", cfg.get("scale_profile", "demo"))
    return cfg


def volume_path(catalog: str, schema: str, volume: str = "raw") -> str:
    return f"/Volumes/{catalog}/{schema}/{volume}"


def fq(catalog: str, schema: str, table: str) -> str:
    return f"{catalog}.{schema}.{table}"
