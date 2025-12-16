"""Data manager for loading and caching pipeline results.

This module encapsulates the logic for computing the heavy data
transformations in ``pipeline.py`` and persisting the results to disk.
It adds a small amount of resilience around caching and uses
``logging`` instead of printing directly to stdout. The cache files
include a version tag to make it easy to invalidate caches when
fundamental changes are made to the pipeline logic.
"""

from __future__ import annotations

import logging
import os
import tempfile
from functools import lru_cache
from pathlib import Path
from typing import Dict

import pandas as pd

from . import config, pipeline

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Cache setup
# ---------------------------------------------------------------------------
# A version tag to embed into the cache filenames. Bump this value
# whenever the underlying ``pipeline`` logic changes in a way that
# invalidates existing caches.
CACHE_VERSION: str = "v1"


def _resolve_cache_dir() -> Path:
    """Select a writable directory for caching.

    The lookup order is:

    1. The ``DATA_CACHE_DIR`` environment variable, if set.
    2. A ``data`` folder at the repository root.
    3. A temporary directory in ``/tmp``.

    Each candidate path is tested for writability by attempting to
    create and delete a sentinel file. The first path that succeeds
    is returned. If none succeed, a final fallback directory in ``/tmp``
    is created and returned.
    """

    candidates: list[Path] = []
    env = os.getenv("DATA_CACHE_DIR")
    if env:
        candidates.append(Path(env).expanduser().resolve())

    candidates.append(Path(__file__).resolve().parent.parent / "data")
    candidates.append(Path(tempfile.gettempdir()) / "employment_ai_cache")

    for path in candidates:
        try:
            path.mkdir(parents=True, exist_ok=True)
            test_file = path / f".write_test_{os.getpid()}"
            test_file.write_text("ok", encoding="utf-8")
            test_file.unlink()
            return path
        except Exception:
            continue

    fallback = Path(tempfile.gettempdir()) / "employment_ai_cache"
    fallback.mkdir(parents=True, exist_ok=True)
    return fallback


DATA_DIR: Path = _resolve_cache_dir()
WEIGHTED_CACHE: Path = DATA_DIR / f"daioe_weighted_{CACHE_VERSION}.csv"
SIMPLE_CACHE: Path = DATA_DIR / f"daioe_simple_{CACHE_VERSION}.csv"


def _atomic_to_csv(df: pd.DataFrame, path: Path) -> None:
    """Write a DataFrame to CSV atomically."""

    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    df.to_csv(tmp_path, index=False)
    tmp_path.replace(path)


@lru_cache(maxsize=1)
def _compute_pipeline_payload() -> Dict[str, pd.DataFrame]:
    """Runs the heavy pipeline calculation."""

    results = pipeline.run_pipeline()
    weighted_frames: list[pd.DataFrame] = []
    simple_frames: list[pd.DataFrame] = []

    for _, taxonomy in config.TAXONOMY_OPTIONS:
        payload = results.get(taxonomy)
        if not payload:
            continue

        weighted = payload.get("weighted")
        simple = payload.get("simple")

        if isinstance(weighted, pd.DataFrame) and not weighted.empty:
            weighted_frames.append(weighted)
        if isinstance(simple, pd.DataFrame) and not simple.empty:
            simple_frames.append(simple)

    if not weighted_frames or not simple_frames:
        raise RuntimeError("Pipeline did not return weighted + simple datasets.")

    return {
        "weighted": pd.concat(weighted_frames, ignore_index=True),
        "simple": pd.concat(simple_frames, ignore_index=True),
    }


def load_payload(force_recompute: bool = False) -> Dict[str, pd.DataFrame]:
    """Load data from disk cache if available, otherwise compute and save."""

    if not force_recompute and WEIGHTED_CACHE.exists() and SIMPLE_CACHE.exists():
        logger.info("Loading pipeline output from cache directory %s", DATA_DIR)
        try:
            dtype = {"taxonomy": str, "code": str}
            weighted_df = pd.read_csv(WEIGHTED_CACHE, dtype=dtype)
            simple_df = pd.read_csv(SIMPLE_CACHE, dtype=dtype)
            return {"weighted": weighted_df, "simple": simple_df}
        except Exception as exc:
            logger.warning(
                "Error reading cache files %s and %s: %s; falling back to recompute",
                WEIGHTED_CACHE,
                SIMPLE_CACHE,
                exc,
            )

    if force_recompute:
        _compute_pipeline_payload.cache_clear()

    logger.info("Computing pipeline data – this may take a while…")
    payload = _compute_pipeline_payload()

    try:
        _atomic_to_csv(payload["weighted"], WEIGHTED_CACHE)
        _atomic_to_csv(payload["simple"], SIMPLE_CACHE)
        logger.info(
            "Cache updated: weighted=%s, simple=%s",
            WEIGHTED_CACHE.name,
            SIMPLE_CACHE.name,
        )
    except Exception as exc:
        logger.warning("Could not write cache files: %s", exc)

    return payload


def load_data(force_recompute: bool = False) -> Dict[str, pd.DataFrame]:
    """Load app-ready frames per taxonomy (from cache when possible)."""

    payload = load_payload(force_recompute=force_recompute)
    weighted_all = payload["weighted"]
    simple_all = payload["simple"]

    frames: Dict[str, pd.DataFrame] = {}
    for _, taxonomy in config.TAXONOMY_OPTIONS:
        dfs = []

        weighted = (
            weighted_all[weighted_all["taxonomy"] == taxonomy]
            if "taxonomy" in weighted_all.columns
            else weighted_all
        )
        simple = (
            simple_all[simple_all["taxonomy"] == taxonomy]
            if "taxonomy" in simple_all.columns
            else simple_all
        )

        if not weighted.empty:
            tmp = weighted.copy()
            tmp["weighting"] = "emp_weighted"
            tmp["weighting_label"] = config.WEIGHTING_OPTIONS[0][0]
            dfs.append(tmp)

        if not simple.empty:
            tmp = simple.copy()
            tmp["weighting"] = "simple_avg"
            tmp["weighting_label"] = config.WEIGHTING_OPTIONS[1][0]
            dfs.append(tmp)

        if dfs:
            full = pd.concat(dfs, ignore_index=True)
            if "year" in full.columns:
                full["year"] = full["year"].astype(int)
            if "level" in full.columns:
                full["level"] = full["level"].astype(int)
            frames[taxonomy] = full

    if not frames:
        raise RuntimeError("No aggregated DAIOE datasets could be built in-memory.")
    return frames
