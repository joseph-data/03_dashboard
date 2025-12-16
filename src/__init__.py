"""DAIOE Explorer core package.

This package is what the app imports. The public surface area is intentionally small:
- `config` for UI options and defaults
- `run_pipeline`/`run_weighting` for pipeline execution
- `fetch_taxonomy_dataframe` for raw SCB weights
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from . import config
from .scb_fetch import Taxonomy, fetch_taxonomy_dataframe

if TYPE_CHECKING:  # pragma: no cover
    from .pipeline import run_pipeline, run_weighting

__all__ = [
    "Taxonomy",
    "config",
    "fetch_taxonomy_dataframe",
    "run_pipeline",
    "run_weighting",
]


def __getattr__(name: str):  # pragma: no cover
    if name in {"run_pipeline", "run_weighting"}:
        from .pipeline import run_pipeline, run_weighting

        return {"run_pipeline": run_pipeline, "run_weighting": run_weighting}[name]
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
