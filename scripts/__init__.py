from __future__ import annotations

"""
Convenience imports and module loader for the numbered pipeline scripts
(SCB pull and weighting). Used by the app and CLI entrypoints.
"""

import importlib.util
from pathlib import Path
from typing import Iterable

# Scripts directory (resolved relative to this file).
ROOT = Path(__file__).resolve().parent


def _load_module(name: str):
    """Load a numbered script module by filename."""
    target = ROOT / f"{name}.py"
    spec = importlib.util.spec_from_file_location(name, target)
    module = importlib.util.module_from_spec(spec)
    if spec.loader is None:  # pragma: no cover - defensive
        raise ImportError(f"Could not load module '{name}' from {target}")
    spec.loader.exec_module(module)
    return module


_scb = _load_module("01_scbpull")
_weighting = _load_module("02_weighting")

# Re-export shared types and helpers for consumers (app.py, CLI, etc.)
Taxonomy = _weighting.Taxonomy
fetch_scb_taxonomy = _scb.fetch_taxonomy_dataframe
run_weighting = _weighting.run_weighting
run_pipeline = _weighting.run_pipeline

__all__: Iterable[str] = [
    "Taxonomy",
    "fetch_scb_taxonomy",
    "run_weighting",
    "run_pipeline",
]
