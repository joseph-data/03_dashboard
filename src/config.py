"""Centralized configuration for DAIOE Explorer.

- Override dataset URLs via env vars (`DAIOE_SSYK2012_URL`, `DAIOE_SSYK96_URL`).
- UI option lists used by `app.py` live here.
"""

from __future__ import annotations

import os
from typing import Dict, List, Tuple

# Remote pre-translated datasets (can be overridden via env vars).
DAIOE_SSYK2012_URL = os.getenv(
    "DAIOE_SSYK2012_URL",
    "https://raw.githubusercontent.com/joseph-data/07_translate_ssyk/main/03_translated_files/daioe_ssyk2012_translated.csv",
)
DAIOE_SSYK96_URL = os.getenv(
    "DAIOE_SSYK96_URL",
    "https://raw.githubusercontent.com/joseph-data/07_translate_ssyk/main/03_translated_files/daioe_ssyk96_translated.csv",
)

DATASET_URLS: Dict[str, str] = {
    "ssyk2012": DAIOE_SSYK2012_URL,
    "ssyk96": DAIOE_SSYK96_URL,
}

# Default CSV separator used by weighting/aggregation.
DEFAULT_SEP: str = os.getenv("DAIOE_CSV_SEP", ",")

# Taxonomy and level options shared across UI/CLI.
TAXONOMY_OPTIONS: List[Tuple[str, str]] = [
    ("🇸🇪 SSYK 2012", "ssyk2012"),
    ("🇸🇪 SSYK 1996", "ssyk96"),
]

LEVEL_OPTIONS: List[Tuple[str, int]] = [
    ("Level 4 (4-digit)", 4),
    ("Level 3 (3-digit)", 3),
    ("Level 2 (2-digit)", 2),
    ("Level 1 (1-digit)", 1),
]

# Default UI selections.
DEFAULT_TAXONOMY = "ssyk2012"
DEFAULT_LEVEL = 3
DEFAULT_WEIGHTING = "emp_weighted"
DEFAULT_TOP_N = 10
DEFAULT_SORT_DESC = True

# Shared UI options.
METRIC_OPTIONS: List[Tuple[str, str]] = [
    ("📚 All Applications", "allapps"),
    ("♟️ Abstract strategy games", "stratgames"),
    ("🎮 Real-time video games", "videogames"),
    ("🖼️🔎 Image recognition", "imgrec"),
    ("🧩🖼️ Image comprehension", "imgcompr"),
    ("🖌️🖼️ Image generation", "imggen"),
    ("📖 Reading comprehension", "readcompr"),
    ("✍️🤖 Language modelling", "lngmod"),
    ("🌐🔤 Translation", "translat"),
    ("🗣️🎙️ Speech recognition", "speechrec"),
    ("🧠✨ Generative AI", "genai"),
]

WEIGHTING_OPTIONS: List[Tuple[str, str]] = [
    ("Employment weighted", "emp_weighted"),
    ("Simple average", "simple_avg"),
]
