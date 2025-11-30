from __future__ import annotations

"""
Translation utilities: apply English occupation labels to DAIOE raw SSYK data
using bundled translation workbooks (or user-specified paths). No SCB fallback.
"""

import argparse
from pathlib import Path
from typing import Dict, Literal, Set, Tuple

import pandas as pd

Taxonomy = Literal["ssyk2012", "ssyk96"]

ROOT = Path(__file__).resolve().parent
PROJECT_ROOT = ROOT.parent
DEFAULT_TRANSLATIONS: dict[Taxonomy, Path] = {
    "ssyk96": PROJECT_ROOT / "data" / "01_translation_files" / "ssyk96_en.xlsx",
    "ssyk2012": PROJECT_ROOT / "data" / "01_translation_files" / "ssyk2012_en.xlsx",
}

# Mapping of DAIOE column names to translation workbook sheets and padding.
LEVEL_SPECS: dict[Taxonomy, dict[str, tuple[str, int]]] = {
    "ssyk96": {
        "ssyk96_1": ("Level_1", 1),
        "ssyk96_2": ("Level_2", 2),
        "ssyk96_3": ("Level_3", 3),
        "ssyk96_4": ("Level_4", 4),
    },
    "ssyk2012": {
        "ssyk2012_1": ("1-digit", 1),
        "ssyk2012_2": ("2-digit", 2),
        "ssyk2012_3": ("3-digit", 3),
        "ssyk2012_4": ("4-digit", 4),
    },
}


def load_translation_map(sheet: str, digits: int, source: str | Path) -> Dict[str, str]:
    df = pd.read_excel(source, sheet_name=sheet, usecols=[0, 1])
    df.columns = ["code", "name"]
    df = df.dropna(subset=["code", "name"])

    def normalize_code(value) -> str | None:
        try:
            text = str(value).strip()
        except Exception:
            return None
        if not text:
            return None
        try:
            return str(int(float(text))).zfill(digits)
        except (TypeError, ValueError):
            return None

    df["code"] = df["code"].apply(normalize_code)
    df = df.dropna(subset=["code"])
    return dict(zip(df["code"], df["name"]))


def translate_column(
    series: pd.Series, mapping: Dict[str, str], digits: int
) -> Tuple[pd.Series, Set[str]]:
    unmatched_codes: Set[str] = set()

    def translate_value(value: str) -> str:
        nonlocal unmatched_codes
        if pd.isna(value):
            return value

        text = str(value).strip()
        if not text:
            return value

        raw_code = text.split(maxsplit=1)[0]
        normalized_code = raw_code.zfill(digits) if raw_code.isdigit() else raw_code
        english_name = mapping.get(normalized_code) or mapping.get(
            raw_code.zfill(digits)
        )

        if not english_name:
            unmatched_codes.add(normalized_code)
            return value

        return f"{normalized_code} {english_name}"

    translated = series.apply(translate_value)
    return translated, unmatched_codes


def translate_taxonomy(
    df: pd.DataFrame,
    taxonomy: Taxonomy,
    translation_source: str | Path | None = None,
) -> Tuple[pd.DataFrame, Dict[str, Set[str]]]:
    """
    Translate Swedish SSYK labels to English. No disk writes performed.

    - Uses translation workbooks only (bundled defaults or provided paths).
    - If no workbook is available, input is returned unchanged with an info note.
    """
    if translation_source is None and taxonomy in DEFAULT_TRANSLATIONS:
        candidate = DEFAULT_TRANSLATIONS[taxonomy]
        if candidate.exists():
            translation_source = candidate

    unmatched_summary: Dict[str, Set[str]] = {}
    translated = df.copy()

    column_levels = {f"{taxonomy}_{level}": level for level in range(1, 5)}

    if translation_source is None:
        unmatched_summary["info"] = {"translation skipped (no workbook available)"}
        return translated, unmatched_summary

    specs = LEVEL_SPECS.get(taxonomy, {})
    translations: Dict[str, Dict[str, str]] = {
        column: load_translation_map(sheet, digits, translation_source)
        for column, (sheet, digits) in specs.items()
    }

    if all(not mapping for mapping in translations.values()):
        unmatched_summary["info"] = {"translation skipped (no mapping available)"}
        return translated, unmatched_summary

    for column, level in column_levels.items():
        if column not in translated.columns:
            raise KeyError(f"Expected column '{column}' not found in input file")

        mapping = translations.get(column, {})
        if not mapping:
            unmatched_summary[column] = set()
            continue

        translated[column], unmatched_codes = translate_column(
            translated[column], mapping, level
        )
        unmatched_summary[column] = unmatched_codes

    return translated, unmatched_summary


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Translate Swedish SSYK96 labels in a DAIOE dataset to English using "
            "an Excel translation workbook."
        )
    )
    parser.add_argument(
        "--input",
        type=str,
        required=True,
        help="Input CSV/TSV URL or local path (tab-separated).",
    )
    parser.add_argument(
        "--translation-source",
        type=str,
        required=True,
        help="Path or URL to the translation Excel workbook.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    df = pd.read_csv(args.input, sep="\t")
    translated, unmatched = translate_taxonomy(
        df, "ssyk96", translation_source=args.translation_source
    )
    print(translated.head())
    print("Unmatched codes per column:", {k: sorted(v) for k, v in unmatched.items()})


if __name__ == "__main__":  # pragma: no cover
    main()
