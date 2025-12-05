from __future__ import annotations

"""
DAIOE weighting pipeline: load pre-translated DAIOE data, attach SCB employment
weights, aggregate across levels, and compute percentiles.
"""

import argparse
import importlib.util
from pathlib import Path
from typing import Dict, Iterable, Literal, Tuple

import pandas as pd

import config

ROOT = Path(__file__).resolve().parent


def _load_module(name: str):
    target = ROOT / f"{name}.py"
    spec = importlib.util.spec_from_file_location(name, target)
    module = importlib.util.module_from_spec(spec)
    if spec.loader is None:  # pragma: no cover - defensive
        raise ImportError(f"Could not load module '{name}' from {target}")
    spec.loader.exec_module(module)
    return module


_scb = _load_module("01_scbpull")
Taxonomy = Literal["ssyk2012", "ssyk96"]

# ----------------------------------------------------------------------------
# Helpers
# ----------------------------------------------------------------------------
def ensure_columns(df: pd.DataFrame, required: list[str]) -> None:
    missing = [col for col in required if col not in df.columns]
    if missing:
        raise KeyError(f"Missing expected columns: {missing}")


def split_code_label(series: pd.Series) -> tuple[pd.Series, pd.Series]:
    parts = series.astype(str).str.split(" ", n=1, expand=True)
    parts = parts.fillna({0: "", 1: ""})
    return parts[0], parts[1]


# ----------------------------------------------------------------------------
# Data loaders
# ----------------------------------------------------------------------------
def load_daioe_raw(taxonomy: Taxonomy, sep: str = config.DEFAULT_SEP) -> pd.DataFrame:
    if taxonomy not in config.DATASET_URLS:
        raise KeyError(f"No dataset URL configured for taxonomy '{taxonomy}'")
    return pd.read_csv(config.DATASET_URLS[taxonomy], sep=sep)


def prepare_raw_dataframe(
    raw: pd.DataFrame, taxonomy: Taxonomy
) -> tuple[pd.DataFrame, list[str]]:
    """Clean DAIOE raw frame and expose code/label columns for levels 1–4."""
    df = raw.drop(columns=["Unnamed: 0"], errors="ignore").copy()
    ensure_columns(df, ["year"])

    daioe_cols = [col for col in df.columns if col.startswith("daioe_")]
    if not daioe_cols:
        raise KeyError("Expected at least one 'daioe_*' column in DAIOE raw file.")

    code_cols = {
        4: f"{taxonomy}_4",
        3: f"{taxonomy}_3",
        2: f"{taxonomy}_2",
        1: f"{taxonomy}_1",
    }
    ensure_columns(df, list(code_cols.values()))

    for level, col in code_cols.items():
        codes, labels = split_code_label(df[col])
        df[f"code{level}"] = codes
        df[f"label{level}"] = labels

    df["code4"] = df["code4"].str.zfill(4)
    for level in (1, 2, 3):
        df[f"code{level}"] = df[f"code{level}"].str.lstrip("0")

    return df, daioe_cols


def attach_employment(df: pd.DataFrame, scb: pd.DataFrame) -> pd.DataFrame:
    """Join employment weights (level 4) onto the DAIOE rows."""
    scb_lvl4 = scb[scb["level"] == 4].copy()
    if scb_lvl4.empty:
        raise ValueError("SCB data must contain level-4 rows for weighting.")

    scb_lvl4["code4"] = scb_lvl4["code"].astype(str).str.zfill(4)
    merged = df.merge(
        scb_lvl4[["code4", "value"]],
        on="code4",
        how="left",
        validate="many_to_one",
    )
    return merged.rename(columns={"value": "emp"})


def compute_children_maps(df: pd.DataFrame) -> dict[int, pd.DataFrame]:
    """Count how many descendants each code has in the next-lower level."""
    counts = {
        1: df.groupby(["year", "code1"])["code2"]
        .nunique()
        .reset_index(name="n_children"),
        2: df.groupby(["year", "code2"])["code3"]
        .nunique()
        .reset_index(name="n_children"),
        3: df.groupby(["year", "code3"])["code4"]
        .nunique()
        .reset_index(name="n_children"),
    }
    lvl4 = df.groupby(["year", "code4"]).size().reset_index(name="n_children")
    lvl4["n_children"] = 1
    counts[4] = lvl4
    return counts


# ----------------------------------------------------------------------------
# Aggregation utilities
# ----------------------------------------------------------------------------
def aggregate_level(
    df: pd.DataFrame,
    *,
    daioe_cols: list[str],
    n_children: dict[int, pd.DataFrame],
    taxonomy: Taxonomy,
    level: int,
    method: Literal["weighted", "simple"],
) -> pd.DataFrame:
    if level not in (1, 2, 3):
        raise ValueError("Only levels 1–3 can be aggregated from level 4.")

    code_col, label_col = f"code{level}", f"label{level}"
    group_cols = ["year", code_col, label_col]

    if method == "weighted":
        tmp = df[group_cols + ["emp"] + daioe_cols].copy()
        for metric in daioe_cols:
            mask = tmp[metric].notna()
            tmp[f"{metric}_wx"] = tmp[metric].where(mask, 0) * tmp["emp"].where(
                mask, 0
            )
            tmp[f"{metric}_w"] = tmp["emp"].where(mask, 0)
        agg_cols = {f"{metric}_wx": "sum" for metric in daioe_cols}
        agg_cols.update({f"{metric}_w": "sum" for metric in daioe_cols})
        grouped = tmp.groupby(group_cols, as_index=False).agg(agg_cols)
        for metric in daioe_cols:
            denom = grouped[f"{metric}_w"].replace(0, pd.NA)
            grouped[metric] = grouped[f"{metric}_wx"] / denom
            grouped.drop(columns=[f"{metric}_wx", f"{metric}_w"], inplace=True)
    else:
        grouped = df[group_cols + daioe_cols].groupby(group_cols, as_index=False).mean()

    grouped = grouped.merge(
        n_children[level],
        left_on=["year", code_col],
        right_on=["year", code_col],
        how="left",
    )

    out = grouped[["year", code_col, label_col, "n_children"] + daioe_cols].copy()
    out["taxonomy"] = taxonomy
    out["level"] = level
    out = out.rename(columns={code_col: "code", label_col: "label"})
    out["code"] = out["code"].astype(str)
    return out


def base_level_four(
    df: pd.DataFrame,
    daioe_cols: list[str],
    taxonomy: Taxonomy,
    n_children: pd.DataFrame,
) -> pd.DataFrame:
    base = df[["year", "code4", "label4"] + daioe_cols].copy()
    base = base.merge(n_children, on=["year", "code4"], how="left")
    base["taxonomy"] = taxonomy
    base["level"] = 4
    base = base.rename(columns={"code4": "code", "label4": "label"})
    base["code"] = base["code"].astype(str)
    return base


def add_percentiles(df: pd.DataFrame, metrics: list[str]) -> list[str]:
    pct_cols: list[str] = []
    for metric in metrics:
        suffix = metric.removeprefix("daioe_")
        rank_col = f"pct_rank_{suffix}"
        df[rank_col] = df.groupby(["year", "level"])[metric].rank(pct=True)
        pct_cols.append(rank_col)
    return pct_cols


def build_pipeline(
    df: pd.DataFrame,
    *,
    daioe_cols: list[str],
    taxonomy: Taxonomy,
    n_children: dict[int, pd.DataFrame],
    method: Literal["weighted", "simple"],
) -> pd.DataFrame:
    lvl4 = base_level_four(df, daioe_cols, taxonomy, n_children[4])
    lvl1 = aggregate_level(
        df,
        daioe_cols=daioe_cols,
        n_children=n_children,
        taxonomy=taxonomy,
        level=1,
        method=method,
    )
    lvl2 = aggregate_level(
        df,
        daioe_cols=daioe_cols,
        n_children=n_children,
        taxonomy=taxonomy,
        level=2,
        method=method,
    )
    lvl3 = aggregate_level(
        df,
        daioe_cols=daioe_cols,
        n_children=n_children,
        taxonomy=taxonomy,
        level=3,
        method=method,
    )

    combined = pd.concat([lvl1, lvl2, lvl3, lvl4], ignore_index=True)
    pct_cols = add_percentiles(combined, daioe_cols)
    ordered = [
        "taxonomy",
        "level",
        "code",
        "label",
        "year",
        "n_children",
        *daioe_cols,
        *pct_cols,
    ]
    return combined[ordered].sort_values(["level", "code", "year"], ignore_index=True)


# ----------------------------------------------------------------------------
# Pipeline driver
# ----------------------------------------------------------------------------
def run_weighting(
    taxonomy: Taxonomy,
    *,
    sep: str = config.DEFAULT_SEP,
) -> Dict[str, object]:
    """End-to-end flow: fetch raw, attach SCB, aggregate."""
    raw = load_daioe_raw(taxonomy, sep=sep)
    scb_df, scb_year = _scb.fetch_taxonomy_dataframe(taxonomy)
    prepared, daioe_cols = prepare_raw_dataframe(raw, taxonomy)
    prepared = attach_employment(prepared, scb_df)
    n_children = compute_children_maps(prepared)

    weighted = build_pipeline(
        prepared,
        daioe_cols=daioe_cols,
        taxonomy=taxonomy,
        n_children=n_children,
        method="weighted",
    )
    simple = build_pipeline(
        prepared,
        daioe_cols=daioe_cols,
        taxonomy=taxonomy,
        n_children=n_children,
        method="simple",
    )

    return {
        "taxonomy": taxonomy,
        "scb_year": scb_year,
        "weighted": weighted,
        "simple": simple,
        "scb": scb_df,
    }


def run_pipeline(
    taxonomies: Iterable[Taxonomy] | None = None,
    *,
    sep: str = config.DEFAULT_SEP,
) -> Dict[Taxonomy, Dict[str, object]]:
    taxonomies = list(taxonomies or ["ssyk2012", "ssyk96"])
    results: Dict[Taxonomy, Dict[str, object]] = {}
    for taxonomy in taxonomies:
        results[taxonomy] = run_weighting(taxonomy, sep=sep)
    return results


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Aggregate DAIOE subindices across SSYK taxonomy levels",
    )
    parser.add_argument(
        "--taxonomy",
        action="append",
        choices=["ssyk2012", "ssyk96"],
        help="Which taxonomy to process (default: both ssyk2012 and ssyk96)",
    )
    parser.add_argument(
        "--sep",
        default=config.DEFAULT_SEP,
        help=f"Delimiter used in DAIOE source files (default: '{config.DEFAULT_SEP}')",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    taxonomies = args.taxonomy or ["ssyk2012", "ssyk96"]
    results = run_pipeline(taxonomies, sep=args.sep)
    for taxonomy, payload in results.items():
        weighted = payload["weighted"]
        simple = payload["simple"]
        print(f"{taxonomy}: weighted rows={len(weighted)}, simple rows={len(simple)}")
        print(f"  SCB year: {payload['scb_year']}")


if __name__ == "__main__":
    main()
