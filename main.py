from __future__ import annotations

"""
CLI entrypoint: runs the DAIOE pipeline end-to-end and logs row counts plus SCB
year, useful for sanity-checking outside the Shiny app.
"""

import argparse
from pathlib import Path
from typing import Iterable


PROJECT_ROOT = Path(__file__).resolve().parent
SCRIPTS_DIR = PROJECT_ROOT / "scripts"

# Scripts are exposed via scripts/__init__.py to avoid numeric imports.
from scripts import Taxonomy, run_pipeline  # noqa: E402


def run_pipeline_cli(
    taxonomies: Iterable[Taxonomy], translation_source: str | None = None
):
    """Run SCB pull + weighting for each taxonomy (no disk writes)."""
    return run_pipeline(taxonomies, translation_source=translation_source)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Pull SCB data and build employment-weighted DAIOE aggregates",
    )
    parser.add_argument(
        "--taxonomy",
        action="append",
        choices=["ssyk2012", "ssyk96"],
        help="Taxonomy to refresh (can be provided multiple times). Defaults to both.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    taxonomies = args.taxonomy or ["ssyk2012", "ssyk96"]
    results = run_pipeline_cli(taxonomies, translation_source=None)

    print("\nDAIOE datasets refreshed in-memory:\n" + "-" * 40)
    for taxonomy, payload in results.items():
        print(f"Taxonomy: {taxonomy}")
        print(f"  SCB year:             {payload['scb_year']}")
        print(f"  Weighted rows:        {len(payload['weighted'])}")
        print(f"  Simple-average rows:  {len(payload['simple'])}")
        if payload["unmatched"]:
            print(f"  Translation notes:    {payload['unmatched']}")


if __name__ == "__main__":
    main()
