from __future__ import annotations

"""
CLI entrypoint: runs the DAIOE pipeline end-to-end and logs row counts plus SCB
year, useful for sanity-checking outside the Shiny app.
"""

import argparse
from typing import Iterable

# Scripts are exposed via scripts/__init__.py to avoid numeric imports.
from scripts import Taxonomy, run_pipeline  # noqa: E402


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
    results = run_pipeline(taxonomies)

    print("\nDAIOE datasets refreshed in-memory:\n" + "-" * 40)
    for taxonomy, payload in results.items():
        print(f"Taxonomy: {taxonomy}")
        print(f"  SCB year:             {payload['scb_year']}")
        print(f"  Weighted rows:        {len(payload['weighted'])}")
        print(f"  Simple-average rows:  {len(payload['simple'])}")


if __name__ == "__main__":
    main()
