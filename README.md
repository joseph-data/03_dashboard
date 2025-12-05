---
title: DAIOE Explorer
emoji: 🌍
colorFrom: yellow
colorTo: indigo
sdk: docker
pinned: false
license: mit
---

DAIOE Explorer
==============

What it does
------------
- Runs the DAIOE pipeline fully in-memory: pulls SCB employment counts, downloads the pre-translated DAIOE data from `joseph-data/07_translate_ssyk` (`03_translated_files`), then builds weighted/simple aggregates.
- Feeds the Shiny app directly from these in-memory results; no local `data/` writes required.

Workflow
--------
1) SCB pull (`scripts/01_scbpull.py`): fetches the latest employment counts for a taxonomy and returns a tidy DataFrame (levels 1–4).  
2) Weighting (`scripts/02_weighting.py`): downloads pre-translated DAIOE CSVs from GitHub, merges SCB weights, computes weighted and simple aggregates, and returns both DataFrames.  
3) App integration: `app.py` calls `run_pipeline` on import to get fresh data for the UI; run `python -m scripts.03_main` to sanity-check the pipeline via CLI.

Running locally
---------------
- Run `python -m scripts.03_main` to confirm the pipeline completes; it will log row counts and SCB year.  
- Start the Shiny app (e.g., `python app.py`) and it will load the freshly built in-memory frames.

Notes
-----
- Legacy scripts in `scripts/01_scbPull.py` and `scripts/02_weighting.py` wrote to disk; the current no-write path uses `01_scbpull.py` and `02_weighting.py`.  
- Network access is required to reach the GitHub dataset and the SCB API.
- Pre-translated source CSVs live at `https://github.com/joseph-data/07_translate_ssyk/tree/main/03_translated_files`; no translation workbooks are needed at runtime.
