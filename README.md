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
- Runs the DAIOE pipeline in-memory (optionally cached to disk): pulls SCB employment counts, downloads the pre-translated DAIOE data from `joseph-data/07_translate_ssyk` (`03_translated_files`), then builds weighted/simple aggregates.
- Feeds the Shiny app from cached or freshly computed results (default cache folder: `data/`, ignored by git).

Workflow
--------
1) SCB pull (`src/scb_fetch.py`): fetches the latest employment counts for a taxonomy and returns a tidy DataFrame (levels 1–4).  
2) Weighting + aggregation (`src/pipeline.py`): downloads pre-translated DAIOE CSVs from GitHub, merges SCB weights, computes weighted and simple aggregates, and returns both DataFrames.  
3) App integration: `app.py` loads fresh data on import via `src/data_manager.py`; run `python -m src.pipeline` to sanity-check the pipeline via CLI.

Running locally
---------------
- Run `python -m src.pipeline` to confirm the pipeline completes; it will log row counts and SCB year.  
- Start the Shiny app (e.g., `python app.py`) and it will load the freshly built in-memory frames.

Notes
-----
- The app uses a lightweight CSV cache for pipeline outputs via `src/data_manager.py` (override with `DATA_CACHE_DIR`).  
- Network access is required to reach the GitHub dataset and the SCB API.
- Pre-translated source CSVs live at `https://github.com/joseph-data/07_translate_ssyk/tree/main/03_translated_files`; no translation workbooks are needed at runtime.
