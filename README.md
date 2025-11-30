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
- Runs the DAIOE pipeline fully in-memory: pulls SCB employment counts, downloads DAIOE raw data from `joseph-data/daioe_dataset`, optionally translates SSYK96 labels, then builds weighted/simple aggregates.
- Feeds the Shiny app directly from these in-memory results; no local `data/` writes required.

Workflow
--------
1) SCB pull (`scripts/01_scbpull.py`): fetches the latest employment counts for a taxonomy and returns a tidy DataFrame (levels 1–4).  
2) Translation (`scripts/02_translate.py`): if processing SSYK96 and a translation workbook is provided, Swedish labels are converted to English; otherwise input is passed through.  
3) Weighting (`scripts/03_weighting.py`): downloads raw DAIOE CSVs from GitHub, merges SCB weights, computes weighted and simple aggregates, and returns both DataFrames.  
4) App integration: `app.py` calls `run_pipeline` on import to get fresh data for the UI; `main.py` can be run to sanity-check the pipeline via CLI.

Running locally
---------------
- Optional: set `SSYK96_TRANSLATION_SOURCE` or `SSYK2012_TRANSLATION_SOURCE` to a path or URL of the translation Excel workbook; bundled defaults live under `data/01_translation_files/`.  
- Run `python main.py` to confirm the pipeline completes; it will log row counts and SCB year.  
- Start the Shiny app (e.g., `python app.py`) and it will load the freshly built in-memory frames.

Notes
-----
- Legacy scripts in `scripts/01_scbPull.py` and `scripts/02_weighting.py` still write to `data/`; the new numbered files (`01_scbpull.py`, `02_translate.py`, `03_weighting.py`) are the intended no-write path.  
- Network access is required to reach the GitHub dataset and the SCB API.
- Translation workbooks: `data/01_translation_files/ssyk96_en.xlsx` and `data/01_translation_files/ssyk2012_en.xlsx` are used automatically when explicit sources are not provided.
