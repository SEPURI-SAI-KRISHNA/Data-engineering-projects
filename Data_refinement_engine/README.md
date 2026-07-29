# Data Refinement Engine

A configurable Streamlit application for turning raw records into clean, enriched records. Users define per-field **transformations**, **validations**, and **derivations** through a point-and-click UI — and can ask an AI assistant to generate new processing functions on the fly.

## What it does

Upload a CSV or JSON file, then configure each field individually:

- **Transform** — reshape values (trim whitespace, normalise case, round numbers, etc.)
- **Validate** — assert constraints (non-null, regex match, range check, etc.)
- **Derive** — compute new fields from existing ones (concatenate, extract substrings, etc.)

The engine applies the configured pipeline to the first record live so you can see the output before committing. When the mapping looks right, click **Run Transformation on All Records** to process the full dataset and download the result as JSON.

An **AI Thinking Box** on each step type accepts a plain-English description of what you want to do. It first searches the existing function registry for a match; if nothing fits it generates new Python code, shows it to you for review, and (on approval) appends it to the registry so it is available immediately and in future sessions.

## Directory layout

```
Data_refinement_engine/
├── Engine/
│   ├── engine.py            # Streamlit entry point — UI and session state
│   ├── processor.py         # Applies a mapping config to a record
│   ├── transformations.py   # Built-in transformation functions
│   ├── validations.py       # Built-in validation functions
│   ├── derivations.py       # Built-in derivation functions
│   ├── ai_engine.py         # LLM integration — registry scan, code generation, file append
│   └── function_metadata.json  # Human-readable descriptions for AI context
└── requirements.txt
```

## Quick start

```bash
cd Data_refinement_engine
python3 -m venv venv
venv/bin/pip install -r requirements.txt
venv/bin/streamlit run Engine/engine.py
```

Open the URL printed by Streamlit (default `http://localhost:8501`).

To enable AI code generation, set your OpenAI API key before starting:

```bash
export OPENAI_API_KEY=sk-...
venv/bin/streamlit run Engine/engine.py
```

## How it fits into this repository

The Data Refinement Engine is a self-contained utility for the **cleaning and enrichment** stage of a data pipeline. It complements the other projects by providing a reusable, interactive layer for transforming raw data before it enters a streaming or batch pipeline.

---

> Part of [Data Engineering Projects](../README.md) · [Contributing](../CONTRIBUTING.md) · [MIT License](../LICENSE)
