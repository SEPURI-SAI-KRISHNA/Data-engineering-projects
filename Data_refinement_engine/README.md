# Data Refinement Engine

Turns a raw record into a refined record through a per-field pipeline of
transformations, validations, and derivations, configured in a Streamlit UI.
The function library is extensible at runtime: describe what you need in
plain language and the app searches the existing registry first, and only
generates (and shows you for approval) new Python code when nothing matches.

## Directory layout

```
Data_refinement_engine/
├── Engine/
│   ├── engine.py            # Streamlit entry point — UI and session state
│   ├── processor.py         # applies a mapping config to a record
│   ├── transformations.py   # built-in transformation functions
│   ├── validations.py       # built-in validation functions
│   ├── derivations.py       # built-in derivation functions
│   ├── ai_engine.py         # LLM search/generation over the registries
│   └── function_metadata.json
├── tests/
└── requirements.txt
```

## Running

```bash
python -m venv venv && source venv/bin/activate
pip install -r requirements.txt

# only needed for the AI assistant features
export OPENAI_API_KEY=sk-...
# export OPENAI_BASE_URL=...   # optional, for local/alternative providers

cd Engine
streamlit run engine.py
```

Upload a CSV or JSON file, configure each field (rename, transform, validate,
derive), preview the result on row 1, then run the whole dataset and download
the refined output plus the schema JSON.

Validation failures and steps that raise are collected per record and shown
in the results panel instead of being silently swallowed.

## Tests

```bash
pip install pytest
pytest tests/
```

---

> Part of [Data Engineering Projects](../README.md) · [Contributing](../CONTRIBUTING.md) · [MIT License](../LICENSE)
