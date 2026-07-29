# Data Refinement Engine

Turns a raw record into a refined record through a per-field pipeline of
transformations, validations, and derivations, configured in a Streamlit UI.
The function library is extensible at runtime: describe what you need in
plain language and the app searches the existing registry first, and only
generates (and shows you for approval) new Python code when nothing matches.

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

## Layout

- `Engine/processor.py` — pure record processing, no UI or LLM dependencies
- `Engine/transformations.py`, `validations.py`, `derivations.py` — the function registries
- `Engine/ai_engine.py` — LLM search/generation over the registries
- `Engine/engine.py` — the Streamlit app

## Tests

```bash
pip install pytest
pytest tests/
```
