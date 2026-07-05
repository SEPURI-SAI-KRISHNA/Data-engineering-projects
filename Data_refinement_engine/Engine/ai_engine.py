import inspect
import json
import logging
import os
import re

from openai import OpenAI

import processor

logger = logging.getLogger(__name__)

METADATA_FILE = "function_metadata.json"


class AIEngine:
    """LLM helpers: describe registry functions, match user intent against
    them, and generate new ones. Pure execution lives in processor.py."""

    @staticmethod
    def scan_registry():
        """Returns {category: {func_name: [param_names]}} for the UI."""
        registry = {}
        for category, cls in processor.REGISTRIES.items():
            registry[category] = {}
            for name, func in inspect.getmembers(cls, predicate=inspect.isfunction):
                sig = inspect.signature(func)
                params = [p for p in sig.parameters if p not in ('value', 'self')]
                registry[category][name] = params
        return registry

    @staticmethod
    def _load_metadata():
        if os.path.exists(METADATA_FILE):
            with open(METADATA_FILE) as f:
                return json.load(f)
        return {"transformation": {}, "validation": {}, "derivation": {}}

    @staticmethod
    def _save_metadata(data):
        with open(METADATA_FILE, "w") as f:
            json.dump(data, f, indent=2)

    @staticmethod
    def _update_descriptions_if_needed(category, api_key, base_url):
        """Generate one-line descriptions for registry functions that don't
        have one in the metadata file yet."""
        registry = AIEngine.scan_registry()
        metadata = AIEngine._load_metadata()

        new_funcs = [f for f in registry[category]
                     if f not in metadata.get(category, {})]
        if not new_funcs or not api_key:
            return

        client = OpenAI(api_key=api_key, base_url=base_url)

        for func_name in new_funcs:
            func_obj = getattr(processor.REGISTRIES[category], func_name)
            try:
                source_code = inspect.getsource(func_obj)
            except OSError:
                source_code = "Source code unavailable."

            prompt = (
                "Analyze this Python function and describe what it does in one "
                "simple sentence for a non-technical user.\n\n"
                f"Function name: {func_name}\n"
                f"Code:\n{source_code}\n\n"
                "Output ONLY the description."
            )

            try:
                response = client.chat.completions.create(
                    model="gpt-4o-mini",
                    messages=[{"role": "user", "content": prompt}],
                    temperature=0.0,
                )
                metadata.setdefault(category, {})[func_name] = \
                    response.choices[0].message.content.strip()
            except Exception as e:
                logger.warning("description generation failed for %s: %s", func_name, e)

        AIEngine._save_metadata(metadata)

    @staticmethod
    def _find_semantic_match(user_query, category, api_key, base_url):
        """Ask the LLM whether an existing function already covers the query."""
        category_data = AIEngine._load_metadata().get(category, {})
        if not category_data:
            return None

        choices_text = "\n".join(f"- {name}: {desc}"
                                 for name, desc in category_data.items())
        prompt = (
            f'User query: "{user_query}"\n\n'
            f"Available functions:\n{choices_text}\n\n"
            "Does one of the available functions match the user query?\n"
            "- If yes, return ONLY the function name.\n"
            '- If no, return EXACTLY the word "NONE".'
        )

        try:
            client = OpenAI(api_key=api_key, base_url=base_url)
            response = client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[{"role": "user", "content": prompt}],
                temperature=0.0,
            )
            result = response.choices[0].message.content.strip()
        except Exception as e:
            logger.warning("semantic match failed: %s", e)
            return None

        result = result.replace("`", "").replace("'", "").strip()
        # only trust names that actually exist
        return result if result in category_data else None

    @staticmethod
    def ask_llm_for_function(user_query, category, api_key, base_url, force_generation=False):
        if not api_key:
            return {"status": "error", "message": "Missing API key."}

        if not force_generation:
            AIEngine._update_descriptions_if_needed(category, api_key, base_url)
            match = AIEngine._find_semantic_match(user_query, category, api_key, base_url)
            if match:
                return {"status": "found", "function_name": match}
            return {"status": "confirmation_needed"}

        system_prompt = (
            "You are a Python code generator.\n"
            "Generate a SINGLE Python static method for a class.\n"
            f"- Category: {category}\n"
            "- The function must accept 'value' as the first argument.\n"
            "- It must be a @staticmethod with type hints and a docstring.\n"
            "- Do NOT include the class definition."
        )

        try:
            client = OpenAI(api_key=api_key, base_url=base_url)
            response = client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": f"Create a function that does this: {user_query}"},
                ],
                temperature=0.2,
            )
        except Exception as e:
            return {"status": "error", "message": str(e)}

        code = response.choices[0].message.content.strip()
        code = code.replace("```python", "").replace("```", "").strip()

        match = re.search(r"def\s+([a-zA-Z_0-9]+)\(", code)
        func_name = match.group(1) if match else "unknown_function"

        return {"status": "generated", "function_name": func_name, "code": code}

    @staticmethod
    def append_function_to_file(category, code_snippet):
        file_map = {
            "transformation": "transformations.py",
            "validation": "validations.py",
            "derivation": "derivations.py",
        }
        filename = file_map[category]

        with open(filename) as f:
            if code_snippet.strip() in f.read():
                return False

        indented = "\n    " + code_snippet.replace("\n", "\n    ")
        with open(filename, "a") as f:
            f.write("\n" + indented + "\n")
        return True
