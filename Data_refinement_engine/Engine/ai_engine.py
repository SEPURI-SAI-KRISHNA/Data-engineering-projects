import inspect
import importlib
import re
import json
import os
from openai import OpenAI
import transformations
import validations
import derivations

# Reload modules
importlib.reload(transformations)
importlib.reload(validations)
importlib.reload(derivations)

METADATA_FILE = "function_metadata.json"


class AIEngine:

    @staticmethod
    def scan_registry():
        """Scans python files and returns structure: {category: {func_name: [params]}}"""
        registry = {"transformation": {}, "validation": {}, "derivation": {}}

        def get_params(func):
            sig = inspect.signature(func)
            return [p for p in sig.parameters if p not in ['value', 'self']]

        for name, func in inspect.getmembers(transformations.TransformationRegistry, predicate=inspect.isfunction):
            registry["transformation"][name] = get_params(func)
        for name, func in inspect.getmembers(validations.ValidationRegistry, predicate=inspect.isfunction):
            registry["validation"][name] = get_params(func)
        for name, func in inspect.getmembers(derivations.DerivationRegistry, predicate=inspect.isfunction):
            registry["derivation"][name] = get_params(func)

        return registry

    @staticmethod
    def _load_metadata():
        if os.path.exists(METADATA_FILE):
            with open(METADATA_FILE, "r") as f:
                return json.load(f)
        return {"transformation": {}, "validation": {}, "derivation": {}}

    @staticmethod
    def _save_metadata(data):
        with open(METADATA_FILE, "w") as f:
            json.dump(data, f, indent=2)

    @staticmethod
    def _update_descriptions_if_needed_v1(category, api_key, base_url):
        """
        1. Checks current registry against JSON file.
        2. If a function is in registry but not in JSON, asks LLM to generate a description.
        3. Updates JSON.
        """
        registry = AIEngine.scan_registry()
        metadata = AIEngine._load_metadata()

        current_funcs = registry[category].keys()
        stored_funcs = metadata.get(category, {}).keys()

        new_funcs = [f for f in current_funcs if f not in stored_funcs]

        if not new_funcs:
            return  # Nothing to update

        # If we have new functions, generate descriptions for them
        if not api_key: return  # Cannot generate without key

        client = OpenAI(api_key=api_key, base_url=base_url)

        for func_name in new_funcs:
            # Get the docstring to help the LLM
            module_map = {
                "transformation": transformations.TransformationRegistry,
                "validation": validations.ValidationRegistry,
                "derivation": derivations.DerivationRegistry
            }
            func_obj = getattr(module_map[category], func_name)
            docstring = func_obj.__doc__ or "No docstring provided."

            prompt = f"""
            Describe the following Python function in one simple sentence for a non-technical user.
            Function Name: {func_name}
            Docstring: {docstring}
            Output ONLY the description.
            """

            try:
                response = client.chat.completions.create(
                    model="gpt-4o-mini",
                    messages=[{"role": "user", "content": prompt}],
                    temperature=0.0
                )
                desc = response.choices[0].message.content.strip()

                if category not in metadata: metadata[category] = {}
                metadata[category][func_name] = desc
            except:
                pass

        AIEngine._save_metadata(metadata)

    @staticmethod
    def _update_descriptions_if_needed(category, api_key, base_url):
        """
        1. Checks current registry against JSON file.
        2. If a function is in registry but not in JSON, asks LLM to generate a description.
        3. Updates JSON.
        """
        registry = AIEngine.scan_registry()
        metadata = AIEngine._load_metadata()

        current_funcs = registry[category].keys()
        stored_funcs = metadata.get(category, {}).keys()

        new_funcs = [f for f in current_funcs if f not in stored_funcs]

        if not new_funcs:
            return

        if not api_key: return

        client = OpenAI(api_key=api_key, base_url=base_url)

        module_map = {
            "transformation": transformations.TransformationRegistry,
            "validation": validations.ValidationRegistry,
            "derivation": derivations.DerivationRegistry
        }

        for func_name in new_funcs:
            func_obj = getattr(module_map[category], func_name)

            # --- UPGRADE: READ SOURCE CODE INSTEAD OF DOCSTRING ---
            try:
                # This grabs the actual python code of the function
                source_code = inspect.getsource(func_obj)
            except:
                source_code = "Source code unavailable."

            prompt = f"""
                Analyze this Python function code and describe what it does in one simple sentence for a non-technical user.

                Function Name: {func_name}
                Code:
                {source_code}

                Output ONLY the description.
                """

            try:
                response = client.chat.completions.create(
                    model="gpt-4o-mini",
                    messages=[{"role": "user", "content": prompt}],
                    temperature=0.0
                )
                desc = response.choices[0].message.content.strip()

                if category not in metadata: metadata[category] = {}
                metadata[category][func_name] = desc
            except:
                pass

        AIEngine._save_metadata(metadata)

    @staticmethod
    def _find_semantic_match(user_query, category, api_key, base_url):
        """
        Uses LLM to match user query against the JSON descriptions.
        """
        metadata = AIEngine._load_metadata()
        category_data = metadata.get(category, {})

        if not category_data: return None

        # Create a list of choices for the LLM
        choices_text = "\n".join([f"- {name}: {desc}" for name, desc in category_data.items()])

        prompt = f"""
        User Query: "{user_query}"

        Available Functions:
        {choices_text}

        Task: Does one of the Available Functions match the User Query?
        - If YES, return ONLY the function name.
        - If NO, return EXACTLY the word "NONE".
        """

        try:
            client = OpenAI(api_key=api_key, base_url=base_url)
            response = client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[{"role": "user", "content": prompt}],
                temperature=0.0
            )
            result = response.choices[0].message.content.strip()

            # Clean up potential formatting
            result = result.replace("`", "").replace("'", "").strip()

            if result == "NONE": return None
            # Verify the function actually exists
            if result in category_data: return result
            return None
        except:
            return None

    @staticmethod
    def execute_function(category, func_name, value, params):
        module_map = {
            "transformation": transformations.TransformationRegistry,
            "validation": validations.ValidationRegistry,
            "derivation": derivations.DerivationRegistry
        }
        try:
            func = getattr(module_map[category], func_name)
            return func(value, **params)
        except Exception:
            return value

    @staticmethod
    def ask_llm_for_function(user_query, category, api_key, base_url, force_generation=False):
        """
        Orchestrates the Smart Search vs Generation flow.
        """
        if not api_key:
            return {"status": "error", "message": "Missing Internal API Key."}

        # PHASE 1: SEARCH (Only if we are NOT forced to generate)
        if not force_generation:
            # 1. Update metadata (ensure we know about all existing code)
            AIEngine._update_descriptions_if_needed(category, api_key, base_url)

            # 2. Semantic Search
            match = AIEngine._find_semantic_match(user_query, category, api_key, base_url)

            if match:
                return {"status": "found", "function_name": match}
            else:
                # 3. No match found -> Ask for confirmation
                return {"status": "confirmation_needed"}

        # PHASE 2: GENERATION (Only if force_generation is True)
        # Construct the Prompt
        system_prompt = f"""
        You are a generic Python Code Generator.
        Generate a SINGLE Python static method inside a class.
        CONTEXT:
        - Category: {category}
        - The function must always accept 'value' as the first argument.
        - It must be a @staticmethod.
        - It must include type hints and a docstring.
        - Do NOT include the class definition.
        """
        user_prompt = f"Create a function that does this: {user_query}"

        try:
            client = OpenAI(api_key=api_key, base_url=base_url)
            response = client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt}
                ],
                temperature=0.2
            )
            generated_code = response.choices[0].message.content.strip()
            generated_code = generated_code.replace("```python", "").replace("```", "").strip()

            match = re.search(r"def\s+([a-zA-Z_0-9]+)\(", generated_code)
            func_name = match.group(1) if match else "unknown_function"

            return {
                "status": "generated",
                "function_name": func_name,
                "code": generated_code
            }
        except Exception as e:
            return {"status": "error", "message": str(e)}

    @staticmethod
    def append_function_to_file(category, code_snippet):
        file_map = {
            "transformation": "transformations.py",
            "validation": "validations.py",
            "derivation": "derivations.py"
        }
        filename = file_map[category]

        # Read file
        with open(filename, "r") as f:
            content = f.read()

        if code_snippet.strip() in content: return False

        # Append
        indented_code = "\n    " + code_snippet.replace("\n", "\n    ")
        with open(filename, "a") as f:
            f.write("\n" + indented_code + "\n")

        # Trigger an immediate metadata update for this new function
        # We can't do it here easily without credentials, but the next "Search" will catch it.
        return True