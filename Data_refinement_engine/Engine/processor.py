"""Core record processing: applies a mapping config to raw records.

Kept free of streamlit and openai imports so it can be tested and reused
outside the UI.
"""

import transformations
import validations
import derivations

REGISTRIES = {
    "transformation": transformations.TransformationRegistry,
    "validation": validations.ValidationRegistry,
    "derivation": derivations.DerivationRegistry,
}


def run_step(category, func_name, value, params):
    func = getattr(REGISTRIES[category], func_name)
    return func(value, **params)


def apply_mapping_to_record(raw_record, mapping_config):
    """Returns (rich_record, errors).

    errors is a list of dicts describing validation failures and steps that
    raised. A failed step leaves the value as it was before that step.
    """
    rich_record = {}
    errors = []

    for raw_key, value in raw_record.items():
        config = mapping_config.get(raw_key)
        if not config or config["is_key_ignored"]:
            continue

        target_key = config["rich_key"]

        current_value = value
        for step in config["transformation"]:
            try:
                current_value = run_step("transformation", step["name"],
                                         current_value, step["parameter_to_function"])
            except Exception as e:
                errors.append({"field": target_key, "category": "transformation",
                               "step": step["name"], "error": str(e)})

        rich_record[target_key] = current_value

        for step in config["validation"]:
            try:
                ok = run_step("validation", step["name"],
                              current_value, step["parameter_to_function"])
            except Exception as e:
                errors.append({"field": target_key, "category": "validation",
                               "step": step["name"], "error": str(e)})
                continue
            if not ok:
                errors.append({"field": target_key, "category": "validation",
                               "step": step["name"],
                               "error": f"value {current_value!r} failed {step['name']}"})

        for step in config["derivation"]:
            new_key = step["parameter_to_function"].get("target_field_name",
                                                        f"{target_key}_derived")
            params = {k: v for k, v in step["parameter_to_function"].items()
                      if k != "target_field_name"}
            try:
                rich_record[new_key] = run_step("derivation", step["name"],
                                                current_value, params)
            except Exception as e:
                errors.append({"field": new_key, "category": "derivation",
                               "step": step["name"], "error": str(e)})

    return rich_record, errors
