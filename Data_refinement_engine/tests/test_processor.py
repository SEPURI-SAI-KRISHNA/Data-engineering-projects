import processor


def field_config(**overrides):
    config = {"is_key_ignored": False, "rich_key": "name", "rich_key_type": "string",
              "transformation": [], "validation": [], "derivation": []}
    config.update(overrides)
    return config


def test_transformations_run_in_order():
    mapping = {"name": field_config(transformation=[
        {"name": "trim_whitespace", "parameter_to_function": {}},
        {"name": "uppercase", "parameter_to_function": {}},
    ])}

    record, errors = processor.apply_mapping_to_record({"name": "  bond "}, mapping)

    assert record == {"name": "BOND"}
    assert errors == []


def test_failing_validation_is_reported():
    mapping = {"email": field_config(rich_key="email", validation=[
        {"name": "is_valid_email", "parameter_to_function": {}},
    ])}

    record, errors = processor.apply_mapping_to_record({"email": "not-an-email"}, mapping)

    assert record == {"email": "not-an-email"}
    assert len(errors) == 1
    assert errors[0]["category"] == "validation"
    assert errors[0]["step"] == "is_valid_email"


def test_validation_checks_the_transformed_value():
    # raw value is invalid, but the transformation fixes it
    mapping = {"email": field_config(rich_key="email",
        transformation=[{"name": "replace_text",
                         "parameter_to_function": {"old_value": " at ", "new_value": "@"}}],
        validation=[{"name": "is_valid_email", "parameter_to_function": {}}],
    )}

    _, errors = processor.apply_mapping_to_record({"email": "bond at mi6.uk"}, mapping)

    assert errors == []


def test_derivation_uses_target_field_name():
    mapping = {"email": field_config(rich_key="email", derivation=[
        {"name": "extract_email_domain",
         "parameter_to_function": {"target_field_name": "domain"}},
    ])}

    record, errors = processor.apply_mapping_to_record({"email": "q@mi6.uk"}, mapping)

    assert record == {"email": "q@mi6.uk", "domain": "mi6.uk"}
    assert errors == []


def test_ignored_and_unmapped_keys_are_dropped():
    mapping = {"keep": field_config(rich_key="kept"),
               "drop": field_config(is_key_ignored=True)}

    record, _ = processor.apply_mapping_to_record(
        {"keep": 1, "drop": 2, "unknown": 3}, mapping)

    assert record == {"kept": 1}


def test_raising_step_is_reported_and_value_kept():
    mapping = {"name": field_config(transformation=[
        {"name": "substring",
         "parameter_to_function": {"start_index": "x", "end_index": "y"}},
    ])}

    record, errors = processor.apply_mapping_to_record({"name": "bond"}, mapping)

    assert record == {"name": "bond"}
    assert len(errors) == 1
    assert errors[0]["category"] == "transformation"
