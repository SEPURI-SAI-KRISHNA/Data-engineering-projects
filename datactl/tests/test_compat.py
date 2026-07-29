from datactl.compat import diff, is_breaking, version_problems
from datactl.schema import Field, Schema, parse_type


def schema(*fields, version=1):
    return Schema(version, tuple(
        Field(name, parse_type(ftype), required) for name, ftype, required in fields))


BASE = schema(("order_id", "string", True), ("amount", "decimal(10,2)", True),
              ("coupon", "string", False))


def only_change(new):
    changes = diff(BASE, new)
    assert len(changes) == 1, changes
    return changes[0]


def test_identical_schemas_have_no_changes():
    assert diff(BASE, BASE) == []


def test_add_optional_is_ok():
    new = schema(("order_id", "string", True), ("amount", "decimal(10,2)", True),
                 ("coupon", "string", False), ("note", "string", False))
    c = only_change(new)
    assert (c.field, c.breaking) == ("note", False)


def test_add_required_is_breaking():
    new = schema(("order_id", "string", True), ("amount", "decimal(10,2)", True),
                 ("coupon", "string", False), ("currency", "string", True))
    c = only_change(new)
    assert (c.field, c.breaking) == ("currency", True)


def test_drop_is_breaking_even_for_optional_fields():
    new = schema(("order_id", "string", True), ("amount", "decimal(10,2)", True))
    c = only_change(new)
    assert (c.field, c.breaking) == ("coupon", True)


def test_widening_is_ok():
    new = schema(("order_id", "string", True), ("amount", "decimal(12,2)", True),
                 ("coupon", "string", False))
    c = only_change(new)
    assert (c.field, c.breaking) == ("amount", False)
    assert "widened" in c.what


def test_narrowing_is_breaking():
    new = schema(("order_id", "string", True), ("amount", "decimal(8,2)", True),
                 ("coupon", "string", False))
    assert only_change(new).breaking


def test_type_change_is_breaking():
    new = schema(("order_id", "string", True), ("amount", "string", True),
                 ("coupon", "string", False))
    assert only_change(new).breaking


def test_required_to_optional_is_ok():
    new = schema(("order_id", "string", True), ("amount", "decimal(10,2)", False),
                 ("coupon", "string", False))
    assert only_change(new).breaking is False


def test_optional_to_required_is_breaking():
    new = schema(("order_id", "string", True), ("amount", "decimal(10,2)", True),
                 ("coupon", "string", True))
    assert only_change(new).breaking


def test_rename_reads_as_drop_plus_add():
    new = schema(("order_id", "string", True), ("amount", "decimal(10,2)", True),
                 ("discount_code", "string", False))
    changes = diff(BASE, new)
    assert {(c.field, c.breaking) for c in changes} == {
        ("coupon", True), ("discount_code", False)}
    assert is_breaking(changes)


def test_changes_accumulate():
    new = schema(("order_id", "string", True), ("amount", "decimal(12,2)", True),
                 ("currency", "string", True))
    changes = diff(BASE, new)
    assert {(c.field, c.breaking) for c in changes} == {
        ("amount", False), ("coupon", True), ("currency", True)}


def test_changed_schema_must_bump_version():
    new = schema(("order_id", "string", True), ("amount", "decimal(12,2)", True),
                 ("coupon", "string", False), version=1)
    problems = version_problems(BASE, new, diff(BASE, new))
    assert problems and "bump" in problems[0]
    assert version_problems(BASE, BASE, []) == []


def test_version_must_not_go_backwards():
    old = schema(("order_id", "string", True), version=3)
    new = schema(("order_id", "string", True), version=2)
    problems = version_problems(old, new, [])
    assert problems and "backwards" in problems[0]
