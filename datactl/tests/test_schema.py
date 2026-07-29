import pytest

from datactl.schema import FieldType, SchemaError, is_widening, parse_type


def test_parses_primitives():
    for name in ("string", "boolean", "int", "long", "float", "double", "date", "timestamp"):
        assert parse_type(name) == FieldType(name)


def test_parses_decimal():
    assert parse_type("decimal(10,2)") == FieldType("decimal", 10, 2)
    assert parse_type("decimal(38, 0)") == FieldType("decimal", 38, 0)


def test_decimal_renders_back_to_source_form():
    assert str(parse_type("decimal(10, 2)")) == "decimal(10,2)"
    assert str(parse_type("long")) == "long"


@pytest.mark.parametrize("bad", ["varchar", "Decimal(10,2)", "decimal(0,0)",
                                 "decimal(39,2)", "decimal(5,6)", "", None])
def test_rejects_bad_types(bad):
    with pytest.raises(SchemaError):
        parse_type(bad)


@pytest.mark.parametrize("old,new,expected", [
    ("int", "long", True),
    ("float", "double", True),
    ("decimal(10,2)", "decimal(12,2)", True),
    ("long", "int", False),
    ("double", "float", False),
    ("decimal(12,2)", "decimal(10,2)", False),
    ("decimal(10,2)", "decimal(10,3)", False),
    ("int", "int", False),
    ("int", "string", False),
])
def test_widening(old, new, expected):
    assert is_widening(parse_type(old), parse_type(new)) is expected
