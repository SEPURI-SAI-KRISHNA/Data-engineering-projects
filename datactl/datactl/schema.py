"""Field and type model shared by spec validation and the compat checker."""

import re
from dataclasses import dataclass

PRIMITIVES = {"string", "boolean", "int", "long", "float", "double", "date", "timestamp"}

_DECIMAL_RE = re.compile(r"^decimal\((\d+),\s*(\d+)\)$")


class SchemaError(ValueError):
    pass


@dataclass(frozen=True)
class FieldType:
    base: str
    precision: int = 0
    scale: int = 0

    def __str__(self):
        if self.base == "decimal":
            return f"decimal({self.precision},{self.scale})"
        return self.base


def parse_type(text):
    if not isinstance(text, str):
        raise SchemaError(f"type must be a string, got {text!r}")
    if text in PRIMITIVES:
        return FieldType(text)
    m = _DECIMAL_RE.match(text)
    if m is None:
        raise SchemaError(
            f"unknown type {text!r}; expected one of {sorted(PRIMITIVES)} or decimal(p,s)"
        )
    precision, scale = int(m.group(1)), int(m.group(2))
    if not 1 <= precision <= 38:
        raise SchemaError(f"decimal precision must be 1..38, got {precision}")
    if not 0 <= scale <= precision:
        raise SchemaError(f"decimal scale must be 0..{precision}, got {scale}")
    return FieldType("decimal", precision, scale)


# The widenings every reader can do losslessly. Same set Iceberg allows,
# which is not a coincidence: the table has to accept the same evolution.
_WIDENINGS = {("int", "long"), ("float", "double")}


def is_widening(old, new):
    if (old.base, new.base) in _WIDENINGS:
        return True
    if old.base == "decimal" and new.base == "decimal":
        return new.scale == old.scale and new.precision > old.precision
    return False


@dataclass(frozen=True)
class Field:
    name: str
    type: FieldType
    required: bool = False
    doc: str | None = None


@dataclass(frozen=True)
class Schema:
    version: int
    fields: tuple

    def by_name(self):
        return {f.name: f for f in self.fields}
