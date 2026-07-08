"""Load and validate dataset specs.

One YAML file describes one dataset: identity, the Kafka topic it streams
through, the Iceberg table it lands in, and the schema both must agree on.
Validation is strict (unknown keys are errors, not warnings) and collects
every problem instead of stopping at the first, so a spec author gets the
whole list in one run.
"""

import re
from dataclasses import dataclass
from pathlib import Path

import yaml

from .schema import Field, Schema, SchemaError, parse_type

DATASET_NAME_RE = re.compile(r"^[a-z][a-z0-9_-]{0,62}$")
FIELD_NAME_RE = re.compile(r"^[a-z][a-z0-9_]{0,62}$")
TOPIC_NAME_RE = re.compile(r"^[a-zA-Z0-9._-]{1,249}$")
TABLE_NAME_RE = re.compile(r"^[a-z][a-z0-9_]*\.[a-z][a-z0-9_]*$")
PARTITION_RE = re.compile(r"^(?:(year|month|day|hour)\((\w+)\)|bucket\((\d+),\s*(\w+)\)|(\w+))$")

STATES = {"active", "retired"}


class SpecError(ValueError):
    def __init__(self, source, problems):
        self.source = source
        self.problems = list(problems)
        super().__init__(f"{source}: " + "; ".join(self.problems))


@dataclass(frozen=True)
class StreamSpec:
    topic: str
    partitions: int
    retention_hours: int
    config: dict


@dataclass(frozen=True)
class TableSpec:
    name: str
    partition_by: tuple
    properties: dict


@dataclass(frozen=True)
class DatasetSpec:
    name: str
    owner: str
    description: str
    state: str
    schema: Schema
    stream: StreamSpec | None
    table: TableSpec | None


def load_spec(path):
    path = Path(path)
    try:
        raw = yaml.safe_load(path.read_text())
    except yaml.YAMLError as e:
        raise SpecError(str(path), [f"not valid YAML: {e}"]) from e
    return parse_spec(raw, source=str(path))


def parse_spec(raw, source="<spec>"):
    if not isinstance(raw, dict):
        raise SpecError(source, ["spec must be a YAML mapping"])
    problems = []

    known = {"dataset", "owner", "description", "state", "stream", "table", "schema"}
    for key in sorted(set(raw) - known):
        problems.append(f"unknown key {key!r}")

    name = raw.get("dataset")
    if not isinstance(name, str) or not DATASET_NAME_RE.match(name):
        problems.append("dataset: required; lowercase letters, digits, _ or -, "
                        "starting with a letter")

    owner = raw.get("owner")
    if not isinstance(owner, str) or not owner.strip():
        problems.append("owner: required")
        owner = ""

    description = raw.get("description", "")
    if not isinstance(description, str):
        problems.append("description: must be a string")
        description = ""

    state = raw.get("state", "active")
    if state not in STATES:
        problems.append(f"state: must be one of {sorted(STATES)}, got {state!r}")

    schema = _parse_schema(raw.get("schema"), problems)
    stream = _parse_stream(raw.get("stream"), problems)
    table = _parse_table(raw.get("table"), schema, problems)

    if "stream" not in raw and "table" not in raw:
        problems.append("a dataset needs a stream, a table, or both")

    if problems:
        raise SpecError(source, problems)
    return DatasetSpec(name, owner.strip(), description, state, schema, stream, table)


def _parse_schema(raw, problems):
    if not isinstance(raw, dict):
        problems.append("schema: required, must be a mapping with version and fields")
        return None

    for key in sorted(set(raw) - {"version", "fields"}):
        problems.append(f"schema: unknown key {key!r}")

    version = raw.get("version")
    if not isinstance(version, int) or isinstance(version, bool) or version < 1:
        problems.append("schema.version: must be an integer >= 1")
        version = 0

    raw_fields = raw.get("fields")
    if not isinstance(raw_fields, list) or not raw_fields:
        problems.append("schema.fields: must be a non-empty list")
        return None

    fields = []
    seen = set()
    for i, rf in enumerate(raw_fields):
        where = f"schema.fields[{i}]"
        if not isinstance(rf, dict):
            problems.append(f"{where}: must be a mapping")
            continue
        fname = rf.get("name")
        if not isinstance(fname, str) or not FIELD_NAME_RE.match(fname):
            problems.append(f"{where}.name: lowercase letters, digits or _, "
                            "starting with a letter")
            continue
        if fname in seen:
            problems.append(f"{where}: duplicate field name {fname!r}")
            continue
        seen.add(fname)

        for key in sorted(set(rf) - {"name", "type", "required", "doc"}):
            problems.append(f"{where} ({fname}): unknown key {key!r}")

        try:
            ftype = parse_type(rf.get("type"))
        except SchemaError as e:
            problems.append(f"{where} ({fname}): {e}")
            continue

        required = rf.get("required", False)
        if not isinstance(required, bool):
            problems.append(f"{where} ({fname}): required must be true or false")
            continue
        fields.append(Field(fname, ftype, required, rf.get("doc")))

    return Schema(version, tuple(fields))


def _parse_stream(raw, problems):
    if raw is None:
        return None
    if not isinstance(raw, dict):
        problems.append("stream: must be a mapping")
        return None
    before = len(problems)

    for key in sorted(set(raw) - {"topic", "partitions", "retention_hours", "config"}):
        problems.append(f"stream: unknown key {key!r}")

    topic = raw.get("topic")
    if not isinstance(topic, str) or not TOPIC_NAME_RE.match(topic):
        problems.append("stream.topic: required; letters, digits, '.', '_' or '-'")

    partitions = raw.get("partitions")
    if not isinstance(partitions, int) or isinstance(partitions, bool) or partitions < 1:
        problems.append("stream.partitions: must be an integer >= 1")

    retention_hours = raw.get("retention_hours")
    if not isinstance(retention_hours, int) or isinstance(retention_hours, bool) \
            or retention_hours < 1:
        problems.append("stream.retention_hours: must be an integer >= 1")

    config = raw.get("config", {})
    if not isinstance(config, dict):
        problems.append("stream.config: must be a mapping")

    if len(problems) > before:
        return None
    return StreamSpec(topic, partitions, retention_hours, dict(config))


def _parse_table(raw, schema, problems):
    if raw is None:
        return None
    if not isinstance(raw, dict):
        problems.append("table: must be a mapping")
        return None
    before = len(problems)

    for key in sorted(set(raw) - {"name", "partition_by", "properties"}):
        problems.append(f"table: unknown key {key!r}")

    name = raw.get("name")
    if not isinstance(name, str) or not TABLE_NAME_RE.match(name):
        problems.append("table.name: required, in the form namespace.table")

    field_names = {f.name for f in schema.fields} if schema else set()
    partition_by = raw.get("partition_by", [])
    if not isinstance(partition_by, list):
        problems.append("table.partition_by: must be a list")
        partition_by = []
    for entry in partition_by:
        m = PARTITION_RE.match(entry) if isinstance(entry, str) else None
        if m is None:
            problems.append(f"table.partition_by: {entry!r} is not a column, "
                            "year|month|day|hour(col), or bucket(n, col)")
            continue
        column = m.group(2) or m.group(4) or m.group(5)
        if field_names and column not in field_names:
            problems.append(f"table.partition_by: {column!r} is not a schema field")

    properties = raw.get("properties", {})
    if not isinstance(properties, dict):
        problems.append("table.properties: must be a mapping")

    if len(problems) > before:
        return None
    return TableSpec(name, tuple(partition_by), dict(properties))
