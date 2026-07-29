import pytest

from datactl.spec import SpecError, load_spec, parse_spec


def valid_raw():
    return {
        "dataset": "orders",
        "owner": "someone@example.com",
        "stream": {"topic": "orders.v1", "partitions": 6, "retention_hours": 168},
        "table": {"name": "lake.orders", "partition_by": ["day(order_ts)"]},
        "schema": {
            "version": 1,
            "fields": [
                {"name": "order_id", "type": "string", "required": True},
                {"name": "amount", "type": "decimal(10,2)", "required": True},
                {"name": "order_ts", "type": "timestamp", "required": True},
                {"name": "coupon", "type": "string"},
            ],
        },
    }


def problems_of(raw):
    with pytest.raises(SpecError) as e:
        parse_spec(raw)
    return e.value.problems


def test_valid_spec_parses():
    spec = parse_spec(valid_raw())
    assert spec.name == "orders"
    assert spec.state == "active"
    assert spec.stream.partitions == 6
    assert spec.table.partition_by == ("day(order_ts)",)
    assert spec.schema.version == 1
    assert [f.name for f in spec.schema.fields] == ["order_id", "amount", "order_ts", "coupon"]
    assert spec.schema.by_name()["coupon"].required is False


def test_unknown_top_level_key_is_an_error():
    raw = valid_raw()
    raw["retention_hors"] = 24
    assert any("retention_hors" in p for p in problems_of(raw))


def test_unknown_stream_key_is_an_error():
    raw = valid_raw()
    raw["stream"]["retenton_hours"] = 24
    assert any("retenton_hours" in p for p in problems_of(raw))


def test_missing_owner():
    raw = valid_raw()
    del raw["owner"]
    assert any(p.startswith("owner") for p in problems_of(raw))


def test_bad_partition_count():
    raw = valid_raw()
    raw["stream"]["partitions"] = "six"
    assert any("partitions" in p for p in problems_of(raw))
    raw["stream"]["partitions"] = 0
    assert any("partitions" in p for p in problems_of(raw))


def test_duplicate_field_names():
    raw = valid_raw()
    raw["schema"]["fields"].append({"name": "amount", "type": "long"})
    assert any("duplicate" in p for p in problems_of(raw))


def test_bad_type_names_the_field():
    raw = valid_raw()
    raw["schema"]["fields"][1]["type"] = "money"
    assert any("amount" in p and "money" in p for p in problems_of(raw))


def test_partition_by_must_reference_a_schema_field():
    raw = valid_raw()
    raw["table"]["partition_by"] = ["day(created_at)"]
    assert any("created_at" in p for p in problems_of(raw))


def test_partition_transforms():
    raw = valid_raw()
    raw["table"]["partition_by"] = ["order_id", "bucket(16, order_id)", "hour(order_ts)"]
    spec = parse_spec(raw)
    assert len(spec.table.partition_by) == 3


def test_needs_stream_or_table():
    raw = valid_raw()
    del raw["stream"]
    del raw["table"]
    assert any("stream" in p and "table" in p for p in problems_of(raw))
    raw["stream"] = valid_raw()["stream"]
    assert parse_spec(raw).table is None


def test_retired_state_is_valid_and_others_are_not():
    raw = valid_raw()
    raw["state"] = "retired"
    assert parse_spec(raw).state == "retired"
    raw["state"] = "deleted"
    assert any(p.startswith("state") for p in problems_of(raw))


def test_collects_every_problem_in_one_pass():
    raw = valid_raw()
    del raw["owner"]
    raw["stream"]["partitions"] = 0
    raw["schema"]["fields"][0]["type"] = "money"
    assert len(problems_of(raw)) >= 3


def test_load_spec_reads_yaml(tmp_path):
    path = tmp_path / "clicks.yaml"
    path.write_text(
        "dataset: clicks\n"
        "owner: someone@example.com\n"
        "stream: {topic: clicks.v1, partitions: 3, retention_hours: 72}\n"
        "schema:\n"
        "  version: 1\n"
        "  fields:\n"
        "    - {name: url, type: string, required: true}\n"
    )
    assert load_spec(path).stream.topic == "clicks.v1"


def test_load_spec_reports_broken_yaml(tmp_path):
    path = tmp_path / "broken.yaml"
    path.write_text("dataset: [unclosed\n")
    with pytest.raises(SpecError) as e:
        load_spec(path)
    assert "YAML" in e.value.problems[0]
