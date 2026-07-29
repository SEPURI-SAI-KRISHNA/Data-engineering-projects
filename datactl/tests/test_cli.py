from pathlib import Path

from datactl.cli import main

REPO = Path(__file__).resolve().parent.parent

GOOD = """\
dataset: clicks
owner: someone@example.com
stream: {topic: clicks.v1, partitions: 3, retention_hours: 72}
schema:
  version: 1
  fields:
    - {name: url, type: string, required: true}
"""


def test_validate_passes_the_shipped_specs():
    assert main(["validate", "--specs", str(REPO / "specs")]) == 0


def test_validate_fails_on_a_bad_spec(tmp_path, capsys):
    (tmp_path / "good.yaml").write_text(GOOD)
    (tmp_path / "bad.yaml").write_text(GOOD.replace("owner:", "onwer:"))
    assert main(["validate", "--specs", str(tmp_path)]) == 1
    err = capsys.readouterr().err
    assert "onwer" in err and "owner" in err


def test_validate_catches_two_specs_claiming_one_topic(tmp_path, capsys):
    (tmp_path / "a.yaml").write_text(GOOD)
    (tmp_path / "b.yaml").write_text(GOOD.replace("dataset: clicks", "dataset: clicks_two"))
    assert main(["validate", "--specs", str(tmp_path)]) == 1
    assert "clicks.v1" in capsys.readouterr().err


def test_check_compat_exit_codes(tmp_path, capsys):
    old = tmp_path / "old.yaml"
    new = tmp_path / "new.yaml"
    old.write_text(GOOD)
    new.write_text(GOOD.replace("version: 1", "version: 2")
                       .replace("{name: url, type: string, required: true}",
                                "{name: url, type: string, required: true}\n"
                                "    - {name: session_id, type: string, required: true}"))

    assert main(["check-compat", str(old), str(new)]) == 2
    assert "BREAKING" in capsys.readouterr().out
    assert main(["check-compat", "--allow-breaking", str(old), str(new)]) == 0
    assert main(["check-compat", str(old), str(old)]) == 0


def test_check_compat_demands_a_version_bump(tmp_path, capsys):
    old = tmp_path / "old.yaml"
    new = tmp_path / "new.yaml"
    old.write_text(GOOD)
    new.write_text(GOOD.replace("required: true", "required: false"))

    assert main(["check-compat", str(old), str(new)]) == 1
    assert "bump" in capsys.readouterr().err
