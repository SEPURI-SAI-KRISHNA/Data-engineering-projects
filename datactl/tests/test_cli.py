from pathlib import Path

from datactl import cli


ROOT = Path(__file__).resolve().parents[1]


def test_validate_example_specs_succeeds(capsys):
    exit_code = cli.main(["validate", "--specs", str(ROOT / "specs")])

    captured = capsys.readouterr()
    assert exit_code == 0
    assert "ok" in captured.out
    assert "orders.yaml" in captured.out
    assert "rider_locations.yaml" in captured.out
    assert captured.err == ""


def test_check_compat_breaking_change_returns_two(capsys):
    exit_code = cli.main(
        [
            "check-compat",
            str(ROOT / "specs" / "orders.yaml"),
            str(ROOT / "examples" / "orders-v2.yaml"),
        ]
    )

    captured = capsys.readouterr()
    assert exit_code == 2
    assert "orders: schema v1 -> v2" in captured.out
    assert "BREAKING" in captured.out
    assert captured.err == ""


def test_check_compat_allow_breaking_returns_zero(capsys):
    exit_code = cli.main(
        [
            "check-compat",
            str(ROOT / "specs" / "orders.yaml"),
            str(ROOT / "examples" / "orders-v2.yaml"),
            "--allow-breaking",
        ]
    )

    captured = capsys.readouterr()
    assert exit_code == 0
    assert "2 breaking changes." in captured.out
    assert captured.err == ""
