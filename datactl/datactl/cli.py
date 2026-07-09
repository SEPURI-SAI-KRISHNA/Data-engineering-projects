"""Command line entry points.

Exit codes are the CI contract: 0 clean, 1 invalid specs, 2 a breaking
schema change -- so a pipeline can tell "fix your YAML" apart from "this
needs a new dataset version".
"""

import argparse
import sys
from pathlib import Path

from .compat import diff, is_breaking, version_problems
from .spec import SpecError, load_spec


def cmd_validate(args):
    directory = Path(args.specs)
    paths = sorted(p for pattern in ("*.yaml", "*.yml") for p in directory.glob(pattern))
    if not paths:
        print(f"no spec files under {directory}", file=sys.stderr)
        return 1

    failed = False
    specs = []
    for path in paths:
        try:
            specs.append(load_spec(path))
            print(f"ok    {path}")
        except SpecError as e:
            failed = True
            print(f"FAIL  {path}", file=sys.stderr)
            for problem in e.problems:
                print(f"      {problem}", file=sys.stderr)

    for problem in _cross_file_problems(specs):
        failed = True
        print(f"FAIL  {problem}", file=sys.stderr)

    return 1 if failed else 0


def _cross_file_problems(specs):
    problems = []
    claimed = {}
    for s in specs:
        resources = [("dataset", s.name)]
        if s.stream:
            resources.append(("topic", s.stream.topic))
        if s.table:
            resources.append(("table", s.table.name))
        for kind, value in resources:
            other = claimed.get((kind, value))
            if other is not None and other != s.name:
                problems.append(f"{kind} {value!r} is claimed by both {other!r} and {s.name!r}")
            claimed[(kind, value)] = s.name
    return problems


def cmd_check_compat(args):
    try:
        old = load_spec(args.old)
        new = load_spec(args.new)
    except SpecError as e:
        print(f"FAIL  {e.source}", file=sys.stderr)
        for problem in e.problems:
            print(f"      {problem}", file=sys.stderr)
        return 1

    if old.name != new.name:
        print(f"note: comparing different datasets ({old.name!r} vs {new.name!r})")

    changes = diff(old.schema, new.schema)
    problems = version_problems(old.schema, new.schema, changes)

    print(f"{new.name}: schema v{old.schema.version} -> v{new.schema.version}")
    if not changes:
        print("  no schema changes")
    for c in changes:
        marker = "BREAKING" if c.breaking else "ok      "
        print(f"  {marker}  {c.field}: {c.what}")

    for problem in problems:
        print(f"FAIL  {problem}", file=sys.stderr)
    if problems:
        return 1

    if is_breaking(changes):
        breaking = sum(1 for c in changes if c.breaking)
        print(f"\n{breaking} breaking change{'s' if breaking != 1 else ''}. "
              "The sanctioned path is a new dataset version "
              "(new topic and table, then backfill); see DESIGN.md.")
        return 0 if args.allow_breaking else 2
    return 0


def main(argv=None):
    parser = argparse.ArgumentParser(
        prog="datactl",
        description="Datasets as code: validate specs and gate schema changes.")
    sub = parser.add_subparsers(dest="command", required=True)

    validate = sub.add_parser("validate", help="check every spec in a directory")
    validate.add_argument("--specs", default="specs", help="directory of dataset specs")
    validate.set_defaults(func=cmd_validate)

    compat = sub.add_parser("check-compat",
                            help="diff two versions of a dataset spec for consumer impact")
    compat.add_argument("old", help="spec file for the current version")
    compat.add_argument("new", help="spec file for the proposed version")
    compat.add_argument("--allow-breaking", action="store_true",
                        help="report breaking changes but exit 0")
    compat.set_defaults(func=cmd_check_compat)

    args = parser.parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
