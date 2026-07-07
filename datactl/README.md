# datactl

Datasets as code. Each dataset — its schema, its Kafka topic, its Iceberg
table — is declared once in a YAML spec that lives in git; `datactl` makes
the infrastructure match the spec and refuses schema changes that would
break the dataset's consumers. Unlike general IaC it understands the one
thing it manages: it can tell you *this change is a safe widening* or *this
change strands every query naming that column*, at plan time, in the PR.

The design rationale — why there is no state file, why dropping an optional
field is breaking here when a schema registry would wave it through, why
"breaking" means "new version" rather than "forbidden" — is in
[DESIGN.md](DESIGN.md). Read that first; it's the real deliverable.

## Try it

Validate every spec in a directory (strict: unknown keys are errors, and
two specs claiming the same topic is an error):

```
$ python3 -m datactl validate --specs specs
ok    specs/orders.yaml
ok    specs/rider_locations.yaml
```

Diff two versions of a spec for consumer impact:

```
$ python3 -m datactl check-compat specs/orders.yaml examples/orders-v2.yaml
orders: schema v1 -> v2
  BREAKING  currency: added as required, but records written before this change have no value for it
  BREAKING  coupon: dropped; every downstream read of this field breaks
  ok        amount: type widened decimal(10,2) -> decimal(12,2)

2 breaking changes. The sanctioned path is a new dataset version
(new topic and table, then backfill); see DESIGN.md.
```

Exit codes are the CI contract: **0** clean, **1** invalid specs (fix your
YAML), **2** breaking change (this needs a new dataset version). A repo of
specs gates PRs with nothing more than these two commands in a workflow.
`--allow-breaking` exists for the case where you've decided the break is
fine — the tool advises, the human decides, and the flag leaves a trace in
the PR that used it.

## Tests

```
python3 -m venv venv && venv/bin/pip install pytest pyyaml
venv/bin/python -m pytest
```

51 tests, all pure logic — spec text in, verdicts out. No infrastructure
needed until phase 2.

## Layout

- `datactl/spec.py` — spec loading and strict validation
- `datactl/schema.py` — the type system and widening rules
- `datactl/compat.py` — the schema contract: what a change means for consumers
- `datactl/cli.py` — `validate`, `check-compat`
- `specs/` — example dataset specs; `examples/` — a proposed v2 for the demo
- `DESIGN.md` — goals, non-goals, drift model, contract rules, failure modes, roadmap

## Status

Phase 1: spec model, validation, and the compatibility gate. Phase 2 adds
`plan`/`apply` against a live Kafka + Iceberg stack (the one in
`streaming-lakehouse-platform/`), phase 3 the reconcile loop with a
kill-mid-apply convergence harness, phase 4 backfill with atomic cutover.
Roadmap at the end of DESIGN.md.
