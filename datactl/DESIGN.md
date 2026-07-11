# datactl — design

A control plane for datasets. Each dataset is declared once, in a YAML spec
that lives in git: its schema, the Kafka topic it streams through, the
Iceberg table it lands in. `datactl` makes the infrastructure match the
spec — creates what is missing, reports what drifted — and refuses schema
changes that would break the dataset's consumers.

The comparison everyone reaches for is Terraform, so it's worth stating the
difference up front: Terraform manages resources it does not understand. It
can tell you a topic config changed; it cannot tell you that dropping the
`coupon` column breaks four downstream queries, because it has no idea what
a column means. This tool manages exactly one kind of thing and understands
it deeply, which is what makes domain-aware diffs — "this change is safe",
"this change strands your consumers" — possible at plan time.

## The problem

The definition of a dataset is smeared across systems that don't know about
each other. Partition count lives in Kafka. The schema lives in whatever the
producer happens to serialize, or a registry if you're lucky. The table
layout lives in the warehouse catalog. Retention lives in two or three
places and disagrees. Nothing checks that these agree with each other, and
nothing reviews a schema change against the people consuming the data — a
change that breaks every downstream job is a code review nobody knows to
request.

The fix is boring and proven: put the whole definition in one file, in git,
and make a machine responsible for both directions — pushing the definition
out to the infrastructure, and gating changes to the definition itself.

## Goals

1. One spec per dataset, in git, reviewed like code. The spec is the source
   of truth; the infrastructure is a projection of it.
2. `plan` before `apply`: show exactly what would change, touch nothing.
3. Drift detection: reality is re-read on every run, so hand-edits and
   out-of-band changes surface instead of silently accumulating.
4. Schema changes gated by a compatibility contract, enforced in CI where
   the review is already happening.
5. Backfills that end in an atomic cutover, not a "please stop querying the
   table for an hour" message.

## Non-goals

- **Not an orchestrator.** No job scheduling, no DAGs, no retries of user
  code. Airflow and Flink own execution; this tool owns structure. The two
  concerns rot when combined.
- **Not general-purpose IaC.** No VMs, no buckets, no IAM. Narrowness is
  the point — see the Terraform comparison above.
- **Not a schema registry service.** Git is the registry. History is `git
  log`, review is a PR, the compat check runs in CI. Standing up a service
  to hold schemas adds a fourth system to keep consistent, which is the
  disease this tool treats, not the cure.
- **No multi-environment promotion** (dev → staging → prod) in v1. One
  target environment keeps the auth story trivial and the diffs readable.
- **No UI.** The CLI output *is* the interface, and it's designed to be
  pasted into a PR comment.

## The spec

One YAML file per dataset:

```yaml
dataset: orders
owner: team-checkout@example.com
description: Order events from checkout, one record per order state change.
state: active            # active | retired

stream:
  topic: orders.v1
  partitions: 6
  retention_hours: 168
  config:
    cleanup.policy: delete

table:
  name: lake.orders
  partition_by: [day(order_ts)]

schema:
  version: 1
  fields:
    - {name: order_id, type: string, required: true, doc: uuid}
    - {name: user_id,  type: string, required: true}
    - {name: amount,   type: "decimal(10,2)", required: true}
    - {name: order_ts, type: timestamp, required: true}
    - {name: coupon,   type: string}
```

Notes on the shape:

- `stream` and `table` are each optional, but a dataset must have at least
  one. A compacted location feed might never land in the lake; a batch-only
  dimension table has no topic.
- There is one `schema`, shared by both. That is the whole point: the topic
  and the table agreeing on the schema stops being a convention and becomes
  an invariant the tool maintains.
- Types are a deliberately small set — `string`, `boolean`, `int`, `long`,
  `float`, `double`, `date`, `timestamp`, `decimal(p,s)` — chosen because
  they map 1:1 onto both Iceberg types and sane Kafka payload fields. No
  nested structs in v1; they'd double the size of the compat rules for a
  feature most operational datasets don't need. Revisit if it hurts.
- `state: retired` is how a dataset dies. Deleting the spec file does
  **not** delete anything (see drift handling below).
- Validation is strict: unknown keys are errors, not warnings. A typo like
  `retention_hors` that is silently ignored is worse than a failing check,
  because the author believes they configured something they didn't.

## Where state lives (there is no state file)

Terraform keeps three copies of the world: desired (code), recorded
(tfstate), and actual. The recorded copy exists so it can detect renames
and deletions and manage resources it can't cheaply enumerate. It is also
the biggest operational hazard in the ecosystem: state drift, state
locking, state surgery at 2am.

datactl keeps two copies: desired is the spec directory, actual is whatever
the Kafka admin API and the Iceberg catalog report *right now*. Every plan
is computed fresh from both. What this buys:

- No lock server, no state backend, no corrupted-state class of bugs.
- `plan` is always truthful. It cannot be confused by a stale record of the
  world, because there is no record of the world.

What it costs, honestly:

- **Renames are invisible.** A renamed topic looks like one missing
  resource plus one unmanaged resource. This is acceptable because renaming
  a live topic in place is almost never what you actually want — the
  sanctioned path for identity changes is a new dataset version plus a
  backfill (below).
- **Everything must be enumerable.** True for Kafka topics and for tables
  in an Iceberg catalog, which is the whole managed surface.
- **The tool can't know "I created this".** So deletion can never be
  inferred from a spec's absence — absence means *unmanaged*, and unmanaged
  means *untouched*. Destroying data requires an explicit `state: retired`
  in a spec that still exists and still gets reviewed. A fat-fingered
  `rm specs/orders.yaml` must never be able to delete a topic.

## plan / apply / reconcile

`plan` reads every spec and every live resource, then classifies:

| class      | meaning                                   | action                          |
|------------|-------------------------------------------|---------------------------------|
| missing    | in spec, not in reality                   | create                          |
| divergent  | in both, but they disagree                | update if safe, warn if not     |
| unmanaged  | in reality, no spec claims it             | report; never touch             |
| retired    | spec says `retired`, resource still there | delete, after explicit confirm  |

"Update if safe" is load-bearing. Some divergence is fixable in place
(retention config, table properties). Some is not: Kafka partition counts
only grow, and even growing one changes key→partition mapping for keyed
streams, so partition divergence is *reported with the consequences spelled
out* rather than auto-fixed. The tool's job is to make the operator's
decision informed, not to make it for them.

`apply` executes the plan. Every step is check-then-act and idempotent —
"create topic" degrades to a no-op if the topic appeared in the meantime.
This is the whole crash-recovery story: an apply killed halfway leaves the
world in a state where re-running apply converges. There is no undo log
because there is nothing to undo — every action moves reality toward the
spec, and the spec doesn't change mid-apply.

The reconcile loop is nothing more than plan+apply on a timer. That falls
out of the design rather than being a feature: because plan trusts no
memory and apply is idempotent, running them repeatedly is safe by
construction. Unmanaged resources stay untouched forever; drift in managed
ones gets repaired or re-reported every cycle.

Two applies racing: topic creation is atomic on the broker (the loser gets
"already exists", which check-then-act treats as convergence), and Iceberg
commits are optimistic-concurrency (the loser retries against the new
metadata). At single-operator scale this doesn't need a lock, and the
design documents that judgment instead of shipping a lock server nobody
operates.

## The schema contract

The compat checker diffs two versions of a spec's schema and classifies
every change:

| change                                  | verdict  | why                                                        |
|-----------------------------------------|----------|------------------------------------------------------------|
| add optional field                      | ok       | old readers ignore it; old records read it as null         |
| add required field                      | breaking | records written before the change have no value for it     |
| drop a field (required or not)          | breaking | every downstream query naming it fails                     |
| rename a field                          | breaking | indistinguishable from drop+add, to the tool *and* to consumers |
| int→long, float→double, decimal(p,s)→decimal(p+n,s) | ok | every reader widens losslessly; same set Iceberg allows    |
| any other type change                   | breaking | existing data does not fit the new type                    |
| required → optional                     | ok       | consumers already handle the value being present           |
| optional → required                     | breaking | history may contain records without it                     |

Two things worth defending:

**Dropping an optional field is breaking here, even though Avro-style
serialization compatibility would wave it through.** Registry compat modes
answer "can old bytes be decoded?" This tool answers a different question:
"do the people using this dataset keep working?" An analyst's query with
`coupon` in the SELECT list breaks the moment the column disappears, no
matter how decodable the records are. Contract compatibility is a superset
of serialization compatibility, and it's the one that pages you.

**Breaking doesn't mean forbidden — it means *versioned*.** The sanctioned
response to a breaking change is not to force it through, it's to mint the
next version of the dataset: `orders` v2 gets a new topic and a new table,
history is backfilled into it under the new schema, consumers migrate on
their own schedule, and v1 is retired when its last consumer leaves. The
checker's exit code (2 for breaking) is what CI gates on. An
`--allow-breaking` flag exists because the tool advises and the human
decides — sometimes the dataset has exactly one consumer and it's you — but
using it leaves a visible trace in the PR that approved it.

The checker also polices the version number itself: a changed schema with
an unbumped version, or a version that goes backwards, fails validation.
The version is the only part of the contract a human maintains by hand, so
it's the part most worth checking.

## Backfill and cutover (phase 4 sketch)

Reprocessing history under new logic or a new schema, without downtime:

1. Build a shadow table (`lake.orders__bf_<id>`) from the source data with
   the new logic. The live table serves reads the entire time.
2. Verify the shadow against invariants before it can go live: row counts
   per partition against the source, checksums over key columns. A backfill
   that produces wrong data must die here, with the live table untouched.
3. Cut over atomically. Two catalog renames (`live`→`old`, `shadow`→`live`)
   have a window where the table doesn't exist — readers in that window
   fail. Instead the cutover is a single Iceberg *replace* commit: the
   shadow's data files are committed into the live table as one atomic
   snapshot swap. Readers see the old data until the commit, the new data
   after it, and nothing strange in between. This is the same "atomicity is
   the whole game" lesson as minilog's snapshot store, one level up the
   stack.

A crash between verify and cutover leaves the shadow table sitting there,
costing storage and nothing else; the cutover is retryable, and abandoned
shadows are namespaced (`__bf_`) so a `gc` subcommand can list and reap
them.

## Failure modes

| failure                                        | behavior                                                              |
|------------------------------------------------|-----------------------------------------------------------------------|
| apply killed mid-run (kill -9, OOM, ^C)        | re-run converges; every step is check-then-act                        |
| spec file deleted by accident                  | resources become *unmanaged*: reported, never touched, data intact    |
| topic config hand-edited out of band           | shows as divergent next plan; safe fields re-applied, unsafe warned   |
| partition count raised by hand                 | reported with key-mapping consequences; never auto-"fixed"            |
| breaking schema merged via `--allow-breaking`  | tool did its job: the override is recorded in the PR that used it     |
| backfill produces wrong data                   | verification fails; live table never touched                          |
| crash between backfill verify and cutover      | shadow table remains; cutover retries or `gc` reaps it                |
| two applies race                               | broker/catalog atomicity picks a winner; loser no-ops or retries      |
| Kafka or catalog unreachable                   | plan fails loudly and completely; no partial plan is ever shown       |
| spec repo itself wrong (typo'd key, bad type)  | strict validation rejects at PR time, before anything runs            |

## Alternatives considered

- **A Terraform provider.** Gets plan/apply and state handling for free.
  Rejected: the schema compat check — the most valuable thing here — would
  live outside the plan as a separate linter, exactly where nobody looks;
  and tfstate imports the operational hazards described above. The
  plan/apply loop is the *easy* part; it's not worth buying at that price.
- **A state file.** Rejected for the reasons in "Where state lives". The
  rename-detection it would enable is a non-goal (renames are versioned,
  not applied in place).
- **A schema registry service** (Confluent-style). Rejected: it checks the
  wrong compatibility (serialization, not contract), and it's another
  stateful service whose availability now gates deploys. Git + CI already
  provide storage, history, review, and enforcement.
- **A metadata service / catalog of record.** Right answer at
  thousand-dataset company scale, wrong here: the interesting problems
  (diffing, contracts, atomic cutover) don't need it, and it would bury
  them under CRUD.

## Testing

Phase 1 is deliberately pure — spec text in, verdicts out — so it's tested
exhaustively with table-driven unit tests and no infrastructure at all.

Phases 2–3 run against the streaming-lakehouse docker stack already in this
repo (Kafka, Iceberg REST catalog, MinIO): integration tests apply a spec,
mutate reality out-of-band with the same client libraries, and assert the
next plan sees exactly the drift that was injected. The convergence claim
gets the minilog treatment: a harness kills apply partway through
(`kill -9`, at random points) and asserts that re-running converges to a
clean plan — the claim is only real once a machine has tried to falsify it.

Phase 4's verify step is tested by sabotage: a backfill that drops rows or
corrupts a column must be caught by verification, with the live table
provably untouched.

## Roadmap

- **Phase 1 (done):** spec model, strict validation, compatibility checker,
  CLI (`validate`, `check-compat`) with CI-friendly exit codes. Pure logic,
  fully unit-tested.
- **Phase 2:** `plan`/`apply` for topics and tables against the lakehouse
  stack; drift classification (missing / divergent / unmanaged / retired).
- **Phase 3:** reconcile loop; kill-mid-apply convergence harness.
- **Phase 4:** backfill with verified atomic cutover; `gc` for abandoned
  shadow tables.
