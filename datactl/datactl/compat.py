"""What a schema change means for the dataset's consumers.

This is contract compatibility, not serialization compatibility: a change
can be perfectly decodable (drop an optional field, say) and still break
every downstream query that names the field. DESIGN.md has the full rules
table and the reasoning behind each verdict.
"""

from dataclasses import dataclass

from .schema import is_widening


@dataclass(frozen=True)
class Change:
    field: str
    what: str
    breaking: bool


def diff(old, new):
    changes = []
    old_fields = old.by_name()
    new_fields = new.by_name()

    for name, f in new_fields.items():
        if name in old_fields:
            continue
        if f.required:
            changes.append(Change(name, "added as required, but records written "
                                        "before this change have no value for it", True))
        else:
            changes.append(Change(name, "added as optional", False))

    for name in old_fields:
        if name not in new_fields:
            changes.append(Change(name, "dropped; every downstream read of this field breaks", True))

    for name, before in old_fields.items():
        after = new_fields.get(name)
        if after is None:
            continue
        if after.type != before.type:
            if is_widening(before.type, after.type):
                changes.append(Change(name, f"type widened {before.type} -> {after.type}", False))
            else:
                changes.append(Change(name, f"type changed {before.type} -> {after.type}; "
                                            "existing data does not fit", True))
        if before.required and not after.required:
            changes.append(Change(name, "required -> optional", False))
        elif after.required and not before.required:
            changes.append(Change(name, "optional -> required, but history may hold "
                                        "records without it", True))
    return changes


def is_breaking(changes):
    return any(c.breaking for c in changes)


def version_problems(old, new, changes):
    problems = []
    if new.version < old.version:
        problems.append(f"schema version went backwards, {old.version} -> {new.version}")
    elif changes and new.version == old.version:
        problems.append(f"schema changed but version is still {old.version}; bump it")
    return problems
