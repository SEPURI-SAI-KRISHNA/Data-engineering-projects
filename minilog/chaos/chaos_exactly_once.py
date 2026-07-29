#!/usr/bin/env python3
"""Crash harness for exactly-once processing.

Builds a log, then crash-loops ChaosCounter over it until it finishes,
twice:

  atomic mode  state and offset in one atomic snapshot. The final counts
               must match a recount of the log exactly, no matter how many
               times the process died. Anything else is a failure.

  naive mode   state flushed every 500 records, offset committed every
               1000, separately -- a typical consumer with auto-commit on.
               A crash landing between the two cadences makes them
               disagree, and the counts come out wrong. That outcome is
               expected and is the point of the demonstration.

Usage: python3 chaos_exactly_once.py [records]
"""

import random
import shutil
import subprocess
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
CLASSPATH = str(HERE.parent / "target" / "classes")
DATA = HERE / "chaos-eo-data"
MAX_ATTEMPTS = 200


def java(*args, **kwargs):
    return subprocess.run(["java", "-cp", CLASSPATH, "com.minilog.tools.ChaosCounter", *args],
                          **kwargs)


def crash_loop(mode_args, min_run=0.25, max_run=0.7):
    """Run the counter under random SIGKILLs until it completes. Returns crash count."""
    crashes = 0
    for _ in range(MAX_ATTEMPTS):
        proc = subprocess.Popen(
            ["java", "-cp", CLASSPATH, "com.minilog.tools.ChaosCounter", *mode_args],
            stdout=subprocess.DEVNULL)
        try:
            if proc.wait(timeout=random.uniform(min_run, max_run)) == 0:
                return crashes
            sys.exit(f"counter exited with {proc.returncode}")
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait()
            crashes += 1
    sys.exit(f"counter never finished within {MAX_ATTEMPTS} attempts")


def main():
    records = sys.argv[1] if len(sys.argv) > 1 else "100000"
    shutil.rmtree(DATA, ignore_errors=True)
    logdir = str(DATA / "log")

    print(f"building a log of {records} records...")
    java("build", logdir, records, check=True, stdout=subprocess.DEVNULL)

    print("\n-- atomic snapshot (state + offset committed together) --")
    snapshot = str(DATA / "atomic.snap")
    crashes = crash_loop(["run", logdir, snapshot])
    result = java("verify", logdir, snapshot, capture_output=True, text=True)
    print(f"{crashes} crashes -> {result.stdout.strip()}")
    if result.returncode != 0:
        print("exactly-once VIOLATED in atomic mode -- this is a bug")
        sys.exit(1)

    print("\n-- naive split (state and offset on separate cadences) --")
    snapshot = str(DATA / "naive.snap")
    crashes = crash_loop(["run", logdir, snapshot, "--naive", str(DATA / "naive-offsets")])
    result = java("verify", logdir, snapshot, capture_output=True, text=True)
    print(f"{crashes} crashes -> {result.stdout.strip()}")
    if result.returncode == 0:
        print("naive mode got lucky this run (no crash landed between the "
              "two commits); rerun to see it fail")
    else:
        print("naive mode lost exactness, as expected: this is why the "
              "offset must live inside the state snapshot")

    print("\natomic snapshots held exactly-once through every crash.")


if __name__ == "__main__":
    main()
