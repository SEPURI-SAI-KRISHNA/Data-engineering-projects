#!/usr/bin/env python3
"""Crash harness for minilog's ack contract.

Each round: start a writer JVM that appends records and prints an ACK line
after every fsynced append, let it run for a random slice of time, SIGKILL
it, then reopen the log (which runs recovery) and verify:

  1. every acked offset is present with exactly the expected payload
  2. offsets are gapless from 0 to the recovered tail

The same log directory is reused across rounds, so every round also
exercises recovery of the previous round's crash. Extra records beyond the
last received ACK are fine (the ACK line can die in the pipe buffer when
the process is killed); a missing or corrupt acked record is a failure.

Usage: python3 chaos_run.py [rounds] [logdir]
"""

import random
import subprocess
import sys
import threading
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
CLASSPATH = str(HERE.parent / "target" / "classes")


def read_acks(pipe, acked):
    for line in pipe:
        if line.startswith("ACK "):
            acked.add(int(line.split()[1]))


def run_round(logdir, min_run=0.2, max_run=1.2):
    writer = subprocess.Popen(
        ["java", "-cp", CLASSPATH, "com.minilog.tools.ChaosWriter", str(logdir)],
        stdout=subprocess.PIPE, text=True)

    acked = set()
    reader = threading.Thread(target=read_acks, args=(writer.stdout, acked))
    reader.start()

    time.sleep(random.uniform(min_run, max_run))
    writer.kill()  # SIGKILL: no shutdown hooks, no flush, no mercy
    writer.wait()
    reader.join()
    return acked


def verify(logdir, all_acked):
    dump = subprocess.run(
        ["java", "-cp", CLASSPATH, "com.minilog.tools.LogDump", str(logdir)],
        capture_output=True, text=True, check=True)

    ok, bad, next_offset = set(), set(), None
    for line in dump.stdout.splitlines():
        tag, value = line.split()
        if tag == "OK":
            ok.add(int(value))
        elif tag == "BAD":
            bad.add(int(value))
        elif tag == "NEXT":
            next_offset = int(value)

    problems = []
    if bad:
        problems.append(f"{len(bad)} records with wrong payload: {sorted(bad)[:5]}...")
    lost = all_acked - ok
    if lost:
        problems.append(f"{len(lost)} acked records missing: {sorted(lost)[:5]}...")
    if next_offset is None or ok != set(range(next_offset)):
        problems.append(f"offsets are not gapless up to {next_offset}")
    return problems, next_offset


def main():
    rounds = int(sys.argv[1]) if len(sys.argv) > 1 else 25
    logdir = Path(sys.argv[2]) if len(sys.argv) > 2 else HERE / "chaos-data"

    all_acked = set()
    for i in range(1, rounds + 1):
        acked = run_round(logdir)
        all_acked |= acked

        problems, next_offset = verify(logdir, all_acked)
        if problems:
            print(f"round {i}: CONTRACT VIOLATED")
            for p in problems:
                print(f"  {p}")
            sys.exit(1)
        print(f"round {i}: killed writer, {len(acked)} acks this round, "
              f"log recovered to offset {next_offset}, all {len(all_acked)} acked records intact")

    print(f"\n{rounds} kill -9 crashes, {len(all_acked)} acked records, zero lost, zero corrupt.")


if __name__ == "__main__":
    main()
