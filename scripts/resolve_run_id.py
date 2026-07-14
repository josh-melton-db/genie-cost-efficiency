#!/usr/bin/env python3
"""Resolve --run-id from argv or latest_run_id.txt for job chaining."""

from __future__ import annotations

import argparse
import sys

from genie_bench.run_ids import load_run_id


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--run-id", default="")
    args = p.parse_args(argv)
    rid = args.run_id.strip() or load_run_id()
    print(rid)
    return 0


if __name__ == "__main__":
    sys.exit(main())
