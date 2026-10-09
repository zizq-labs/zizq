#!/usr/bin/env python3
"""Summarise zizq-bench results across repeated runs.

Groups results files by configuration and prints one row per group: the
number of runs, and the median and range of each summary figure. Medians
rather than means, so one bad run can't drag a result.

    ./summary.py results/0.7.3
    ./summary.py --markdown results/0.7.3/*-500000-*.ndjson

Directories are searched for `.ndjson` files. Only columns that differ
between groups are shown, so a comparison of two builds shows the label
and nothing else.
"""

import argparse
import json
import statistics
import sys
from collections import defaultdict
from pathlib import Path

MIB = 1024 * 1024

# Configuration that identifies a group, in column and sort order.
CONFIG = [
    ("version", "version"),
    ("label", "label"),
    ("mode", "mode"),
    ("jobs", "jobs"),
    ("workers", "workers"),
    ("concurrency", "concurrency"),
    ("prefetch", "prefetch"),
    ("batch_size", "batch"),
    ("enqueue_concurrency", "enqueuers"),
    ("format", "format"),
    ("server_args", "server args"),
]


def files(paths):
    for path in paths:
        if path.is_dir():
            yield from sorted(path.rglob("*.ndjson"))
        else:
            yield path


def load(path):
    """The header and summary of a results file. The summary is None if
    the run did not finish."""
    header = summary = None
    with open(path) as f:
        for line in f:
            record = json.loads(line)
            if record["type"] == "header":
                header = record
            elif record["type"] == "summary":
                summary = record
    if header is None:
        sys.exit(f"{path}: not a zizq-bench results file")
    return header, summary


def config(header):
    args = header["args"]
    values = {"version": header["version"], "label": header["label"]}
    for key, _ in CONFIG:
        if key not in values:
            value = args.get(key)
            values[key] = " ".join(value) if isinstance(value, list) else value
    return tuple(values[key] for key, _ in CONFIG)


def spread(values, fmt, unit):
    """Median, with the range when there is more than one value."""
    median = f"{fmt(statistics.median(values))}{unit}"
    if len(values) == 1:
        return median
    return f"{median} ({fmt(min(values))}–{fmt(max(values))})"


def number(value):
    return f"{value:,.0f}"


def mib(value):
    return f"{value / MIB:,.0f}"


def secs(value):
    return f"{value:,.1f}"


def main():
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("paths", nargs="+", type=Path,
                        help="results files, or directories of them")
    parser.add_argument("--markdown", action="store_true",
                        help="print a Markdown table")
    parser.add_argument("--allow-mixed-hosts", action="store_true",
                        help="summarise runs from different machines together")
    args = parser.parse_args()

    groups = defaultdict(list)
    failed = defaultdict(int)
    hosts = set()

    for path in files(args.paths):
        header, summary = load(path)
        hosts.add(json.dumps(header["host"], sort_keys=True))
        key = config(header)
        if summary is None:
            failed[key] += 1
        else:
            groups[key].append(summary)

    if not groups and not failed:
        sys.exit("no results found")
    if len(hosts) > 1 and not args.allow_mixed_hosts:
        sys.exit("runs are from different hosts and are not comparable "
                 "(pass --allow-mixed-hosts to summarise them anyway)")

    keys = sorted(set(groups) | set(failed),
                  key=lambda k: tuple("" if v is None else v for v in k))
    varying = [
        i for i in range(len(CONFIG))
        if len({k[i] for k in keys}) > 1
    ] or [1]

    headings = [CONFIG[i][1] for i in varying] + ["runs", "drain", "enqueue",
                                                  "peak memory", "total"]
    if any(failed.values()):
        headings.insert(len(varying) + 1, "failed")

    rows = []
    for key in keys:
        runs = groups.get(key, [])
        row = [f"{key[i]:,}" if isinstance(key[i], int) else str(key[i] or "")
               for i in varying]
        row.append(str(len(runs)))
        if any(failed.values()):
            row.append(str(failed.get(key, 0)))
        if runs:
            row += [
                spread([r["drain_rate"] for r in runs], number, "/s"),
                spread([r["enqueue_rate"] for r in runs], number, "/s"),
                spread([r["peak_rss"] for r in runs], mib, " MiB"),
                spread([r["total_secs"] for r in runs], secs, "s"),
            ]
        else:
            row += ["", "", "", ""]
        rows.append(row)

    if args.markdown:
        print("| " + " | ".join(headings) + " |")
        print("|" + "|".join("---" for _ in headings) + "|")
        for row in rows:
            print("| " + " | ".join(row) + " |")
    else:
        widths = [max(len(c) for c in col) for col in zip(headings, *rows)]
        for row in [headings] + rows:
            print("  ".join(c.ljust(w) for c, w in zip(row, widths)).rstrip())


if __name__ == "__main__":
    main()
