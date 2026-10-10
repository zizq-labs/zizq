#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.10"
# dependencies = ["matplotlib>=3.8"]
# ///
"""Plot zizq-bench results.

Given one results file, plots that run with its phases shaded. Given
several, overlays them, e.g. two server versions or two builds of one.

    uv run plot.py results/0.7.3/musl-upfront-500000-*.ndjson
    uv run plot.py --bucket 60 -o compare.svg results/0.7.3/*-5000000-*.ndjson

Panels, top to bottom: throughput, queue depth, memory and disk, CPU.
The output format follows the extension of `-o` (svg, png, pdf).
"""

import argparse
import json
import sys
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

MIB = 1024 * 1024

PHASE_COLOURS = {"enqueue": "#4c78a8", "both": "#9d755d", "drain": "#54a24b"}


def load(path):
    """Read a results file into its header, samples and summary."""
    header, samples, summary = None, [], None
    with open(path) as f:
        for line in f:
            record = json.loads(line)
            kind = record["type"]
            if kind == "header":
                header = record
            elif kind == "sample":
                samples.append(record)
            elif kind == "summary":
                summary = record
    if header is None or not samples:
        sys.exit(f"{path}: not a zizq-bench results file")
    return {"path": path, "header": header, "samples": samples, "summary": summary}


def name(run, runs):
    """Legend label: only the parts that differ between the runs."""
    parts = name_parts(run)
    varying = [
        key
        for key in parts
        if len({name_parts(r)[key] for r in runs}) > 1
    ] or ["version", "label"]
    return " ".join(parts[key] for key in varying)


def name_parts(run):
    h = run["header"]
    a = h["args"]
    return {
        "version": h["version"],
        "label": h["label"],
        "mode": a["mode"],
        "jobs": f"{a['jobs']:,} jobs",
        "workers": f"{a['workers']}w",
        "concurrency": f"c{a['concurrency']}",
        "format": a["format"],
    }


def bucketed(samples, bucket):
    """The last sample in each `bucket`-second window, so rates can be
    taken over the window rather than per sample."""
    if not bucket:
        return samples
    last = {}
    for s in samples:
        last[int(s["t"] // bucket)] = s
    return [last[k] for k in sorted(last)]


def rates(samples, field):
    """Jobs per second between consecutive samples, plotted at the end of
    each interval."""
    ts, values = [], []
    for prev, cur in zip(samples, samples[1:]):
        dt = cur["t"] - prev["t"]
        if dt > 0:
            ts.append(cur["t"])
            values.append((cur[field] - prev[field]) / dt)
    return ts, values


def shade_phases(ax, samples):
    """Shade the background by benchmark phase."""
    start = samples[0]
    for prev, cur in zip(samples, samples[1:] + [None]):
        if cur is None or cur["phase"] != prev["phase"]:
            end = prev if cur is None else cur
            ax.axvspan(
                start["t"],
                end["t"],
                color=PHASE_COLOURS.get(start["phase"], "#cccccc"),
                alpha=0.08,
                linewidth=0,
            )
            start = cur


def plot(runs, bucket, title):
    single = len(runs) == 1
    fig, (tput, depth, mem, cpu) = plt.subplots(
        4, 1, sharex=True, figsize=(11, 11), constrained_layout=True
    )

    for i, run in enumerate(runs):
        colour = f"C{i}"
        label = name(run, runs)
        samples = run["samples"]
        windows = bucketed(samples, bucket)
        t = [s["t"] for s in samples]

        ts, completed = rates(windows, "completed")
        tput.plot(ts, completed, color=colour, label=f"{label} completed")
        # Only while enqueueing, rather than a flat line along zero after.
        total = samples[-1]["enqueued"]
        enqueuing = [w for w in windows if w["enqueued"] < total]
        ts, enqueued = rates(windows[: len(enqueuing) + 1], "enqueued")
        tput.plot(ts, enqueued, color=colour, linestyle="--", alpha=0.6,
                  label=f"{label} enqueued")

        depth.plot(t, [s["enqueued"] - s["completed"] for s in samples],
                   color=colour, label=label)

        mem.plot(t, [s["rss"] / MIB for s in samples], color=colour,
                 label=f"{label} memory")
        mem.plot(t, [s["disk"] / MIB for s in samples], color=colour,
                 linestyle=":", label=f"{label} disk")

        cpu.plot(t, [s["server_cpu"] for s in samples], color=colour,
                 label=f"{label} server")
        cpu.plot(t, [s["bench_cpu"] for s in samples], color=colour,
                 linestyle="--", alpha=0.6, label=f"{label} bench")

        if single:
            for ax in (tput, depth, mem, cpu):
                shade_phases(ax, samples)

    per = f"per {bucket:g}s" if bucket else "per sample"
    tput.set_ylabel(f"jobs/s ({per})")
    tput.set_title("Throughput")
    depth.set_ylabel("jobs")
    depth.set_title("Queue depth")
    mem.set_ylabel("MiB")
    mem.set_title("Server memory (solid) and disk (dotted)")
    cpu.set_ylabel("% of one core")
    cpu.set_title("CPU: server (solid) and benchmark client (dashed)")
    cpu.set_xlabel("seconds")

    for ax in (tput, depth, mem, cpu):
        ax.set_ylim(bottom=0)
        ax.grid(alpha=0.3)
        ax.legend(fontsize="small", loc="upper right")

    fig.suptitle(title)
    return fig


def title_for(runs):
    h = runs[0]["header"]
    host = h["host"]
    if len(runs) == 1:
        a = h["args"]
        s = runs[0]["summary"] or {}
        return (
            f"zizq {h['version']} ({h['label']}), {a['mode']}, {a['jobs']:,} jobs: "
            f"drain {s.get('drain_rate', 0):,.0f}/s, "
            f"peak memory {s.get('peak_rss', 0) / MIB:,.0f} MiB\n"
            f"{host['cpu']}, {host['cores']} cores, {host['os']}"
        )
    hosts = {(r["header"]["host"]["cpu"], r["header"]["host"]["cores"]) for r in runs}
    if len(hosts) > 1:
        return "Mixed hosts: " + " vs ".join(
            f"{cpu}, {cores} cores" for cpu, cores in sorted(hosts))
    return f"{host['cpu']}, {host['cores']} cores, {host['os']}"


def main():
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("files", nargs="+", type=Path, help="results files")
    parser.add_argument("-o", "--output", type=Path,
                        help="output file (default: first input with .svg)")
    parser.add_argument("--bucket", type=float, default=0,
                        help="seconds to average throughput over (default: per sample)")
    parser.add_argument("--allow-mixed-hosts", action="store_true",
                        help="overlay runs from different machines")
    args = parser.parse_args()

    runs = [load(path) for path in args.files]

    hosts = {json.dumps(r["header"]["host"], sort_keys=True) for r in runs}
    if len(hosts) > 1 and not args.allow_mixed_hosts:
        sys.exit("runs are from different hosts and are not comparable "
                 "(pass --allow-mixed-hosts to plot them anyway)")

    output = args.output or args.files[0].with_suffix(".svg")
    plot(runs, args.bucket, title_for(runs)).savefig(output)
    print(output)


if __name__ == "__main__":
    main()
