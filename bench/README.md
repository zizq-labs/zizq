# zizq-bench

Benchmark harness for the Zizq server. It starts a given `zizq` binary with a
fresh root directory, pushes jobs through it with the Rust client, and samples
throughput, queue depth, memory, disk and CPU once a second.

Any binary can be benchmarked: a release download, a local build, or an
experimental variant. So one harness compares versions, builds and settings.

## Running

```shell
cargo run --release -- --server /path/to/zizq --jobs 500000
```

| Option | Default | |
|---|---|---|
| `--server` | | The `zizq` binary to benchmark |
| `--label` | SHA-256 prefix | Names the build, e.g. `musl`, `glibc` |
| `--mode` | `upfront` | `upfront` enqueues everything, then drains. `concurrent` starts the workers first and enqueues while they drain |
| `--jobs` | `50000` | Jobs to push through |
| `--workers` | `1` | Workers, each with its own take connection |
| `--concurrency` | `100` | Jobs each worker processes at once |
| `--format` | `json` | `json` or `msgpack` |
| `--interval-ms` | `1000` | Time between samples |
| `--out` | `results` | Results directory |

Arguments after `--` are passed to `zizq serve`:

```shell
cargo run --release -- --server ./zizq --jobs 500000 \
    -- --default-completed-job-retention 7d
```

Each run writes `results/<version>/<label>-<mode>-<jobs>-<timestamp>.ndjson`
and the server's log alongside it. The file holds a header describing the run
and the machine, one line per sample, and a summary.

### Getting comparable numbers

- Compare runs from the same machine only. The plot script refuses to mix
  hosts.
- Run each configuration several times and compare medians.
- If `bench_cpu` is high while `server_cpu` is not, the client is the
  bottleneck. Add `--workers`.
- The server's root directory is created in the system temp directory. Set
  `TMPDIR` to put it on a different disk, or on `/dev/shm` to take storage out
  of the comparison entirely, e.g. when comparing allocators.

## Running the Scenario Matrix

`matrix.sh` runs every combination of mode, job count and completed-job
retention against one or more builds, several times over:

```shell
./matrix.sh musl=./zizq-0.7.3 mimalloc=../target/release/zizq
```

Each build is given as `<label>=<path>`. Builds take turns within each
repetition, so drift on the machine over a long matrix affects every build
alike. Runs with retention have it added to their label, e.g. `musl+7d`.

| Variable | Default | |
|---|---|---|
| `MODES` | `upfront concurrent` | Modes to run |
| `JOBS` | `50000 500000 5000000` | Job counts to run |
| `RETENTION` | `0 7d` | Completed job retention. `0` deletes jobs on completion, the server default |
| `REPEAT` | `3` | Repetitions of the whole matrix |
| `OUT` | `results` | Results directory |
| `BENCH_ARGS` | | Extra options for every run, e.g. `"--workers 2"` |
| `DRY_RUN` | | `1` lists the runs without starting them |

The defaults amount to 36 runs per build, so check the size of a matrix with
`DRY_RUN=1` before starting it:

```shell
DRY_RUN=1 JOBS="5000000 10000000" MODES=upfront ./matrix.sh mimalloc=./zizq
```

A failed run, such as the server running out of memory, is logged and the
matrix carries on. The script exits non-zero at the end if any run failed.

## Summarising

`summary.py` groups results by configuration and prints a row per group: how
many runs, and the median with its range for drain and enqueue rates, peak
memory and total time. Medians rather than means, so one bad run can't drag
a result. It needs only Python 3.

```shell
./summary.py results/0.7.3
./summary.py --markdown results/0.7.3/*-500000-*.ndjson
```

```text
label         runs  drain                     enqueue                   peak memory        total
shm-glibc     3     13,100/s (13,026–13,275)  51,880/s (51,450–52,865)  466 MiB (464–473)  47.8s (47.1–48.1)
shm-mimalloc  3     14,325/s (13,710–14,635)  62,864/s (62,518–63,468)  317 MiB (309–320)  42.8s (42.2–44.4)
shm-musl      3     9,867/s (9,417–9,931)     36,227/s (35,572–36,523)  265 MiB (261–265)  64.5s (64.4–66.8)
```

Only the settings that differ between groups get a column. Runs that did not
finish, e.g. because the server ran out of memory, are counted as failed.

## Plotting

`plot.py` needs [uv](https://docs.astral.sh/uv/), which installs matplotlib
for it on first run.

```shell
uv run plot.py results/0.7.3/musl-upfront-500000-*.ndjson
uv run plot.py -o compare.svg results/0.7.3/{musl,glibc}-upfront-500000-*.ndjson
```

One file plots that run with its phases shaded. Several files are overlaid.
`--bucket 60` averages throughput per minute rather than per sample, for long
runs. The output format follows the extension of `-o`: svg, png or pdf.
