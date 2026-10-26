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

### Running the server on another machine

With `--remote`, the server runs on another machine and the benchmark loads
it over the network, e.g. the server on an M4 Mac and the client on a Pi:

```shell
cargo run --release -- --remote me@m4.local \
    --remote-bench ~/bin/zizq-bench --server ~/bin/zizq --jobs 500000
```

`zizq-bench` must be installed on the server's machine too, and SSH must work
without a password prompt (keys or an agent). The benchmark runs
`zizq-bench --agent` there over SSH. The agent starts the server listening on
all interfaces with a fresh root directory, and reports its memory, CPU and
disk use back over the same connection, so results files are the same as for
a local run. The server's log is copied back alongside them.

| Option | Default | |
|---|---|---|
| `--remote` | | SSH destination for the server's machine, e.g. `user@host` |
| `--server` | | The `zizq` binary, as a path on the server's machine |
| `--remote-bench` | `zizq-bench` | How to run `zizq-bench` there. Not quoted, so it can carry a prefix, e.g. `env TMPDIR=/dev/shm ~/bin/zizq-bench` |
| `--remote-addr` | the SSH host | Address the client reaches the server at, e.g. a private network address |

The server listens on a random port, which must be reachable from the
client's machine. The header records both machines, and the summary and plot
scripts only compare runs that used the same pair.

### Running the client on another machine

`--remote-client` is the other way round: the server runs on this machine and
the load (the producers and workers) runs on another, e.g. the server on an
M4 Mac and the load from a Pi. Only this machine makes an SSH connection, so
the server's machine needn't accept SSH at all:

```shell
cargo run --release -- --server ./zizq --remote-client pi@raspberrypi.local \
    --remote-bench ~/bin/zizq-bench --jobs 500000
```

The server is started and sampled here, listening on all interfaces, and
`zizq-bench --load-agent` runs the load on the other machine, reporting its
progress and its own CPU use back. Results and the server's log are written
here as usual, with the agent's own errors in a `.client.log` alongside.

| Option | Default | |
|---|---|---|
| `--remote-client` | | SSH destination for the client's machine, e.g. `user@host` |
| `--remote-bench` | `zizq-bench` | How to run `zizq-bench` there |
| `--server-addr` | this machine's hostname | Address the client reaches this server at, e.g. `fer-chippie.local` |

The other machine must be able to reach the server's port here. The agent
checks it can before starting the load, and fails rather than waiting if it
can't.

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
