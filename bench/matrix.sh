#!/usr/bin/env bash
#
# Run the benchmark scenario matrix against one or more server builds.
#
# Usage:
#   ./matrix.sh <label>=<path/to/zizq> [<label>=<path/to/zizq> ...]
#
#   ./matrix.sh musl=./zizq-0.7.3 mimalloc=../target/release/zizq
#   JOBS="50000 500000" REPEAT=5 ./matrix.sh musl=./zizq
#
# Every combination of MODES x JOBS x RETENTION is run REPEAT times for
# each build. Builds take turns within each repetition, so drift on the
# machine over a long matrix affects every build alike.
#
# Settings, as environment variables:
#
#   MODES      default "upfront concurrent"
#   JOBS       default "50000 500000 5000000"
#   RETENTION  completed job retention, default "0 7d". 0 deletes jobs
#              on completion (the server default). A non-zero value
#              keeps them, and is added to the label, e.g. musl+7d
#   REPEAT     default 3
#   OUT        results directory, default "results"
#   DRY_RUN    1 prints the runs without starting them
#
# Anything else, such as --workers or --format, can be passed to every
# run with BENCH_ARGS, e.g. BENCH_ARGS="--workers 2".
#
# A run that fails is logged and the matrix carries on: a server that
# runs out of memory at 5M jobs is a result in itself.

set -euo pipefail

cd "$(dirname "$0")"

MODES=${MODES:-"upfront concurrent"}
JOBS=${JOBS:-"50000 500000 5000000"}
RETENTION=${RETENTION:-"0 7d"}
REPEAT=${REPEAT:-3}
OUT=${OUT:-results}
DRY_RUN=${DRY_RUN:-0}
BENCH_ARGS=${BENCH_ARGS:-}

if [ $# -eq 0 ]; then
    sed -n '3,9p' "$0" | sed 's/^# \{0,1\}//'
    exit 1
fi

labels=()
servers=()
for build in "$@"; do
    label="${build%%=*}"
    server="${build#*=}"
    if [ "$label" = "$build" ] || [ ! -x "$server" ]; then
        echo "Expected <label>=<path/to/zizq>, got '${build}'." >&2
        exit 1
    fi
    labels+=("$label")
    servers+=("$(realpath "$server")")
done

total=$(( REPEAT * $(wc -w <<< "$MODES") * $(wc -w <<< "$JOBS") \
    * $(wc -w <<< "$RETENTION") * ${#servers[@]} ))

if [ "$DRY_RUN" != 1 ]; then
    cargo build --release --quiet
    mkdir -p "$OUT"
fi

log="${OUT}/matrix-$(date +%Y%m%d-%H%M%S).log"
n=0
failed=0

for rep in $(seq "$REPEAT"); do
    for jobs in $JOBS; do
        for mode in $MODES; do
            for retention in $RETENTION; do
                for i in "${!servers[@]}"; do
                    n=$((n + 1))
                    label="${labels[$i]}"
                    server_args=()
                    if [ "$retention" != 0 ]; then
                        label="${label}+${retention}"
                        server_args=(-- --default-completed-job-retention "$retention")
                    fi

                    cmd=(./target/release/zizq-bench
                        --server "${servers[$i]}"
                        --label "$label"
                        --mode "$mode"
                        --jobs "$jobs"
                        --out "$OUT"
                        $BENCH_ARGS
                        ${server_args[@]+"${server_args[@]}"})

                    echo "[${n}/${total}] ${label} ${mode} ${jobs} (repeat ${rep})"

                    if [ "$DRY_RUN" = 1 ]; then
                        echo "    ${cmd[*]}"
                        continue
                    fi

                    if "${cmd[@]}" 2>&1 | tee -a "$log" | grep -v '^Results:' | sed 's/^/    /'; then
                        :
                    else
                        failed=$((failed + 1))
                        echo "    FAILED (see ${log})"
                        echo "FAILED: ${cmd[*]}" >> "$log"
                    fi
                done
            done
        done
    done
done

if [ "$DRY_RUN" != 1 ]; then
    echo "Done: $((n - failed)) of ${total} runs succeeded. Log: ${log}"
    [ "$failed" -eq 0 ]
fi
