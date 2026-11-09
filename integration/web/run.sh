#!/usr/bin/env bash

# Run the admin web UI's browser tests against a zizq binary.
#
# Usage:
#   ./integration/web/run.sh --binary PATH [playwright test arguments]
#
# Needs Node.js and Playwright's browsers. CI runs this inside the official
# Playwright image, which has both; locally, the same image works:
#
#   docker run --rm --ipc host --user "$(id -u):$(id -g)" -e HOME=/tmp \
#     -v "$PWD:/work" -w /work mcr.microsoft.com/playwright:v1.65.0-noble \
#     ./integration/web/run.sh --binary target/debug/zizq
#
# Any further arguments go to `playwright test`, e.g. `--project chromium`.

set -euo pipefail

BINARY=""
while [[ $# -gt 0 ]]; do
    case "$1" in
        --binary) BINARY="$2"; shift 2 ;;
        *) break ;;
    esac
done

if [[ -z "$BINARY" ]]; then
    echo "Usage: $0 --binary PATH [playwright test arguments]" >&2
    exit 2
fi

ZIZQ_BINARY="$(cd "$(dirname "$BINARY")" && pwd)/$(basename "$BINARY")"
export ZIZQ_BINARY

cd "$(dirname "$0")"
npm ci --no-audit --no-fund
exec npx playwright test "$@"
