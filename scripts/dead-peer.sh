#!/usr/bin/env bash

# Measure how long the server takes to notice a take-stream peer that has
# vanished without closing its connection, and to requeue its job.
#
# Usage:
#   ./scripts/dead-peer.sh [--binary PATH] [--max-wait SECS]
#                          [--expect-within SECS] [--tcp-retries2 N]
#
# A worker whose *process* dies is noticed immediately, because its kernel
# closes the socket. A worker whose *host* dies (power loss, kernel panic,
# network partition) sends nothing at all, so the server only finds out
# when its own writes go unacknowledged for long enough that its kernel
# gives up on the connection.
#
# The script:
#
#   1. Starts a server on loopback and enqueues one job
#   2. Takes that job with a plain `curl` take stream
#   3. Drops every packet on that connection with the platform's packet
#      filter, in both directions, so the worker is silent but its socket
#      stays open
#   4. Polls the job until it is back in `ready`, reporting how long that
#      took alongside the server socket's state
#
# Packets are dropped as they are *received*, not as they are sent. A
# packet dropped on the way out fails locally and the sender's kernel
# knows it was never sent, which takes a different path through TCP than
# a real dead peer. Dropped on the way in, it has left the sender's stack
# and simply never arrives, so the sender waits for an ACK that never
# comes, exactly as it would for a host that vanished.
#
# Linux: the script re-executes itself inside a private user and network
# namespace (`unshare --user --map-root-user --net`) and drops packets
# with nftables. No root is needed, and nothing outside the namespace is
# touched: the firewall rules and sysctls vanish with it on exit. Where
# unprivileged user namespaces are disabled (e.g. Ubuntu 24.04), run it
# with sudo.
#
# macOS: there are no network namespaces, so packets are dropped by pf
# rules in a temporary anchor under `com.apple/`, which the default
# /etc/pf.conf evaluates. pfctl needs root, so it is run through sudo,
# which may prompt for a password. On exit the anchor is flushed and the
# script's reference to pf being enabled is released.
#
# --tcp-retries2 (Linux only) sets net.ipv4.tcp_retries2 inside the
# namespace, which shortens the kernel's give-up time for a quicker run.
# Leave it unset to measure the kernel default.
#
# --expect-within makes the script exit non-zero if the job is not
# requeued within that many seconds.
#
# Requires curl and jq, plus:
#   Linux: unshare (util-linux), ip, nft, ss
#   macOS: pfctl, netstat, sudo

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

BINARY="$SCRIPT_DIR/../target/release/zizq"
MAX_WAIT=1200
EXPECT_WITHIN=""
TCP_RETRIES2=""

while [[ $# -gt 0 ]]; do
    case "$1" in
        --binary)        BINARY="$2"; shift 2 ;;
        --max-wait)      MAX_WAIT="$2"; shift 2 ;;
        --expect-within) EXPECT_WITHIN="$2"; shift 2 ;;
        --tcp-retries2)  TCP_RETRIES2="$2"; shift 2 ;;
        *) echo "Unknown arg: $1"; exit 1 ;;
    esac
done

BINARY="$(cd "$(dirname "$BINARY")" && pwd)/$(basename "$BINARY")"

if [[ ! -x "$BINARY" ]]; then
    echo "Error: binary not found or not executable: $BINARY"
    exit 1
fi

OS="$(uname -s)"

case "$OS" in
    Linux)  REQUIRED=(unshare ip nft ss curl jq) ;;
    Darwin) REQUIRED=(pfctl netstat sudo curl jq) ;;
    *) echo "Error: unsupported platform: $OS"; exit 1 ;;
esac

if [[ -n "$TCP_RETRIES2" && "$OS" != Linux ]]; then
    echo "Error: --tcp-retries2 is only supported on Linux."
    exit 1
fi

for cmd in "${REQUIRED[@]}"; do
    if ! command -v "$cmd" > /dev/null; then
        echo "Error: '$cmd' is required but not installed."
        exit 1
    fi
done

# --- Platform specifics ---
#
# Each platform defines:
#
#   sandbox_enter        isolate the run, where the platform can
#   sandbox_describe     print any network settings that affect the result
#   worker_port          local port of the established take stream
#   blackhole PORT       drop every packet to and from PORT, on receipt
#   blackhole_cleanup    undo anything blackhole did outside a sandbox
#   socket_state         one line about the server side of the stream

if [[ "$OS" == Linux ]]; then
    sandbox_enter() {
        if [[ -z "${ZIZQ_DEAD_PEER_NETNS:-}" ]]; then
            local args=(--binary "$BINARY" --max-wait "$MAX_WAIT")
            [[ -n "$EXPECT_WITHIN" ]] && args+=(--expect-within "$EXPECT_WITHIN")
            [[ -n "$TCP_RETRIES2" ]] && args+=(--tcp-retries2 "$TCP_RETRIES2")

            ZIZQ_DEAD_PEER_NETNS=1 exec unshare --user --map-root-user --net \
                "$0" "${args[@]}"
        fi

        ip link set lo up

        if [[ -n "$TCP_RETRIES2" ]]; then
            echo "$TCP_RETRIES2" > /proc/sys/net/ipv4/tcp_retries2
        fi
    }

    sandbox_describe() {
        echo "    tcp_retries2 = $(cat /proc/sys/net/ipv4/tcp_retries2)"
    }

    worker_port() {
        ss -Htn state established "( dport = :$SERVER_PORT )" \
            | awk '{ split($3, a, ":"); print a[length(a)] }'
    }

    blackhole() {
        nft add table inet dead_peer
        nft add chain inet dead_peer input \
            '{ type filter hook input priority 0; policy accept; }'
        nft add rule inet dead_peer input tcp sport "$1" drop
        nft add rule inet dead_peer input tcp dport "$1" drop
    }

    # The rules vanish with the namespace.
    blackhole_cleanup() { :; }

    # The retransmit timer (time to next attempt, attempts so far), the
    # exponential backoff exponent, and segments sent but unacknowledged.
    socket_state() {
        local out
        out="$(ss -Htino "( sport = :$SERVER_PORT and dport = :$WORKER_PORT )" \
            | tr -s ' \t\n' ' ')"

        if [[ -z "$out" ]]; then
            echo "(socket closed)"
            return
        fi

        echo "$out" | grep -oE 'timer:\([^)]*\)|backoff:[0-9]+|unacked:[0-9]+' \
            | tr '\n' ' ' || true
        echo
    }
else
    PF_ANCHOR="com.apple/zizq-dead-peer"
    PF_TOKEN=""

    sandbox_enter() {
        # Prompt for a password now, rather than midway through the run.
        sudo -v

        # Rules in our anchor only take effect if the main ruleset
        # evaluates com.apple/*, as the default /etc/pf.conf does.
        if ! sudo pfctl -sr 2>/dev/null | grep -q 'anchor "com.apple/\*"'; then
            echo "Error: pf's main ruleset does not evaluate anchor \"com.apple/*\"."
            echo "Is /etc/pf.conf loaded? Try: sudo pfctl -f /etc/pf.conf"
            exit 1
        fi
    }

    sandbox_describe() { :; }

    # netstat prints addresses as 127.0.0.1.PORT.
    worker_port() {
        netstat -anp tcp \
            | awk -v s="127.0.0.1.$SERVER_PORT" \
                '$5 == s && $6 == "ESTABLISHED" { n = split($4, a, "."); print a[n] }'
    }

    blackhole() {
        # `pfctl -E` enables pf if it is not already, and returns a token
        # that later releases just this reference, so pf stays enabled if
        # something else had enabled it too.
        PF_TOKEN="$(sudo pfctl -E 2>&1 | awk '/^Token/ { print $3 }')"

        # pfctl warns that -f could flush the main ruleset whenever it is
        # used, even though -a confines it to our anchor, and -q does not
        # silence that. Only show its output if loading fails.
        local out
        if ! out="$(printf '%s\n' \
            "block drop in quick on lo0 proto tcp from any port $1 to any" \
            "block drop in quick on lo0 proto tcp from any to any port $1" \
            | sudo pfctl -q -a "$PF_ANCHOR" -f - 2>&1)"; then
            echo "$out"
            exit 1
        fi
    }

    blackhole_cleanup() {
        sudo pfctl -q -a "$PF_ANCHOR" -F rules 2>/dev/null || true
        if [[ -n "$PF_TOKEN" ]]; then
            sudo pfctl -q -X "$PF_TOKEN" 2>/dev/null || true
        fi
    }

    # macOS exposes no retransmission timers, but bytes the peer has not
    # acknowledged accumulate in the send queue.
    socket_state() {
        local out
        out="$(netstat -anp tcp \
            | awk -v l="127.0.0.1.$SERVER_PORT" -v r="127.0.0.1.$WORKER_PORT" \
                '$4 == l && $5 == r { print "send-q=" $3 }')"
        echo "${out:-(socket closed)}"
    }
fi

sandbox_enter

# --- Set up isolated work directory ---

WORKDIR="$(mktemp -d)"

cleanup() {
    for pid in "${TAKE_PID:-}" "${SERVER_PID:-}"; do
        if [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null; then
            kill "$pid" 2>/dev/null
            wait "$pid" 2>/dev/null || true
        fi
    done
    blackhole_cleanup
    rm -rf "$WORKDIR"
}
trap cleanup EXIT

echo "==> Dead peer detection ($("$BINARY" --version))"
sandbox_describe

# --- Start the server ---

SERVER_LOG="$WORKDIR/server.log"

"$BINARY" serve --port 0 --no-admin --root-dir "$WORKDIR/root" \
    --log-format json --log-level info > "$SERVER_LOG" 2>&1 &
SERVER_PID=$!

ZIZQ_URL=""
DEADLINE=$((SECONDS + 10))
while [[ $SECONDS -lt $DEADLINE ]]; do
    if ! kill -0 "$SERVER_PID" 2>/dev/null; then
        echo "Error: server exited unexpectedly:"
        cat "$SERVER_LOG"
        exit 1
    fi

    LINE="$(grep '"api":"primary"' "$SERVER_LOG" 2>/dev/null || true)"
    if [[ -n "$LINE" ]]; then
        ADDR="$(echo "$LINE" | jq -r '.fields.addr')"
        ZIZQ_URL="http://${ADDR}"
        SERVER_PORT="${ADDR##*:}"
        break
    fi

    sleep 0.1
done

if [[ -z "$ZIZQ_URL" ]]; then
    echo "Error: timed out waiting for server to start."
    cat "$SERVER_LOG"
    exit 1
fi

echo "    Server listening on ${ZIZQ_URL}"

job_status() {
    curl -sf "$ZIZQ_URL/jobs/$JOB_ID" | jq -r '.status'
}

# --- Enqueue and take one job ---

JOB_ID="$(curl -sf -X POST "$ZIZQ_URL/jobs" \
    -H 'Content-Type: application/json' \
    -d '{"type": "dead_peer", "queue": "dead_peer", "payload": {}}' \
    | jq -r '.id')"

echo "    Enqueued job $JOB_ID"

TAKE_LOG="$WORKDIR/take.ndjson"

curl -sN -H 'Worker-Id: dead-peer' \
    "$ZIZQ_URL/jobs/take?queue=dead_peer&prefetch=1" > "$TAKE_LOG" &
TAKE_PID=$!

DEADLINE=$((SECONDS + 10))
until grep -q "$JOB_ID" "$TAKE_LOG" 2>/dev/null; do
    if [[ $SECONDS -ge $DEADLINE ]]; then
        echo "Error: job was not delivered on the take stream."
        exit 1
    fi
    sleep 0.1
done

STATUS="$(job_status)"
if [[ "$STATUS" != "in_flight" ]]; then
    echo "Error: expected job to be in_flight, got '$STATUS'."
    exit 1
fi

# The take stream is the only connection to the server at this point,
# since every other curl above has exited.
WORKER_PORT="$(worker_port)"

if [[ -z "$WORKER_PORT" || "$WORKER_PORT" == *$'\n'* ]]; then
    echo "Error: could not identify the take stream's local port."
    exit 1
fi

echo "    Job is in_flight on take stream from port $WORKER_PORT"

# --- Make the worker vanish ---

blackhole "$WORKER_PORT"

echo "    Dropping all packets to and from port $WORKER_PORT"
echo

# --- Wait for the server to notice ---

START=$SECONDS
while true; do
    ELAPSED=$((SECONDS - START))
    STATUS="$(job_status)"

    if [[ "$STATUS" == "ready" ]]; then
        break
    fi

    if (( ELAPSED % 15 == 0 )) && [[ $ELAPSED != "${LAST_REPORT:-}" ]]; then
        printf '    %5ss  job=%-9s  %s\n' "$ELAPSED" "$STATUS" "$(socket_state)"
        LAST_REPORT=$ELAPSED
    fi

    if [[ $ELAPSED -ge $MAX_WAIT ]]; then
        echo
        echo "==> Job still $STATUS after ${MAX_WAIT}s, giving up."
        exit 1
    fi

    sleep 1
done

echo
echo "==> Job requeued ${ELAPSED}s after the worker vanished."

if [[ -n "$EXPECT_WITHIN" && $ELAPSED -gt $EXPECT_WITHIN ]]; then
    echo "    Expected within ${EXPECT_WITHIN}s."
    exit 1
fi
