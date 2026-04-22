# Quick Start

Zizq is incredibly simple to get up and running. The server is a static binary
and needs no configuration out of the box.

> [!TIP]
> Read the [Command Line Reference](/docs/cli/) for full details on the `zizq`
> command.

## 🔧 Set up the server instance { #setup }

You run once instance of the server and connect many clients to it. The server
does almost all of the work.

### 1. Download the binary.

```shell
curl -sLO https://github.com/zizq-labs/zizq/releases/download/v0.1.0/zizq-0.1.0-linux-x86_64.tar.gz
```

### 2. Extract it.

```shell
tar -xvzf zizq-0.1.0-linux-x86_64.tar.gz
```

### 3. Run it to start the server.

```shell
./zizq serve

Zizq 0.1.0
2026-04-05T05:41:26.893318Z  INFO zizq::commands::serve: no license key provided, running in free tier
2026-04-05T05:41:27.081150Z  INFO zizq::commands::serve: store opened root_dir=./zizq-root
2026-04-05T05:41:27.081547Z  INFO zizq::commands::serve: admin API listening addr=127.0.0.1:8901 scheme=http
2026-04-05T05:41:27.081842Z  INFO zizq::commands::serve: in-memory indexes rebuilt ready=0 scheduled=0
2026-04-05T05:41:27.081890Z  INFO zizq::commands::serve: primary API listening addr=127.0.0.1:7890 scheme=http
Listening on http://127.0.0.1:8901 (admin)
Listening on http://127.0.0.1:7890 (primary)
```

---------------------------------

## ⚙️ Enqueue and process your first job { #enqueue }

> [!TIP]
> Detailed documents on the HTTP API for enqueueing and processing jobs are
> available in the [HTTP API Reference](/docs/api/).

With the server running, we can already use it to enqueue jobs and simulate
processing those jobs without the use of a client library. This example uses
`curl` (and a small amount of `jq` to help with the JSON) in `bash`, and serves
purely to illustrate the flexibility of Zizq.

> [!NOTE]
> This is a novel example. In reality applications will use a
> [client library](/docs/clients) or will integrate with the
> [HTTP API](/docs/api/) programatically.

### 1. Enqueue a job

```bash
curl -XPOST \
  http://localhost:7890/jobs \
  -s \
  -H 'Content-Type: application/json' \
  -d '{
    "queue":"example",
    "type":"hello_world",
    "payload":{"greet":"Universe"}
  }' | jq
```

```text
{
  "id": "03g04auga7wsnux60e4r7wip1",
  "type": "hello_world",
  "queue": "example",
  "priority": 32768,
  "status": "ready",
  "ready_at": 1777005093930,
  "attempts": 0,
  "duplicate": false
}
```

### 2. Process the job

> [!NOTE]
> This script does not exit until it is iterrupted. The `while` loop receives
> lines of JSON from the server. Blank lines are heartbeats and are skipped.

```bash
curl -XGET \
  "http://localhost:7890/jobs/take" \
  -sN | while IFS= read -r job; do
    [[ "$job" = "" ]] && continue

    id="$(echo "$job" | jq -r ".id")"
    type="$(echo "$job" | jq -r ".type")"

    echo "Processing job with id=${id} type=${type}..."
    echo "$job" | jq

    echo "Acknowledging completion..."
    curl -XPOST -s "http://localhost:7890/jobs/${id}/success"

    echo "Done."
  done
```

```text
Processing job with id=03g04auga7wsnux60e4r7wip1 type=hello_world...
{
  "id": "03g04auga7wsnux60e4r7wip1",
  "type": "hello_world",
  "queue": "example",
  "priority": 32768,
  "status": "in_flight",
  "payload": {
    "greet": "Universe"
  },
  "ready_at": 1777005093930,
  "attempts": 0,
  "dequeued_at": 1777005818220
}
Acknowledging completion...
Done.
```

### 3. Keep going

Try enqueueing more jobs while that little `bash` script worker is still
running. Does the worker pick those up?

What happens if you comment out the acknowledgement (`/jobs/${id}/success`)
line in the worker script?

---------------------------------

## 🔌 Integrate with your application { #integrate }

> [!NOTE]
> Read the [specific documentation](/docs/clients) for official clients, and
> the full [Zizq HTTP API Reference](/docs/api/) for the API.

Clients —and consequently workers— can be written in any programming language.
Any number of clients can connect to the server (subject to operating system
limits).

You can:

1. Add an official [client library](/docs/clients) to your application
2. Integrate with the [HTTP API](/docs/api/) directly

🎉 Congratulations, you’re all set to go!
