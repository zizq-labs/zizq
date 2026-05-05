# Zizq

Zizq is a simple, single-binary persistent job queue. It is very fast and has
no external dependencies, including dependencies on services such as Redis or
a RDBMS.

The server runs as a straightforward HTTP/2 and HTTP/1.1 API, with easy-to-use
clients currently implemented in Node.js and Ruby — and others planned soon.

 [![CI](https://github.com/zizq-labs/zizq/actions/workflows/ci.yml/badge.svg)](https://github.com/zizq-labs/zizq/actions/workflows/ci.yml)

## Features

Zizq supports a growing number of features.

* Single self-contained executable— no separate data store required
* Durable and atomic job queues
* Easy to use `HTTP/1.1` and `HTTP/2` API with JSON or MsgPack
* Cross-language job enqueueing and execution
* Unlimited named queues
* FIFO ordered and granular priority jobs
* Scheduled jobs
* Configurable backoff/retry policies
* Configurable job retention policies
* Unique job support
* APIs to manage the queue contents
* Very high job throughput (over 100K jobs/sec on a Macbook Pro M4)
* An insightful [`zizq top`](#viewing-live-queue-activity) command

## Resources

* [All Documentation](https://zizq.io/docs)
* [Getting Started Docs](https://zizq.io/docs/getting-started/)
* [Zizq Command Reference](https://zizq.io/docs/cli/)
* [Official Client Libraries](https://zizq.io/docs/clients)

## Getting started

Download a [release](https://github.com/zizq-labs/zizq/releases) compatible
with your system and extract it. You should put the executable somewhere on
your PATH but you can also just run it from the current directory.

```shell
curl -sLO https://github.com/zizq-labs/zizq/releases/download/v0.3.0/zizq-0.3.0-linux-x86_64.tar.gz
tar -xvzf zizq-0.3.0-linux-x86_64.tar.gz
```

### Starting the server

> [!NOTE]
> [Full documentation](https://zizq.io/docs/cli/) for the `zizq` command is
> available on the website.

Zizq is a single binary organised into subcommands. The `serve` subcommand
starts the server. This is the default when no other subcommand is specified.

```shell
$ ./zizq serve
Zizq 0.3.0
Listening on 127.0.0.1:8901 (admin)
Listening on 127.0.0.1:7890 (primary)
```

When the server starts, it creates a root directory in which it stores all
queue data. By default this directory is `{PWD}/zizq-root/` but can be
explicitly specified by providing the `--root-dir` flag, or by specifying the
`$ZIZQ_ROOT` environment variable. The server listens on `127.0.0.1` port
`7890` unless otherwise specified by `--host` and `--port`, or `$ZIZQ_HOST` and
`$ZIZQ_PORT`.

Run `zizq serve --help` to see a complete list of available options. The
defaults should be good even for production use, provided the server is not set
up to listen on a public IP address.

### Client libraries

> [!NOTE]
> [Full documentation](https://zizq.io/docs/clients) for our client libraries
> is available on the website.

Zizq provides official client libraries under the MIT license. The goal is to
provide clients for a number of common languages. We have started with Node.js
and Ruby.

* [Official Ruby Client](https://github.com/zizq-labs/zizq-ruby)
* [Official Node.js Client](https://github.com/zizq-labs/zizq-node)

Want a client for Zizq in a language not currently supported?
[leave a feature request](https://github.com/zizq-labs/zizq/issues) or
[build your own](https://zizq.io/docs/api/) easily.

### Communicating with the server

> [!TIP]
> Read our [Getting Started](https://zizq.io/docs/getting-started/) guide and
> the [Zizq HTTP API Reference](https://zizq.io/docs/api/) for more info on
> using the API directly.

With the server up and running we can make some requests to enqueue and perform
some jobs. You would usually use a [client library](#client-libraries) to do
this, but the Zizq API is easy to use directly over HTTP too. Examples here use
[HTTPie](https://httpie.io/) for simplicity.

Let’s enqueue our first job!

```bash
http POST http://localhost:7890/jobs --raw '{
    "queue":"example",
    "type":"hello_world",
    "payload":{"greet":"Universe"}
}'
```

```http
HTTP/1.1 201 Created
content-length: 163
content-type: application/json
date: Sat, 25 Apr 2026 11:56:45 GMT
```
```json
{
    "attempts": 0,
    "duplicate": false,
    "id": "03g0ej3ybwh5uh1ap10xgufm3",
    "priority": 32768,
    "queue": "example",
    "ready_at": 1777118205503,
    "status": "ready",
    "type": "hello_world"
}
```

Now let's process that job with a little `bash` script loop and a sprinkle of
[`jq`](https://jqlang.org/) just to help with the JSON in bash.

> [!NOTE]
> This script does not exit until it is iterrupted. The while loop receives
> lines of JSON from the server. Blank lines are heartbeats and are skipped.

```bash
http --stream GET http://localhost:7890/jobs/take | while IFS= read -r job; do
  [[ "$job" = "" ]] && continue

  id="$(echo "$job" | jq -r ".id")"
  type="$(echo "$job" | jq -r ".type")"

  echo "Processing job with id=${id} type=${type}..."
  echo "$job" | jq

  echo "Acknowledging completion..."
  http --quiet POST "http://localhost:7890/jobs/${id}/success" </dev/null

  echo "Done."
done
```

```text
Processing job with id=03g0ej3ybwh5uh1ap10xgufm3 type=hello_world...
{
  "id": "03g0ej3ybwh5uh1ap10xgufm3",
  "type": "hello_world",
  "queue": "example",
  "priority": 32768,
  "status": "in_flight",
  "payload": {
    "greet": "Universe"
  },
  "ready_at": 1777118205503,
  "attempts": 0,
  "dequeued_at": 1777118245966
}
Acknowledging completion...
Done.
```

### Viewing live queue activity

Zizq includes a terminal-based live queue viewer, which provides real time
insight into what the queue is currently doing, what the backlog looks like and
how deep the queue is at different priority levels. The `top` subcommand
provides this functionality.

```shell
$ ./zizq top
```

By default `zizq top` connects to `127.0.0.1:8901`. Specify `--url` to connect
to a different host.

Scroll up and down the list to visualise the backlog. The current position in
the list is presented in the overall queue depth in real time. You can wiew the
jobs that are currently in-flight, ready and scheduled for a later time.

<p align="center">
    <img src="./images/zizq_top.png" width="862" height="600" alt="zizq top" title="zizq top">
</p>

## Support & Feedback

If you need help using Zizq,
[create an issue](https://github.com/zizq-labs/zizq/issues) on the
[Zizq](https://github.com/zizq-labs/zizq) repo. Feedback is very welcome.
