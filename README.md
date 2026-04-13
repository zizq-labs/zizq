# Zizq

Zizq is a simple, single-binary persistent job queue. It is very fast
(processes over ~ 100,000 jobs per second in the Node.js client on a Macbook
Pro) and has no external dependencies, including on services such as Redis.

Runs as a HTTP/2 and HTTP/1.1 server backend, with straightforward clients
currently implemented in Node.js and Ruby, and many more planned.

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
* Very high job throughput
* An insightful [`zizq top`](#viewing-live-queue-activity) command

## Documentation

Read the full documentation at [zizq.io/docs](https://zizq.io/docs).

## Getting started

Download [a release](https://github.com/zizq-labs/zizq/releases) compatible
with your system and unzip it. You should put the executable somewhere on your
`PATH` but you can also just run it from the current directory.

### Starting the server

Zizq is a single binary organised into subcommands. The `serve` subcommand
starts the server. This is the default when no other subcommand is specified.

```shell
$ ./zizq serve
Zizq 0.1.0
Listening on 127.0.0.1:8901 (admin)
Listening on 127.0.0.1:7890 (primary)
```

If you have a [pro license](https://zizq.io/pricing), specify the license key
with `--license-key @/path/to/license.jwt`.

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

Zizq provides official client libraries. The goal is to provide clients for a
wide range of languages. We have started with Node.js and Ruby.

* [Official Ruby Client](https://github.com/zizq-labs/zizq-ruby)
* [Official Node.js Client](https://github.com/zizq-labs/zizq-node)

Want a client for Zizq in a language not currently supported?
[Reach out to express interest](mailto:chris@zizq.io).

### Talking to the server

With the server up and running we can make some requests to enqueue and perform
some jobs. You would usually use a [client library](#client-libraries) to do
this, but the Zizq API is easy to use directly over HTTP too. Examples here use
[HTTPie](https://httpie.io/) for simplicity.

First we can check what version of the software is running on the server.

```shell
$ http GET 127.0.0.1:7890/version
HTTP/1.1 200 OK
content-length: 19
content-type: application/json
date: Thu, 12 Mar 2026 03:46:14 GMT

{
    "version": "0.1.0"
}
```

Read the [Zizq API Reference](https://zizq.io/docs/api) for full details on the
Zizq API.

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
