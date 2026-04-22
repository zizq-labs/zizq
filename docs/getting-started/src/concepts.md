# Zizq Concepts

## Zizq at a glance

Zizq is a simple, single binary, zero dependency job queue system that is
designed to be simple to run and easy to integrate.

* Single binary with subcommands (e.g. `serve`, `top`)
* Uses a clean HTTP API for all interactions
* Language-agnostic — works in any stack
* Source-available — built in the open on GitHub. Core features are free, with
  optional Pro features using a license

> [!NOTE]
> Licenses are applied to the server only. Clients do not require a license.
> [Official client libraries](/docs/clients) are open source and MIT licensed.

## Jobs

A job is a unit of work to be processed. Job have two important attributes used
by your application:

* `type` — a string like `"send_email"`
* `payload` — arbitrary JSON data

Jobs are enqueued by your application and processed by _workers_.

## Queues & Workers

- A queue is a named stream of jobs, created automatically when jobs are
  enqueued to it
- A worker pulls jobs from queues and executes them

You can run multiple workers and use multiple queues to organise work.

## Priorities

Jobs have a granular numeric priority (`0`–`65536`). Zero is the highest
priority. The default is `32768` (the midpoint).

Higher priority jobs are always processed first. Jobs with the same priority
are processed in FIFO (first-in first-out) order.

## Job Lifecycle

Jobs are assigned a `status`. All jobs start life in either `scheduled` or
`ready` and move through the given lifecycle in which they terminate in
`completed` or `dead`:

```text
scheduled ---> ready ---> in_flight ---> completed
 ^                               |  ---> dead
 |                               |
 +-------------------------------+
                           (retry)
```

* `scheduled` — will move to `ready` at a future time
* `ready` — available to process now
* `in_flight` — currently being handled by a worker
* `completed` — finished successfully
* `dead` — exceeded its retry limit, will no longer retry

## Retries & Scheduling

* Jobs can be retried automatically with configurable backoff
* Jobs can be scheduled to run at a future time

## Other capabilities

* Unique jobs — prevent duplicates at enqueue time
* Retention policies — keep completed/failed jobs for a configured period
* Job management APIs — inspect, update, move, or delete jobs
