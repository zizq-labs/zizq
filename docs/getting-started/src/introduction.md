# Introduction

Zizq is a simple, zero dependency, single binary job queue system that is both
fast and persistent. It uses a client-server architecture and is designed to
work aross any number of programming languages.

This document describes the purpose of a background job queue like Zizq and a
number of use cases where it helps applications run faster, more robustly and
with simplified approaches to some common situations.

> [!TIP]
> Already familiar with the concept? Jump straight to the
> [Quick Start](./quick-start.md) guide.

## What is a job queue?

A job queue is used to move work out of your main application flow and process
it in the background.

Instead of doing everything synchronously (e.g. during an HTTP request), you
enqueue a job and let a _worker_ process it separately.

This is useful when:

* The work is slow (sending emails, processing videos, generating reports)
* The work can fail and needs retries
* The work should happen at a later time (scheduling)
* The work should be processed reliably, even under load

-----------------------------------------

## Why do applications use Zizq?

### 1. Offload slow work

Instead of blocking a request while doing something expensive:

* Sending emails
* Resizing images
* Processing video
* Calling external APIs

You enqueue a job and return immediately.

#### Scenario

Your application allows users to upload videos, which need to be resized and
converted to a standard format. Video processing is slow and CPU intensive, so
your application immediately offloads the work to a background job and let's
the user know the video is currently being processed. This processing may
happen on a dedicated server better suited to video processing than the server
on which your HTTP listener runs.

**Result:** Faster response times and a better user experience.

### 2. Handle retries and failures

Background jobs often fail due to:

* network issues
* rate limits
* temporary outages

Zizq lets you configure retry and backoff policies globally or per job.

#### Scenario

Your application processes payment webhooks by offloading to a job queue. This
job needs to make a series of API calls to other services in order to fulfil an
order, and one of those services is currently unavailable. The job throws an
error and fails, but Zizq retries the job again repeatedly with exponential
backoff until the problem service becomes available and the order is fulfilled
with minimal customer impact.

**Result:** Your application is more resilient to failure.

### 3. Schedule work

Not everything should happen immediately, sometimes you need work to happen at
a later time.

Examples:

* Send a reminder email tomorrow
* Run a cleanup task every night
* Delay retries for a failed operation

Zizq supports scheduling jobs for a future time.

#### Scenario

Your application sends push notifications to users when certain events occur.
Because you are a good citizen, you allow users to opt out of receiving
notifications when they are likely asleep. You enqueue a job to send the push
notification, and when it is overnight for the recipient, you specify a time in
the morning when that notification should be sent. Zizq accepts the job but
defers processing it until that time.

### 4. Control priority

Not all jobs are equally urgent.

Zizq supports granular priorities, so you can ensure important work gets
processed first.

#### Scenario

You are making a significant database schema change as part of building a new
feature, and you need to backfill millions of records using a scripted
approach. You enqueue jobs to process this backfill work via Zizq. Your queues
are relatively quiet overnight, so your application spends most of its time
processing the backfill. As business hours arrive, customers start making
purchases which are fulfilled via the same job queue. Your application makes
sure that the backfill jobs have been assigned a lower priority than the
order fulfilment jobs, so new orders continue to be fulfilled immediately
despite there being a large volume of work on the queue.

**Result:** Your application is able to absorb large volumes of work without
impacting users.

### 5. Organise work with queues

Zizq supports unlimited named queues, created on demand.

Example queues:

* `emails`
* `payments`
* `image-processing`

This allows your application to, isolate workloads across specialized
environments, scale workers independently and apply different processing
strategies to different queues.

#### Scenario

Sending emails requires access to port 465 for SMTP. Your infrastructure
prevents access to port 465, but you have a dedicated pool of servers able to
access the port. You run a Zizq worker process listening to the `emails` queue
in this environment. Jobs running on this queue are able to send emails via
port 465, which jobs running on other queues (on other workers elsewhere in
your infrastructure) cannot. Your application sends emails reliably by
enqueueing jobs to the `emails` queue. As your application grows and demand
increases, you need to send more emails. You scale up the number of workers
listening to the `emails` queue to meet that demand.

**Result:** Your application is more secure and has clearly defined security
perimiter boundaries, and parts of your application can scale indepdendently of
others.

### 6. Prevent duplicate work

> [!NOTE]
> This feature requires a [Pro license](/pricing).

Sometimes the same job gets enqueued multiple times.

Zizq supports unique jobs, allowing you to deduplicate jobs at enqueue time.

Examples:
* Avoid sending the same email twice
* Prevent duplicate report generation
* Ensure idempotent background processing

#### Scenario

Your application receives webhooks for successful payments. It also handles
successful payments via a redirect to a URL in the browser. In both cases
processing is handled via a background job. Your application specifies a unique
key for that job and Zizq safely detects the duplicate for whichever is
enqueued second. Your application does not need to worry about that duplicate.

**Result:** Your application is better prepared to handle race conditions
idempotently.

### 7. Inspect and manage jobs

Zizq provides APIs to inspect, filter, and manage jobs in real time. This can
be done for individual jobs, or in bulk operations.

You can:

* search for jobs, including with `jq` filters
* retry or delete them
* move them between queues
* change priority

#### Scenario

Your application enqueued jobs to process a large backfill, but those jobs were
not assigned an appropriate priority and are impacting other jobs that should
be processed earlier. You perform a bulk update on the backfill jobs to lower
their priority and allow other jobs to be processed first.

**Result:** Your application has visibility and control of what your queues are
doing.

### 8. Retain job history

Zizq supports configurable retention policies for completed and failed jobs.

Why this matters:

- debugging
- auditing
- visibility into system behaviour

You can choose how long jobs are kept before being cleaned up.

#### Scenario

Your application does content moderation via the job queue. It is important
that records are kept for a period of time after moderation occurs so that
there is evidence of that work being done. Jobs for content moderation are
enqueued with a retention policy enforcing that completed jobs are kept in the
system for 14 days.

-----------------------------------------

## How Zizq fits into your stack

Zizq is designed to be simple to run and easy to integrate.

### Single binary

Zizq is fast and runs as a single native binary with no external dependencies.

Use case:

* Run locally without setup
* Deploy without managing Redis, Postgres/MySQL, or other infrastructure

### Language-agnostic HTTP API

> [!NOTE]
> While not needed, [Official Client Libraries](/docs/clients) exist for some
> common languages.

Zizq exposes a simple HTTP/1.1 and HTTP/2 API which allows it to slot
seamlessly into pretty much any environment.

Use case:

* Integrate with any language
* No tight coupling to one particular framework

### Cross-language processing

Jobs can be enqueued in one language and processed in another.

Examples:

- Enqueue from a Node.js API, process in Ruby
- Enqueue from a Python service, process in Go

This allows specialized jobs to be processed in specialized environments, and
supports large and complex polyglot applications and microservice
architectures.

### Client libraries

Zizq provides [client libraries](/docs/clients) (starting with
[Ruby](/docs/clients/ruby/) and [Node.js](/docs/clients/node/)) to simplify
integration.

More clients for other languages will be added over time and based on demand,
but the HTTP API is always available.

----------------------------------------

## Zizq end-to-end

A typical flow involving Zizq looks something like:

1. Your application receives a request
2. It enqueues a job via a client library or the Zizq API
3. A worker picks up the job from the appropriate queue
4. The job is processed, retried if necessary, and eventually completed

All without blocking your main application.

-----------------------------------------

## Summary

Zizq helps you:

* Move work out of your request cycle
* Handle retries and failures reliably
* Schedule and prioritise jobs
* Manage and inspect work in real time
* Coordinate work across your whole stack
* Run everything with minimal setup

It is designed to be straightforward, simplify common scenarios and to scale
with your application as demand grows.
