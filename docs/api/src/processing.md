# Taking & Processing Jobs

Jobs are processed by workers by _taking_ them from the queue, doing work
specific to your application and then notifying the server of success (ack) or
failure (nack).

Workers [_stream_ jobs](#get-jobs-take) from specific queues, or from all
queues. This endpoint never closes its connection so the worker can continue
taking jobs indefinitely. By default workers receive at most one job at a time
and will not receive any more jobs until the server receives a success or
failure for that job. Workers can specify a `prefetch` limit to request more
than one job at a time, e.g. to increase throughput, or because they dispatch
to multiple threads to process more than one job concurrently.

When jobs fail Zizq applies a backoff policy, as configured on the server
and/or configured on that specific job. After the configured `retry_limit` is
reached, jobs move to the `dead` set and are optionally retained for a period
of time based on the configured retention policy.

When jobs complete successfully, they may be retained for a period of time
based on the configured retention policy.

## `GET /jobs/take` { #get-jobs-take }

> [!NOTE]
> This is a streaming endpoint and is available in both `application/x-ndjson`
> and `application/vnd.zizq.msgpack-stream` formats.

Opens a persistent streaming connection that receives jobs for specified queues
or all queues in realtime as new jobs become ready for processing.

> [!NOTE]
> Blank _heartbeat_ messages are sent over the stream at periodic intervals so
> that the server can detect disconnects early. The client should consume but
> ignore these messages.

Take jobs from one or more queues. This is a **streaming endpoint** — the
connection stays open and jobs are delivered as newline-delimited JSON as they
become available.

> [!CAUTION]
> Any `in_flight` jobs are automatically returned to the queue (the `ready`
> status) whenever the connection is closed. This is correct and robust, so
> other workers may receive those in-flight jobs in the case of interruption,
> however if the reason for disconnection is part of a coordinated worker
> shutdown process, make sure to acknowledge or fail in-flight jobs before
> closing the stream.

### Parameters { #get-jobs-take-parameters }

<table>
    <thead>
        <tr>
            <th>Field</th>
            <th>Description</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td>
                <div><code>queue</code> <em>query</em></div>
                <div><pre>string</pre></div>
            </td>
            <td>
                Optional comma-separated list of queue names from which to take
                jobs. Defaults to <em>all queues</em>.
            </td>
        </tr>
        <tr>
            <td>
                <div><code>prefetch</code> <em>query</em></div>
                <div><pre>int32</pre></div>
            </td>
            <td>
                Optional number of jobs to prefetch at once without reporting
                success or failure. The default is 1. Workers that process
                multiple jobs concurrently should increase this accordingly.
            </td>
        </tr>
    </tbody>
</table>

### Responses { #get-jobs-take-response }

#### `200` OK

Streaming list of jobs. See the [Content Type](./introduction.md#content-type)
section of the introduction for details on the streaming content types. Each
job has the following structure.

> [!NOTE]
> If the connection closes prematurely, any jobs that were in-flight for this
> worker are automatically returned to the <code>ready</code> status so other
> workers can take the job.

{{#include ./job-response-with-payload.md}}

## `POST /jobs/{id}/success` { #post-jobs-success }

> [!NOTE]
> This endpoint is available in both `application/json` and
`application/msgpack` formats.

Notify the backend that an `in_flight` job has completed successfully (ack).

> [!TIP]
> If your client supports HTTP/2, you should use multiplexing to send multiple
> acknowledgements over the same stream without waiting for the response of
> each acknowledgment synchronously.
>
> If your client only supports HTTP/1.1, you should use a keep-alive connection
> so all acknowledgements share the same connection.

### Parameters { #post-jobs-success-parameters }

<table>
    <thead>
        <tr>
            <th>Field</th>
            <th>Description</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td>
                <div><code>id</code> <em>path</em></div>
                <div><pre>string</pre></div>
            </td>
            <td>
                The ID of the job that is currently <code>in_flight</code> and
                has completed successfully.
            </td>
        </tr>
    </tbody>
</table>

### Responses { #post-jobs-success-response }

#### `204` No Content

Acknowledgement successfully received.

#### `404` Not Found

The job does not exist or is no longer in-flight.

> [!NOTE]
> Clients can generally ignore this error as there is action to be taken as a
> result.

{{#include ./error-response.md}}

## `POST /jobs/success` { #post-jobs-success-bulk }

> [!NOTE]
> This endpoint is available in both `application/json` and
`application/msgpack` formats.

Notify the backend that multiple `in_flight` jobs have completed successfully
(bulk ack).

> [!TIP]
> If your client supports HTTP/2, you should use multiplexing to send multiple
> batches of acknowledgements over the same stream without waiting for the
> response of each bulk acknowledgment synchronously.
>
> If your client only supports HTTP/1.1, you should use a keep-alive connection
> so all acknowledgements share the same connection.

### Request Body { #post-jobs-success-bulk-body }

<table>
    <thead>
        <tr>
            <th>Field</th>
            <th>Description</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td>
                <div><code>ids</code> <em>required</em></div>
                <div><pre>array</pre></div>
            </td>
            <td>
                The IDs of the jobs that are currently <code>in_flight</code>
                and have completed successfully.
            </td>
        </tr>
    </tbody>
</table>

### Responses { #post-jobs-success-bulk-response }

#### `204` No Content

Acknowledgement successfully received.

#### `422` Unprocessible Entity

Returned when the operation was partially (or completely) unsuccessfull due to
the presence of job IDs that do not exist or are no longer in-flight. Only the
invalid IDs are not processed. All other IDs are acknowledged successfully.

> [!NOTE]
> Clients can generally ignore this error as there is action to be taken as a
> result.

<table>
    <thead>
        <tr>
            <th>Field</th>
            <th>Description</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td>
                <div><code>not_found</code> <em>required</em></div>
                <div><pre>array</pre></div>
            </td>
            <td>Array of input job IDs that were not valid in-flight IDs</td>
        </tr>
    </tbody>
</table>

## `POST /jobs/{id}/failure` { #post-jobs-failure }

> [!NOTE]
> This endpoint is available in both `application/json` and
`application/msgpack` formats.

Notify the backend that an `in_flight` job has failed (nack). Zizq may retry
this job according to the backoff policy.

### Parameters { #post-jobs-failure-parameters }

<table>
    <thead>
        <tr>
            <th>Field</th>
            <th>Description</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td>
                <div><code>id</code> <em>path</em></div>
                <div><pre>string</pre></div>
            </td>
            <td>
                The ID of the job that is currently <code>in_flight</code> and
                has failed.
            </td>
        </tr>
    </tbody>
</table>

### Request Body { #post-jobs-failure-body }

<table>
    <thead>
        <tr>
            <th>Field</th>
            <th>Description</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td>
                <div><code>message</code> <em>required</em></div>
                <div><pre>string</pre></div>
            </td>
            <td>
                Error message to be recorded as the reason for this failure.
            </td>
        </tr>
        <tr>
            <td>
                <div><code>error_type</code></div>
                <div><pre>string</pre></div>
            </td>
            <td>
                Optional error type (e.g. exception class) to be recorded with
                the error.
            </td>
        </tr>
        <tr>
            <td>
                <div><code>backtrace</code></div>
                <div><pre>string</pre></div>
            </td>
            <td>
                Optional full backtrace to be recorded with the error.
            </td>
        </tr>
        <tr>
            <td>
                <div><code>retry_at</code></div>
                <div><pre>int64</pre></div>
            </td>
            <td>
                Optional timestamp specifying that this job should be retried
                at the specified time. This overrides any configured backoff
                policy.
            </td>
        </tr>
        <tr>
            <td>
                <div><code>kill</code></div>
                <div><pre>boolean</pre></div>
            </td>
            <td>
                When set to <code>true</code>, overrides the backoff policy and
                marks the job as <code>dead</code> immediately. Setting this
                field to <code>false</code> <em>does not</em> prevent
                the server from marking this job dead if it has exceeded its
                retry limit.
            </td>
        </tr>
    </tbody>
</table>

### Responses { #post-jobs-failure-response }

#### `200` OK

Error details successfully received.

{{#include ./job-response-without-payload.md}}

#### `404` Not Found

The job does not exist or is no longer in-flight.

> [!NOTE]
> Clients can generally ignore this error as there is action to be taken as a
> result.

<table>
    <thead>
        <tr>
            <th>Field</th>
            <th>Description</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td>
                <div><code>error</code> <em>required</em></div>
                <div><pre>string</pre></div>
            </td>
            <td>A description of the error.</td>
        </tr>
    </tbody>
</table>

## Examples

### Streaming jobs from all queues

```shell
http --stream GET http://127.0.0.1:7890/jobs/take
```

```http
HTTP/1.1 200 OK
content-type: application/x-ndjson
date: Sat, 14 Mar 2026 04:59:48 GMT
transfer-encoding: chunked

{
    "attempts": 0,
    "dequeued_at": 1773464388204,
    "id": "03fr82s077azjmurys29qjch4",
    "payload": {
        "greet": "World On Queue #1"
    },
    "priority": 200,
    "queue": "example_1",
    "ready_at": 1773464269527,
    "status": "in_flight",
    "type": "hello_world"
}
```

### Streaming jobs from specified queues

```shell
http --stream GET "http://127.0.0.1:7890/jobs/take?queue=example_5,example_7"
```

```http
HTTP/1.1 200 OK
content-type: application/x-ndjson
date: Sat, 14 Mar 2026 05:01:44 GMT
transfer-encoding: chunked

{
    "attempts": 0,
    "dequeued_at": 1773464504362,
    "id": "03fr82s4q2jktvg5p1acpeqfe",
    "payload": {
        "greet": "World On Queue #5"
    },
    "priority": 200,
    "queue": "example_5",
    "ready_at": 1773464270599,
    "status": "in_flight",
    "type": "hello_world"
}
```

### Streaming jobs with prefetching

By default `prefetch=1` so only one job is received before each
acknowledgement. Specifying a higher prefetch value allows the worker to take
more jobs at once.

```shell
http --stream GET "http://127.0.0.1:7890/jobs/take?prefetch=3"
```

```http
HTTP/1.1 200 OK
content-type: application/x-ndjson
date: Sat, 14 Mar 2026 05:04:41 GMT
transfer-encoding: chunked

{
    "attempts": 0,
    "dequeued_at": 1773464681299,
    "id": "03fr82s077azjmurys29qjch4",
    "payload": {
        "greet": "World On Queue #1"
    },
    "priority": 200,
    "queue": "example_1",
    "ready_at": 1773464269527,
    "status": "in_flight",
    "type": "hello_world"
}

{
    "attempts": 0,
    "dequeued_at": 1773464681299,
    "id": "03fr82s1c7s2arw4bxboy1ibe",
    "payload": {
        "greet": "World On Queue #2"
    },
    "priority": 200,
    "queue": "example_2",
    "ready_at": 1773464269797,
    "status": "in_flight",
    "type": "hello_world"
}

{
    "attempts": 0,
    "dequeued_at": 1773464681299,
    "id": "03fr82s2honqa7zobvzmwql9u",
    "payload": {
        "greet": "World On Queue #3"
    },
    "priority": 200,
    "queue": "example_3",
    "ready_at": 1773464270070,
    "status": "in_flight",
    "type": "hello_world"
}
```

### Reporting job success (ack)

```shell
http POST http://127.0.0.1:7890/jobs/03fr82s1c7s2arw4bxboy1ibe/success
```

```http
HTTP/1.1 204 No Content
date: Sat, 14 Mar 2026 05:07:08 GMT
```

### Reporting bulk job success (bulk ack)

```shell
http POST http://127.0.0.1:7890/jobs/success <<'JSON'
{
    "ids": [
        "03fr82s1c7s2arw4bxboy1ibe",
        "03fr82s2honqa7zobvzmwql9u",
        "03fr82s3lxshnj4znseuvlaub"
    ]
}
JSON
```

```http
HTTP/1.1 204 No Content
date: Sat, 14 Mar 2026 05:10:09 GMT
```

### Reporting job failure (nack)

```shell
http POST http://127.0.0.1:7890/jobs/03fr82s4q2jktvg5p1acpeqfe/failure <<'JSON'
{
    "message": "Something went wrong",
    "error_type": "RuntimeError"
}
JSON
```

```http
HTTP/1.1 200 OK
content-length: 203
content-type: application/json
date: Sat, 14 Mar 2026 05:11:53 GMT

{
    "attempts": 1,
    "dequeued_at": 1773465062881,
    "failed_at": 1773465113506,
    "id": "03fr82s4q2jktvg5p1acpeqfe",
    "priority": 200,
    "queue": "example_5",
    "ready_at": 1773465156928,
    "status": "scheduled",
    "type": "hello_world"
}
```

### Acknowledging jobs while streaming

Your application would usually do this in code, not on the command line like
this. This example uses [HTTPie](https://httpie.io/) to pipe the JSON through
[`jq`](https://jqlang.org/) which extracts the `id` of each
job before piping it through
[`xargs`](https://man7.org/linux/man-pages/man1/xargs.1.html) to send an
acknowledgement for each job so that the stream receives the next job until no
more jobs remain. Because the server never closes the stream this pipe command
will never exit.

```shell
$ http --stream GET http://127.0.0.1:7890/jobs/take \
  | jq -r --unbuffered .id \
  | xargs -I {id} http POST http://127.0.0.1:7890/jobs/{id}/success

HTTP/1.1 204 No Content
date: Sat, 14 Mar 2026 05:53:44 GMT



HTTP/1.1 204 No Content
date: Sat, 14 Mar 2026 05:53:44 GMT



HTTP/1.1 204 No Content
date: Sat, 14 Mar 2026 05:53:45 GMT



HTTP/1.1 204 No Content
date: Sat, 14 Mar 2026 05:53:45 GMT



HTTP/1.1 204 No Content
date: Sat, 14 Mar 2026 05:53:45 GMT



HTTP/1.1 204 No Content
date: Sat, 14 Mar 2026 05:53:45 GMT



HTTP/1.1 204 No Content
date: Sat, 14 Mar 2026 05:53:46 GMT
```
