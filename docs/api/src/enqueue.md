# Enqueuing Jobs

> [!NOTE]
> These endpoints are available in both `application/json` and
> `application/msgpack` formats.

Jobs are pushed to the queue by your application so that workers can process
them asynchronously. Jobs can be scheduled for a future date by specifying a
`ready_at` timestamp in the future, or by default jobs will be ready for
processing immediately.

There are two endpoints for enqueueing jobs: [single enqueue](#post-jobs),
or [bulk enqueue](#post-jobs-bulk). Both take jobs inputs in the exact same
shape. The server responds with the job(s) and their generated IDs.

## Common Job Parameters { #job-parameters }

Both endpoints accept and return the same structure, except the bulk enqueue
endpoint wraps an array of `{"jobs": [...]}`.

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
                <div><code>queue</code> <em>required</em></div>
                <div><pre>string</pre></div>
            </td>
            <td>
                Arbitrary queue name to which the job is assigned. Must not
                contain <code>","</code> commas.
            </td>
        </tr>
        <tr>
            <td>
                <div><code>type</code> <em>required</em></div>
                <div><pre>string</pre></div>
            </td>
            <td>
                Job type known to your application. Must not contain
                <code>","</code> commas.
            </td>
        </tr>
        <tr>
            <td>
                <div><code>ready_at</code></div>
                <div><pre>int64</pre></div>
            </td>
            <td>
                If the client wishes to schedule this job for a future time, this
                field is set to the timestamp at which the job is ready for
                processing.
            </td>
        </tr>
        <tr>
            <td>
                <div><code>payload</code> <em>required</em></div>
                <div><pre>object</pre></div>
            </td>
            <td>
                Any JSON-serializable type to be processed by your application
            </td>
        </tr>
        <tr>
            <td>
                <div><code>unique_key</code></div>
                <div><pre>string</pre></div>
            </td>
            <td>
                Optional unique key for this job, which is used to protect
                against duplicate job enqueues. This is paired with the
                optional <code>unique_while</code> field which defines the
                scope within which the job is considered unique. Uniqueness is
                status-bound, not time-bound. There is no arbitrary expiry.
                Conflicting enqueues <em>do not</em> produce errors, but
                instead behave idempotently. A success response is returned
                with details of the existing matching job, and its
                <code>duplicate</code> field set to <code>true</code>. This key
                is intentionally global across all queues and job types.
                Clients should prefix it as necessary.
                <strong>Requires a <em>pro</em> license</strong>.
            </td>
        </tr>
        <tr>
            <td>
                <div><code>unique_while</code></div>
                <div><pre>string</pre></div>
            </td>
            <td>
                When the job has a unique key, specifies the scope within which
                that job is considered unique. One of:
                <dl>
                    <dt><code>queued</code></dt>
                    <dd>
                        The job will not be enqueued if another exists in the
                        <code>scheduled</code> or <code>ready</code> statuses.
                    </dd>
                    <dt><code>active</code></dt>
                    <dd>
                        The job will not be enqueued if another exists in the
                        <code>scheduled</code>, <code>ready</code> or
                        <code>in_flight</code> statuses.
                    </dd>
                    <dt><code>exists</code></dt>
                    <dd>
                        The job will not be enqueued if another exists in any
                        status (i.e. until that job is reaped, according to the
                        retention policy).
                    </dd>
                </dl>
                The default scope is <code>queued</code>.
            </td>
        </tr>
        <tr>
            <td>
                <div><code>backoff</code></div>
                <div><pre>object</pre></div>
            </td>
            <td>
                Optional backoff policy which overrides the server's default
                policy. All fields are required. Zizq computes the backoff delay as
                <code>
                    base_ms +
                    (attempts^exponent) +
                    (rand(0.0..jitter_ms)*attempts)
                </code>.
                The <code>jitter_ms</code> mitigates retry flooding when failures
                occur clustered together.
            </td>
        </tr>
        <tr>
            <td>
                <div><code>backoff.base_ms</code></div>
                <div><pre>int32</pre></div>
            </td>
            <td>
                The minimum delay in milliseconds between job retries.
            </td>
        </tr>
        <tr>
            <td>
                <div><code>backoff.exponent</code></div>
                <div><pre>float</pre></div>
            </td>
            <td>
                A multiplier applied to the number of attempts on each retry,
                used as <code>pow(attempts, exponent)</code> to produce an
                increasing delay in milliseconds.
            </td>
        </tr>
        <tr>
            <td>
                <div><code>backoff.jitter_ms</code></div>
                <div><pre>int32</pre></div>
            </td>
            <td>
                A random delay added onto each attempt. Multiplied by the total
                number of attempts, such as <code>attempts * rand(0..jitter)</code>.
                Prevents retries clutering together.
            </td>
        </tr>
        <tr>
            <td>
                <div><code>retry_limit</code></div>
                <div><pre>int32</pre></div>
            </td>
            <td>
                Overrides the severs default retry limit for this job. Once
                this limit is reached, the server marks the job <code>dead</code>.
            </td>
        </tr>
        <tr>
            <td>
                <div><code>retention</code></div>
                <div><pre>object</pre></div>
            </td>
            <td>
                Optional retention policy for <code>dead</code> and
                <code>completed</code> jobs which overrides the server's
                default policy. All fields are optional.
            </td>
        </tr>
        <tr>
            <td>
                <div><code>retention.dead_ms</code></div>
                <div><pre>int64</pre></div>
            </td>
            <td>
                The number of milliseconds for which to retain
                <code>dead</code> jobs after all retries have been exhausted.
                When not set, the server's default value (7 days) applies. When
                set to zero, jobs are purged as soon as all retries have been
                exhausted.
            </td>
        </tr>
        <tr>
            <td>
                <div><code>retention.completed_ms</code></div>
                <div><pre>int64</pre></div>
            </td>
            <td>
                The number of milliseconds for which to retain
                <code>completed</code> jobs after successful processing. When
                not set, the server's default value (zero) applies. When set to
                zero, jobs are purged immediately upon completion.
            </td>
        </tr>
    </tbody>
</table>

## Common Job Response { #job-response }

Both endpoints accept and return the same structure, except the bulk enqueue
endpoint wraps an array of `{"jobs": [...]}`.

{{#include ./job-response-without-payload.md}}

## `POST /jobs` { #post-jobs }

Enqueues a single job.

### Parameters { #post-jobs-parameters }

See [Common Job Parameters](#job-parameters).

### Responses { #post-jobs-response }

#### `200` OK

The request was processed but the specified job was a duplicate of an existing
job according to its `unique_key` and `unique_while` scope. The returned data
is that of the existing job, and the `duplicate` flag is set to `true`.

See [Common Job Response](#job-response).

#### `201` Created

The request was processed and a new job has been enqueued.

See [Common Job Response](#job-response).

#### `400` Bad Request

Returned when given invalid inputs.

{{#include ./error-response.md}}

#### `403` Forbidden

Returned when the client attempts to use pro features but the server is not
configured with a pro license.

{{#include ./error-response.md}}

## `POST /jobs/bulk` { #post-jobs-bulk }

Enqueues multiple jobs atomically.

### Parameters { #post-jobs-bulk-parameters }

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
                <div><code>jobs</code> <em>required</em></div>
                <div><pre>array</pre></div>
            </td>
            <td>
                Array of <a href="#job-parameters">jobs</a> in the same shape
                as for a single enqueue request.
            </td>
        </tr>
    </tbody>
</table>

### Responses { #post-jobs-bulk-response }

#### `200` OK

The request was processed but all the specified jobs were duplicates of
existing jobs according to their `unique_key` and `unique_while` scopes. The
returned data is that of the existing jobs, and their `duplicate` flags are set
to `true`.

See [Common Job Response](#job-response).

#### `201` Created

The request was processed and new jobs have been enqueued. Where `unique_key`
values were present, any duplicates are identified by their `duplicate` flags.

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
                <div><code>jobs</code> <em>required</em></div>
                <div><pre>array</pre></div>
            </td>
            <td>
                Array of <a href="#job-response">jobs</a> in the same shape as
                for a single enqueue response, and in the same order as the
                input request.
            </td>
        </tr>
    </tbody>
</table>

#### `400` Bad Request

Returned when given invalid inputs.

{{#include ./error-response.md}}

#### `403` Forbidden

Returned when the client attempts to use pro features but the server is not
configured with a pro license.

{{#include ./error-response.md}}

## Examples

### Enqueue a single job

```shell
http POST http://127.0.0.1:7890/jobs <<'JSON'
{
    "queue": "example",
    "priority": 500,
    "type": "hello_world",
    "payload": {"greet": "World"}
}
JSON
```

```http
HTTP/1.1 201 Created
content-length: 143
content-type: application/json
date: Fri, 13 Mar 2026 08:53:47 GMT

{
    "attempts": 0,
    "id": "03fr1jkpcsipbsckqj0y6pgr7",
    "priority": 500,
    "queue": "example",
    "ready_at": 1773392027425,
    "status": "ready",
    "type": "hello_world"
}
```

### Enqueue a scheduled Job

Jobs are explicitly scheduled by providing a `ready_at` timestamp with a future
dated value.

```shell
http POST http://127.0.0.1:7890/jobs <<'JSON'
{
    "queue": "example",
    "priority": 500,
    "type": "hello_world",
    "payload": {"greet": "Later"},
    "ready_at": 1773396035647
}
JSON
```

```http
HTTP/1.1 201 Created
content-length: 147
content-type: application/json
date: Fri, 13 Mar 2026 09:01:08 GMT

{
    "attempts": 0,
    "id": "03fr1l0cl1quc0sfe6y2711op",
    "priority": 500,
    "queue": "example",
    "ready_at": 1773396035647,
    "status": "scheduled",
    "type": "hello_world"
}
```

### Enqueue jobs with unique keys

Unique jobs required a [pro license](https://zizq.io/pricing).

```shell
http POST http://127.0.0.1:7890/jobs <<'JSON'
{
    "queue": "example",
    "priority": 500,
    "type": "hello_world",
    "unique_key": "hello_world:world",
    "payload": {"greet": "World"}
}
JSON
```

```http
HTTP/1.1 201 Created
content-length: 218
content-type: application/json
date: Mon, 23 Mar 2026 11:19:58 GMT

{
    "attempts": 0,
    "duplicate": false,
    "id": "03ft8h3ubrx53abhw1fxbora3",
    "priority": 500,
    "queue": "example",
    "ready_at": 1774264798519,
    "status": "ready",
    "type": "hello_world",
    "unique_key": "hello_world:world",
    "unique_while": "queued"
}
```

```shell
http POST http://127.0.0.1:7890/jobs <<'JSON'
{
    "queue": "example",
    "priority": 500,
    "type": "hello_world",
    "unique_key": "hello_world:world",
    "payload": {"greet": "World"}
}
JSON
```

```http
HTTP/1.1 200 OK
content-length: 217
content-type: application/json
date: Mon, 23 Mar 2026 11:20:26 GMT

{
    "attempts": 0,
    "duplicate": true,
    "id": "03ft8h3ubrx53abhw1fxbora3",
    "priority": 500,
    "queue": "example",
    "ready_at": 1774264798519,
    "status": "ready",
    "type": "hello_world",
    "unique_key": "hello_world:world",
    "unique_while": "queued"
}
```

### Bulk enqueue multiple jobs

An array of jobs is passed in the request, and the server responds with an
array containing the same number of jobs, in the same order as the input
request. This operation is atomic. If any jobs are invalid or fail to be
enqueued, no jobs are enqueued and an error response is returned.

```shell
http POST http://127.0.0.1:7890/jobs/bulk <<'JSON'
{
    "jobs": [
        {
            "queue": "example",
            "priority": 500,
            "type": "hello_world",
            "payload": {"greet": "World"}
        },
        {
            "queue": "example",
            "priority": 500,
            "type": "hello_world",
            "payload": {"greet": "Later"},
            "ready_at": 1773396035647
        }
    ]
}
JSON
```

```http
HTTP/1.1 201 Created
content-length: 302
content-type: application/json
date: Fri, 13 Mar 2026 09:07:17 GMT

{
    "jobs": [
        {
            "attempts": 0,
            "id": "03fr1m7p1mwctku2fptz1x5p4",
            "priority": 500,
            "queue": "example",
            "ready_at": 1773392837882,
            "status": "ready",
            "type": "hello_world"
        },
        {
            "attempts": 0,
            "id": "03fr1m7p1mwctku2fpx425jzr",
            "priority": 500,
            "queue": "example",
            "ready_at": 1773396035647,
            "status": "scheduled",
            "type": "hello_world"
        }
    ]
}
```

### Enqueue a job with explicit backoff policy

```shell
http POST http://127.0.0.1:7890/jobs <<'JSON'
{
    "queue": "example",
    "priority": 500,
    "type": "hello_world",
    "payload": {"greet": "World"},
    "backoff": {
        "base_ms": 1000,
        "exponent": 1.5,
        "jitter_ms": 10000
    }
}
JSON
```

```http
HTTP/1.1 201 Created
content-length: 203
content-type: application/json
date: Sat, 14 Mar 2026 03:24:16 GMT

{
    "attempts": 0,
    "backoff": {
        "base_ms": 1000,
        "exponent": 1.5,
        "jitter_ms": 10000
    },
    "id": "03fr7ki3x5kqf1epbydrfebkz",
    "priority": 500,
    "queue": "example",
    "ready_at": 1773458656424,
    "status": "ready",
    "type": "hello_world"
}
```

### Enqueue a job with explicit retention policy

```shell
http POST http://127.0.0.1:7890/jobs <<'JSON'
{
    "queue": "example",
    "priority": 500,
    "type": "hello_world",
    "payload": {"greet": "World"},
    "retention": {
        "completed_ms": 86400000,
        "dead_ms": 604800000
    }
}
JSON
```

```http
HTTP/1.1 201 Created
content-length: 201
content-type: application/json
date: Sat, 14 Mar 2026 03:26:01 GMT

{
    "attempts": 0,
    "id": "03fr7kudjeradun2wk1v3tn7b",
    "priority": 500,
    "queue": "example",
    "ready_at": 1773458761086,
    "retention": {
        "completed_ms": 86400000,
        "dead_ms": 604800000
    },
    "status": "ready",
    "type": "hello_world"
}
```
