# Querying Job Data

> [!NOTE]
> These endpoints are available in both `application/json` and
> `application/msgpack` formats.

Zizq is designed with _visibility_ front of mind. A number of endpoints exist
that allow retrieving job data from the server without actually dequeueing the
jobs.

## Common Job Type { #job-type }

All endpoints return the same Job structure.

{{#include ./job-response-with-payload.md}}

## `GET /queues` { #get-queues-list }

Get the list of all queues known to the server.

> [!NOTE]
> Queues are not explicitly created in Zizq. Jobs are assigned to arbitrary
> queue names.

### Responses { #get-queues-list-response }

#### `200` OK

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
                <div><code>queues</code> <em>required</em></div>
                <div><pre>array</pre></div>
            </td>
            <td>
                Array of queue names (strings).
            </td>
        </tr>
    </tbody>
</table>

## `GET /jobs` { #get-jobs-list }

Retrieve a filtered, paginated list of all jobs. Jobs are returned in FIFO
order by default (i.e. ordered by the job ID, not necessarily prioritised).

> [!NOTE]
> Zizq uses cursor-based pagination. Pages are enumerated by following the
> links in the response data

> [!TIP]
> For more details on the query language used in the `?filter=` parameter, read
> the language specification on the
> [jaq website](https://gedenkt.at/jaq/manual/#corelang) or on
> [jq](https://jqlang.org/manual/#basic-filters).

### Parameters { #get-jobs-list-parameters }

All options are additive.

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
                <div><code>id</code> <em>query</em></div>
                <div><pre>string</pre></div>
            </td>
            <td>
                Optional comma-separated list of job IDs to retrieve.
            </td>
        </tr>
        <tr>
            <td>
                <div><code>queue</code> <em>query</em></div>
                <div><pre>string</pre></div>
            </td>
            <td>
                Optional comma-separated list of queue names for which to list
                jobs. Defaults to <em>all queues</em>.
            </td>
        </tr>
        <tr>
            <td>
                <div><code>type</code> <em>query</em></div>
                <div><pre>string</pre></div>
            </td>
            <td>
                Optional comma-separated list of job types for which to list
                jobs. Defaults to <em>all types</em>.
            </td>
        </tr>
        <tr>
            <td>
                <div><code>status</code> <em>query</em></div>
                <div><pre>string</pre></div>
            </td>
            <td>
                Optional comma-separated list of job statuses for which to list
                jobs. Defaults to <em>all statuses</em>.
            </td>
        </tr>
        <tr>
            <td>
                <div><code>filter</code> <em>query</em></div>
                <div><pre>string</pre></div>
            </td>
            <td>
                Optional <code>jq</code> expression by which to filter jobs by
                <code>payload</code>. This enables matching on the entire
                payload, or arbitrarily on a subset of the payload. Filtering
                is done via
                <a href="https://gedenkt.at/jaq/manual/#corelang">jaq</a> which
                is compatible with <code>jq</code>.
            </td>
        </tr>
        <tr>
            <td>
                <div><code>order</code> <em>query</em></div>
                <div><pre>string</pre></div>
            </td>
            <td>
                Whether to return the results ordered ascending or descending
                by ID. One of:
                <ul>
                    <li><code>asc</code></li>
                    <li><code>desc</code></li>
                </ul>
                The default order is <code>asc</code>.
            </td>
        </tr>
        <tr>
            <td>
                <div><code>limit</code> <em>query</em></div>
                <div><pre>int16</pre></div>
            </td>
            <td>
                The maximum number of jobs to include per page. Valid values
                are between 1 and 2000. The default is 50.
            </td>
        </tr>
    </tbody>
</table>

### Responses { #get-jobs-list-response }

#### `200` OK

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
                Array of <a href="#job-type">jobs</a> for a single page using
                the common Job type.
            </td>
        </tr>
        <tr>
            <td>
                <div><code>pages</code> <em>required</em></div>
                <div><pre>object</pre></div>
            </td>
            <td>
                Links for the client to navigate to previous and next pages in
                the result. Each link is an absolute path that must be appended
                to the base URL.
            </td>
        </tr>
        <tr>
            <td>
                <div><code>pages.self</code> <em>required</em></div>
                <div><pre>string</pre></div>
            </td>
            <td>
                Link to be used to retrieve the current page of jobs.
            </td>
        </tr>
        <tr>
            <td>
                <div><code>pages.next</code></div>
                <div><pre>string</pre></div>
            </td>
            <td>
                Link to be used to retrieve the next page of jobs if more
                pages exist.
            </td>
        </tr>
        <tr>
            <td>
                <div><code>pages.prev</code></div>
                <div><pre>string</pre></div>
            </td>
            <td>
                Link to be used to retrieve the previous page of jobs if
                previous pages exist.
            </td>
        </tr>
    </tbody>
</table>

#### `400` Bad Request

When given invalid input parameters.

{{#include ./error-response.md}}

## `GET /jobs/{id}` { #get-job }

Retrieve a single job given a known ID.

### Parameters { #get-job-parameters }

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
                ID of the job to retrieve.
            </td>
        </tr>
    </tbody>
</table>

### Responses { #get-job-response }

#### `200` OK

See [Common Job Type](#job-type).

#### `404` Not Found

{{#include ./error-response.md}}

## `GET /jobs/{id}/errors` { #get-job-errors-list }

Retrieve a paginated list of errors for a known job in order of attempt.

> [!NOTE]
> Zizq uses cursor-based pagination. Pages are enumerated by following the
> links in the response data

### Parameters { #get-job-errors-list-parameters }

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
                ID of the job for which to retrieve errors.
            </td>
        </tr>
    </tbody>
</table>

### Responses { #get-jobs-errors-list-response }

#### `200` OK

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
                <div><code>errors</code> <em>required</em></div>
                <div><pre>array</pre></div>
            </td>
            <td>
                Array of error objects for this page.
            </td>
        </tr>
        <tr>
            <td>
                <div><code>errors[*].attempt</code> <em>required</em></div>
                <div><pre>int32</pre></div>
            </td>
            <td>
                The attempt number of the job that failed.
            </td>
        </tr>
        <tr>
            <td>
                <div><code>errors[*].message</code> <em>required</em></div>
                <div><pre>string</pre></div>
            </td>
            <td>
                The error message sent by the client when the job failed.
            </td>
        </tr>
        <tr>
            <td>
                <div><code>errors[*].error_type</code></div>
                <div><pre>string</pre></div>
            </td>
            <td>
                The error type sent by the client when the job failed.
            </td>
        </tr>
        <tr>
            <td>
                <div><code>errors[*].backtrace</code></div>
                <div><pre>string</pre></div>
            </td>
            <td>
                The backtrace associated with the error, if specified by the
                client.
            </td>
        </tr>
        <tr>
            <td>
                <div><code>errors[*].dequeued_at</code> <em>required</em></div>
                <div><pre>int64</pre></div>
            </td>
            <td>
                The timestamp at which the failing job was dequeued by the
                worker.
            </td>
        </tr>
        <tr>
            <td>
                <div><code>errors[*].failed_at</code> <em>required</em></div>
                <div><pre>int64</pre></div>
            </td>
            <td>
                The timestamp at which the job failed.
            </td>
        </tr>
        <tr>
            <td>
                <div><code>pages</code> <em>required</em></div>
                <div><pre>object</pre></div>
            </td>
            <td>
                Links for the client to navigate to previous and next pages in
                the result. Each link is an absolute path that must be appended
                to the base URL.
            </td>
        </tr>
        <tr>
            <td>
                <div><code>pages.self</code> <em>required</em></div>
                <div><pre>string</pre></div>
            </td>
            <td>
                Link to be used to retrieve the current page of errors.
            </td>
        </tr>
        <tr>
            <td>
                <div><code>pages.next</code></div>
                <div><pre>string</pre></div>
            </td>
            <td>
                Link to be used to retrieve the next page of errors if more
                pages exist.
            </td>
        </tr>
        <tr>
            <td>
                <div><code>pages.prev</code></div>
                <div><pre>string</pre></div>
            </td>
            <td>
                Link to be used to retrieve the previous page of errors if
                previous pages exist.
            </td>
        </tr>
    </tbody>
</table>

#### `400` Bad Request

When given invalid input parameters.

{{#include ./error-response.md}}

#### `404` Not Found

When the specified job does not exist.

{{#include ./error-response.md}}

## `GET /jobs/{id}/errors/{attempt}` { #get-job-error }

Retrieve a specific error for a known job and attempt.

### Parameters { #get-job-error-parameters }

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
                ID of the job for which to retrieve errors.
            </td>
        </tr>
        <tr>
            <td>
                <div><code>attempt</code> <em>path</em></div>
                <div><pre>int32</pre></div>
            </td>
            <td>
                The attempt number of the job that failed.
            </td>
        </tr>
    </tbody>
</table>

### Responses { #get-job-error-response }

#### `200` OK

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
                <div><code>attempt</code> <em>required</em></div>
                <div><pre>int32</pre></div>
            </td>
            <td>
                The attempt number of the job that failed.
            </td>
        </tr>
        <tr>
            <td>
                <div><code>message</code> <em>required</em></div>
                <div><pre>string</pre></div>
            </td>
            <td>
                The error message sent by the client when the job failed.
            </td>
        </tr>
        <tr>
            <td>
                <div><code>error_type</code></div>
                <div><pre>string</pre></div>
            </td>
            <td>
                The error type sent by the client when the job failed.
            </td>
        </tr>
        <tr>
            <td>
                <div><code>backtrace</code></div>
                <div><pre>string</pre></div>
            </td>
            <td>
                The backtrace associated with the error, if specified by the
                client.
            </td>
        </tr>
        <tr>
            <td>
                <div><code>dequeued_at</code> <em>required</em></div>
                <div><pre>int64</pre></div>
            </td>
            <td>
                The timestamp at which the failing job was dequeued by the
                worker.
            </td>
        </tr>
        <tr>
            <td>
                <div><code>failed_at</code> <em>required</em></div>
                <div><pre>int64</pre></div>
            </td>
            <td>
                The timestamp at which the job failed.
            </td>
        </tr>
    </tbody>
</table>

#### `400` Bad Request

When given invalid input parameters.

{{#include ./error-response.md}}

#### `404` Not Found

When the specified job or error does not exist.

{{#include ./error-response.md}}

## Examples

### List all queues

```shell
http 127.0.0.1:7890/queues
```

```http
HTTP/1.1 200 OK
content-length: 22
content-type: application/json
date: Fri, 03 Apr 2026 11:07:43 GMT

{
    "queues": [
        "comms",
        "default",
        "example",
        "payments"
    ]
}
```

### List `ready` jobs on a specific queue

```shell
http GET "http://127.0.0.1:7890/jobs?queue=example&status=ready&limit=2"
```

```http
HTTP/1.1 200 OK
content-length: 584
content-type: application/json
date: Fri, 03 Apr 2026 11:00:17 GMT

{
    "jobs": [
        {
            "attempts": 0,
            "id": "03fvmaj8q5po1huy5nd4xmi5f",
            "payload": {
                "greet": "World"
            },
            "priority": 500,
            "queue": "example",
            "ready_at": 1775213710452,
            "status": "ready",
            "type": "hello_world"
        },
        {
            "attempts": 0,
            "id": "03fvmame0wyuiexbc2033jby2",
            "payload": {
                "greet": "World"
            },
            "priority": 500,
            "queue": "example",
            "ready_at": 1775213737304,
            "status": "ready",
            "type": "hello_world",
            "unique_key": "hello_world:world",
            "unique_while": "queued"
        }
    ],
    "pages": {
        "next": "/jobs?from=03fvmame0wyuiexbc2033jby2&order=asc&limit=2&status=ready&queue=example",
        "prev": null,
        "self": "/jobs?order=asc&limit=2&status=ready&queue=example"
    }
}
```

### Filter jobs by payload content

> [!NOTE]
> The following example is intentionally not correctly percent-encoded for
> readability. HTTPie handles this ok.

```shell
http GET 'http://127.0.0.1:7890/jobs?filter=.greet | startswith("Uni")'
```

```http
HTTP/1.1 200 OK
content-length: 301
content-type: application/json
date: Fri, 03 Apr 2026 11:02:51 GMT

{
    "jobs": [
        {
            "attempts": 0,
            "id": "03fvmbsuryhdkxvb6vjy4qhxp",
            "payload": {
                "greet": "Universe"
            },
            "priority": 500,
            "queue": "example",
            "ready_at": 1775214099613,
            "status": "ready",
            "type": "hello_world"
        }
    ],
    "pages": {
        "next": null,
        "prev": null,
        "self": "/jobs?order=asc&limit=50&filter=.greet%20%7C%20startswith%28%22Uni%22%29"
    }
}
```
