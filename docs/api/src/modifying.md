# Modifying Job Data

> [!NOTE]
> These endpoints are available in both `application/json` and
> `application/msgpack` formats.

Zizq is designed with _visibility_ and _control_ front of mind. A number of
endpoints exist that allow updating and deleting job data from the server.

Jobs in the `"completed"` and `"dead"` statuses are immutable and cannot be
modified, though they can be deleted.

The following fields are mutable:

* `queue`
* `priority`
* `ready_at`
* `retry_limit`
* `backoff`
* `retention`

## `DELETE /jobs/{id}` { #delete-job }

Delete a single job given a known ID.

### Parameters { #delete-job-parameters }

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
                ID of the job to delete.
            </td>
        </tr>
    </tbody>
</table>

### Responses { #delete-job-response }

#### `204` No Content

Job was successfully deleted.

#### `404` Not Found

{{#include ./error-response.md}}

## `DELETE /jobs` { #delete-jobs-bulk }

Delete jobs matching the given filters. When no filters are specified, all jobs
are deleted.

> [!TIP]
> For more details on the query language used in the `?filter=` parameter, read
> the language specification on the
> [jaq website](https://gedenkt.at/jaq/manual/#corelang) or on
> [jq](https://jqlang.org/manual/#basic-filters).

### Parameters { #delete-jobs-bulk-parameters }

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
                Optional comma-separated list of job IDs to delete.
            </td>
        </tr>
        <tr>
            <td>
                <div><code>queue</code> <em>query</em></div>
                <div><pre>string</pre></div>
            </td>
            <td>
                Optional comma-separated list of queue names to filter by.
            </td>
        </tr>
        <tr>
            <td>
                <div><code>type</code> <em>query</em></div>
                <div><pre>string</pre></div>
            </td>
            <td>
                Optional comma-separated list of job types to filter by.
            </td>
        </tr>
        <tr>
            <td>
                <div><code>status</code> <em>query</em></div>
                <div><pre>string</pre></div>
            </td>
            <td>
                Optional comma-separated list of job statuses to filter by.
            </td>
        </tr>
        <tr>
            <td>
                <div><code>filter</code> <em>query</em></div>
                <div><pre>string</pre></div>
            </td>
            <td>
                Optional <code>jq</code> expression to filter jobs by
                <code>payload</code>.
            </td>
        </tr>
    </tbody>
</table>

### Responses { #delete-jobs-bulk-response }

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
                <div><code>deleted</code> <em>required</em></div>
                <div><pre>int64</pre></div>
            </td>
            <td>
                The number of jobs that were deleted.
            </td>
        </tr>
    </tbody>
</table>

#### `400` Bad Request

When given invalid input parameters.

{{#include ./error-response.md}}

## `PATCH /jobs/{id}` { #patch-job }

Update a single job's mutable fields. Only fields included in the request body
are changed. Fields set to `null` are cleared to the server default. Fields
omitted from the request are left unchanged.

> [!NOTE]
> Jobs in a terminal state (`completed` or `dead`) cannot be patched. The
> server returns `422 Unprocessable Entity` in this case.

> [!TIP]
> Setting `ready_at` to a future timestamp on a `"ready"` job moves it to the
> `scheduled` status. Setting `ready_at` to `null` on a `scheduled` job
> makes it immediately `ready`.

### Parameters { #patch-job-parameters }

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
                ID of the job to update.
            </td>
        </tr>
    </tbody>
</table>

### Request Body { #patch-job-body }

All fields are optional. Only include the fields you wish to change.

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
                <div><code>queue</code></div>
                <div><pre>string</pre></div>
            </td>
            <td>
                Move the job to a different queue. Must not contain
                any of the follow reserved characters: <code>,</code>,
                <code>*</code>, <code>?</code>, <code>[</code>, <code>]</code>,
                <code>{</code>, <code>}</code>, <code>\</code>.
                When the key is present, cannot be <code>null</code>.
            </td>
        </tr>
        <tr>
            <td>
                <div><code>priority</code></div>
                <div><pre>int16</pre></div>
            </td>
            <td>
                Change the job's priority. Lower numbers are higher priority.
                When the key is present, cannot be <code>null</code>.
            </td>
        </tr>
        <tr>
            <td>
                <div><code>ready_at</code></div>
                <div><pre>int64 | null</pre></div>
            </td>
            <td>
                Change when the job becomes ready (milliseconds since epoch).
                Setting to <code>null</code> makes a scheduled job immediately
                ready.
            </td>
        </tr>
        <tr>
            <td>
                <div><code>retry_limit</code></div>
                <div><pre>int32 | null</pre></div>
            </td>
            <td>
                Override the retry limit. Setting to <code>null</code> clears
                back to the server default.
            </td>
        </tr>
        <tr>
            <td>
                <div><code>backoff</code></div>
                <div><pre>object | null</pre></div>
            </td>
            <td>
                Override the backoff configuration. Setting to <code>null</code>
                clears back to the server default. When provided, all three
                sub-fields (<code>exponent</code>, <code>base_ms</code>,
                <code>jitter_ms</code>) are required.
            </td>
        </tr>
        <tr>
            <td>
                <div><code>retention</code></div>
                <div><pre>object | null</pre></div>
            </td>
            <td>
                Override the retention configuration. Setting to
                <code>null</code> clears back to the server default. When
                provided as an object, individual sub-fields are
                merge-patched &mdash; omitted sub-fields are left unchanged,
                sub-fields set to <code>null</code> are cleared.
            </td>
        </tr>
    </tbody>
</table>

### Responses { #patch-job-response }

#### `200` OK

Returns the updated job without the payload.

{{#include ./job-response-without-payload.md}}

#### `404` Not Found

{{#include ./error-response.md}}

#### `422` Unprocessable Entity

When the job is in a terminal state or invalid values are provided.

{{#include ./error-response.md}}

## `PATCH /jobs` { #patch-jobs-bulk }

Update all jobs matching the given filters. The request body specifies the
fields to change (same as [PATCH /jobs/{id}](#patch-job)). The query
parameters specify which jobs to update (same filters as
[DELETE /jobs](#delete-jobs-bulk)).

Jobs in a terminal state (`"completed"` or `"dead"`) are silently skipped unless
explicitly requested via `?status=`, in which case the server returns
`422 Unprocessable Entity`.

> [!TIP]
> For more details on the query language used in the `?filter=` parameter, read
> the language specification on the
> [jaq website](https://gedenkt.at/jaq/manual/#corelang) or on
> [jq](https://jqlang.org/manual/#basic-filters).

### Parameters { #patch-jobs-bulk-parameters }

All filter options are additive.

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
                Optional comma-separated list of job IDs to update.
            </td>
        </tr>
        <tr>
            <td>
                <div><code>queue</code> <em>query</em></div>
                <div><pre>string</pre></div>
            </td>
            <td>
                Optional comma-separated list of queue names to filter by.
            </td>
        </tr>
        <tr>
            <td>
                <div><code>type</code> <em>query</em></div>
                <div><pre>string</pre></div>
            </td>
            <td>
                Optional comma-separated list of job types to filter by.
            </td>
        </tr>
        <tr>
            <td>
                <div><code>status</code> <em>query</em></div>
                <div><pre>string</pre></div>
            </td>
            <td>
                Optional comma-separated list of job statuses to filter by.
                Must not include <code>completed</code> or <code>dead</code>.
            </td>
        </tr>
        <tr>
            <td>
                <div><code>filter</code> <em>query</em></div>
                <div><pre>string</pre></div>
            </td>
            <td>
                Optional <code>jq</code> expression to filter jobs by
                <code>payload</code>.
            </td>
        </tr>
    </tbody>
</table>

### Request Body { #patch-jobs-bulk-body }

Same as [PATCH /jobs/{id} Request Body](#patch-job-body).

### Responses { #patch-jobs-bulk-response }

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
                <div><code>patched</code> <em>required</em></div>
                <div><pre>int64</pre></div>
            </td>
            <td>
                The number of jobs that were updated.
            </td>
        </tr>
    </tbody>
</table>

#### `400` Bad Request

When given invalid input parameters.

{{#include ./error-response.md}}

#### `422` Unprocessable Entity

When the status filter includes terminal statuses, or invalid field values are
provided.

{{#include ./error-response.md}}

## Examples

### Update a job's queue and priority

```shell
http PATCH 127.0.0.1:7890/jobs/03fvmbsuryhdkxvb6vjy4qhxp <<'JSON'
{"queue": "other", "priority": 100}
JSON
```

```http
HTTP/1.1 200 OK
content-length: 141
content-type: application/json
date: Fri, 03 Apr 2026 11:10:58 GMT

{
    "attempts": 0,
    "id": "03fvmbsuryhdkxvb6vjy4qhxp",
    "priority": 100,
    "queue": "other",
    "ready_at": 1775214099613,
    "status": "ready",
    "type": "hello_world"
}
```

### Move a job from `ready` to `scheduled`

```shell
http PATCH 127.0.0.1:7890/jobs/03fvmbsuryhdkxvb6vjy4qhxp <<'JSON'
{"ready_at": 1775217412000}
JSON
```

```http
HTTP/1.1 200 OK
content-length: 145
content-type: application/json
date: Fri, 03 Apr 2026 11:13:10 GMT

{
    "attempts": 0,
    "id": "03fvmbsuryhdkxvb6vjy4qhxp",
    "priority": 100,
    "queue": "other",
    "ready_at": 1775217412000,
    "status": "scheduled",
    "type": "hello_world"
}
```

### Clear a field back to server default

Setting an optional field to `null` resets it to the server's default value.

```shell
http PATCH 127.0.0.1:7890/jobs/03fvmbsuryhdkxvb6vjy4qhxp <<'JSON'
{"retry_limit": null}
JSON
```

```http
HTTP/1.1 200 OK
content-length: 145
content-type: application/json
date: Fri, 03 Apr 2026 11:15:20 GMT

{
    "attempts": 0,
    "id": "03fvmbsuryhdkxvb6vjy4qhxp",
    "priority": 100,
    "queue": "other",
    "ready_at": 1775217412000,
    "status": "scheduled",
    "type": "hello_world"
}
```

### Move all jobs from one queue to another

```shell
http PATCH http://127.0.0.1:7890/jobs?queue=example <<'JSON'
{"queue": "other"}
JSON
```

```http
HTTP/1.1 200 OK
content-length: 13
content-type: application/json
date: Fri, 03 Apr 2026 11:17:09 GMT

{
    "patched": 4
}
```

### Remove all scheduled jobs on a queue

```shell
http DELETE "http://127.0.0.1:7890/jobs?queue=example&status=scheduled"
```

```http
HTTP/1.1 200 OK
content-length: 13
content-type: application/json
date: Fri, 03 Apr 2026 11:18:36 GMT

{
    "deleted": 2
}
```

### Safely delete jobs matching filters in pages

To delete jobs matching a filter in a paginated way, a two step approach is
used:

1. Query the jobs using the desired filters.
2. Delete the jobs using filters *and* the IDs on each page.

It's important to retain the filters to handle race conditions if the jobs are
modified between fetching the page and executing the delete.

```shell
http GET 'http://127.0.0.1:7890/jobs?filter=.greet | startswith("Wo")&limit=2'
```

```http
HTTP/1.1 200 OK
content-length: 624
content-type: application/json
date: Fri, 03 Apr 2026 11:22:34 GMT

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
        "next": "/jobs?from=03fvmame0wyuiexbc2033jby2&order=asc&limit=2&filter=.greet%20%7C%20startswith%28%22Wo%22%29",
        "prev": null,
        "self": "/jobs?order=asc&limit=2&filter=.greet%20%7C%20startswith%28%22Wo%22%29"
    }
}
```

```shell
http DELETE 'http://127.0.0.1:7890/jobs?filter=.greet | startswith("Wo")&id=03fvmaj8q5po1huy5nd4xmi5f,03fvmame0wyuiexbc2033jby2'
```

```http
HTTP/1.1 200 OK
content-length: 13
content-type: application/json
date: Fri, 03 Apr 2026 11:23:52 GMT

{
    "deleted": 2
}
```
