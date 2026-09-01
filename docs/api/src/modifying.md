# Modifying Job Data

> [!NOTE]
> These endpoints are available in both `application/json` and
> `application/msgpack` formats.

Zizq is designed with _visibility_ and _control_ front of mind. A number of
endpoints exist that allow updating and deleting job data from the server.

Jobs in the `"completed"` and `"dead"` statuses are immutable and cannot be
modified, though they can be deleted. Additionally, when modifying
[budget bindings](./rate-limiting.md) jobs in the `"in_flight"` status cannot
be updated (the operation can be retried once the job is no longer in-flight).

The following fields are mutable:

* `queue`
* `priority`
* `ready_at`
* `retry_limit`
* `backoff`
* `retention`
* `budgets`

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

{{#include ./range-syntax.md}}

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
        {{#include ./query-filter-parameter-rows.md}}
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

{{#include ./range-syntax.md}}

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
        {{#include ./query-filter-parameter-rows.md}}
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

## `POST /jobs/{id}/budgets/{key}` { #post-jobs-id-budgets-key }

Bind a single job to the named budget.

### Parameters { #post-jobs-id-budgets-key-parameters }

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
                ID of the job to which the budget will be bound.
            </td>
        </tr>
        <tr>
            <td>
                <div><code>key</code> <em>path</em></div>
                <div><pre>string</pre></div>
            </td>
            <td>
                The identifier for the budget. Must be
                valid UTF-8 and must not contain any of the follow reserved
                characters: <code>,</code>, <code>*</code>, <code>?</code>,
                <code>[</code>, <code>]</code>, <code>{</code>, <code>}</code>,
                <code>\</code>.
            </td>
        </tr>
    </tbody>
</table>

### Request Body { #post-jobs-id-budgets-key-body }

{{#include ./bind-budget-parameters.md}}

### Responses { #post-jobs-id-budgets-key-response }

#### `200` OK

Returns the updated job without the payload.

{{#include ./job-response-without-payload.md}}

#### `403` Forbidden

When the server is not confiured with a Pro license.

{{#include ./error-response.md}}

#### `404` Not Found

{{#include ./error-response.md}}

#### `409` Conflict

When a budget with the named `key` is already bound to this job.

{{#include ./error-response.md}}

#### `422` Unprocessable Entity

When the job is in a terminal state, is in-flight or invalid values are
provided.

{{#include ./error-response.md}}

## `PUT /jobs/{id}/budgets/{key}` { #put-jobs-id-budgets-key }

Replace the named budget binding on a single job. Creates it if does not
already exist.

### Parameters { #put-jobs-id-budgets-key-parameters }

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
                ID of the job to which the budget will be bound.
            </td>
        </tr>
        <tr>
            <td>
                <div><code>key</code> <em>path</em></div>
                <div><pre>string</pre></div>
            </td>
            <td>
                The identifier for the budget. Must be
                valid UTF-8 and must not contain any of the follow reserved
                characters: <code>,</code>, <code>*</code>, <code>?</code>,
                <code>[</code>, <code>]</code>, <code>{</code>, <code>}</code>,
                <code>\</code>.
            </td>
        </tr>
    </tbody>
</table>

### Request Body { #put-jobs-id-budgets-key-body }

{{#include ./bind-budget-parameters.md}}

### Responses { #put-jobs-id-budgets-key-response }

#### `200` OK

Returns the updated job without the payload.

{{#include ./job-response-without-payload.md}}

#### `403` Forbidden

When the server is not confiured with a Pro license.

{{#include ./error-response.md}}

#### `404` Not Found

{{#include ./error-response.md}}

#### `409` Conflict

When a budget with the named `key` is already bound to this job.

{{#include ./error-response.md}}

#### `422` Unprocessable Entity

When the job is in a terminal state, is in-flight or invalid values are
provided.

{{#include ./error-response.md}}

## `PATCH /jobs/{id}/budgets/{key}` { #patch-jobs-id-budgets-key }

Update the named budget binding on a single job. Currently the only patchable
field is `cost`.

### Parameters { #patch-jobs-id-budgets-key-parameters }

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
                ID of the job for which the budget is to be patched.
            </td>
        </tr>
        <tr>
            <td>
                <div><code>key</code> <em>path</em></div>
                <div><pre>string</pre></div>
            </td>
            <td>
                The identifier for the budget.
            </td>
        </tr>
    </tbody>
</table>

### Request Body { #patch-jobs-id-budgets-key-body }

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
                <div><code>cost</code> <em>required</em></div>
                <div><pre>int32</pre></div>
            </td>
            <td>
                The number of tokens the job takes from the budget.
            </td>
        </tr>
    </tbody>
</table>

### Responses { #patch-jobs-id-budgets-key-response }

#### `200` OK

Returns the updated job without the payload.

{{#include ./job-response-without-payload.md}}

#### `403` Forbidden

When the server is not confiured with a Pro license.

{{#include ./error-response.md}}

#### `404` Not Found

{{#include ./error-response.md}}

#### `422` Unprocessable Entity

When the job is in a terminal state, is in-flight or invalid values are
provided.

{{#include ./error-response.md}}

## `DELETE /jobs/{id}/budgets/{key}` { #delete-jobs-id-budgets-key }

Remove the named budget binding from a single job.

### Parameters { #delete-jobs-id-budgets-key-parameters }

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
                ID of the job for which the budget is to be removed.
            </td>
        </tr>
        <tr>
            <td>
                <div><code>key</code> <em>path</em></div>
                <div><pre>string</pre></div>
            </td>
            <td>
                The identifier for the budget.
            </td>
        </tr>
    </tbody>
</table>

### Responses { #delete-jobs-id-budgets-key-response }

#### `200` OK

Returns the updated job without the payload.

{{#include ./job-response-without-payload.md}}

#### `403` Forbidden

When the server is not confiured with a Pro license.

{{#include ./error-response.md}}

#### `404` Not Found

{{#include ./error-response.md}}

#### `422` Unprocessable Entity

When the job is in a terminal state or is in-flight.

{{#include ./error-response.md}}

## `PUT /jobs/{id}/budgets` { #put-jobs-id-budgets-bulk }

Replace all budgets on the specified job.

### Parameters { #put-jobs-id-budgets-bulk }

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
                ID of the job for which to replace budgets.
            </td>
        </tr>
    </tbody>
</table>

### Request Body { #put-jobs-id-budgets-bulk-body }

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
                <div><code>budgets</code> <em>required</em></div>
                <div><pre>array</pre></div>
            </td>
            <td>
                Array of <a href="./rate-limiting.md">budget bindings</a>
                used to control concurrency and/or rate limiting of dispatched
                jobs.
                <strong>Requires a <em>pro</em> license</strong>.
            </td>
        </tr>
        <tr>
            <td>
                <div><code>budgets[*].key</code> <em>required</em></div>
                <div><pre>string</pre></div>
            </td>
            <td>
                The identifier for the budget. Must be valid UTF-8 and must
                not contain any of the follow reserved
                characters: <code>,</code>, <code>*</code>, <code>?</code>,
                <code>[</code>, <code>]</code>, <code>{</code>, <code>}</code>,
                <code>\</code>.
            </td>
        </tr>
        <tr>
            <td>
                <div><code>budgets[*].cost</code></div>
                <div><pre>int32</pre></div>
            </td>
            <td>
                The number of tokens this job takes from the budget. Defaults
                to 1.
            </td>
        </tr>
        <tr>
            <td>
                <div><code>budgets[*].create_with</code></div>
                <div><pre>object</pre></div>
            </td>
            <td>
                Specification from which to create this budget atomically
                with the job if it does not already exist. Without this, the
                budget must exist or a 422 response will be returned. Does not
                overwrite any existing budget.
            </td>
        </tr>
        <tr>
            <td>
                <div><code>budgets[*].create_with.allocation</code> <em>required</em></div>
                <div><pre>int32</pre></div>
            </td>
            <td>
                The total number of tokens available in this budget's pool for
                use by its configured strategy. No jobs can exist bound to this
                budget with a <code>cost</code> that exceeds the allocation.
            </td>
        </tr>
        <tr>
            <td>
                <div><code>budgets[*].create_with.strategy</code> <em>required</em></div>
                <div><pre>object</pre></div>
            </td>
            <td>
                Details of the specific strategy that is used to manage the
                tokens available under this budget.
            </td>
        </tr>
        <tr>
            <td>
                <div><code>budgets[*].create_with.strategy.type</code> <em>required</em></div>
                <div><pre>string</pre></div>
            </td>
            <td>
                Names the strategy used to manage the tokens within the budget.
                One of:
                <dl>
                    <dt><code>while_in_flight</code></dt>
                    <dd>
                        Concurrency control — tokens are spent from the budget
                        when jobs are dispatched to workers, and returned when
                        the job completes or fails, or the worker disconnects
                        uncleanly. For example, for an allocation of 5, at most
                        5 jobs bound to this budget can be in-flight at any
                        given time.
                    </dd>
                    <dt><code>time_based</code></dt>
                    <dd>
                        Rate limit — tokens are spent from the budget when jobs
                        are dispatched to workers and are only returned after a
                        configured period of time, regardless of the outcome of
                        the job.
                    </dd>
                </dl>
            </td>
        </tr>
        <tr>
            <td>
                <div><code>budgets[*].create_with.strategy.duration_ms</code></div>
                <div><pre>int64</pre></div>
            </td>
            <td>
                Required for <code>time_based</code> strategies. Invalid for
                <code>while_in_flight</code>. Specifies the period of time in
                milliseconds over which a <code>time_based</code> rate limit is
                measured. For example, for an allocation of 1000 and a
                <code>duration_ms</code> of 60000, the rate limit is
                <code>1000/minute</code>.
            </td>
        </tr>
        <tr>
            <td>
                <div><code>budgets[*].create_with.strategy.burst</code></div>
                <div><pre>int32</pre></div>
            </td>
            <td>
                The maximum number of tokens that may be accumulated at once
                for a <code>time_based</code> budget. Defaults to whatever the
                configured <code>allocation</code> is. So for a
                <code>1000/hour</code> rate limit, the budget would technically
                permit a short burst of 1000 jobs if no other jobs have used
                tokens from the budget for a whole hour. Setting a burst of 1
                means tokens cannot accumulate and jobs are always paced
                according to the configured rate limit. It is also possible to
                intentionally set a burst higher than the configured
                allocation, such as a burst of 2000 for a
                <code>1000/hour</code> allocation. In this case if the budget
                has been idle for 2 hours, it would permit a sudden burst of
                2000 jobs at any moment. No jobs can exist bound to this
                budget with a <code>cost</code> that exceeds the burst.
            </td>
        </tr>
    </tbody>
</table>

### Responses { #put-jobs-id-budgets-bulk-response }

#### `200` OK

Returns the updated job without the payload.

{{#include ./job-response-without-payload.md}}

#### `403` Forbidden

When the server is not confiured with a Pro license.

{{#include ./error-response.md}}

#### `404` Not Found

{{#include ./error-response.md}}

#### `422` Unprocessable Entity

When the job is in a terminal state, is in-flight or invalid values are
provided.

{{#include ./error-response.md}}

## `DELETE /jobs/{id}/budgets` { #delete-jobs-id-budgets-bulk }

Remove all budgets from the specified job.

### Parameters { #delete-jobs-id-budgets-bulk }

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
                ID of the job for which to remove budgets.
            </td>
        </tr>
    </tbody>
</table>

### Responses { #delete-jobs-id-budgets-bulk-response }

#### `200` OK

Returns the updated job without the payload.

{{#include ./job-response-without-payload.md}}

#### `403` Forbidden

When the server is not confiured with a Pro license.

{{#include ./error-response.md}}

#### `404` Not Found

{{#include ./error-response.md}}

#### `422` Unprocessable Entity

When the job is in a terminal state, is in-flight or invalid values are
provided.

{{#include ./error-response.md}}

## `POST /jobs/budgets/{key}` { #post-jobs-budgets-key }

Bind a budget to all matching jobs specified by the filter. Jobs that already
have the binding are gracefully skipped. Jobs in a terminal status are
gracefully skipped. Jobs in the `in_flight` status are _blocked_ and reported
for retry.

### Parameters { #post-jobs-budgets-key-parameters }

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
                <div><code>key</code> <em>path</em></div>
                <div><pre>string</pre></div>
            </td>
            <td>
                The identifier for the budget. Must be
                valid UTF-8 and must not contain any of the follow reserved
                characters: <code>,</code>, <code>*</code>, <code>?</code>,
                <code>[</code>, <code>]</code>, <code>{</code>, <code>}</code>,
                <code>\</code>.
            </td>
        </tr>
        {{#include ./query-filter-parameter-rows.md}}
    </tbody>
</table>

### Request Body { #post-jobs-budgets-key-body }

{{#include ./bind-budget-parameters.md}}

### Responses { #post-jobs-budgets-key-response }

#### `200` OK

Returns the number of updated jobs, and the list of any job IDs that could not
be updated because those jobs were `in_flight` — in which case those specific
IDs may be retried.

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
                <div><code>changed</code> <em>required</em></div>
                <div><pre>int64</pre></div>
            </td>
            <td>
                The number of jobs that were modified in the operation.
            </td>
        </tr>
        <tr>
            <td>
                <div><code>blocked</code> <em>required</em></div>
                <div><pre>array</pre></div>
            </td>
            <td>
                Array of Job IDs that were not modified because they were
                <code>in_flight</code>. The operation may be retried, including
                just those IDs in the <code>id</code> query parameter.
            </td>
        </tr>
    </tbody>
</table>

#### `403` Forbidden

When the server is not confiured with a Pro license.

{{#include ./error-response.md}}

#### `422` Unprocessable Entity

When invalid values are provided.

{{#include ./error-response.md}}

## `PUT /jobs/budgets/{key}` { #put-jobs-budgets-key }

Replace budget to all matching jobs specified by the filter. Jobs that already
have the binding are updated. Jobs that do not have the binding are given it.
Jobs in a terminal status are gracefully skipped. Jobs in the `in_flight`
status are _blocked_ and reported for retry.

### Parameters { #put-jobs-budgets-key-parameters }

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
                <div><code>key</code> <em>path</em></div>
                <div><pre>string</pre></div>
            </td>
            <td>
                The identifier for the budget. Must be
                valid UTF-8 and must not contain any of the follow reserved
                characters: <code>,</code>, <code>*</code>, <code>?</code>,
                <code>[</code>, <code>]</code>, <code>{</code>, <code>}</code>,
                <code>\</code>.
            </td>
        </tr>
        {{#include ./query-filter-parameter-rows.md}}
    </tbody>
</table>

### Request Body { #post-jobs-budgets-key-body }

{{#include ./bind-budget-parameters.md}}

### Responses { #post-jobs-budgets-key-response }

#### `200` OK

Returns the number of updated jobs, and the list of any job IDs that could not
be updated because those jobs were `in_flight` — in which case those specific
IDs may be retried.

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
                <div><code>changed</code> <em>required</em></div>
                <div><pre>int64</pre></div>
            </td>
            <td>
                The number of jobs that were modified in the operation.
            </td>
        </tr>
        <tr>
            <td>
                <div><code>blocked</code> <em>required</em></div>
                <div><pre>array</pre></div>
            </td>
            <td>
                Array of Job IDs that were not modified because they were
                <code>in_flight</code>. The operation may be retried, including
                just those IDs in the <code>id</code> query parameter.
            </td>
        </tr>
    </tbody>
</table>

#### `403` Forbidden

When the server is not confiured with a Pro license.

{{#include ./error-response.md}}

#### `422` Unprocessable Entity

When invalid values are provided.

{{#include ./error-response.md}}

## `PATCH /jobs/budgets/{key}` { #patch-jobs-budgets-key }

Update the details of the named budget on all matching jobs specified by the
filter. Currently only the `cost` can be patched. Jobs that do not have the
binding are gracefull skipped. Jobs that have the binding are updated. Jobs in
a terminal status are gracefully skipped. Jobs in the `in_flight` status are
_blocked_ and reported for retry.

### Parameters { #patch-jobs-budgets-key-parameters }

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
                <div><code>key</code> <em>path</em></div>
                <div><pre>string</pre></div>
            </td>
            <td>
                The identifier for the budget.
            </td>
        </tr>
        {{#include ./query-filter-parameter-rows.md}}
    </tbody>
</table>

### Request Body { #patch-jobs-budgets-key-body }

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
                <div><code>cost</code> <em>required</em></div>
                <div><pre>int32</pre></div>
            </td>
            <td>
                The number of tokens jobs take from the budget.
            </td>
        </tr>
    </tbody>
</table>

### Responses { #patch-jobs-budgets-key-response }

#### `200` OK

Returns the number of updated jobs, and the list of any job IDs that could not
be updated because those jobs were `in_flight` — in which case those specific
IDs may be retried.

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
                <div><code>changed</code> <em>required</em></div>
                <div><pre>int64</pre></div>
            </td>
            <td>
                The number of jobs that were modified in the operation.
            </td>
        </tr>
        <tr>
            <td>
                <div><code>blocked</code> <em>required</em></div>
                <div><pre>array</pre></div>
            </td>
            <td>
                Array of Job IDs that were not modified because they were
                <code>in_flight</code>. The operation may be retried, including
                just those IDs in the <code>id</code> query parameter.
            </td>
        </tr>
    </tbody>
</table>

#### `403` Forbidden

When the server is not confiured with a Pro license.

{{#include ./error-response.md}}

#### `422` Unprocessable Entity

When invalid values are provided.

{{#include ./error-response.md}}

## `DELETE /jobs/budgets/{key}` { #delete-jobs-budgets-key }

Remove the named budget from all matching jobs specified by the filter. Jobs
that do not have the binding are gracefull skipped. Jobs that have the binding
are updated. Jobs in a terminal status are gracefully skipped. Jobs in the
`in_flight` status are _blocked_ and reported for retry.

### Parameters { #delete-jobs-budgets-key-parameters }

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
                <div><code>key</code> <em>path</em></div>
                <div><pre>string</pre></div>
            </td>
            <td>
                The identifier for the budget.
            </td>
        </tr>
        {{#include ./query-filter-parameter-rows.md}}
    </tbody>
</table>

### Responses { #delete-jobs-budgets-key-response }

#### `200` OK

Returns the number of updated jobs, and the list of any job IDs that could not
be updated because those jobs were `in_flight` — in which case those specific
IDs may be retried.

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
                <div><code>changed</code> <em>required</em></div>
                <div><pre>int64</pre></div>
            </td>
            <td>
                The number of jobs that were modified in the operation.
            </td>
        </tr>
        <tr>
            <td>
                <div><code>blocked</code> <em>required</em></div>
                <div><pre>array</pre></div>
            </td>
            <td>
                Array of Job IDs that were not modified because they were
                <code>in_flight</code>. The operation may be retried, including
                just those IDs in the <code>id</code> query parameter.
            </td>
        </tr>
    </tbody>
</table>

#### `403` Forbidden

When the server is not confiured with a Pro license.

{{#include ./error-response.md}}

#### `422` Unprocessable Entity

When invalid values are provided.

{{#include ./error-response.md}}

## `DELETE /jobs/budgets` { #delete-jobs-budgets-bulk }

Remove _all_ budgets from all matching jobs specified by the filter. Jobs in a
terminal status are gracefully skipped. Jobs in the `in_flight` status are
_blocked_ and reported for retry.

### Parameters { #delete-jobs-budgets-bulk-parameters }

All options are additive.

<table>
    <thead>
        <tr>
            <th>Field</th>
            <th>Description</th>
        </tr>
    </thead>
    <tbody>
        {{#include ./query-filter-parameter-rows.md}}
    </tbody>
</table>

### Responses { #delete-jobs-budgets-bulk-response }

#### `200` OK

Returns the number of updated jobs, and the list of any job IDs that could not
be updated because those jobs were `in_flight` — in which case those specific
IDs may be retried.

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
                <div><code>changed</code> <em>required</em></div>
                <div><pre>int64</pre></div>
            </td>
            <td>
                The number of jobs that were modified in the operation.
            </td>
        </tr>
        <tr>
            <td>
                <div><code>blocked</code> <em>required</em></div>
                <div><pre>array</pre></div>
            </td>
            <td>
                Array of Job IDs that were not modified because they were
                <code>in_flight</code>. The operation may be retried, including
                just those IDs in the <code>id</code> query parameter.
            </td>
        </tr>
    </tbody>
</table>

#### `403` Forbidden

When the server is not confiured with a Pro license.

{{#include ./error-response.md}}

#### `422` Unprocessable Entity

When invalid values are provided.

{{#include ./error-response.md}}

## Examples

### Update a job's queue and priority

> Request:
>
> ```bash
> http PATCH 127.0.0.1:7890/jobs/03fvmbsuryhdkxvb6vjy4qhxp --raw '{
>     "queue": "other",
>     "priority": 100
> }'
> ```

> Response:
>
> ```http
> HTTP/1.1 200 OK
> content-length: 141
> content-type: application/json
> date: Fri, 03 Apr 2026 11:10:58 GMT
> ```
> ```json
> {
>     "attempts": 0,
>     "id": "03fvmbsuryhdkxvb6vjy4qhxp",
>     "priority": 100,
>     "queue": "other",
>     "ready_at": 1775214099613,
>     "status": "ready",
>     "type": "hello_world"
> }
> ```

### Move a job from `ready` to `scheduled`

> Request:
>
> ```bash
> http PATCH 127.0.0.1:7890/jobs/03fvmbsuryhdkxvb6vjy4qhxp --raw '{
>     "ready_at": 1775217412000
> }'
> ```

> Response:
>
> ```http
> HTTP/1.1 200 OK
> content-length: 145
> content-type: application/json
> date: Fri, 03 Apr 2026 11:13:10 GMT
> ```
> ```json
> {
>     "attempts": 0,
>     "id": "03fvmbsuryhdkxvb6vjy4qhxp",
>     "priority": 100,
>     "queue": "other",
>     "ready_at": 1775217412000,
>     "status": "scheduled",
>     "type": "hello_world"
> }
> ```

### Clear a field back to server default

Setting an optional field to `null` resets it to the server's default value.

> Request:
>
> ```bash
> http PATCH 127.0.0.1:7890/jobs/03fvmbsuryhdkxvb6vjy4qhxp --raw '{
>     "retry_limit": null
> }'
> ```

> Response:
>
> ```http
> HTTP/1.1 200 OK
> content-length: 145
> content-type: application/json
> date: Fri, 03 Apr 2026 11:15:20 GMT
> ```
> ```json
> {
>     "attempts": 0,
>     "id": "03fvmbsuryhdkxvb6vjy4qhxp",
>     "priority": 100,
>     "queue": "other",
>     "ready_at": 1775217412000,
>     "status": "scheduled",
>     "type": "hello_world"
> }
> ```

### Move all jobs from one queue to another

> Request:
>
> ```bash
> http PATCH http://127.0.0.1:7890/jobs?queue=example --raw '{
>     "queue": "other"
> }'
> ```

> Response:
>
> ```http
> HTTP/1.1 200 OK
> content-length: 13
> content-type: application/json
> date: Fri, 03 Apr 2026 11:17:09 GMT
> ```
> ```json
> {
>     "patched": 4
> }
> ```

### Remove all scheduled jobs on a queue

> Request:
>
> ```bash
> http DELETE "http://127.0.0.1:7890/jobs?queue=example&status=scheduled"
> ```

> Response:
>
> ```http
> HTTP/1.1 200 OK
> content-length: 13
> content-type: application/json
> date: Fri, 03 Apr 2026 11:18:36 GMT
> ```
> ```json
> {
>     "deleted": 2
> }
> ```

### Safely delete jobs matching filters in pages

To delete jobs matching a filter in a paginated way, a two step approach is
used:

1. Query the jobs using the desired filters.
2. Delete the jobs using filters *and* the IDs on each page.

It's important to retain the filters to handle race conditions if the jobs are
modified between fetching the page and executing the delete.

> Find Request:
>
> ```bash
> http GET 'http://127.0.0.1:7890/jobs?filter=.greet | startswith("Wo")&limit=2'
> ```

> Find Response:
>
> ```http
> HTTP/1.1 200 OK
> content-length: 624
> content-type: application/json
> date: Fri, 03 Apr 2026 11:22:34 GMT
> ```
> ```json
> {
>     "jobs": [
>         {
>             "attempts": 0,
>             "id": "03fvmaj8q5po1huy5nd4xmi5f",
>             "payload": {
>                 "greet": "World"
>             },
>             "priority": 500,
>             "queue": "example",
>             "ready_at": 1775213710452,
>             "status": "ready",
>             "type": "hello_world"
>         },
>         {
>             "attempts": 0,
>             "id": "03fvmame0wyuiexbc2033jby2",
>             "payload": {
>                 "greet": "World"
>             },
>             "priority": 500,
>             "queue": "example",
>             "ready_at": 1775213737304,
>             "status": "ready",
>             "type": "hello_world",
>             "unique_key": "hello_world:world",
>             "unique_while": "queued"
>         }
>     ],
>     "pages": {
>         "next": "/jobs?from=03fvmame0wyuiexbc2033jby2&order=asc&limit=2&filter=.greet%20%7C%20startswith%28%22Wo%22%29",
>         "prev": null,
>         "self": "/jobs?order=asc&limit=2&filter=.greet%20%7C%20startswith%28%22Wo%22%29"
>     }
> }
> ```

> Delete Request:
>
> ```bash
> http DELETE 'http://127.0.0.1:7890/jobs?filter=.greet | startswith("Wo")&id=03fvmaj8q5po1huy5nd4xmi5f,03fvmame0wyuiexbc2033jby2'
> ```

> Delete Response:
>
> ```http
> HTTP/1.1 200 OK
> content-length: 13
> content-type: application/json
> date: Fri, 03 Apr 2026 11:23:52 GMT
> ```
> ```json
> {
>     "deleted": 2
> }
> ```
