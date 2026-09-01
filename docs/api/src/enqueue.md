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
                Arbitrary queue name to which the job is assigned. Must be
                valid UTF-8 and must not contain any of the follow reserved
                characters: <code>,</code>, <code>*</code>, <code>?</code>,
                <code>[</code>, <code>]</code>, <code>{</code>, <code>}</code>,
                <code>\</code>.
            </td>
        </tr>
        <tr>
            <td>
                <div><code>type</code> <em>required</em></div>
                <div><pre>string</pre></div>
            </td>
            <td>
                Job type known to your application. Must be valid UTF-8 and
                must not contain any of the follow reserved characters:
                <code>,</code>, <code>*</code>, <code>?</code>, <code>[</code>,
                <code>]</code>, <code>{</code>, <code>}</code>, <code>\</code>.
            </td>
        </tr>
        <tr>
            <td>
                <div><code>priority</code></div>
                <div><pre>int16</pre></div>
            </td>
            <td>
                Optional numeric priority for the job. Lower values are
                processed first (higher priority). The default value is
                <code>32768</code>.
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
                        Other jobs with the same <code>unique_key</code> will
                        not be enqueued while this job is in the
                        <code>scheduled</code> or <code>ready</code> statuses.
                    </dd>
                    <dt><code>active</code></dt>
                    <dd>
                        Other jobs with the same <code>unique_key</code> will
                        not be enqueued while this job is in the
                        <code>scheduled</code>, <code>ready</code> or
                        <code>in_flight</code> statuses.
                    </dd>
                    <dt><code>exists</code></dt>
                    <dd>
                        Other jobs with the same <code>unique_key</code> will
                        not be enqueued for as long as this job exists (i.e.
                        until this job is reaped, according to the retention
                        policy).
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
        <tr>
            <td>
                <div><code>batch</code></div>
                <div><pre>object</pre></div>
            </td>
            <td>
                Optional batched-job configuration. When present, subsequent
                enqueues sharing the same <code>batch.key</code> are
                <em>folded</em> into this job's pending payload via the
                <code>when</code> and <code>fold</code> jq expressions,
                rather than creating separate pending jobs. See the
                <a href="#batched-jobs">Batched jobs</a> section below for
                the full semantics. All three inner fields are required
                when this field is set. Mutually exclusive with
                <code>unique_key</code> — supplying both returns
                <code>400 Bad Request</code>.
                <strong>Requires a <em>pro</em> license</strong>.
            </td>
        </tr>
        <tr>
            <td>
                <div><code>batch.key</code></div>
                <div><pre>string</pre></div>
            </td>
            <td>
                Identifies the batch. Only one unsealed batched job exists
                per key at a time. Enqueues sharing this key fold into the
                existing pending job (or start a new one if none exists or
                the existing batch was already sealed).
            </td>
        </tr>
        <tr>
            <td>
                <div><code>batch.when</code></div>
                <div><pre>string</pre></div>
            </td>
            <td>
                <a href="https://jqlang.org/manual/">jq</a> predicate that
                decides whether an incoming enqueue folds into the existing
                pending job. Evaluated with <code>$existing</code> bound to
                the current pending payload and <code>$new</code> bound to
                the incoming payload. Truthy means fold; falsy seals the
                existing batch and starts a fresh one from the incoming
                enqueue. Invalid jq syntax, or an expression that returns
                multiple outputs, returns <code>422 Unprocessable Entity</code>.
            </td>
        </tr>
        <tr>
            <td>
                <div><code>batch.fold</code></div>
                <div><pre>string</pre></div>
            </td>
            <td>
                <a href="https://jqlang.org/manual/">jq</a> expression that
                produces the merged payload when a fold occurs. Runs with the
                same <code>$existing</code> and <code>$new</code> bindings as
                <code>when</code>. Must produce exactly one output; multiple
                outputs return <code>422 Unprocessable Entity</code>.
            </td>
        </tr>
        <tr>
            <td>
                <div><code>budgets</code></div>
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

## Common Job Response { #job-response }

Both endpoints accept and return the same structure, except the bulk enqueue
endpoint wraps an array of `{"jobs": [...]}`.

{{#include ./job-response-without-payload.md}}

## `POST /jobs` { #post-jobs }

Enqueues a single job.

### Request Body { #post-jobs-body }

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

### Request Body { #post-jobs-bulk-body }

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

> Request:
>
> ```bash
> http POST http://127.0.0.1:7890/jobs --raw '{
>     "queue": "example",
>     "priority": 500,
>     "type": "hello_world",
>     "payload": {"greet": "World"}
> }'
> ```

> Response:
>
> ```http
> HTTP/1.1 201 Created
> content-length: 143
> content-type: application/json
> date: Fri, 13 Mar 2026 08:53:47 GMT
> ```
> ```json
> {
>     "attempts": 0,
>     "id": "03fr1jkpcsipbsckqj0y6pgr7",
>     "priority": 500,
>     "queue": "example",
>     "ready_at": 1773392027425,
>     "status": "ready",
>     "type": "hello_world"
> }
> ```

### Enqueue a scheduled Job

Jobs are explicitly scheduled by providing a `ready_at` timestamp with a future
dated value.

> Request:
>
> ```bash
> http POST http://127.0.0.1:7890/jobs --raw '{
>     "queue": "example",
>     "priority": 500,
>     "type": "hello_world",
>     "payload": {"greet": "Later"},
>     "ready_at": 1773396035647
> }'
> ```

> Response:
>
> ```http
> HTTP/1.1 201 Created
> content-length: 147
> content-type: application/json
> date: Fri, 13 Mar 2026 09:01:08 GMT
> ```
> ```json
> {
>     "attempts": 0,
>     "id": "03fr1l0cl1quc0sfe6y2711op",
>     "priority": 500,
>     "queue": "example",
>     "ready_at": 1773396035647,
>     "status": "scheduled",
>     "type": "hello_world"
> }
> ```

### Enqueue jobs with unique keys

Unique jobs require a [pro license](https://zizq.io/pricing).

> First Request:
>
> ```bash
> http POST http://127.0.0.1:7890/jobs --raw '{
>     "queue": "example",
>     "priority": 500,
>     "type": "hello_world",
>     "unique_key": "hello_world:world",
>     "payload": {"greet": "World"}
> }'
> ```

> First Response:
>
> ```http
> HTTP/1.1 201 Created
> content-length: 218
> content-type: application/json
> date: Mon, 23 Mar 2026 11:19:58 GMT
> ```
> ```json
> {
>     "attempts": 0,
>     "duplicate": false,
>     "id": "03ft8h3ubrx53abhw1fxbora3",
>     "priority": 500,
>     "queue": "example",
>     "ready_at": 1774264798519,
>     "status": "ready",
>     "type": "hello_world",
>     "unique_key": "hello_world:world",
>     "unique_while": "queued"
> }
> ```

> Subsequent Request:
>
> ```bash
> http POST http://127.0.0.1:7890/jobs --raw '{
>     "queue": "example",
>     "priority": 500,
>     "type": "hello_world",
>     "unique_key": "hello_world:world",
>     "payload": {"greet": "World"}
> }'
> ```

> Subsequent Response:
>
> ```http
> HTTP/1.1 200 OK
> content-length: 217
> content-type: application/json
> date: Mon, 23 Mar 2026 11:20:26 GMT
> ```
> ```json
> {
>     "attempts": 0,
>     "duplicate": true,
>     "id": "03ft8h3ubrx53abhw1fxbora3",
>     "priority": 500,
>     "queue": "example",
>     "ready_at": 1774264798519,
>     "status": "ready",
>     "type": "hello_world",
>     "unique_key": "hello_world:world",
>     "unique_while": "queued"
> }
> ```

### Enqueue a job against multiple budgets

Budgets are used for rate limiting and concurrency control. In this example
budgets have already been created and the newly enqueued job is bound to those
budgets.

> Request:
> ```sh
> http POST http://127.0.0.1:7890/jobs --raw '{
>     "type": "example",
>     "queue": "example",
>     "payload": {},
>     "budgets": [
>         {
>             "key": "schema-bot",
>             "cost": 2
>         },
>         {"key": "image-service"}
>     ]
> }'
> ```

> Response:
> ```http
> HTTP/1.1 201 Created
> content-length: 249
> content-type: application/json
> date: Mon, 31 Aug 2026 22:49:49 GMT
> ```
> ```json
> {
>     "attempts": 0,
>     "budgets": [
>         {
>             "cost": 2,
>             "key": "schema-bot"
>         },
>         {
>             "cost": 1,
>             "key": "image-service"
>         }
>     ],
>     "duplicate": false,
>     "folded": false,
>     "id": "03gsa8rjaoxgqgnrsnlt7niqb",
>     "priority": 32768,
>     "queue": "example",
>     "ready_at": 1788216589726,
>     "status": "ready",
>     "type": "example"
> }
> ```

### Enqueue a job against budgets using `create_with`

In this example, the budget for a job need not be created ahead of time. If it
does not exist, Zizq will create it atomically with the job.

> Request:
> ```sh
> http POST http://127.0.0.1:7890/jobs --raw '{
>     "type": "example",
>     "queue": "example",
>     "payload": {},
>     "budgets": [
>         {
>             "key": "cpu-intensive",
>             "create_with": {
>                 "allocation": 100,
>                 "strategy": {
>                     "type": "while_in_flight"
>                 }
>             }
>         }
>     ]
> }'
> ```

> Response:
> ```http
> HTTP/1.1 201 Created
> content-length: 219
> content-type: application/json
> date: Mon, 31 Aug 2026 22:53:01 GMT
> ```
> ```json
> {
>     "attempts": 0,
>     "budgets": [
>         {
>             "cost": 1,
>             "key": "cpu-intensive"
>         }
>     ],
>     "duplicate": false,
>     "folded": false,
>     "id": "03gsa9e0vd59nf3zgysm1yxd0",
>     "priority": 32768,
>     "queue": "example",
>     "ready_at": 1788216781592,
>     "status": "ready",
>     "type": "example"
> }
> ```

### Batched jobs {#batched-jobs}

Batched jobs let successive enqueues accumulate into a single pending job.
The client attaches a `batch` object to the enqueue containing a `key`, a
`when` jq predicate, and a `fold` jq expression:

- The **key** identifies the batch. Only one unsealed job exists per key at
  a time.
- The **when** predicate decides, on each subsequent enqueue with the same
  key, whether to fold into the existing pending job or seal it and start a
  fresh one. It runs with `$existing` bound to the current pending payload
  and `$new` bound to the incoming payload; truthy folds, falsy seals.
- The **fold** expression produces the merged payload when a fold happens.
  Same `$existing` / `$new` bindings as `when`.

Batched jobs require a [pro license](https://zizq.io/pricing).

Both `when` and `fold` are compiled and dry-run against the incoming payload
on every batched enqueue. Bad expressions (syntax errors, undefined
variables, or shape errors that only manifest against actual data) return
`422 Unprocessable Entity` up front rather than failing at first fold. An
expression that returns multiple outputs is also `422`.

`batch` and `unique_key` are mutually exclusive on the same enqueue.
Supplying both returns `400 Bad Request`.

**Scheduling opt-out**: an enqueue with `batch` and a future `ready_at`
persists as a normal scheduled job with its `batch` config attached for
observability, but no fold happens across a `ready_at` boundary in either
direction. Folding is strictly `ready` → `ready`.

**First-enqueue config wins**: the `when` and `fold` stored on the initial
pending job are what apply for every subsequent fold against it. Changing
the config in later enqueues has no effect until the current batch is
sealed and a new one begins.

The response includes a `folded` boolean indicating whether the enqueue
was folded into an existing pending job (`true`, status `200`) or created
a new one (`false`, status `201`). Reading the job later via `GET
/jobs/{id}` includes the stored `batch` config for visibility into what
the server is evaluating on subsequent folds.

> First Request:
>
> ```bash
> http POST http://127.0.0.1:7890/jobs --raw '{
>     "queue": "push",
>     "type": "push.notifications",
>     "payload": {"device_ids": ["abc"], "platform": "apple"},
>     "batch": {
>         "key": "push:apple",
>         "when": "(($existing | .device_ids) + ($new | .device_ids)) | length <= 100",
>         "fold": "$existing | .device_ids += ($new | .device_ids)"
>     }
> }'
> ```

> First Response:
>
> ```http
> HTTP/1.1 201 Created
> content-length: 419
> content-type: application/json
> date: Mon, 23 Mar 2026 11:22:14 GMT
> ```
> ```json
> {
>     "attempts": 0,
>     "batch": {
>         "key": "push:apple",
>         "when": "(($existing | .device_ids) + ($new | .device_ids)) | length <= 100",
>         "fold": "$existing | .device_ids += ($new | .device_ids)"
>     },
>     "folded": false,
>     "id": "03ft8h9pkr50xbf7ncrhy2wnk",
>     "priority": 32768,
>     "queue": "push",
>     "ready_at": 1774264934000,
>     "status": "ready",
>     "type": "push.notifications"
> }
> ```

> Subsequent Request (same batch key, different device_ids):
>
> ```bash
> http POST http://127.0.0.1:7890/jobs --raw '{
>     "queue": "push",
>     "type": "push.notifications",
>     "payload": {"device_ids": ["def", "ghi"], "platform": "apple"},
>     "batch": {
>         "key": "push:apple",
>         "when": "(($existing | .device_ids) + ($new | .device_ids)) | length <= 100",
>         "fold": "$existing | .device_ids += ($new | .device_ids)"
>     }
> }'
> ```

> Subsequent Response:
>
> ```http
> HTTP/1.1 200 OK
> content-length: 418
> content-type: application/json
> date: Mon, 23 Mar 2026 11:22:31 GMT
> ```
> ```json
> {
>     "attempts": 0,
>     "batch": {
>         "key": "push:apple",
>         "when": "(($existing | .device_ids) + ($new | .device_ids)) | length <= 100",
>         "fold": "$existing | .device_ids += ($new | .device_ids)"
>     },
>     "folded": true,
>     "id": "03ft8h9pkr50xbf7ncrhy2wnk",
>     "priority": 32768,
>     "queue": "push",
>     "ready_at": 1774264934000,
>     "status": "ready",
>     "type": "push.notifications"
> }
> ```

Note that the `id` on the subsequent response matches the first — the
second enqueue folded into the existing pending job rather than creating
a new one. Fetching the job now returns a merged payload combining
`["abc"]` and `["def", "ghi"]`.

### Bulk enqueue multiple jobs

An array of jobs is passed in the request, and the server responds with an
array containing the same number of jobs, in the same order as the input
request. This operation is atomic. If any jobs are invalid or fail to be
enqueued, no jobs are enqueued and an error response is returned.

> Request:
>
> ```bash
> http POST http://127.0.0.1:7890/jobs/bulk --raw '{
>     "jobs": [
>         {
>             "queue": "example",
>             "priority": 500,
>             "type": "hello_world",
>             "payload": {"greet": "World"}
>         },
>         {
>             "queue": "example",
>             "priority": 500,
>             "type": "hello_world",
>             "payload": {"greet": "Later"},
>             "ready_at": 1773396035647
>         }
>     ]
> }'
> ```

> Response:
>
> ```http
> HTTP/1.1 201 Created
> content-length: 302
> content-type: application/json
> date: Fri, 13 Mar 2026 09:07:17 GMT
> ```
> ```json
> {
>     "jobs": [
>         {
>             "attempts": 0,
>             "id": "03fr1m7p1mwctku2fptz1x5p4",
>             "priority": 500,
>             "queue": "example",
>             "ready_at": 1773392837882,
>             "status": "ready",
>             "type": "hello_world"
>         },
>         {
>             "attempts": 0,
>             "id": "03fr1m7p1mwctku2fpx425jzr",
>             "priority": 500,
>             "queue": "example",
>             "ready_at": 1773396035647,
>             "status": "scheduled",
>             "type": "hello_world"
>         }
>     ]
> }
> ```

### Enqueue a job with explicit backoff policy

> Request:
>
> ```bash
> http POST http://127.0.0.1:7890/jobs --raw '{
>     "queue": "example",
>     "priority": 500,
>     "type": "hello_world",
>     "payload": {"greet": "World"},
>     "backoff": {
>         "base_ms": 1000,
>         "exponent": 1.5,
>         "jitter_ms": 10000
>     }
> }'
> ```

> Response:
>
> ```http
> HTTP/1.1 201 Created
> content-length: 203
> content-type: application/json
> date: Sat, 14 Mar 2026 03:24:16 GMT
> ```
> ```json
> {
>     "attempts": 0,
>     "backoff": {
>         "base_ms": 1000,
>         "exponent": 1.5,
>         "jitter_ms": 10000
>     },
>     "id": "03fr7ki3x5kqf1epbydrfebkz",
>     "priority": 500,
>     "queue": "example",
>     "ready_at": 1773458656424,
>     "status": "ready",
>     "type": "hello_world"
> }
> ```

### Enqueue a job with explicit retention policy

> Request:
>
> ```bash
> http POST http://127.0.0.1:7890/jobs --raw '{
>     "queue": "example",
>     "priority": 500,
>     "type": "hello_world",
>     "payload": {"greet": "World"},
>     "retention": {
>         "completed_ms": 86400000,
>         "dead_ms": 604800000
>     }
> }'
> ```

> Response:
>
> ```http
> HTTP/1.1 201 Created
> content-length: 201
> content-type: application/json
> date: Sat, 14 Mar 2026 03:26:01 GMT
> ```
> ```json
> {
>     "attempts": 0,
>     "id": "03fr7kudjeradun2wk1v3tn7b",
>     "priority": 500,
>     "queue": "example",
>     "ready_at": 1773458761086,
>     "retention": {
>         "completed_ms": 86400000,
>         "dead_ms": 604800000
>     },
>     "status": "ready",
>     "type": "hello_world"
> }
> ```
