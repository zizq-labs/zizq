# Concurrency Control &amp; Rate Limiting

> [!NOTE]
> These endpoints are available in both `application/json` and
> `application/msgpack` formats.

> [!NOTE]
> This feature requires a [Pro license](https://zizq.io/pricing).

Applications typically enqueue jobs to offload expensive work to a background
worker. Some jobs can put pressure on upstream systems if running with high
throughput. For example, you may have jobs that resize images through an image
service, and that service may have a maximum throughput of 10,000 requests per
hour. Or you may send push notifications through your queue worker, but at
times bursts of notifications dominate your queue, starving other jobs of
processing time. Both of these problems can be solved with a feature Zizq calls
_budgets_.

Budgets are Zizq's approach to limiting throughput, both from a pure concurrency
control perspective (no more than N jobs in-flight at any given time), and also
from a rate limiting perspective (no more than N jobs dispatched over a period
of time). The server stores named budgets, which are pools of available _tokens_
managed under a specified _strategy_. Currently two strategies exist:
`while_in_flight`, and `time_based`. Jobs are enqueued referencing one or more
of these budgets, along with optional costs to run those jobs, where that cost
defaults to `1` token. Before a job can be dispatched to a worker, it needs to
successfully debit its cost from each of its budgets' shared token pools.

Unlike with some other job queues, throughput is controlled entirely by the
server, so workers do not need to receive a job and then wait or retry because
it exceeded some rate limit. Instead, workers remain naive to the dispatching
logic and just process every job they receive in the same way. If the budget
does not have enough tokens available for a job to run, Zizq does not dispatch
that job to a worker in the first place. It _parks_ that job and dispatches it
the moment its budget allows. Other jobs — those without budget restrictions
and those on other budgets that have tokens available — continue to be
dispatched to workers without stalling.

> [!NOTE]
> Budgets are a shared resource. The Zizq server currently enforces an upper
> limit on the number of distinct budgets that can be created. The default is
> `8192` different budgets, which should be far in excess of what typical
> applications would require, but this limit can be configured on the server
> via `--max-budgets` (`$ZIZQ_MAX_BUDGETS`). A future release will introduce a
> sub-bucket concept for dynamically allocated budget scenarios.

## Budget Strategies

There are currently two available strategies for budgeting: `while_in_flight`
implements pure concurrency control, and `time_based` implements a dispatch
rate limit over time. Both take a total `allocation` value, which is the number
of tokens made available in the budget's pool. Jobs can be bound to more than
one budget, mixing and matching across different strategies. In this case,
_all_ budgets must be satisified before the job can run.

Budgets must have an allocation greater than or equal to the `cost` of each
job that references that budget. That is, the Zizq server will reject any
attempt to enqueue a job that costs more than its budget will ever allow, and
it will reject any attempt to update a budget to allocate less tokens than some
job referencing that budget would cost.

### `while_in_flight`

For pure concurrency control, where you need to ensure at most `N` workers are
processing a given job at once, use the `while_in_flight` strategy. In this
strategy, the budget has a given token allocation of say `20` and no other
configuration. We'll see how to allocate budgets below, but this is the shape
of a `while_in_flight` budget.

> `while_in_flight` budget:
> ```json
> {
>     "allocation": 20,
>     "strategy": {
>         "type": "while_in_flight"
>     }
> }
> ```

The above budget would allow at most 20 concurrent jobs with the default cost
of `1`, or 10 concurrent jobs with a cost of `2`, or any valid combination of
costs that is less than or equal to 20.

- `20 x cost=1`
- `10 x cost=2`
- `(5 x cost=2) + (10 x cost=1)`
- `(3 x cost=5) + (2 x cost=2)`

When a job is dispatched that uses a `while_in_flight` strategy, it must debit
its cost in full from the budget's token pool. If the pool is too depleted to
do that, the job remains parked until more tokens are available in the pool.
Once dispatched, tokens remain debited from the pool for as long as that job is
`in_flight`. As soon as the worker acknowledges the job with a successful
completion, or reports a failure, the job is no longer `in_flight` and its
tokens are released back to the pool, allowing other jobs to run within that
same budget.

To illustrate how this works, take this example job that takes a consistent 2
seconds to run, bound to a `while_in_flight` budget with an allocation of 5.

> 2 second jobs with `while_in_flight` 5:
> ```sh
> 2026-08-31T19:16:28+10:00: Job 03gs5tvn78uny555dm88vjsoj running in process 3098324
> 2026-08-31T19:16:28+10:00: Job 03gs5tvn78uny555dmbvk61n3 running in process 3098338
> 2026-08-31T19:16:28+10:00: Job 03gs5tvn78uny555dmdlmcrhh running in process 3098358
> 2026-08-31T19:16:28+10:00: Job 03gs5tvn78uny555dmew56mwy running in process 3098370
> 2026-08-31T19:16:28+10:00: Job 03gs5tvn78uny555dmgjcsuw2 running in process 3098385
> 2026-08-31T19:16:30+10:00: Job 03gs5tvn78uny555dm88vjsoj exited with status 0
> 2026-08-31T19:16:30+10:00: Job 03gs5tvn78uny555dmbvk61n3 exited with status 0
> 2026-08-31T19:16:30+10:00: Job 03gs5tvn78uny555dmdlmcrhh exited with status 0
> 2026-08-31T19:16:30+10:00: Job 03gs5tvn78uny555dmew56mwy exited with status 0
> 2026-08-31T19:16:30+10:00: Job 03gs5tvn78uny555dmim3oaw5 running in process 3098448
> 2026-08-31T19:16:30+10:00: Job 03gs5tvn78uny555dmgjcsuw2 exited with status 0
> 2026-08-31T19:16:30+10:00: Job 03gs5tvn78uny555dmkz0iee0 running in process 3098485
> 2026-08-31T19:16:30+10:00: Job 03gs5tvn78uny555dmmee2rjg running in process 3098504
> 2026-08-31T19:16:30+10:00: Job 03gs5tvn78uny555dmppqcuqe running in process 3098518
> 2026-08-31T19:16:30+10:00: Job 03gs5tvn78uny555dmrdrgvdm running in process 3098533
> 2026-08-31T19:16:32+10:00: Job 03gs5tvn78uny555dmim3oaw5 exited with status 0
> 2026-08-31T19:16:32+10:00: Job 03gs5tvn78uny555dmkz0iee0 exited with status 0
> 2026-08-31T19:16:32+10:00: Job 03gs5tvn78uny555dms8wyffo running in process 3098578
> 2026-08-31T19:16:32+10:00: Job 03gs5tvn78uny555dmmee2rjg exited with status 0
> 2026-08-31T19:16:32+10:00: Job 03gs5tvn78uny555dmppqcuqe exited with status 0
> 2026-08-31T19:16:32+10:00: Job 03gs5tvn78uny555dmrdrgvdm exited with status 0
> ```

Jobs are running 5 at once, taking 2 seconds to complete and then another 5
jobs run at once. Note that what _looks like_ overlap here is just an artifact
of the concurrent processing locally. These logs are genuinely copied &amp;
pasted from a live example. The overlap is just the way the logs were output,
with the acknowledgment reaching the server before the success log was printed.

### `time_based`

Where concurrency is not the concern, but overall throughput is, the
`time_based` strategy can be used to enforce a rate limit. Just like a
`while_in_flight` budget, a `time_based` one has a token allocation, which
represents the number of tokens that can be _spent_ over some period of time.
Unlike the `while_in_flight` strategy, a `time_based` strategy is specified
along with a `duration_ms`, specifying the number of milliseconds over which
its allocation can be spent.

> `time_based` budget:
> ```json
> {
>     "allocation": 10000,
>     "strategy": {
>         "type": "time_based",
>         "duration_ms": 3600000
>     }
> }
> ```

The above budget states that at most 10,000 tokens can be spent over a
3,600,000 millisecond period, hence it is a rate limit of `10000/hour`.

When a job is dispatched that uses a `time_based` budget, it must debit its
cost in full, otherwise it remains parked until enough tokens become available.
Unlike `while_in_flight`, tokens are not released back to the pool when the job
completes, but rather are released back to the pool on the cadence specified by
the duration. This means a `time_based` budget controls how many jobs are
_dispatched_ over time, but it cares not how many of those jobs finish up
running at once (i.e. jobs that take longer than `duration_ms` to run may
overlap). The server implements this _lazily_. There is no constant scanning of
the database to look for jobs that can now be dispatched. The server is smart
enough to know when tokens will next become available and sleeps until that
time, or until some other event wakes it.

The `time_based` strategy implements a _continuous_ (drip) rate limiter. Also
known as a [leaky bucket](https://en.wikipedia.org/wiki/Leaky_bucket) rate
limit. Unlike some rate limiters which bucket tokens into fixed time intervals
— e.g. for a 5 minute limit, `00:00 - 00:05`, `00:05 - 00:10`, ... — a
continuous drip rate limiter sets a pace. For example if 100 tokens are
available over 5 minutes, and the pool is empty, after 1 minute the pool has 20
tokens available, after 4 minutes it has 80 tokens available, and after the
full 5 minutes it has all 100 tokens available. This naturally spreads work
over time, rather than sending sharp bursts of jobs across fixed bucket
boundaries, then stalling until the next bucket etc. When the pool is _full_
however, it has 100 tokens available and therefore a sudden burst of 100 jobs
with a cost of 1 could all go at once, followed by a steady pace of around 1
job every 3 seconds. This is generally desirable in order to accommodate
short-lived spikes, but not always, and the behaviour is configurable through
the `burst` parameter on on the strategy.

To illustrate how this works, take this example which runs a job that takes a
consistent 2 seconds per execution, using a budget configured at 6/minute.

> `time_based` at 6/minute:
> ```sh
> 2026-08-31T19:21:55+10:00: Job 03gs5uwrfxd4pt761yklejqko running in process 3099369
> 2026-08-31T19:21:57+10:00: Job 03gs5uwrfxd4pt761yklejqko exited with status 0
> 2026-08-31T19:22:05+10:00: Job 03gs5uwrfxd4pt761ymwt6p98 running in process 3099409
> 2026-08-31T19:22:07+10:00: Job 03gs5uwrfxd4pt761ymwt6p98 exited with status 0
> 2026-08-31T19:22:15+10:00: Job 03gs5uwrfxd4pt761yp3o5g8j running in process 3099443
> 2026-08-31T19:22:17+10:00: Job 03gs5uwrfxd4pt761yp3o5g8j exited with status 0
> 2026-08-31T19:22:25+10:00: Job 03gs5uwrg2tzskvxfx78r00j5 running in process 3099479
> 2026-08-31T19:22:27+10:00: Job 03gs5uwrg2tzskvxfx78r00j5 exited with status 0
> ```

As you can see, each job takes its 2 seconds to complete, but the worker
continues receiving and processing these jobs at a rate of 6 per second.

The `burst` is how full the token pool can be at any single point in time. When
not specified, the `allocation` is used, so for our 100 jobs/5 minute example
the default burst is 100, as descibed above. Budgets specifying a different
`burst` look like so:

> `time_based` budget with burst:
> ```json
> {
>     "allocation": 10000,
>     "strategy": {
>         "type": "time_based",
>         "duration_ms": 3600000,
>         "burst": 500
>     }
> }
> ```

In this example, no more than 500 jobs can be dispatched at any moment, then
10,000/hour at a steady pace thereafter. Setting a `burst` of just `1` is
equivalent to enforcing the 10,000/hour always. It is also possible to set the
`burst` _higher_ than the total allocation — say 20,000 tokens — which allows
for brief spikes of high throughput that exceed the rate limit by design, if
and only if the budget was otherwise unused for an equivalent period of time.

Again, to illustrate how this works, here's that 6/minute job with its upfront
default burst of 6 jobs in one go.

> `time_based` at 6/minute, with its burst:
> ```sh
> 2026-08-31T19:21:45+10:00: Job 03gs5uwrfxd4pt761y86gg2qs running in process 3099195
> 2026-08-31T19:21:45+10:00: Job 03gs5uwrfxd4pt761y9pfrlq1 running in process 3099214
> 2026-08-31T19:21:45+10:00: Job 03gs5uwrfxd4pt761yck5ma8c running in process 3099228
> 2026-08-31T19:21:45+10:00: Job 03gs5uwrfxd4pt761yegaxtjt running in process 3099245
> 2026-08-31T19:21:45+10:00: Job 03gs5uwrfxd4pt761yh9m250z running in process 3099263
> 2026-08-31T19:21:45+10:00: Job 03gs5uwrfxd4pt761yhxue08x running in process 3099278
> 2026-08-31T19:21:47+10:00: Job 03gs5uwrfxd4pt761y86gg2qs exited with status 0
> 2026-08-31T19:21:47+10:00: Job 03gs5uwrfxd4pt761y9pfrlq1 exited with status 0
> 2026-08-31T19:21:47+10:00: Job 03gs5uwrfxd4pt761yck5ma8c exited with status 0
> 2026-08-31T19:21:47+10:00: Job 03gs5uwrfxd4pt761yegaxtjt exited with status 0
> 2026-08-31T19:21:47+10:00: Job 03gs5uwrfxd4pt761yh9m250z exited with status 0
> 2026-08-31T19:21:47+10:00: Job 03gs5uwrfxd4pt761yhxue08x exited with status 0
> 2026-08-31T19:21:55+10:00: Job 03gs5uwrfxd4pt761yklejqko running in process 3099369
> 2026-08-31T19:21:57+10:00: Job 03gs5uwrfxd4pt761yklejqko exited with status 0
> 2026-08-31T19:22:05+10:00: Job 03gs5uwrfxd4pt761ymwt6p98 running in process 3099409
> 2026-08-31T19:22:07+10:00: Job 03gs5uwrfxd4pt761ymwt6p98 exited with status 0
> 2026-08-31T19:22:15+10:00: Job 03gs5uwrfxd4pt761yp3o5g8j running in process 3099443
> 2026-08-31T19:22:17+10:00: Job 03gs5uwrfxd4pt761yp3o5g8j exited with status 0
> 2026-08-31T19:22:25+10:00: Job 03gs5uwrg2tzskvxfx78r00j5 running in process 3099479
> 2026-08-31T19:22:27+10:00: Job 03gs5uwrg2tzskvxfx78r00j5 exited with status 0
> ```

Here it is visible that before the worker settles into receiving these jobs at
a rate of 6 per second, it receives an upfront burst of 6 jobs in one go. This
only happens:

1. If no jobs have been dispatched for the configured duration (i.e. the token
   pool is full); or
2. The budget is freshly allocated (newly created, or the Zizq server was
   restarted).

When using `burst`, all jobs that reference the budget must have a cost less
than or equal to the configured burst, and any attempts to update the budget
such that this condition is violated are rejected.

## Budget Bindings

> [!NOTE]
> See [Enqueueing Jobs](./enqueue.md) for full details on the fields available
> to jobs.

Budgets are a shared resource, referenced under a known _key_. Jobs _bind_ to
those budgets by specifying them when enqueued. Jobs can be bound to zero or
more budgets, which they specify through the options `budgets` array.

> Budget bindings on a job:
> ```json
> {
>     "budgets": [
>         {"key": "image-service"},
>         {"key": "notifications", "cost": 5}
>     ]
> }
> ```

When no budgets are specified, that job is unbudgeted and is dispatched the
moment it reaches the front of the queue. When one budget is specified, even if
the job has reached the front of the queue, it must debit its `cost` from the
budget's token pool before it can be dispatched, otherwise it is parked and
other jobs continue to be dispatched without interruption. When multiple
budgets are specified, it must debit its cost from _all_ of those budgets
before it can be dispatched.

For example, a job could be configured against a `while_in_flight` limit of 10,
and a `time_based` limit of 1000/hour. Both controls are equally authoritative:
no more than 10 jobs will ever run concurrently, and no more than 1000 jobs
will ever be dispatched per hour.

While budgets are shared resources that can be created ahead of time, it is
also possible for jobs to provide a `create_with` specification when they are
enqueued against a budget. If the budget already exists, the `create_with` is
ignored and the existing budget is authoritative. If the budget does not exist
however, it is atomically created along with the job that is bound to it, and
subsequent enqueues will see that budget.

> Using `create_with`:
> ```json
> {
>     "budgets": [
>         {"key": "image-service"},
>         {
>             "key": "notifications",
>             "cost": 5,
>             "create_with": {
>                 "allocaton": 50000,
>                 "strategy": {
>                     "type": "while_in_flight"
>                 }
>             }
>         }
>     ]
> }
> ```

## `GET /budgets` { #get-budgets }

Returns a list of all configured budgets.

### Responses { #get-budgets-response }

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
                <div><code>budgets</code> <em>required</em></div>
                <div><pre>array</pre></div>
            </td>
            <td>
                List of budgets configured on the server.
            </td>
        </tr>
        <tr>
            <td>
                <div><code>budgets[*].key</code> <em>required</em></div>
                <div><pre>string</pre></div>
            </td>
            <td>
                The identifier for this budget, used to bind jobs to it, and
                when calling other endpoints that operate on the budget.
            </td>
        </tr>
        <tr>
            <td>
                <div><code>budgets[*].allocation</code> <em>required</em></div>
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
                <div><code>budgets[*].strategy</code> <em>required</em></div>
                <div><pre>object</pre></div>
            </td>
            <td>
                Details of the specific strategy that is used to manage the
                tokens available under this budget.
            </td>
        </tr>
        <tr>
            <td>
                <div><code>budgets[*].strategy.type</code> <em>required</em></div>
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
                        no more than 5 jobs bound to this budget can be
                        in-flight at any given time.
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
                <div><code>budgets[*].strategy.duration_ms</code></div>
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
                <div><code>budgets[*].strategy.burst</code></div>
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
        <tr>
            <td>
                <div><code>budgets[*].created_at</code> <em>required</em></div>
                <div><pre>int64</pre></div>
            </td>
            <td>
                When this budget was created on the server. Unix timestamp in
                milliseconds.
            </td>
        </tr>
        <tr>
            <td>
                <div><code>budgets[*].updated_at</code> <em>required</em></div>
                <div><pre>int64</pre></div>
            </td>
            <td>
                When this budget was last updated. Unix timestamp in
                milliseconds.
            </td>
        </tr>
    </tbody>
</table>

#### `403` Forbidden

When the server is not configured with a [Pro license](https://zizq.io/pricing).

{{#include ./error-response.md}}

## `GET /budgets/{key}` { #get-budgets-key }

Fetch the named budget.

### Parameters { #get-budgets-key-parameters }

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
    </tbody>
</table>

### Responses { #get-budgets-key-response }

#### `200` OK

{{#include ./budget-response.md}}

#### `403` Forbidden

When the server is not configured with a [Pro license](https://zizq.io/pricing).

{{#include ./error-response.md}}

#### `404` Not Found

When the specified budget does not exist.

{{#include ./error-response.md}}

## `POST /budgets/{key}` { #post-budgets-key }

Create the named budget, or reject if the budget already exists.

### Parameters { #post-budgets-key-parameters }

{{#include ./budget-parameters.md}}

### Responses { #post-budgets-key-response }

#### `201` Created

{{#include ./budget-response.md}}

#### `403` Forbidden

When the server is not configured with a [Pro license](https://zizq.io/pricing).

{{#include ./error-response.md}}

#### `409` Conflict

When the specified budget already exists.

{{#include ./error-response.md}}

#### `422` Unprocessible Entity

When the specified budget is not semantically valid (e.g. invalid key).

{{#include ./error-response.md}}

## `PUT /budgets/{key}` { #put-budgets-key }

Create the named budget or overwrite it if it already exists.

### Parameters { #put-budgets-key-parameters }

{{#include ./budget-parameters.md}}

### Responses { #put-budgets-key-response }

#### `200` OK

{{#include ./budget-response.md}}

#### `403` Forbidden

When the server is not configured with a [Pro license](https://zizq.io/pricing).

{{#include ./error-response.md}}

#### `422` Unprocessible Entity

When the specified budget is not semantically valid (e.g. invalid key
or allocation below existing jobs bound to the budget).

{{#include ./error-response.md}}

## `PATCH /budgets/{key}` { #patch-budgets-key }

Update specified fields within the given budget. This operation is a
JSON merge patch. Omitted fields are left unchanged, fields with values
are updated, and fields set to `null` are cleared or reset to the default.

### Parameters { #patch-budgets-key-parameters }

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
        <tr>
            <td>
                <div><code>allocation</code></div>
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
                <div><code>strategy</code></div>
                <div><pre>object</pre></div>
            </td>
            <td>
                Details of the specific strategy that is used to manage the
                tokens available under this budget.
            </td>
        </tr>
        <tr>
            <td>
                <div><code>strategy.type</code></div>
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
                <div><code>strategy.duration_ms</code></div>
                <div><pre>int64</pre></div>
            </td>
            <td>
                Only for <code>time_based</code> strategies. Invalid for
                <code>while_in_flight</code>. Specifies the period of time in
                milliseconds over which a <code>time_based</code> rate limit is
                measured. For example, for an allocation of 1000 and a
                <code>duration_ms</code> of 60000, the rate limit is
                <code>1000/minute</code>.
            </td>
        </tr>
        <tr>
            <td>
                <div><code>strategy.burst</code></div>
                <div><pre>int32 | null</pre></div>
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

### Responses { #put-budgets-key-response }

#### `200` OK

{{#include ./budget-response.md}}

#### `403` Forbidden

When the server is not configured with a [Pro license](https://zizq.io/pricing).

{{#include ./error-response.md}}

#### `404` Not Found

When the specified job does not exist.

{{#include ./error-response.md}}

#### `422` Unprocessible Entity

When the specified budget is not semantically valid (e.g. invalid key
or allocation below existing jobs bound to the budget).

{{#include ./error-response.md}}

## `DELETE /budgets/{key}` { #delete-budgets-key }

Delete the named budget.

### Parameters { #delete-budgets-key-parameters }

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
    </tbody>
</table>

### Responses { #delete-budgets-key-response }

#### `204` No Content

No content is returned on successful deletion.

#### `403` Forbidden

When the server is not configured with a [Pro license](https://zizq.io/pricing).

{{#include ./error-response.md}}

#### `404` Not Found

When the specified budget does not exist.

{{#include ./error-response.md}}

#### `409` Conflict

When deleting the specified budget is not valid because one or more jobs
are still bound to it.

{{#include ./error-response.md}}

## Examples

### Create a `time_based` budget

> Request:
> ```sh
> http POST http://127.0.0.1:7890/budgets/image-service --raw '{
>     "allocation": 10000,
>     "strategy": {
>         "type": "time_based",
>         "duration_ms": 3600000
>     }
> }'
> ```

> Response:
> ```http
> HTTP/1.1 201 Created
> content-length: 151
> content-type: application/json
> date: Mon, 31 Aug 2026 22:35:27 GMT
> ```
> ```json
> {
>     "allocation": 10000,
>     "created_at": 1788215727072,
>     "key": "image-service",
>     "strategy": {
>         "duration_ms": 3600000,
>         "type": "time_based"
>     },
>     "updated_at": 1788215727072
> }
> ```

### Duplicate create rejected

> Request:
> ```sh
> http POST http://127.0.0.1:7890/budgets/image-service --raw '{
>     "allocation": 10000,
>     "strategy": {
>         "type": "time_based",
>         "duration_ms": 7200000
>     }
> }
> '
> ```

> Response:
> ```http
> HTTP/1.1 409 Conflict
> content-length: 49
> content-type: application/json
> date: Mon, 31 Aug 2026 22:37:37 GMT
> ```
> ```json
> {
>     "error": "budget 'image-service' already exists"
> }
> ```

### Replace an existing budget

Also creates it if it doesn't exist.

> Request:
> ```sh
> http PUT http://127.0.0.1:7890/budgets/image-service --raw '{
>     "allocation": 10000,
>     "strategy": {
>         "type": "time_based",
>         "duration_ms": 3600000
>     }
> }'
> ```

> Response:
> ```http
> HTTP/1.1 200 OK
> content-length: 151
> content-type: application/json
> date: Mon, 31 Aug 2026 22:40:00 GMT
> ```
> ```json
> {
>     "allocation": 10000,
>     "created_at": 1788215727072,
>     "key": "image-service",
>     "strategy": {
>         "duration_ms": 3600000,
>         "type": "time_based"
>     },
>     "updated_at": 1788216000273
> }
> ```

### Create a `while_in_flight` budget

> Request:
> ```sh
> http PUT http://127.0.0.1:7890/budgets/schema-bot --raw '{
>     "allocation": 10,
>     "strategy": {
>         "type": "while_in_flight"
>     }
> }'
> ```

> Response:
> ```http
> HTTP/1.1 200 OK
> content-length: 128
> content-type: application/json
> date: Mon, 31 Aug 2026 22:42:17 GMT
> ```
> ```json
> {
>     "allocation": 10,
>     "created_at": 1788216137108,
>     "key": "schema-bot",
>     "strategy": {
>         "type": "while_in_flight"
>     },
>     "updated_at": 1788216137108
> }
> ```

### Enqueue a job against multiple budgets

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

### Enqueue a job using `create_with`

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

### Patch an existing budget

> Request:
> ```sh
> http PATCH http://127.0.0.1:7890/budgets/image-service --raw '{
>     "strategy": {
>         "burst": 50000
>     }
> }'
> ```

> Response:
> ```http
> HTTP/1.1 200 OK
> content-length: 165
> content-type: application/json
> date: Mon, 31 Aug 2026 22:44:08 GMT
> ```
> ```json
> {
>     "allocation": 10000,
>     "created_at": 1788215727072,
>     "key": "image-service",
>     "strategy": {
>         "burst": 50000,
>         "duration_ms": 3600000,
>         "type": "time_based"
>     },
>     "updated_at": 1788216248745
> }
> ```

### List budgets

> Request:
> ```sh
> http GET http://127.0.0.1:7890/budgets
> ```

> Response:
> ```http
> HTTP/1.1 200 OK
> content-length: 308
> content-type: application/json
> date: Mon, 31 Aug 2026 22:45:42 GMT
> ```
> ```json
> {
>     "budgets": [
>         {
>             "allocation": 10000,
>             "created_at": 1788215727072,
>             "key": "image-service",
>             "strategy": {
>                 "burst": 50000,
>                 "duration_ms": 3600000,
>                 "type": "time_based"
>             },
>             "updated_at": 1788216248745
>         },
>         {
>             "allocation": 10,
>             "created_at": 1788216137108,
>             "key": "schema-bot",
>             "strategy": {
>                 "type": "while_in_flight"
>             },
>             "updated_at": 1788216137108
>         }
>     ]
> }
> ```

### Delete a budget

> Request:
> ```sh
> http DELETE http://127.0.0.1:7890/budgets/schema-bot
> ```

> Response:
> ```http
> HTTP/1.1 204 No Content
> date: Mon, 31 Aug 2026 22:46:57 GMT
> ```

### Delete a referenced budget rejected

> Request:
> ```sh
> http DELETE http://127.0.0.1:7890/budgets/image-service
> ```

> Response:
> ```http
> HTTP/1.1 409 Conflict
> content-length: 128
> content-type: application/json
> date: Mon, 31 Aug 2026 22:54:27 GMT
> ```
> ```json
> {
>     "error": "budget 'image-service' is referenced by 1 unfinished job. Delete them or wait for them to finish before deleting it."
> }
> ```

### Reduce a budget below a job's cost rejected


> Request:
> ```sh
> http PATCH http://127.0.0.1:7890/budgets/schema-bot --raw '{
>     "allocation": 1
> }'
> ```

> Response:
> ```http
> HTTP/1.1 422 Unprocessable Entity
> content-length: 151
> content-type: application/json
> date: Mon, 31 Aug 2026 22:56:44 GMT
> ```
> ```json
> {
>     "error": "budget 'schema-bot' cannot allocate 1: 1 unfinished job draw up to 2 from it. Raise the allocation, delete them, or wait for them to drain."
> }
> ```
