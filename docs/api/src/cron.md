# Cron Scheduling

> [!NOTE]
> These endpoints are available in both `application/json` and
> `application/msgpack` formats.

> [!NOTE]
> This feature requires a [Pro license](https://zizq.io/pricing).

The Zizq server supports managing recurring job schedules, through which jobs
are enqueued on a cadence described by time zone-aware cron expressions.
Workers process these jobs like any other jobs. There is no requirement to
allocate a dedicated worker for the purpose of processing recurring jobs.

Collections of _cron entries_ are grouped together into one or more
_cron groups_. Entire groups, and individual entries can be paused and resumed.

The API is designed to facilitate clients performing a single `PUT` operation
to define (or redefine) one or more schedules at application startup time. Many
clients can all perform this same operation safely. The result is idempotent.
Other endpoints are available to support more granular methods of updating cron
schedules.

## `GET /crons` { #get-crons }

Returns a list of available cron groups.

### Responses { #get-crons-response }

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
                <div><code>crons</code> <em>required</em></div>
                <div><pre>array</pre></div>
            </td>
            <td>
                Array of cron group names (strings).
            </td>
        </tr>
    </tbody>
</table>

#### `403` Forbidden

Returned when the server is not configured with a pro license.

{{#include ./error-response.md}}

## `GET /crons/{group}` { #get-crons-group }

Return the definition for an existing cron group.

### Parameters { #get-crons-group-parameters }

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
                <div><code>group</code> <em>path</em></div>
                <div><pre>string</pre></div>
            </td>
            <td>
                Application-defined name that identifies the cron group.
            </td>
        </tr>
    </tbody>
</table>

### Responses { #get-crons-group-response }

#### `200` OK

{{#include ./cron-group-response.md}}

#### `403` Forbidden

Returned when the server is not configured with a pro license.

{{#include ./error-response.md}}

#### `404` Not Found

The specified cron group does not exist.

{{#include ./error-response.md}}

## `PUT /crons/{group}` { #put-crons-group }

Define a cron group (a collection of cron entries) on the server. This
operation is atomic and idempotent. It is intended for use in application
startup code. Multiple processes may all call the same endpoint with the same
schedule data unconditionally and Zizq is smart enough to handle it safely.

* If the schedule does not yet exist, it is created.
* If the schedule exists, it is updated in place.
* Entries that have not changed are retained, along with all state.
* Entries that have been removed are deleted.
* Entries that have been modified are replaced.

### Parameters { #put-crons-group-parameters }

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
                <div><code>group</code> <em>path</em></div>
                <div><pre>string</pre></div>
            </td>
            <td>
                Application-defined name that identifies the cron group.
            </td>
        </tr>
        <tr>
            <td>
                <div><code>paused</code></div>
                <div><pre>boolean</pre></div>
            </td>
            <td>
                Whether or not this cron group should be paused. When paused
                Zizq sets the <code>paused_at</code> timestamp on the group and
                stops enqueueing jobs within the group, though the scheduler
                does continue advancing the time. When the value was previously
                <code>false</code> and is set to <code>true</code> Zizq sets
                the <code>resumed_at</code> timestamp on the group and
                re-commences enqueueing jobs for all unpaused entries in the
                group.
            </td>
        </tr>
        <tr>
            <td>
                <div><code>entries</code> <em>required</em></div>
                <div><pre>array</pre></div>
            </td>
            <td>
                List of entries that will be present within the cron group.
                Each entry is uniquely identified within the group by an
                application-defined name.
            </td>
        </tr>
        <tr>
            <td>
                <div><code>entries[*].name</code> <em>required</em></div>
                <div><pre>string</pre></div>
            </td>
            <td>
                Application-defined identifier for this entry within the group.
            </td>
        </tr>
        <tr>
            <td>
                <div><code>entries[*].expression</code> <em>required</em></div>
                <div><pre>string</pre></div>
            </td>
            <td>
                Either a 5-field or a 6-field cron expression (e.g.
                <code>*/15 0-6 * * *</code>). 6-field expressions enable
                second-level precision.
            </td>
        </tr>
        <tr>
            <td>
                <div><code>entries[*].timezone</code></div>
                <div><pre>string</pre></div>
            </td>
            <td>
                Optional IANA timezone identifier in which the cron expression
                should be evaluated. When not specified, the expression is
                evaluated in the system timezone where the Zizq server is
                running.
            </td>
        </tr>
        <tr>
            <td>
                <div><code>entries[*].paused</code></div>
                <div><pre>boolean</pre></div>
            </td>
            <td>
                Whether or not this entry should be paused. When paused
                Zizq sets the <code>paused_at</code> timestamp on the entry and
                stops enqueueing jobs for that entry, though the scheduler
                does continue advancing the time. When the value was previously
                <code>false</code> and is set to <code>true</code> Zizq sets
                the <code>resumed_at</code> timestamp on the entry and
                re-commences enqueueing jobs for it.
            </td>
        </tr>
        <tr>
            <td>
                <div><code>entries[*].job</code> <em>required</em></div>
                <div><pre>object</pre></div>
            </td>
            <td>
                Details equivalent to those used for enqueueing jobs normally,
                to be used when the scheduler enqueues jobs for this entry.
            </td>
        </tr>
        <tr>
            <td>
                <div><code>entries[*].job.queue</code> <em>required</em></div>
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
                <div><code>entries[*].job.type</code> <em>required</em></div>
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
                <div><code>entries[*].job.payload</code> <em>required</em></div>
                <div><pre>object</pre></div>
            </td>
            <td>
                Any valid JSON type to be associated with the job, used by the
                worker when handling the job.
            </td>
        </tr>
        <tr>
            <td>
                <div><code>entries[*].job.priority</code></div>
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
                <div><code>entries[*].job.unique_key</code></div>
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
                <div><code>entries[*].job.unique_while</code></div>
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
                <div><code>entries[*].job.backoff</code></div>
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
                <div><code>entries[*].job.backoff.base_ms</code></div>
                <div><pre>int32</pre></div>
            </td>
            <td>
                The minimum delay in milliseconds between job retries.
            </td>
        </tr>
        <tr>
            <td>
                <div><code>entries[*].job.backoff.exponent</code></div>
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
                <div><code>entries[*].job.backoff.jitter_ms</code></div>
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
                <div><code>entries[*].job.retry_limit</code></div>
                <div><pre>int32</pre></div>
            </td>
            <td>
                Overrides the severs default retry limit for this job. Once
                this limit is reached, the server marks the job <code>dead</code>.
            </td>
        </tr>
        <tr>
            <td>
                <div><code>entries[*].job.retention</code></div>
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
                <div><code>entries[*].job.retention.dead_ms</code></div>
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
                <div><code>entries[*].job.retention.completed_ms</code></div>
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

### Responses { #put-crons-group-response }

#### `200` OK

{{#include ./cron-group-response.md}}

#### `400` Bad Request

Invalid inputs were provided.

{{#include ./error-response.md}}

#### `403` Forbidden

Returned when the server is not configured with a pro license.

{{#include ./error-response.md}}

## `PATCH /crons/{group}` { #patch-crons-group }

Update an existing cron group. Currently only used to pause/resume the group.

### Parameters { #patch-crons-group-parameters }

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
                <div><code>group</code> <em>path</em></div>
                <div><pre>string</pre></div>
            </td>
            <td>
                Application-defined name that identifies the cron group.
            </td>
        </tr>
        <tr>
            <td>
                <div><code>paused</code></div>
                <div><pre>boolean</pre></div>
            </td>
            <td>
                Whether or not this cron group should be paused. When paused
                Zizq sets the <code>paused_at</code> timestamp on the group and
                stops enqueueing jobs within the group, though the scheduler
                does continue advancing the time. When the value was previously
                <code>false</code> and is set to <code>true</code> Zizq sets
                the <code>resumed_at</code> timestamp on the group and
                re-commences enqueueing jobs for all unpaused entries in the
                group.
            </td>
        </tr>
    </tbody>
</table>

### Responses { #patch-crons-group-response }

#### `200` OK

{{#include ./cron-group-response.md}}

#### `400` Bad Request

Invalid inputs were provided.

{{#include ./error-response.md}}

#### `403` Forbidden

Returned when the server is not configured with a pro license.

{{#include ./error-response.md}}

#### `404` Not Found

The specified cron group does not exist.

{{#include ./error-response.md}}

## `DELETE /crons/{group}` { #delete-crons-group }

Delete an entire cron group and its entries.

### Parameters { #delete-crons-group-parameters }

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
                <div><code>group</code> <em>path</em></div>
                <div><pre>string</pre></div>
            </td>
            <td>
                Application-defined name that identifies the cron group.
            </td>
        </tr>
    </tbody>
</table>

### Responses { #delete-crons-group-response }

#### `204` No Content

No response body included for a successful response.

#### `403` Forbidden

Returned when the server is not configured with a pro license.

{{#include ./error-response.md}}

#### `404` Not Found

The specified cron group does not exist.

{{#include ./error-response.md}}

## `GET /crons/{group}/entries/{entry}` { #get-crons-group-entry }

Get the definition of an individual entry within a cron group.

### Parameters { #get-crons-group-entry-parameters }

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
                <div><code>group</code> <em>path</em></div>
                <div><pre>string</pre></div>
            </td>
            <td>
                Application-defined name that identifies the cron group.
            </td>
        </tr>
        <tr>
            <td>
                <div><code>entry</code> <em>path</em></div>
                <div><pre>string</pre></div>
            </td>
            <td>
                Application-defined name that identifies the entry within the
                group.
            </td>
        </tr>
    </tbody>
</table>

### Responses { #get-crons-group-entry-response }

#### `200` OK

{{#include ./cron-group-entry-response.md}}

#### `403` Forbidden

Returned when the server is not configured with a pro license.

{{#include ./error-response.md}}

#### `404` Not Found

The specified entry does not exist.

{{#include ./error-response.md}}

## `PUT /crons/{group}/entries/{entry}` { #put-crons-group-entry }

Define an entry within a cron group on the server. The group does not need to
pre-exist — it will be automatically created when the first entry is created
within it. This operation is atomic and idempotent when given the same entry.

* Entries that have not changed are retained, along with all state.
* Entries that have been modified are replaced.

### Parameters { #put-crons-group-entry-parameters }

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
                <div><code>group</code> <em>path</em></div>
                <div><pre>string</pre></div>
            </td>
            <td>
                Application-defined name that identifies the cron group.
            </td>
        </tr>
        <tr>
            <td>
                <div><code>entry</code> <em>path</em></div>
                <div><pre>string</pre></div>
            </td>
            <td>
                Application-defined name that identifies the entry within the
                group.
            </td>
        </tr>
        <tr>
            <td>
                <div><code>name</code> <em>required</em></div>
                <div><pre>string</pre></div>
            </td>
            <td>
                Application-defined identifier for this entry within the group.
                This should be the same as the <code>entry</code> value in the
                path.
            </td>
        </tr>
        <tr>
            <td>
                <div><code>expression</code> <em>required</em></div>
                <div><pre>string</pre></div>
            </td>
            <td>
                Either a 5-field or a 6-field cron expression (e.g.
                <code>*/15 0-6 * * *</code>). 6-field expressions enable
                second-level precision.
            </td>
        </tr>
        <tr>
            <td>
                <div><code>timezone</code></div>
                <div><pre>string</pre></div>
            </td>
            <td>
                Optional IANA timezone identifier in which the cron expression
                should be evaluated. When not specified, the expression is
                evaluated in the system timezone where the Zizq server is
                running.
            </td>
        </tr>
        <tr>
            <td>
                <div><code>paused</code></div>
                <div><pre>boolean</pre></div>
            </td>
            <td>
                Whether or not this entry should be paused. When paused
                Zizq sets the <code>paused_at</code> timestamp on the entry and
                stops enqueueing jobs for that entry, though the scheduler
                does continue advancing the time. When the value was previously
                <code>false</code> and is set to <code>true</code> Zizq sets
                the <code>resumed_at</code> timestamp on the entry and
                re-commences enqueueing jobs for it.
            </td>
        </tr>
        <tr>
            <td>
                <div><code>job</code> <em>required</em></div>
                <div><pre>object</pre></div>
            </td>
            <td>
                Details equivalent to those used for enqueueing jobs normally,
                to be used when the scheduler enqueues jobs for this entry.
            </td>
        </tr>
        <tr>
            <td>
                <div><code>job.queue</code> <em>required</em></div>
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
                <div><code>job.type</code> <em>required</em></div>
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
                <div><code>job.payload</code> <em>required</em></div>
                <div><pre>object</pre></div>
            </td>
            <td>
                Any valid JSON type to be associated with the job, used by the
                worker when handling the job.
            </td>
        </tr>
        <tr>
            <td>
                <div><code>job.priority</code></div>
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
                <div><code>job.unique_key</code></div>
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
                <div><code>job.unique_while</code></div>
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
                <div><code>job.backoff</code></div>
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
                <div><code>job.backoff.base_ms</code></div>
                <div><pre>int32</pre></div>
            </td>
            <td>
                The minimum delay in milliseconds between job retries.
            </td>
        </tr>
        <tr>
            <td>
                <div><code>job.backoff.exponent</code></div>
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
                <div><code>job.backoff.jitter_ms</code></div>
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
                <div><code>job.retry_limit</code></div>
                <div><pre>int32</pre></div>
            </td>
            <td>
                Overrides the severs default retry limit for this job. Once
                this limit is reached, the server marks the job <code>dead</code>.
            </td>
        </tr>
        <tr>
            <td>
                <div><code>job.retention</code></div>
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
                <div><code>job.retention.dead_ms</code></div>
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
                <div><code>job.retention.completed_ms</code></div>
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

### Responses { #put-crons-group-entry-response }

#### `200` OK

{{#include ./cron-group-entry-response.md}}

#### `400` Bad Request

Invalid inputs were provided.

{{#include ./error-response.md}}

#### `403` Forbidden

Returned when the server is not configured with a pro license.

{{#include ./error-response.md}}

## `PATCH /crons/{group}/entries/{entry}` { #patch-crons-group-entry }

Update an existing entry within a cron group. Currently only used to
pause/resume the individual entry.

### Parameters { #patch-crons-group-entry-parameters }

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
                <div><code>group</code> <em>path</em></div>
                <div><pre>string</pre></div>
            </td>
            <td>
                Application-defined name that identifies the cron group.
            </td>
        </tr>
        <tr>
            <td>
                <div><code>entry</code> <em>path</em></div>
                <div><pre>string</pre></div>
            </td>
            <td>
                Application-defined name that identifies the entry within the
                group.
            </td>
        </tr>
        <tr>
            <td>
                <div><code>paused</code></div>
                <div><pre>boolean</pre></div>
            </td>
            <td>
                Whether or not this entry should be paused. When paused
                Zizq sets the <code>paused_at</code> timestamp on the entry and
                stops enqueueing jobs for that entry, though the scheduler
                does continue advancing the time. When the value was previously
                <code>false</code> and is set to <code>true</code> Zizq sets
                the <code>resumed_at</code> timestamp on the entry and
                re-commences enqueueing jobs for it.
            </td>
        </tr>
    </tbody>
</table>

### Responses { #patch-crons-group-entry-response }

#### `200` OK

{{#include ./cron-group-entry-response.md}}

#### `400` Bad Request

Invalid inputs were provided.

{{#include ./error-response.md}}

#### `403` Forbidden

Returned when the server is not configured with a pro license.

{{#include ./error-response.md}}

#### `404` Not Found

The specified entry does not exist.

{{#include ./error-response.md}}

## `DELETE /crons/{group}/entries/{entry}` { #delete-crons-group-entry }

Delete an individual entry from a cron group.

### Parameters { #delete-crons-group-entry-parameters }

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
                <div><code>group</code> <em>path</em></div>
                <div><pre>string</pre></div>
            </td>
            <td>
                Application-defined name that identifies the cron group.
            </td>
        </tr>
        <tr>
            <td>
                <div><code>entry</code> <em>path</em></div>
                <div><pre>string</pre></div>
            </td>
            <td>
                Application-defined name that identifies the entry within the
                group.
            </td>
        </tr>
    </tbody>
</table>

### Responses { #delete-crons-group-entry-response }

#### `204` No Content

No response body included for a successful response.

#### `403` Forbidden

Returned when the server is not configured with a pro license.

{{#include ./error-response.md}}

#### `404` Not Found

The specified entry does not exist.

{{#include ./error-response.md}}

## Examples

### List Cron Groups

```shell
http 127.0.0.1:7890/crons
```

```http
HTTP/1.1 200 OK
content-length: 31
content-type: application/json
date: Tue, 05 May 2026 02:54:26 GMT
```
```json
{
    "crons": [
        "group-1",
        "group-2"
    ]
}
```

### Define or Redefine a Cron Group

This makes sense to be a step in application startup.

```shell
http PUT 127.0.0.1:7890/crons/default <<'JSON'
{
    "entries": [
        {
            "name": "refresh_data_warehouse",
            "expression": "0 6 * * *",
            "timezone": "Europe/Rome",
            "job": {
                "queue": "data_warehouse",
                "type": "refresh_data_warehouse",
                "priority": 1000,
                "payload": {
                    "incremental": true
                }
            }
        },
        {
            "name": "expire_access_tokens",
            "expression": "*/15 * * * *",
            "job": {
                "queue": "identity",
                "type": "expire_access_tokens",
                "payload": {},
                "unique_key": "expire_access_tokens",
                "unique_while": "active"
            }
        }
    ]
}
JSON
```

```http
HTTP/1.1 200 OK
content-length: 606
content-type: application/json
date: Tue, 05 May 2026 04:00:07 GMT
```
```json
{
    "entries": [
        {
            "expression": "0 6 * * *",
            "job": {
                "payload": {
                    "incremental": true
                },
                "priority": 1000,
                "queue": "data_warehouse",
                "type": "refresh_data_warehouse"
            },
            "last_enqueue_at": 1777953600001,
            "name": "refresh_data_warehouse",
            "next_enqueue_at": 1778040000000,
            "paused": false,
            "timezone": "Europe/Rome"
        },
        {
            "expression": "*/15 * * * *",
            "job": {
                "payload": {},
                "priority": 32768,
                "queue": "identity",
                "type": "expire_access_tokens",
                "unique_key": "expire_access_tokens",
                "unique_while": "active"
            },
            "last_enqueue_at": 1777953600002,
            "name": "expire_access_tokens",
            "next_enqueue_at": 1777954500000,
            "paused": false
        }
    ],
    "name": "default",
    "paused": false
}
```

### Pause an Entry Within a Schedule

```shell
http PATCH 127.0.0.1:7890/crons/default/entries/refresh_data_warehouse <<'JSON'
{"paused": true}
JSON
```

```http
HTTP/1.1 200 OK
content-length: 299
content-type: application/json
date: Tue, 05 May 2026 04:02:43 GMT
```
```json
{
    "expression": "0 6 * * *",
    "job": {
        "payload": {
            "incremental": true
        },
        "priority": 1000,
        "queue": "data_warehouse",
        "type": "refresh_data_warehouse"
    },
    "last_enqueue_at": 1777953600001,
    "name": "refresh_data_warehouse",
    "next_enqueue_at": 1778040000000,
    "paused": true,
    "paused_at": 1777953763596,
    "timezone": "Europe/Rome"
}
```

### Resume an Entry Within a Schedule

```shell
http PATCH 127.0.0.1:7890/crons/default/entries/refresh_data_warehouse <<'JSON'
{"paused": false}
JSON
```

```http
HTTP/1.1 200 OK
content-length: 327
content-type: application/json
date: Tue, 05 May 2026 04:03:40 GMT
```
```json
{
    "expression": "0 6 * * *",
    "job": {
        "payload": {
            "incremental": true
        },
        "priority": 1000,
        "queue": "data_warehouse",
        "type": "refresh_data_warehouse"
    },
    "last_enqueue_at": 1777953600001,
    "name": "refresh_data_warehouse",
    "next_enqueue_at": 1778040000000,
    "paused": false,
    "paused_at": 1777953763596,
    "resumed_at": 1777953820667,
    "timezone": "Europe/Rome"
}
```

### Delete a Schedule

```shell
http DELETE 127.0.0.1:7890/crons/default
```

```http
HTTP/1.1 204 No Content
date: Tue, 05 May 2026 04:04:37 GMT
```
