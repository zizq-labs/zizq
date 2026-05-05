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
                <div><code>name</code> <em>required</em></div>
                <div><pre>string</pre></div>
            </td>
            <td>
                Application-defined name that identifies the cron group.
            </td>
        </tr>
        <tr>
            <td>
                <div><code>paused</code> <em>required</em></div>
                <div><pre>boolean</pre></div>
            </td>
            <td>
                Whether or not this cron group is currently paused. When paused
                Zizq stops enqueueing jobs within the group, though the
                scheduler does continue advancing the time. Check the values of
                <code>paused_at</code> and <code>resumed_at</code> for details
                on when the group was last paused/resumed.
            </td>
        </tr>
        <tr>
            <td>
                <div><code>paused_at</code></div>
                <div><pre>int64</pre></div>
            </td>
            <td>
                Timestamp indicating when this group was marked paused. Resets
                every time the queue is paused again.
            </td>
        </tr>
        <tr>
            <td>
                <div><code>resumed_at</code></div>
                <div><pre>int64</pre></div>
            </td>
            <td>
                Timestamp indicating when this group was unpaused. Resets every
                time the queue is paused again.
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
                second-level precision. See the timestamps in
                <code>next_enqueue_at</code> and <code>last_enqueue_at</code>
                for details on future/historical scheduling.
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
                Whether or not this entry is currently paused. When paused
                Zizq stops enqueueing jobs for that entry, though the scheduler
                does continue advancing the time. See the values of
                <code>paused_at</code> and <code>resumed_at</code> on the entry
                for details on when the entry was last paused/resumed.
            </td>
        </tr>
        <tr>
            <td>
                <div><code>entries[*].paused_at</code></div>
                <div><pre>int64</pre></div>
            </td>
            <td>
                Timestamp indicating when this entry was paused. Resets every
                time the entry is paused again.
            </td>
        </tr>
        <tr>
            <td>
                <div><code>entries[*].resumed_at</code></div>
                <div><pre>int64</pre></div>
            </td>
            <td>
                Timestamp indicating when this entry was unpaused. Resets every
                time the entry is paused again.
            </td>
        </tr>
        <tr>
            <td>
                <div><code>entries[*].next_enqueue_at</code></div>
                <div><pre>int64</pre></div>
            </td>
            <td>
                Timestamp indicating when this entry is next due.
            </td>
        </tr>
        <tr>
            <td>
                <div><code>entries[*].last_enqueue_at</code></div>
                <div><pre>int64</pre></div>
            </td>
            <td>
                Timestamp indicating when this entry was last enqueued.
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
                Arbitrary queue name to which the job is assigned.
            </td>
        </tr>
        <tr>
            <td>
                <div><code>entries[*].job.type</code> <em>required</em></div>
                <div><pre>string</pre></div>
            </td>
            <td>
                Job type known to your application.
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
