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
                <div><code>id</code> <em>required</em></div>
                <div><pre>string</pre></div>
            </td>
            <td>
                Unique time-sequenced job ID assigned by the server.
            </td>
        </tr>
        <tr>
            <td>
                <div><code>queue</code> <em>required</em></div>
                <div><pre>string</pre></div>
            </td>
            <td>
                Arbitrary queue name to which the job is assigned
            </td>
        </tr>
        <tr>
            <td>
                <div><code>type</code> <em>required</em></div>
                <div><pre>string</pre></div>
            </td>
            <td>
                Job type known to your application
            </td>
        </tr>
        <tr>
            <td>
                <div><code>priority</code> <em>required</em></div>
                <div><pre>int16</pre></div>
            </td>
            <td>
                Numeric priority for the job. Lower values are processed first
                (higher priority). The default value is <code>32768</code>.
            </td>
        </tr>
        <tr>
            <td>
                <div><code>status</code> <em>required</em></div>
                <div><pre>string</pre></div>
            </td>
            <td>
                The job status on the server. One of:
                <ul>
                    <li><code>scheduled</code></li>
                    <li><code>ready</code></li>
                    <li><code>in_flight</code></li>
                    <li><code>completed</code></li>
                    <li><code>dead</code></li>
                </ul>
                Actual statuses shown will be context-dependent.
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
                scope within which the job is considered unique.
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
                        Conflicting jobs will not be enqueued while this job is
                        in the <code>scheduled</code> or <code>ready</code>
                        statuses.
                    </dd>
                    <dt><code>active</code></dt>
                    <dd>
                        Conflicting jobs will not be enqueued while this job is
                        in the <code>scheduled</code>, <code>ready</code> or
                        <code>in_flight</code> statuses.
                    </dd>
                    <dt><code>exists</code></dt>
                    <dd>
                        Conflicting jobs will not be enqueued while this job
                        exists in any status (i.e. until the job is reaped,
                        according to the retention policy).
                    </dd>
                </dl>
                The default scope is <code>queued</code>.
            </td>
        </tr>
        <tr>
            <td>
                <div><code>payload</code> <em>required</em></div>
                <div><pre>object</pre></div>
            </td>
            <td>
                Any JSON-serializable type to be processed by your application.
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
                rather than creating separate pending jobs. All three inner
                fields are required when this field is set. Mutually exclusive
                with <code>unique_key</code> — supplying both returns
                <code>400 Bad Request</code>.
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
            </td>
        </tr>
        <tr>
            <td>
                <div><code>budgets[*].key</code> <em>required</em></div>
                <div><pre>string</pre></div>
            </td>
            <td>
                The identifier for the budget.
            </td>
        </tr>
        <tr>
            <td>
                <div><code>budgets[*].cost</code> <em>required</em></div>
                <div><pre>int32</pre></div>
            </td>
            <td>
                The number of tokens this job takes from the budget.
            </td>
        </tr>
        <tr>
            <td>
                <div><code>ready_at</code> <em>required</em></div>
                <div><pre>int64</pre></div>
            </td>
            <td>
                The timestamp at which this job is ready to be dequeued by
                workers.
            </td>
        </tr>
        <tr>
            <td>
                <div><code>attempts</code> <em>required</em></div>
                <div><pre>int32</pre></div>
            </td>
            <td>
                The number of times this job has been previously attempted
                (starts at zero).
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
                <div><code>budgets</code></div>
                <div><pre>array</pre></div>
            </td>
            <td>
                Array of <a href="./rate-limiting.md">budget bindings</a>
                used to control concurrency and/or rate limiting of dispatched
                jobs.
            </td>
        </tr>
        <tr>
            <td>
                <div><code>budgets[*].key</code> <em>required</em></div>
                <div><pre>string</pre></div>
            </td>
            <td>
                The identifier for the budget.
            </td>
        </tr>
        <tr>
            <td>
                <div><code>budgets[*].cost</code></div>
                <div><pre>int32</pre> <em>required</em></div>
            </td>
            <td>
                The number of tokens this job takes from the budget.
            </td>
        </tr>
    </tbody>
</table>

