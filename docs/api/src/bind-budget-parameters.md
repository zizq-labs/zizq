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
                <div><code>cost</code></div>
                <div><pre>int32</pre></div>
            </td>
            <td>
                The number of tokens the job takes from the budget. Defaults
                to 1.
            </td>
        </tr>
        <tr>
            <td>
                <div><code>create_with</code></div>
                <div><pre>object</pre></div>
            </td>
            <td>
                Specification from which to create this budget atomically
                with the binding if it does not already exist. Without this, the
                budget must exist or a 422 response will be returned. Does not
                overwrite any existing budget.
            </td>
        </tr>
        <tr>
            <td>
                <div><code>create_with.allocation</code> <em>required</em></div>
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
                <div><code>create_with.strategy</code> <em>required</em></div>
                <div><pre>object</pre></div>
            </td>
            <td>
                Details of the specific strategy that is used to manage the
                tokens available under this budget.
            </td>
        </tr>
        <tr>
            <td>
                <div><code>create_with.strategy.type</code> <em>required</em></div>
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
                <div><code>create_with.strategy.duration_ms</code></div>
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
                <div><code>create_with.strategy.burst</code></div>
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
