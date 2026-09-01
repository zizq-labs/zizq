<tr>
    <td>
        <div><code>id</code> <em>query</em></div>
        <div><pre>string</pre></div>
    </td>
    <td>
        Optional comma-separated list of job IDs to include.
    </td>
</tr>
<tr>
    <td>
        <div><code>queue</code> <em>query</em></div>
        <div><pre>string</pre></div>
    </td>
    <td>
        Optional comma-separated list of queue names to include. Defaults
        to <em>all queues</em>.
    </td>
</tr>
<tr>
    <td>
        <div><code>type</code> <em>query</em></div>
        <div><pre>string</pre></div>
    </td>
    <td>
        Optional comma-separated list of job types to include. Defaults
        to <em>all types</em>.
    </td>
</tr>
<tr>
    <td>
        <div><code>status</code> <em>query</em></div>
        <div><pre>string</pre></div>
    </td>
    <td>
        Optional comma-separated list of job statuses to include. Defaults
        to <em>all statuses</em>.
    </td>
</tr>
{{#include ./range-params.md}}
<tr>
    <td>
        <div><code>budgets.key</code> <em>query</em></div>
        <div><pre>string</pre></div>
    </td>
    <td>
        Optional comma-separated list of
        <a href="./rate-limiting.md">budget keys</a> to which jobs are
        bound. Jobs matching <em>any</em> of the keys are included.
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
