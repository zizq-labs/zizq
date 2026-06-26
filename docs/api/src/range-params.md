<tr>
    <td>
        <div><code>priority</code> <em>query</em></div>
        <div><pre>range</pre></div>
    </td>
    <td>
        Optional inclusive range filter on the job's priority. Lower
        numbers are higher priority.
    </td>
</tr>
<tr>
    <td>
        <div><code>ready_at</code> <em>query</em></div>
        <div><pre>range</pre></div>
    </td>
    <td>
        Optional inclusive range filter on the job's <code>ready_at</code>
        timestamp (milliseconds since the Unix epoch).
    </td>
</tr>
<tr>
    <td>
        <div><code>attempts</code> <em>query</em></div>
        <div><pre>range</pre></div>
    </td>
    <td>
        Optional inclusive range filter on the job's failure count.
        For example <code>0</code> selects jobs that have never failed,
        <code>1..</code> selects anything that has failed at least once.
    </td>
</tr>
