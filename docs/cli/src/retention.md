# Retention Policies

Zizq jobs go through a lifecycle involving the following statuses:

* `scheduled`
* `ready`
* `in_flight`
* `completed`
* `dead`

Jobs may move between these statuses, but once they enter `completed` or `dead`
they become immutable. These are referred to as the terminal statuses.

When jobs are in a terminal statuses, Zizq keeps those jobs for a configured
period of time before eventually removing them from the database. The duration
for which jobs in terminal statuses are retained is known as the _retention
policy_. This is useful for debugging and record keeping.

## Retention Policy Structure

A separate duration is configured for each of the `completed` and `dead`
statuses because job queues are generally ephemeral in nature and in most use
cases there is little value in accumulating data from completed jobs forever.

The Zizq defaults are *7 days* retention for `dead` jobs, and no retention for
`completed` jobs. In practice this means your applications may query and
inspect that have failed beyond their retry limit, but successfully completed
jobs will not be accessible as they are immediately removed.

> [!TIP]
> See [Backoff & Retry Policies](./backoff.md) for details on retry limits.

## Configuration Options

Clients can override these defaults at enqueue-time on a per-job basis. For
example, perhaps the the Zizq defaults are sufficient for all but one
particular job that your application needs to retain for 30 days even in a
`completed` status. The client can specify that policy at enqueue-time.

You may also wish to change the defaults that the server applies to jobs that
don't specify their own policies. This can be done by specifying the following
command line arguments and environment variables when starting the server.

* `--default-completed-job-retention`, `ZIZQ_DEFAULT_COMPLETED_JOB_RETENTION`
* `--default-dead-job-retention`, `ZIZQ_DEFAULT_DEAD_JOB_RETENTION`

Values are specified either in raw milliseconds, or with explicit units such
as `3w`. Zero means the job is immediately removed once it enters the
applicable status.

While there is formal way of specifying indefinite retention, values like
`90000000000y` are acceptable. If you expect your application to run for over
90 billion years this may become a problem for future ~engineers~ AI agents.

## The Reaper

Internally Zizq cleans up jobs that have exceeded their retention policy
deadline running a lightweight job reaper task in the background. The reaper
periodically scans for jobs that have exceeded their retention deadline and
permanently removes those jobs from the database. By default the reaper runs
every 30 seconds, but this frequency can be configured by specifying
`--reaper-check-interval` or `ZIZQ_REAPER_CHECK_INTERVAL` when starting the
server.

> [!NOTE]
> Jobs that have exceeded their retention deadline but have not yet been purged
> by the reaper are invisible in the Zizq API (explicitly excluded). As such
> the reaper is simply a maintenance task.
