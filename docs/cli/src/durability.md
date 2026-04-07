# Durability

> [!TIP]
> Unless you really understand what you are doing, stick with the defaults.
> They are fast and high durability for most uses cases.

Zizq is implemented on top of a LSM database that writes a Write Ahead Log, or
a journal file any time data is modified. This ensures changes are _durable_
so that in the case of a crash any data that was committed will still be in the
database. The way the WAL is written however makes a direct trade-off between
pure durability and performance.

## Commit Modes

By default Zizq writes to the WAL on every commit (i.e. before any success
response from the API), but it only flushes that WAL write to OS buffers, not
completely to disk. This is how most filesystem operations are performed in
a typical application. The OS will ensure the changes are flushed to disk
asynchronously because directly flushing to disk on every write is slow, even
on modern SSDs.

There are two possible commit modes in Zizq: `buffered` (fast, OS cache) and
`fsync` (slow, to-disk). The default mode is `buffered` which is very fast.

In practice this default means commits are guaranteed to be durable even if the
Zizq server crashes because as long as the OS itself is still running, the WAL
will be flushed to disk by the OS. However, in the case of sudden power loss
for example that most recent WAL write could be lost, meaning that data may not
be present when the system resarts.

## Configuration Options

The following command line arguments and environment variables are used to
adjust how durable writes to Zizq are:

* `--default-commit-mode`, `ZIZQ_DEFAULT_COMMIT_MODE`
* `--enqueue-commit-mode`, `ZIZQ_ENQUEUE_COMMIT_MODE`

Possible values are `buffered` (default) or `fsync`. The default commit mode
affects _all write operations_. The enqueue commit mode overrides the default
mode specifically for _enqueue operations_.

It is possible to configure Zizq to always fsync the WAL to disk on every
commit, which means that even in the case of sudden power loss that successful
commit is truly durable. When the system restarts that data will be present in
the Zizq database. However this guarantee comes at a _significant_ cost and in
most uses cases for a job queue is unnecessary.

> [!WARNING]
> Only enable this mode if you understand the performance trade-off. Expect
> Zizq to perform ~5-10x slower than the default mode. If throughput is not a
> major concern for your application this may be ok.

``` shell
zizq serve --default-commit-mode fsync
```

It is also possible to configure Zizq to fsync _enqueues_ to disk immediately
while keeing dequeues and other write operations fast by only flushing to OS
buffers. This is a reasonable middle ground where enqueues will be slower, but
workers can still get high dequeue throughput. In practice this means jobs go
from being _guaranteed delivery_ as long as the OS keeps running, to guaranteed
delivery as long as the API returned a success response, but if e.g. a worker
had successfully processed a job and the OS suddenly loses power, that job may
be given to another worker upon system restart. Applications should always be
designed to assume the same job may be performed _at least once_ but possibly
_more than once_ in any case.

``` shell
zizq serve --enqueue-commit-mode fsync
```

The strong recommendation is to stick with the defaults. This is considerably
more durable than, for example, Redis, which relies on a periodic background
flush. If to-disk durability is critical, the preference should generally be to
consider using `--enqueue-commit-mode fsync` before going as far as
`--default-commit-mode fsync`.
