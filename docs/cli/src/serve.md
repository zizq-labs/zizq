# Running the Server

Zizq operates as a HTTP/2 or HTTP/1.1 API which all clients connect to and
enqueue and process jobs. The server is started with the `zizq serve` command.

By default this API listens on `http://127.0.0.1:7890`.

Try `zizq serve --help` for a list of options.

> [!TIP]
> For more info on the Zizq API exposed by `zizq serve`, read the
> [Zizq API Reference](/docs/api/).

``` shell
zizq serve
```

``` text
Zizq 0.3.0
2026-04-05T05:41:26.893318Z  INFO zizq::commands::serve: no license key provided, running in free tier
2026-04-05T05:41:27.081150Z  INFO zizq::commands::serve: store opened root_dir=./zizq-root
2026-04-05T05:41:27.081547Z  INFO zizq::commands::serve: admin API listening addr=127.0.0.1:8901 scheme=http
Listening on http://127.0.0.1:8901 (admin)
2026-04-05T05:41:27.081842Z  INFO zizq::commands::serve: in-memory indexes rebuilt ready=0 scheduled=0
2026-04-05T05:41:27.081890Z  INFO zizq::commands::serve: primary API listening addr=127.0.0.1:7890 scheme=http
Listening on http://127.0.0.1:7890 (primary)
```

You can verify that the server is up and running correctly by accessing
`http://127.0.0.1:7890/version`.

``` shell
curl http://127.0.0.1:7890/version
```

``` json
{"version":"0.3.0"}
```

## Specifying the Root Directory

> [!TIP]
> It is recommended to use a known stable location for the root directory.

Zizq stores its queue data in a LSM database, which is stored under the _root
directory_. By default this root directory is automatically created at
`./zizq-root` in the current working directory. When the Zizq server is
restarted it should refer to the same root directory, otherwise it will restart
with an empty database.

Rather than relying on the current working directory you can specify a known
location for the root directory using `--root-dir`, or the environment variable
`ZIZQ_ROOT_DIR`.

``` shell
zizq serve --root-dir /var/lib/zizq
```

``` text
...
2026-04-05T06:02:52.688935Z  INFO zizq::commands::serve: store opened root_dir=/var/lib/zizq
...
```

## Configuring the Listen Address

> [!CAUTION]
> Do not bind the server to an address that is accessible to the internet
> without using Mutual TLS. Only run Zizq on trusted internal networks without
> [mTLS](#mtls).

By default the server listens on port `7890` on `127.0.0.1`, meaning the server
can only be accessed on `localhost`.

The command line flags `--host` and `--port` allow specifying different values.
The environment variables `ZIZQ_HOST` and `ZIZQ_PORT` perform the same
function.

``` shell
zizq serve --host 0.0.0.0 --port 8888
```

Clients will need configuring to use the correct address.

## Logging Configuration

The server writes logs to stdout by default. For containerized deployments this
is a preferred configuration.

### Log Directory

If you wish to have logs written to disk instead you can specify `--log-dir` or
`ZIZQ_LOG_DIR`. Zizq will the write logs to `{log-dir}/zizq.log`, and will
automatically rotate log files daily and when they reach 100MB.

``` shell
zizq serve --log-dir /var/log/zizq
```

### Log Level

The default log level is `warn`. Alternative log levels can be specified via
`--log-level` or the `ZIZQ_LOG_LEVEL` environment variable.

``` shell
zizq serve --log-level debug
```

### Log Rotation

When logging to disk, Zizq automatically performs log rotation periodically.
The default log rotation frequency is daily, and any time the log file reaches
100MB. These settings can be configured with the `--log-rotation` and
`--log-max-size` flags, or the `ZIZQ_LOG_ROTATION` and `ZIZQ_LOG_MAX_SIZE`
environment variables.

Valid frequencies for `--log-rotation` are:

* `daily` (default)
* `hourly`
* `never`

Valid values for `--log-max-size` are byte size integers, or human readable
sizes such as `500MB` or `2GB`. The default value is `100MB`.

Finally, the maximum number of rotated log files that are retained is
configured by the `--log-max-files` flag or the `ZIZQ_LOG_MAX_FILES`
environment variable. The default value is `10`.

``` shell
zizq serve \
    --log-dir /var/log/zizq \
    --log-rotation hourly \
    --log-max-size 200MB \
    --log-max-files 20
```

### Structured Logs

Structured JSON logging can be enabled with `--log-format json`, or with the
environment variable `ZIZQ_LOG_FORMAT=json`.

``` shell
zizq serve --log-format json
```

``` text
Zizq 0.1.0
{"timestamp":"2026-04-05T06:32:55.298737Z","level":"INFO","fields":{"message":"no license key provided, running in free tier"},"target":"zizq::commands::serve"}
{"timestamp":"2026-04-05T06:32:57.797395Z","level":"INFO","fields":{"message":"store opened","root_dir":"/var/lib/zizq"},"target":"zizq::commands::serve"}
{"timestamp":"2026-04-05T06:32:57.797693Z","level":"INFO","fields":{"message":"admin API listening","addr":"127.0.0.1:8901","scheme":"http"},"target":"zizq::commands::serve"}
Listening on http://127.0.0.1:8901 (admin)
{"timestamp":"2026-04-05T06:32:57.797829Z","level":"INFO","fields":{"message":"primary API listening","addr":"127.0.0.1:7890","scheme":"http"},"target":"zizq::commands::serve"}
Listening on http://127.0.0.1:7890 (primary)
{"timestamp":"2026-04-05T06:32:57.797829Z","level":"INFO","fields":{"message":"in-memory indexes rebuilt","ready":0,"scheduled":0},"target":"zizq::commands::serve"}
```

## Using a License Key

If you have a [pro license](https://zizq.io/pricing) the license key must be
provided to `zizq` when it is launched. License keys are formatted as JSON Web
Tokens and can be provided either as clear text, or as a file path (prefixed
with `@`). The `--license-key` flag, or `ZIZQ_LICENSE_KEY` environment variable
specifies the license key.

> [!TIP]
> The preferred approach is to put the license key in a file and use
> `@/path/to/zizq.license.jwt` so that the server can pick up license key
> rotations automatically. See [License Key Management](./licenses.md) for more
> info.

Using the raw license key string:

``` shell
zizq serve --license-key <long JWT string>
```

Using a filename:

``` shell
zizq serve --license-key @/etc/zizq/license.jwt
```

``` text
Zizq 0.1.0
2026-04-05T06:47:57.542077Z  INFO zizq::commands::serve: license validated licensee=Test Corp tier=pro expires_at=1806552653 remaining=11months 26days 1h 13m 20s
2026-04-05T06:47:57.543468Z  INFO zizq::commands::serve: store opened root_dir=./zizq-root
2026-04-05T06:47:57.543523Z  INFO zizq::commands::serve: license key file watcher started path=/etc/zizq/license.jwt interval_secs=5
2026-04-05T06:47:57.543583Z  INFO zizq::commands::serve: admin API listening addr=127.0.0.1:8901 scheme=http
Listening on http://127.0.0.1:8901 (admin)
2026-04-05T06:47:57.543729Z  INFO zizq::commands::serve: in-memory indexes rebuilt ready=0 scheduled=0
2026-04-05T06:47:57.543747Z  INFO zizq::commands::serve: primary API listening addr=127.0.0.1:7890 scheme=http
Listening on http://127.0.0.1:7890 (primary)
```

No internet connection is required to use a license key. Zizq is able to verify
the license completely offline and renewals are done by updating the license
manually.

## Enabling TLS (HTTPS) { #tls }

By default the Zizq server runs with plain unencrypted HTTP. HTTP/2 is still
supported in this mode (a.k.a `h2c`). HTTPS can be enabled by providing the
server with a PEM-encoded certificate and private key using the `--tls-cert`
and `--tls-key` command line arguments, or the `ZIZQ_TLS_CERT` and
`ZIZQ_TLS_KEY` environment variables. These must be used together and they
refer to files on disk.

If you have a certificate issued by a public certificate authority (e.g.
Let's Encrypt or Digicert) everything should just work provided the correct
hostname is used by clients to access the server.

If you do not have a certificate from a public CA, for example because you're
connecting to an internal hostname, you can generate your own CA and use that
to issue certificates. See [Generating TLS Certificates](/tls.md) for details
on how to do this with `zizq tls`.

``` shell
zizq serve --tls-cert /etc/zizq/server.cert.pem --tls-key /etc/zizq/server.key.pem
```

``` text
...
2026-04-06T09:29:53.047065Z  INFO zizq::commands::serve: primary API listening addr=127.0.0.1:7890 scheme=https
...
```

> [!NOTE]
> The server defaults to port `7890` for both plain HTTP and HTTPS. Don't
> forget to update your client configurations to use the correct scheme or they
> will fail to connect.

## Enabling Mutual TLS (mTLS) { #mtls }

> [!NOTE]
> This feature requires a [pro license](https://zizq.io/pricing).

Mutual TLS requires _clients_ to provide a certificate verifying their
authenticity before allowing the connection. In this mode, it is safe to expose
the Zizq server to the public internet. Starting the server with the argument
`--tls-client-ca` or the environment variable `ZIZQ_TLS_CLIENT_CA` tells Zizq
that it must verify clients have presented a valid certificate from the given
certificate authority in order to communicate. The value is the path to a PEM
encoded CA certificate.

See [Generating TLS Certificates](/tls.md) for details on generating a CA and
one or more client certificates with `zizq tls`.

``` shell
zizq serve \
    --tls-cert /etc/zizq/server.cert.pem \
    --tls-key /etc/zizq/server.key.pem \
    --tls-client-ca /etc/zizq/client.ca.pem
```

## Configuring the Admin API

The Admin API the interface through which internal utilities, such as
`zizq top` and `zizq backup` communicate with the server. In order to allow
securing the API differently to the primary API ordinary clients connect to,
the Admin API runs on a different port (and optionally a different address). By
default it listens on `http://127.0.0.1:8901`.

The Admin API can be configured in the same way as the primary API. The host
and port can be set, TLS can be enabled and Mutual TLS can be required (if you
have a [pro license](https://zizq.io/pricing)).

Use the following command line flags and environment variables to configure the
Admin API:

* `--admin-host`, `ZIZQ_ADMIN_HOST`
* `--admin-port`, `ZIZQ_ADMIN_PORT`
* `--admin-tls-cert`, `ZIZQ_ADMIN_TLS_CERT`
* `--admin-tls-key`, `ZIZQ_ADMIN_TLS_KEY`
* `--admin-tls-client-ca`, `ZIZQ_ADMIN_TLS_CLIENT_CA`

``` shell
zizq serve \
    --admin-tls-cert /etc/zizq/server.cert.pem \
    --admin-tls-key /etc/zizq/server.key.pem \
    --admin-tls-client-ca /etc/zizq/client.ca.pem
```

``` text
...
2026-04-06T09:53:47.890907Z  INFO zizq::commands::serve: admin API listening addr=127.0.0.1:8901 scheme=https
...
```

> [!NOTE]
> Don't forget to configure `zizq top` and `zizq backup` to use HTTPS with
> `--url https://127.0.0.1:8901` and optionally to provide the `--client-cert`
> and `--client-key` for Mutual TLS.

## Default Backoff Policy

> [!NOTE]
> Full details on how exponential backoff works, along with a visualisation are
> documented in [Backoff & Retry Policies](./backoff.md).

Zizq applies exponential backoff when jobs fail and can be retried. Clients
can explicitly specify their own policies on a per-job basis, but the server
otherwise applies its default policy.

There are two logical parts to the backoff policy:

1. The retry limit (maximum number of permitted retries).
2. The exponential backoff formula itself.

The Zizq defaults are very sensible but can be configured when starting the
server. See [Backoff & Retry Policies](./backoff.md) for details on the meaning of the
parameters.

The defaults can be configured by using the following command line arguments
and environment variables.

* `--default-retry-limit`, `ZIZQ_DEFAULT_RETRY_LIMIT`
* `--default-backoff-base`, `ZIZQ_DEFAULT_BACKOFF_BASE`
* `--default-backoff-exponent`, `ZIZQ_DEFAULT_BACKOFF_EXPONENT`
* `--default-backoff-jitter`, `ZIZQ_DEFAULT_BACKOFF_JITTER`

Values for `--default-backoff-base` and `--default-backoff-litter` are either
provided in raw milliseconds, or with an explicit unit, such as `12.5s`.

> [!NOTE]
> When any of `--default-backoff-base`, `--default-backoff-exponent` or
> `--default-backoff-jitter` are provided, all three must be provided as they
> form a single formula in unison.

``` shell
zizq serve \
    --default-retry-limit 40 \
    --default-backoff-base 3s \
    --default-backoff-exponent 3.75 \
    --default-backoff-jitter 15s
```

## Default Retention Policy

> [!NOTE]
> Full details on how retention works are documented in
> [Retention Policies](./retention.md).

At the end of their lifecycle, jobs in Zizq move into one of two possible
terminal statuses: `completed` or `dead`. The server decides how long to keep
jobs in these terminal statuses based on a retention policy. Clients can
specify their own policies on a per-job basis, but the server otherwise applies
its default policy.

By default Zizq retains `dead` jobs for 7 days, and it does not retain
`completed` jobs at all.

These defaults can be configured by using the following command line arguments
and environment variables:

* `--default-completed-job-retention`, `ZIZQ_DEFAULT_COMPLETED_JOB_RETENTION`
* `--default-dead-job-retention`, `ZIZQ_DEFAULT_DEAD_JOB_RETENTION`

Values are specified either in raw milliseconds, or with explicit units such
as `3w`. Zero means the job is immediately removed once it enters the
applicable status.

``` shell
zizq serve --default-dead-job-retention 90d --default-completed-job-retention 14d
```

## Configuring the Commit Mode

> [!CAUTION]
> Full details on commit modes are documented in [Durability](./durability.md).
> You should read that document before changing these settings, as there is a
> signficant performance penalty.

Zizq is implemented on top of a LSM database that writes a Write Ahead Log, or
a journal file any time data is modified. This ensures changes are _durable_
and in the case of a crash any data that was committed will still be in the
database. The way the WAL is written however makes a direct trade-off between
pure durability and performance.

By default Zizq writes to the WAL on every commit (i.e. before any success
response from the API), but it only flushes that write to OS buffers, not
completely to disk.

The following command line arguments and environment variables are used to
adjust how durable writes to Zizq are:

* `--default-commit-mode`, `ZIZQ_DEFAULT_COMMIT_MODE`
* `--enqueue-commit-mode`, `ZIZQ_ENQUEUE_COMMIT_MODE`

Possible values are `buffered` (default) or `fsync`. The default commit mode
affects _all write operations_ (huge performance penalty). The enqueue commit
mode overrides the default mode specifically for _enqueue operations_ which is
a reasonable middle ground if you know for sure you need this level of
durability.

``` shell
zizq serve --default-commit-mode buffered --enqueue-commit-mode fsync
```

## Cache Settings

For most use cases the Zizq defaults will "just work". The underlying LSM
database performs best when there is sufficient in-memory cache capacity
available to avoid regular round trips to disk. By default Zizq sets this cache
size at `256MB`, which in testing has performed well enqueueuing and dequeueing
tens of millions of jobs over a sustained period. However this setting can be
configured explicitly on server startup with `--cache-size` or
`ZIZQ_CACHE_SIZE`. If you are able to comfortably allocate 25% of system memory
you should consider doing so, though in practice the default should be ok.

Acceptable values are either raw byte counts, or explicit byte sizes with human
readable units, such as `500MB`. Values lower than `192MB` are likely to
perform poorly as data needs to be scanned on disk more frequently.
