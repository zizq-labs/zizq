# Zizq

Zizq is a fast and durable job queue packed into a single native binary, built
on an embedded LSM database. It has no external dependencies such as Redis or
an RDBMS.

Applications enqueue and process jobs over a simple HTTP/2 and HTTP/1.1 API.
Official clients currently exist for
[Node.js](https://zizq.io/docs/clients/node/),
[Ruby](https://zizq.io/docs/clients/ruby/),
[Elixir](https://zizq.io/docs/clients/elixir/) and
[Rust](https://zizq.io/docs/clients/rust/). Job enqueueds from one language can
be processed by workers written in another.

* **Website:** [zizq.io](https://zizq.io)
* **Documentation:** [zizq.io/docs](https://zizq.io/docs)
* **Source and issues:** [github.com/zizq-labs/zizq](https://github.com/zizq-labs/zizq)

## Quick Start

```shell
docker run -d --name zizq -p 7890:7890 -v zizq-data:/var/lib/zizq zizqlabs/zizq:0.7.4
```

Enqueue a job:

```shell
curl -XPOST http://localhost:7890/jobs \
  -H 'Content-Type: application/json' \
  -d '{"queue":"example","type":"hello_world","payload":{"greet":"World"}}'
```

Watch the queue live in a terminal UI:

```shell
docker exec -it zizq zizq top
```

The best place to get started is with the
[Quick Start guide](https://zizq.io/docs/getting-started/quick-start.html).

## Tags

Images are published for `linux/amd64` and `linux/arm64`, in two variants.

| Tags | Contents |
|---|---|
| `0.7.4`, `0.7`, `latest` | The `zizq` binary alone, with no shell or other tools. |
| `0.7.4-alpine`, `0.7-alpine`, `alpine` | The `zizq` binary on Alpine Linux, with a shell, `curl` and `jq` for calling the API from inside the container. |

The `0.7` and `latest` tags move with each release. Pin the exact version in
production so that a restarted container always runs the version you tested.

The same images are also available as `ghcr.io/zizq-labs/zizq`.

## Configuration

The image runs `zizq serve` by default, configured as follows.

| Setting | Value |
|---|---|
| Root directory | `/var/lib/zizq` (`ZIZQ_ROOT_DIR`), declared as a volume |
| Primary API | `0.0.0.0:7890` (`ZIZQ_HOST`), exposed |
| Admin API | `127.0.0.1:8901`, not exposed |
| User | uid/gid `1000` |

Every other option is set with the same `ZIZQ_*` environment variables, or
flags, as the binary. See
[Running the Server](https://zizq.io/docs/cli/serve.html) for the full list.

```shell
docker run -d --name zizq \
  -p 7890:7890 \
  -v zizq-data:/var/lib/zizq \
  -e ZIZQ_DEFAULT_RETRY_LIMIT=10 \
  zizqlabs/zizq:0.7.4
```

The entrypoint is `zizq`, so any other subcommand can be run in place of
`serve`:

```shell
docker run --rm zizqlabs/zizq:0.7.4 serve --help
```

## Persisting Data

Mount a volume at `/var/lib/zizq` which holds the Zizq root directory,
otherwise the queue data is lost with the container. A directory bind mounted
from the host must be writable by uid `1000`.

Only one process may use the Zizq root directory at a time. Do not point two
containers at the same volume.

## The Admin API

The admin API, used by `zizq top` and `zizq backup`, is not exposed outside
the container. By default, tools that rely on the admin API can be used by
exec'ing into the container directly.

```shell
docker exec -it zizq zizq top
```

On Kubernetes, use `kubectl exec`, or `kubectl port-forward deploy/zizq 8901`
to run `zizq top` from your own machine.

If the admin API must be reachable from elsewhere, set `ZIZQ_ADMIN_HOST` to
`0.0.0.0` and secure it with
[mutual TLS](https://zizq.io/docs/cli/serve.html#mtls) or a service mesh.

## License Key

Features such as unique jobs, cron scheduling and mutual TLS need a
[Pro license](https://zizq.io/pricing). Mount the license key as a file and
prefix its path with `@`, so it is never exposed via the container's
environment. The server reloads the license key when the file changes.

```yaml
# compose.yaml
services:
  zizq:
    image: zizqlabs/zizq:0.7.4
    ports:
      - "7890:7890"
    volumes:
      - zizq-data:/var/lib/zizq
    environment:
      ZIZQ_LICENSE_KEY: "@/run/secrets/zizq_license"
    secrets:
      - zizq_license

volumes:
  zizq-data:

secrets:
  zizq_license:
    file: ./license.jwt
```

## Kubernetes

* Run **`replicas: 1` with `strategy: Recreate`**, so the old pod has stopped
  before the new one opens the volume.
* Set **`fsGroup: 1000`** in the pod's `securityContext`, so the volume is
  writable by the server.
* Set **`enableServiceLinks: false`**, so Kubernetes does not inject
  `ZIZQ_PORT=tcp://...` for a Service named `zizq`.

A complete example Deployment is in the
[installation docs](https://zizq.io/docs/cli/installation.html#running-on-kubernetes).

## License

Zizq is source-available under the
[Business Source License 1.1](https://github.com/zizq-labs/zizq/blob/main/LICENSE).
The client libraries are open source.
