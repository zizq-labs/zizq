# Installation

Zizq is a single binary, which will run from any directory. There is no formal
installation procedure and no external dependencies. It is sufficient to
[download a release](https://github.com/zizq-labs/zizq/releases) and execute it
directly, or to [run it with Docker](#running-with-docker).

## Downloading a Release

All Zizq releases and release notes are available on the
[GitHub releases page](https://github.com/zizq-labs/zizq/releases). Make sure
to choose a release for your operating system and architecture.

``` shell
curl -sLO https://github.com/zizq-labs/zizq/releases/download/v0.7.4/zizq-0.7.4-linux-x86_64.tar.gz
```

Once you have downloaded a release it will need to be extracted.

## Extracting the Binary

Release binaries are gzipped and contain the version number. Extract the file
from the archive and it should be executable.

```shell
tar -xvzf zizq-0.7.4-linux-x86_64.tar.gz
./zizq --help
```

You may prefer to move the `zizq` executable to a standard system path, such as
`/usr/bin/zizq` or `/usr/local/bin/zizq`.

``` shell
tar -xvzf zizq-0.7.4-linux-x86_64.tar.gz
sudo chown root: ./zizq && sudo mv ./zizq /usr/bin/zizq
zizq --help
```

## Creating a Root Directory

> [!NOTE]
> This step is entirely optional. If you are just experimenting with Zizq you
> can skip this step and use the default root directory.

When Zizq runs it needs a consistent root directory. Zizq will automatically
create the root directory if it does not exist. By default this is
`./zizq-root` relative to the current working directory. It is recommended to
pick a constant known location and either specify `ZIZQ_ROOT_DIR` or
`--root-dir` when starting the server.

This could be anywhere at all, but if you prefer standard system directories a
reasonable option on a POSIX style operating system would be `/var/lib/zizq`.

You can create this directory upfront and ensure it has permissions for
whichever system user will run `zizq serve`.

``` shell
sudo mkdir -p /var/lib/zizq
sudo chown username: /var/lib/zizq
```

The server would then always be started with:

``` shell
sudo -u username /usr/bin/zizq serve --root-dir /var/lib/zizq
```

## Running with Docker

Container images are published for every release, for `linux/amd64` and
`linux/arm64`, to [Docker Hub](https://hub.docker.com/r/zizqlabs/zizq) and the
GitHub Container Registry.

* `zizqlabs/zizq`
* `ghcr.io/zizq-labs/zizq`

``` shell
docker run -d --name zizq -p 7890:7890 -v zizq-data:/var/lib/zizq zizqlabs/zizq:0.7.4
```

### Image Variants and Tags

There are two variants of each release.

| Tag | Contents |
|---|---|
| `0.7.4`, `0.7`, `latest` | The `zizq` binary alone, with no shell or other tools. |
| `0.7.4-alpine`, `0.7-alpine`, `alpine` | The `zizq` binary on Alpine Linux, with a shell, `curl` and `jq` for calling the API from inside the container. |

The `M.m` and `latest` tags move with each release. Pin the exact version in
production so that a restarted container always runs the version you tested.

### Image Defaults

The image runs `zizq serve` by default, configured as follows.

| Setting | Value |
|---|---|
| Root directory | `/var/lib/zizq` (`ZIZQ_ROOT_DIR`), declared as a volume |
| Primary API | `0.0.0.0:7890` (`ZIZQ_HOST`), exposed |
| Admin API | `127.0.0.1:8901`, not exposed |
| User | uid/gid `1000` |

Every other option is configured with the same flags and `ZIZQ_*` environment
variables as the binary, described in [Running the Server](./serve.md).

``` shell
docker run -d --name zizq \
  -p 7890:7890 \
  -v zizq-data:/var/lib/zizq \
  -e ZIZQ_DEFAULT_RETRY_LIMIT=10 \
  zizqlabs/zizq:0.7.4
```

The entrypoint is `zizq`, so any other subcommand can be run in place of
`serve`.

``` shell
docker run --rm zizqlabs/zizq:0.7.4 serve --help
```

### Persisting Data

Mount a volume at `/var/lib/zizq`, otherwise the queue data is lost with the
container. A named volume, as above, needs no preparation. A directory bind
mounted from the host must be writable by uid `1000`.

``` shell
sudo mkdir -p /var/lib/zizq
sudo chown 1000:1000 /var/lib/zizq
docker run -d --name zizq -p 7890:7890 -v /var/lib/zizq:/var/lib/zizq zizqlabs/zizq:0.7.4
```

Only one server may use a root directory at a time. Never point two containers
at the same volume.

### Accessing the Admin API

The admin API is not exposed outside the container. Tools that use it, such as
[`zizq top`](./zizq-top.md) and [`zizq backup`](./backups.md), can be run
inside the container instead.

``` shell
docker exec -it zizq zizq top
```

On Kubernetes, use `kubectl exec`, or forward the port to run `zizq top` from
your own machine.

``` shell
kubectl port-forward deploy/zizq 8901:8901
zizq top
```

If the admin API must be reachable from elsewhere, set `ZIZQ_ADMIN_HOST` and
secure it with [mutual TLS](./serve.md#mtls) or a service mesh.

### Providing a License Key

Mount the license key as a file and prefix its path with `@`, so it is never
stored in the container's environment. The server reloads the file when it
changes. See [License Key Management](./licenses.md).

``` yaml
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

### Running on Kubernetes

There are a few important settings when running the image on Kubernetes.

* **`replicas: 1` with `strategy: Recreate`**, so the old pod has stopped
  before the new one opens the volume.
* **`fsGroup: 1000`** in the pod's `securityContext`, so the volume is
  writable by the server.
* **`enableServiceLinks: false`**, so Kubernetes does not inject
  `ZIZQ_PORT=tcp://...` for a Service named `zizq`. See
  [Configuring the Listen Address](./serve.md#configuring-the-listen-address).

``` yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: zizq
spec:
  replicas: 1
  strategy:
    type: Recreate
  selector:
    matchLabels:
      app: zizq
  template:
    metadata:
      labels:
        app: zizq
    spec:
      enableServiceLinks: false
      securityContext:
        fsGroup: 1000
      containers:
        - name: zizq
          image: zizqlabs/zizq:0.7.4
          ports:
            - name: api
              containerPort: 7890
          volumeMounts:
            - name: data
              mountPath: /var/lib/zizq
          readinessProbe:
            tcpSocket:
              port: api
      volumes:
        - name: data
          persistentVolumeClaim:
            claimName: zizq-data
```
