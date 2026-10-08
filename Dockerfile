# syntax=docker/dockerfile:1

# Zizq server container image.
#
# Packages the release archives published on GitHub Releases rather
# than compiling, so the image runs exactly the binary users download.
# The build context is a directory holding those archives and their
# `.sha256` files (`target/release` after `./release.sh`, or a
# directory of downloaded release assets):
#
#   docker buildx build \
#     --platform linux/amd64,linux/arm64 \
#     --build-arg ZIZQ_VERSION=0.7.2 \
#     -f Dockerfile target/release
#
# Two variants are built from this file:
#
#   (default)        The binary alone, on `scratch`. Nothing for a
#                    vulnerability scanner to report on beyond Zizq.
#   --target alpine  Adds a shell, curl and jq, for calling the API
#                    from inside the container.
#
# `zizq top` works in both: `docker exec -it <container> zizq top`.
#
# Runtime contract:
#
#   ZIZQ_ROOT_DIR=/var/lib/zizq  Declared as a volume. Mount one, or
#                                data is lost with the container.
#   ZIZQ_HOST=0.0.0.0            The primary API is reachable on 7890.
#
# The admin API keeps its default of 127.0.0.1:8901, so it is reachable
# from inside the container but not exposed. Reach it with
# `docker exec -it <container> zizq top`, `kubectl exec`, or
# `kubectl port-forward <pod> 8901` to run `zizq top` locally. If it
# must be exposed, set ZIZQ_ADMIN_HOST and secure it with mutual TLS or
# a service mesh.
#
# Runs as uid/gid 1000. On Kubernetes, set `fsGroup: 1000` so the volume
# is writable.

ARG ALPINE_VERSION=3.22

# --- Verify and unpack the release archive ---
#
# Runs on the build host's own architecture: it only moves files, so a
# multi-platform build needs no emulation here.

FROM --platform=$BUILDPLATFORM alpine:${ALPINE_VERSION} AS unpack

ARG TARGETARCH
ARG ZIZQ_VERSION

COPY zizq-*-linux-*.tar.gz zizq-*-linux-*.tar.gz.sha256 /dist/

RUN set -eu; \
    if [ -z "${ZIZQ_VERSION}" ]; then \
        echo "ZIZQ_VERSION build arg is required" >&2; exit 1; \
    fi; \
    case "${TARGETARCH}" in \
        amd64) PLATFORM=linux-x86_64 ;; \
        arm64) PLATFORM=linux-arm64 ;; \
        *) echo "unsupported architecture: ${TARGETARCH}" >&2; exit 1 ;; \
    esac; \
    ARCHIVE="zizq-${ZIZQ_VERSION}-${PLATFORM}.tar.gz"; \
    cd /dist; \
    sha256sum -c "${ARCHIVE}.sha256"; \
    mkdir -p /rootfs/usr/local/bin /rootfs/etc /rootfs/var/lib/zizq /rootfs/tmp; \
    tar -xzf "${ARCHIVE}" -C /rootfs/usr/local/bin zizq; \
    echo 'zizq:x:1000:1000:zizq:/var/lib/zizq:/sbin/nologin' > /rootfs/etc/passwd; \
    echo 'zizq:x:1000:' > /rootfs/etc/group; \
    chmod 1777 /rootfs/tmp

# --- Variant with a shell, curl and jq ---

FROM alpine:${ALPINE_VERSION} AS alpine

ARG ZIZQ_VERSION

LABEL org.opencontainers.image.title="Zizq" \
      org.opencontainers.image.description="A fast and durable job queue in a single binary" \
      org.opencontainers.image.url="https://zizq.io" \
      org.opencontainers.image.source="https://github.com/zizq-labs/zizq" \
      org.opencontainers.image.documentation="https://zizq.io/docs" \
      org.opencontainers.image.licenses="BUSL-1.1" \
      org.opencontainers.image.version="${ZIZQ_VERSION}"

RUN apk add --no-cache curl jq \
    && addgroup -g 1000 zizq \
    && adduser -u 1000 -G zizq -h /var/lib/zizq -s /sbin/nologin -D zizq

COPY --from=unpack /rootfs/usr/local/bin/zizq /usr/local/bin/zizq

ENV ZIZQ_ROOT_DIR=/var/lib/zizq \
    ZIZQ_HOST=0.0.0.0

USER 1000:1000
WORKDIR /var/lib/zizq
VOLUME /var/lib/zizq
EXPOSE 7890

ENTRYPOINT ["/usr/local/bin/zizq"]
CMD ["serve"]

# --- Default variant: the binary alone ---
#
# Last, so that a build without `--target` produces it.

FROM scratch AS minimal

ARG ZIZQ_VERSION

LABEL org.opencontainers.image.title="Zizq" \
      org.opencontainers.image.description="A fast and durable job queue in a single binary" \
      org.opencontainers.image.url="https://zizq.io" \
      org.opencontainers.image.source="https://github.com/zizq-labs/zizq" \
      org.opencontainers.image.documentation="https://zizq.io/docs" \
      org.opencontainers.image.licenses="BUSL-1.1" \
      org.opencontainers.image.version="${ZIZQ_VERSION}"

# CA certificates for verifying TLS when `zizq top` or `zizq backup`
# connect to an admin API using a publicly trusted certificate.
COPY --from=unpack /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=unpack /rootfs/etc/passwd /rootfs/etc/group /etc/
COPY --from=unpack /rootfs/tmp /tmp
COPY --from=unpack --chown=1000:1000 /rootfs/var/lib/zizq /var/lib/zizq
COPY --from=unpack /rootfs/usr/local/bin/zizq /usr/local/bin/zizq

ENV ZIZQ_ROOT_DIR=/var/lib/zizq \
    ZIZQ_HOST=0.0.0.0

USER 1000:1000
WORKDIR /var/lib/zizq
VOLUME /var/lib/zizq
EXPOSE 7890

ENTRYPOINT ["/usr/local/bin/zizq"]
CMD ["serve"]
