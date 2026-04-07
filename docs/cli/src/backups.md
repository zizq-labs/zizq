# Backup & Restore

Zizq stores its data in its root directory, which by default is in
`./zizq-root` relative to the current working directory, but can be specified
via `--root-dir` or `ZIZQ_ROOT_DIR`. In order to enable taking backups of this
directory, Zizq provides `zizq backup` and `zizq restore` commands.

## Online Backups

When the server is running, the only safe way to take a backup is by using
`zizq backup` which safely takes a point-in-time snapshot of the running
server as a gzipped tarball archive. This process does not block the running
server.

When `zizq backup` runs, it connects to the Admin API on the Zizq server and
requests a backup. This backup is downloaded by the `zizq backup` command for
safe storage at your control.

By default, `zizq backup` will connect to `http://127.0.0.1:8901`. If you have
configured the Admin API on the Zizq server to use a different host or port, or
to use HTTP or Mutual TLS, you will need to specify a `--url` explicitly.

``` shell
zizq backup --url https://zizq.internal:8901 backup.tar.gz
```

``` text
Requesting backup from https://zizq.internal:8901...
Backup saved to backup.tar.gz (42.0 MiB)
```

> [!NOTE]
> If the Admin API is configured to use Mutual TLS, make sure to provide
> `--client-cert` and `--client-key` to `zizq backup`.

## Restore from a Backup

The `zizq restore` command takes a backup archive and extracts it to create a
new root directory with the backup's contents. This is an entirely offline
operation and cannot be performed on a running Zizq server. If the root
directory already exists and is non-empty, `zizq restore` will refuse to
overwrite it.

In order to specify which directory to extract the archive into, make sure to
pass `--root-dir` (or set `ZIZQ_ROOT_DIR`).

``` shell
zizq restore --root-dir ./new-zizq-root backup.tar.gz
```

``` text
Restoring from backup.tar.gz to new-zizq-root...
Restore complete.
```

The server can now be started using the restored root directory.

## Offline Backups

> [!WARNING]
> It is not safe to copy the root directory while the server is running. The
> result is likely to be corrupt. Use `zizq backup` instead.

If the server is not running, it is sufficient to take an archive of the entire
root directory.

``` shell
tar cvzf backup.tar.gz zizq-root
```
