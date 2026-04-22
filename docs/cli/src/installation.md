# Installation

Zizq is a single binary, which will run from any directory. There is no formal
installation procedure and no external dependencies. It is sufficient to
[download a release](https://github.com/zizq-labs/zizq/releases) and execute it
directly.

## Downloading a Release

All Zizq releases and release notes are available on the
[GitHub releases page](https://github.com/zizq-labs/zizq/releases). Make sure
to choose a release for your operating system and architecture.

Once you have downloaded a release it will need to be extracted.

## Extracting the Binary

Release binaries are gzipped and contain the version number. Extract the file
from the archive and it should be executable.

```shell
tar -xvzf zizq-0.1.0-linux-x86_64.tar.gz
./zizq --help
```

You may prefer to move the `zizq` executable to a standard system path, such as
`/usr/bin/zizq` or `/usr/local/bin/zizq`.

``` shell
tar -xvzf zizq-0.1.0-linux-x86_64.tar.gz
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
