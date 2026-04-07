# Overview

Zizq is a simple, single binary, zero dependency job queue system that runs on
most platforms and provides integration with clients written in just about any
programming language. The `zizq` binary provides everything needed to run Zizq
in your environment.

Zizq is open source software and most features are available free forever. Some
more advanced features require a [license](https://zizq.io/pricing).

You can download a release compatible with your environment from the
[releases page](https://github.com/zizq-labs/zizq/releases) on Github.

## Key Concepts

Before using Zizq it helps to understand some of the core concepts.

### Versioning

In order to minimize bloat the server does *not* provide multiple versions of
each endpoint.

Zizq and all official client libraries are versioned with the same version
numbers, following the SemVer structure `[MAJOR].[MINOR].[PATCH]`.

Whenever a new major or minor version of the server is released, client
libraries with the same major and minor version numbers are also released.

Clients should use the same major version number as the server version, and
should use a minor version number equal or lower than the server version.
Clients should never exceed the version number on the server.

### Subcommands

The `zizq` binary is a single binary that contains everything your system needs
to operate Zizq. The executable is arranged into subcommands, with the default
subcommand being `serve`.

### Configuration

In general Zizq can be configured either by specifying a command line flag
(e.g. `--root-dir`) or an equivalent environment variable
(e.g. `ZIZQ_ROOT_DIR`). Most configuration is done this way, rather than
through configuration files. This works nicely for containerized deployments.

Some command line flags (e.g. `--root-dir`) are global and apply to all
commands while others are specific to their subcommands.

### Root Directory

Zizq does not depend on external storage such as a RDBMS or a Redis cluster.
Instead, Zizq manages its own storage using a LSM database. The directory in
which Zizq stores this database, along with other important files, is known as
the root directory. By default this directory is `./zizq-root` relative to the
working directory from which `zizq` is executed. This can be specified through
a command line flag or an environment variable.

It is important that Zizq is always started with the same root directory for
data to persist across restarts. This directory can, and should be backed up.

### Client/Server

The `zizq` binary provides the server through which clients connect. Clients
are implemented in different programming languages and can be scaled
horizontally. You could have clients connecting from your Ruby application
along with clients from Node and Go applications, either operating
independently of one another, or working together as a single system.

### License Keys

Zizq is open source. Most features are free and do not require a license
key, but some more advanced features do require a license key. Licenses can be
[purchased through the website](https://zizq.io/pricing) and function entirely
offline. Zizq does not require connectivity to a license server.

Clients do not use license keys; license keys are known only to the server.

License key rotation is supported on a live and running Zizq server instance.
