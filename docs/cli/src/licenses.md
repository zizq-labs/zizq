# License Key Management

Licenses can be purchased [through the website](https://zizq.io/pricing) for
access to pro features, such as Mutual TLS authentication and Unique Jobs.

## License Key Format

License keys are JSON Web Tokens signed with Ed25519 and include an expiry
date. Zizq verifies the validity of the license key completely offline. This is
an intentional design choice so that there is no requirement to create holes in
firewall rules or proxy configurations to reach a license server in order for
Zizq to function.

## Using a License Key

When license keys are purchased, there are options for how long the license is
valid and whether or not automatic renewal is included. In both cases, you will
receive a license key in the format described above. This license expires after
the selected period of time at the time of purchase.

In order to use the license key, the Zizq server is started with the
`--license-key` argument or `ZIZQ_LICENSE_KEY` environment variable. Valid
values are either a file path prefixed with `@`, or the license key content
itself. The recommendation is to use a filename prefixed with `@` (see
[License Key Rotation](#rotation)).

Whe the server starts, Zizq validates the license key and logs the details of
the license.

``` shell
zizq serve --license-key @/etc/zizq/license.jwt
```

``` text
...
2026-04-05T06:47:57.542077Z  INFO zizq::commands::serve: license validated licensee=Test Corp tier=pro expires_at=1806552653 remaining=11months 26days 1h 13m 20s
2026-04-05T06:47:57.543523Z  INFO zizq::commands::serve: license key file watcher started path=/etc/zizq/license.jwt interval_secs=5
...
```

Once the license expires, Zizq continues running but degrades back to the free
tier. In order to continue operating with a valid license, the license key must
be rotated before it expires.

## License Key Rotation { #rotation }

If you purchased a single-use license (without automatic renewal) and you wish
to continue using pro features after that license expires, you will need to
purchase a new license before the current license expires.

If your license purchase included automatic renewal, we'll send you a new
license key approximately 2 weeks before your current license stops working.

In both cases the new license must be provided to the Zizq server.

Provided the server was started with a license key using the `@` prefix to
refer to a filename , it is sufficient to overwrite the old license key file
with the new license while Zizq is still running. Zizq will detect the updated
license key file and log the details of the new license.

> [!TIP]
> Another approach is to use `--license-key @/path/to/symlink.jwt` and redirect
> the symlink to the new license key file.

``` text
...
2026-04-07T06:47:57.543523Z  INFO zizq::commands::serve: license key file changed, updating license
2026-04-07T06:47:57.542077Z  INFO zizq::commands::serve: license validated licensee=Test Corp tier=pro expires_at=1839298253 remaining=12months 14days
...
```

If the server was started with a raw license key string, the server will need
to be restarted using the new license key.
