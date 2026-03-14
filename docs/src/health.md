# Health & Version

> [!NOTE]
> These endpoints are available in both `application/json` and
> `application/msgpack` formats.

## `GET /health` { #get-health }

Returns `200 OK` when the server is running and ready to accept traffic. Use
this for load balancer health checks.

### Responses

#### `200` OK

<table>
    <thead>
        <tr>
            <th>Field</th>
            <th>Description</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td>
                <div><code>status</code> <em>required</em></div>
                <div><pre>string</pre></div>
            </td>
            <td>
                Currently always <code>"ok"</code>.
            </td>
        </tr>
    </tbody>
</table>

## `GET /version` { #get-version }

Returns the version of Zizq running on the server. Clients should use this to
check compatability.

> [!NOTE]
> Major version changes should be considered backward-incompatible changes.
> Minor version changes are always backward compatible.

### Responses

#### `200` OK

<table>
    <thead>
        <tr>
            <th>Field</th>
            <th>Description</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td>
                <div><code>version</code> <em>required</em></div>
                <div><pre>string</pre></div>
            </td>
            <td>
                Returns the version of the software running on the server.
                Version strings are semver structured
                <code>MAJOR.MINOR.PATCH</code>. Only major version changes may
                be backward incompatible.
            </td>
        </tr>
    </tbody>
</table>

## Examples

### Check availability

```shell
http GET http://127.0.0.1:7890/health
```

```http
HTTP/1.1 200 OK
content-length: 15
content-type: application/json
date: Fri, 13 Mar 2026 08:15:41 GMT

{
    "status": "ok"
}
```

### Check the server version

``` shell
http GET http://127.0.0.1:7890/version
```

```http
HTTP/1.1 200 OK
content-length: 19
content-type: application/json
date: Fri, 13 Mar 2026 08:18:34 GMT

{
    "version": "0.1.0"
}
```
