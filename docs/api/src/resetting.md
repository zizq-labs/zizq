# Resetting

> [!NOTE]
> These endpoints are available in both `application/json` and
> `application/msgpack` formats.

> [!WARNING]
> The endpoints described here irreversibly delete data. There is no
> confirmation step. Reach for them deliberately.

When developing locally or running fixtures in tests, it's common to want to
return the server to a known-empty state between iterations. Per-resource bulk
deletion is documented elsewhere:

- [`DELETE /jobs`](./modifying.md#delete-jobs-bulk) — bulk-delete jobs by
  filter (or every job, when called with no filters).
- [`DELETE /crons`](./cron.md#delete-crons-bulk) — delete every cron group
  and its entries in one request.

This page covers the single endpoint that does both at once.

## `POST /reset` { #post-reset }

Wipe every cron group and every job in a single request. Composes the
[bulk job delete](./modifying.md#delete-jobs-bulk) and [bulk cron delete](./cron.md#delete-crons-bulk)
paths so that any clients subscribed to job or cron lifecycle changes see the
same per-deletion events they would from those endpoints individually.

In-flight jobs are deleted along with everything else. Workers currently
processing those jobs will receive `404` responses when they later attempt
to acknowledge them.

### Responses { #post-reset-response }

#### `204` No Content

No response body is included for a successful response.
