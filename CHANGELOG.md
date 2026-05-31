# Changelog

## 0.4.0

- Force a full LSM compaction after large bulk deletes and bulk patches to
  reclaim tombstone space that leveled compaction would otherwise leave in
  upper levels on quiet databases. Threshold is configurable via
  `ZIZQ_AUTO_COMPACT_THRESHOLD` (default 10000, set to 0 to disable).
- Added `POST /compact` admin endpoint to trigger a full compaction on
  demand. Returns 204 No Content on success.
- Added `zizq compact` CLI subcommand that calls the admin endpoint.
- Added `DELETE /crons` to wipe every cron group in one call (Pro).
- Added `POST /reset` to wipe every cron group and every job in a single
  request. Returns 204 No Content. Useful for testing.
- Press `p` in `zizq top` to pause/resume the live job lists. Header totals
  keep updating; the job rows freeze where they were so you can scroll
  through them without the view shifting under your cursor. Navigation
  keys (`j`/`k`/`g`/`G`/PgUp/PgDn) clamp to the frozen buffer rather than
  the server-side total. Detail toggle is disabled while paused. A "Resume
  to scroll further" hint appears at the buffer edges.
- Fixed `zizq top`: pressing `G` (go to end) on a live, churning list could
  leave the view blank because the in-flight Subscribe response landed with
  an offset that no longer covered the cursor. The TUI now requeues a
  Subscribe when a stale snapshot leaves the cursor outside the buffer.
- Fixed admin WebSocket: when the store-event broadcast lagged (e.g. during
  a large bulk enqueue), the server resynced by re-seeding the connection
  with default subscriptions and `detail = false`, silently demoting the
  client's prior `SetDetailLevel{detail: true}`. The resync now preserves
  the connection's detail flag and subscription windows so payloads keep
  flowing across all tabs in `zizq top`.

## 0.3.1

- Improved key bindings in `zizq top` (g, G, Home, End, ^C, ^Z)
- Press `i` in `zizq top` to see job detail

## 0.3.0

- Add Cron scheduling support (Pro) with multiple cron "groups"

## 0.2.1

- Internal Store refactors around enqueue and bulk enqueue


## 0.2.0

- Added `GET /jobs/count` (with filter params)

## 0.1.1

- Internal Store refactors (mostly around group commit)
- Restructured API internals — some log prefixes will have changed

## 0.1.0

- Initial release
- Persistent job queue with underlying LSM storage
- HTTP/1.1 and HTTP/2 API
- Streaming job dequeueing (NDJSON + MessagePack)
- Prioritised queues with FIFO ordering within same priority
- Configurable retry with exponential backoff
- Job retention polcies and automatic purging
- Unique job enqueues (Pro)
- Mutual TLS authentication (Pro)
- Interactive live queue viewer (`zizq top`)
- TLS certificate generation util (`zizq tls`)
- Online backup and restore (`zizq backup` and `zizq restore`)
- Bulk enqueue, delete, and update operations
- jq-based payload filtering for bulk operations
