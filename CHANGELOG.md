# Changelog

## 0.4.2

- Fixed `DELETE /jobs/{id}`: the 204 No Content response was
  accidentally including a serialized empty-body payload (JSON `null`
  or its msgpack equivalent). HTTP/1.1 clients silently tolerated the
  spec violation and ignored the body, but HTTP/2 strictly enforces
  RFC 7230 § 3.3.3's "204 MUST NOT have a body" — any DATA frame
  after a 204 HEADERS frame triggers `NGHTTP2_PROTOCOL_ERROR` and
  closes the stream. Only affected clients negotiating h2 (TLS or
  h2c); h1.1 clients were unaffected. Other 204 sites in the codebase
  were already correct.
- Added server-side opportunistic batching for enqueue requests.
  Singular `enqueue` and bulk `enqueue_bulk` now route through one
  shared channel; a dedicated background thread coalesces whatever
  arrived at fjall-mutex-acquire time into a single write
  transaction. Atomicity is preserved at the commit boundary — a
  bulk's all-or-nothing contract still holds because the coalesced
  commit succeeds or fails as a unit. Op-count bounded via
  `--enqueue-batch-size` (`ZIZQ_ENQUEUE_BATCH_SIZE`, default 1000); a
  bulk request counts as one op regardless of job count. The win
  materializes when many independent client processes are enqueueing
  concurrently — local single-client benchmarks see no measurable
  difference, neither helps nor hurts.
- Added server-side opportunistic batching for completion (ack)
  requests, same shape as the enqueue batcher. Composes with the
  per-worker `AckProcessor`-style batching that clients already do:
  the client batcher reduces HTTP framing per-request; the server
  batcher reduces `tx.commit()` cost when many independent workers
  ack concurrently. Configurable via `--complete-batch-size`
  (`ZIZQ_COMPLETE_BATCH_SIZE`, default 1000).

## 0.4.1

- Internal refactoring of the `zizq top` code structure
- Lots of dependency version bumps

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
- Press `D` (Shift+d) on a row in `zizq top` to delete a job. A `[y/N]`
  confirmation prompt replaces the help bar; `y` confirms, `n`/Esc/`q`
  cancel. New `delete_job` message type on the admin WebSocket protocol.
- Fixed `GET /jobs/take`: the heartbeat used `send().await`, which would
  block when the response body's `mpsc(1)` buffer was full. That parked
  the take loop's `select!`, swallowing any subsequent `tx.closed()`
  notification. The result was orphaned in-flight jobs that never got
  requeued when a worker was killed. Heartbeats now use `try_send`, which
  treats a full buffer as "the previous heartbeat is still in flight,
  skip this tick" and keeps the disconnect path live.
- Fixed `GET /jobs/take`: when a worker disconnected partway through a
  prefetched batch, only the jobs that had been dispatched so far were
  tracked in the local in-flight set and requeued on cleanup. The tail
  of the batch was already InFlight on disk (committed by
  `take_next_n_jobs`) and had bumped the in-flight counter, but was
  never seen by cleanup — leaving those jobs stranded InFlight forever
  and the counter drifting above the actual on-disk count. The handler
  now records the entire batch in the in-flight set before dispatching,
  so cleanup requeues every job in the batch.
- Fixed admin WebSocket: when a job was requeued (worker disconnect
  cleanup), the `JobCreated` event was treated only as "new ready job"
  — the handler diffed the ready/scheduled windows but never evicted
  the id from `in_flight_ids`. `zizq top` therefore kept showing
  stale rows in the in-flight tab even though the header total had
  dropped to zero. `JobCreated` now removes the id from the connection's
  in-flight set if it was there, so the next `diff_in_flight` emits an
  `in_flight_removed` event.

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
