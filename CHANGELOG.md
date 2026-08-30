# Changelog

## 0.7.0 (Unreleased)

- Added **budgets**, a server-side throttle on how fast jobs are
  dispatched. A budget is a named token bucket that jobs draw from; a
  job whose budgets have no capacity waits until they replenish.
  Workers are unaware of any of it — a throttled job looks like any
  other when it finally arrives. Pro tier, alongside unique jobs, cron
  and batching.

  Two strategies, covering the two things people mean by "rate limit":

  - `time_based` refills the whole allocation every `duration_ms`, as a
    continuous drip rather than in fixed windows. A counter that reset
    on the minute would allow the allocation at 59.9s and again at
    60.0s — twice the configured rate across the boundary, every
    boundary. Accrual is computed lazily from elapsed time, so an idle
    budget costs nothing.
  - `while_in_flight` returns a token when the job leaves the in-flight
    state, by acknowledgement, failure, deletion or worker disconnect.
    This is a concurrency limit, of which a mutex is the
    `allocation: 1` case.

  Budgets are managed at `GET /budgets`, and `GET` / `POST` / `PUT` /
  `PATCH` / `DELETE` on `/budgets/{key}`. `POST` creates and returns
  `409` if the key is taken, so an application can declare the budgets
  it expects on every boot without clobbering an allocation an operator
  tightened during an incident; `PUT` replaces a policy whole and
  `PATCH` merges named fields. A budget's `created_at` survives both.

  Jobs bind to budgets with a `budgets` array on enqueue, each entry
  naming a `key` and an optional `cost` (default 1). A job may draw on
  several, and acquires from all of them or none. An entry may carry a
  `create_with` policy, which creates the budget if it does not exist
  and is ignored if it does — the server stays authoritative, so an
  enqueue can never quietly restate a throttle. Referencing a budget
  that does not exist without a `create_with` is rejected with `422`
  rather than dispatching unthrottled. Bindings are returned on every
  read of a job for visibility.

  `time_based` also takes an optional `burst` alongside `duration_ms`,
  capping how many tokens may be banked without changing how fast they
  arrive. A bucket starts full, so the first work after an idle period
  draws a whole allocation at once and only then settles to the drip —
  meaning `10 per minute` permits twenty in the first minute. That
  overshoot is a one-off and the long-run rate is unaffected, but it is
  visible, and `burst: 1` removes it entirely by pacing dispatches
  evenly. A burst above the allocation is allowed, and banks several
  idle periods. Note that where a burst is set it is the burst, not the
  allocation, that a job's `cost` must fit inside.

  Cron entries may reference budgets in their job template. Budgets a
  template needs are created when the entry is installed rather than
  when it first fires, so an entry cannot be accepted into a schedule
  and then turn out to be unfireable because the server has since
  reached its budget cap.

  A budget cannot be deleted, or have its allocation shrunk below a
  cost already committed to it, while anything still draws on it —
  unfinished jobs or a cron entry's template. Both are reported
  separately, because the remedies differ: a job will drain on its own,
  where a cron entry is a standing claim that has to be edited. A
  budget referenced only by finished jobs deletes cleanly.

  The number of budgets a server will hold is capped by
  `--max-budgets` / `ZIZQ_MAX_BUDGETS`, defaulting to 8192. The limit
  exists because budget keys can be generated from application data,
  and remain peristed indefinitely. Future releases will enable more
  dynamic sub-buckets within budgets.

  A job's budgets can be changed after it is enqueued, through routes on
  `/jobs/{id}/budgets`: `POST` a key to bind one (409 if already bound,
  and `create_with` works here as it does on an enqueue), `PUT` to bind
  or replace, `PATCH` to change what it draws, `DELETE` to unbind.
  `PUT /jobs/{id}/budgets` replaces the whole set and
  `DELETE /jobs/{id}/budgets` clears it. Only queued jobs may change —
  an in-flight job holds tokens against the bindings it was dispatched
  under, so moving them would either invent a slot on the new budget or
  strand one on the old, and the request is refused with 422.

  Jobs can be filtered by budget with `?budgets.key=`, comma-delimited
  like `queue` and `type`, on `GET /jobs`, `GET /jobs/count`,
  `PATCH /jobs` and `DELETE /jobs`. It matches a job drawing on any of
  the named budgets. This is what makes the refusal above actionable:
  `DELETE /jobs?budgets.key=stripe` clears the jobs holding a budget
  open so it can then be deleted. There is no index from budget to job,
  so combined with a `status`, `queue`, `type` or `id` filter it narrows
  within that scan, and on its own it is a full scan — the same cost
  profile the existing payload filter carries.

  Policy changes take effect immediately, including on work already
  running. Narrowing a `while_in_flight` budget below what is currently
  in flight leaves it over-committed, and those slots are surrendered as
  jobs finish rather than handed straight to replacements — so cutting
  an allocation from six to one while six are running settles back to
  one, instead of staying at six for as long as work keeps arriving.

  Policy changes also take effect immediately on jobs already waiting.
  Speeding a rate limit up re-arms the dispatcher rather than
  leaving parked jobs to wait out the old period, and progress already
  accrued toward the next token is kept across a change of rate — half a
  minute spent waiting on a one-a-minute budget still counts when it
  becomes two a second. Changing the *period* discards that progress,
  since it is measured against the period it was accrued under.

  `POST /reset` now clears budgets along with jobs and cron groups.

  Token state is deliberately not persisted, so a server restart brings
  every bucket back full. For a concurrency budget that is simply
  accurate — recovery returns in-flight jobs to the queue, so nothing
  holds a slot. For a rate limit it means a restart forgives whatever
  the previous process had spent, which is the trade-off between an
  efficient lazily computed in-memory solution vs a slower one that
  constantly writes to disk to record state.

- Added a **group-level cron timezone**. `PUT /crons/{group}` and
  `PATCH /crons/{group}` now accept a `timezone` field alongside
  `paused`, and it is returned on every cron group response. Entries
  that do not name a timezone of their own are evaluated in the
  group's; entries that do are unaffected. Without either, entries
  continue to run in the server's local timezone, so existing
  schedules are unchanged. Previously a client wanting one timezone
  for a whole schedule had to copy it onto every entry, which did not
  survive a read-back as a group-level fact.

  `PATCH /crons/{group}` follows JSON merge patch semantics for this:
  an absent field is left alone, and `"timezone": null` clears the
  group's timezone. `paused` is now optional there for the same
  reason — a patch may change only the timezone. Because
  `PUT /crons/{group}` replaces a group in full, omitting `timezone`
  on a `PUT` clears it.

  Changing a group's timezone reschedules every entry that inherits
  it, rather than leaving the old firing times in place until each
  entry next fires.

- Fixed cron entries keeping a stale `next_enqueue_at` when only their
  `timezone` changed. `PUT /crons/{group}` and
  `PUT /crons/{group}/entries/{entry}` preserve scheduling state when
  an entry is unchanged, but compared expressions alone — so moving an
  entry between timezones left it firing at the old wall-clock time
  until its next occurrence passed. The comparison now covers the
  effective timezone as well.

## 0.6.1

- Fixed a units error in the retry backoff formula where the
  `attempts^exponent` term was treated as milliseconds instead of
  seconds. With the shipped defaults this silently shortened the
  total retry window from the intended ~25 days to under 4 hours,
  so a transient production incident could exhaust all retries
  within a single outage rather than spanning multiple days. Added
  a pinning test that asserts the total retry window with the
  default configuration stays within a 24-26 day band, so a future
  regression on the same axis is caught in CI.

## 0.6.0

- Added **batched jobs** (Pro): a new `batch` field on enqueue
  requests that accumulates multiple enqueues into a single pending
  job's payload. Callers supply `batch.key` (identifies the batch),
  `batch.when` (a jq predicate deciding whether to fold), and
  `batch.fold` (a jq reducer producing the merged payload). Both
  expressions run with `$existing` bound to the current pending
  payload and `$new` bound to the incoming one. When `when` returns
  truthy the payloads are merged in place at a fresh payload key
  (preserving FIFO position); when falsy the existing batch seals
  and a new pending job takes over. Once claimed, a batched job is
  a normal job — workers see one merged payload and ack once.
  Suited to downstream services that accept batched calls (APNs
  push, SES bulk send, etc.) but whose callers enqueue one unit at
  a time. The first enqueue's `when`/`fold` are stored on the job
  and win for the whole batch — follow-up enqueues supply payload
  against the existing config until seal. Enqueue responses gain a
  `folded: true | false` field, symmetric with `duplicate`.
  Combining `unique_key` and `batch` on a single enqueue is a 400.
  Cron entries may carry a `batch` config; the fold logic runs at
  fire time. Job reads (`GET /jobs`, `GET /jobs/{id}`, enqueue
  responses) include the stored `batch` config so callers can
  inspect exactly what `when`/`fold` the batch is running against.
  Scheduled batched enqueues (`ready_at > now`) opt out of the fold
  path — they persist as normal scheduled jobs with `batch`
  metadata for observability, but no fold happens across a
  `ready_at` boundary in either direction. Fold semantics remain
  strictly immediate-Ready <-> immediate-Ready.

## 0.5.1

- Added `/` search to `zizq top`. Two labeled fields — Type and Queue
  — with `Tab` to switch between them. Each field supports the standard
  editing keys (Left/Right, Home/End, Delete, Backspace) via the
  `tui-input` crate. `n` steps to the next match, `N` to the previous,
  Esc unpins. Comma- or space-separate multiple values in a field to
  OR them. Values match as substrings (`audit` matches `audit.create`,
  `payment.audit`); the two fields AND together. Reopening `/` while
  a search is active prefills the current query. `/` and `n`/`N` walk
  forward from the cursor's current row rather than restarting at the
  top of the list — `g` then `n` searches from the beginning.
- Reworked list navigation in `zizq top` around an anchor-based
  subscription model. The client only holds a buffer of rows around
  the cursor and requests more as you scroll; the cursor is
  identified by job id when tracking a search match (so it stays on
  the same row through queue churn), and by depth when free-scrolling
  (so the viewport row you're on stays stable while items shift
  underneath). Scrolling past the loaded edge stops at the edge and
  waits for a prefetch instead of blanking the viewport. Prefetch
  subscribes are rate-limited to at most one per 100 ms per tab so
  a held arrow key or `JobChanged` burst doesn't spam the server.
- Reformatted the `zizq top` help bar in htop's key-and-label style
  (no space between key and label; color contrast marks the boundary)
  and added a `/Find` entry.
- Added an in-memory `InFlightIndex` to the store, mirroring the
  existing `ReadyIndex` and `ScheduledIndex`. Powers the in-flight
  view in the admin API without walking the LSM tree.

## 0.5.0

- Added `priority`, `ready_at`, and `attempts` range filters to
  `GET /jobs`, `GET /jobs/count`, `DELETE /jobs`, and `PATCH /jobs`.
  Each accepts four shapes — `N` (single value), `A..B` (bounded),
  `..B` (unbounded lower), and `A..` (unbounded upper) — with inclusive
  bounds on both ends. The lower bound must not exceed the upper bound
  or the request is rejected with 400. Examples:
  `?priority=100..200`, `?ready_at=..1735689600000`,
  `?attempts=1..` (anything that has failed at least once). Range
  parameters compose with `status`, `queue`, `type`, `id`, and the jq
  `filter` via intersection, and are preserved across paginated
  responses. There are no dedicated indexes for these fields: the
  scan applies them as a post-hydration check after the
  status/queue/type/id intersection narrows the candidate set, so for
  efficient narrowing on large stores combine each range with at least
  one indexed filter.

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
