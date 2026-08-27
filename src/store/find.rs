// Copyright (c) 2026 Chris Corbyn <chris@zizq.io>
// Licensed under the Business Source License 1.1. See LICENSE file for details.

//! `less`-style search over the in-memory Ready / InFlight / Scheduled
//! indexes.
//!
//! `find_ready`, `find_in_flight`, and `find_scheduled` each walk their
//! respective index in a single pass, resolve an optional anchor id to
//! an absolute position, apply a queue+type predicate to each candidate,
//! and return the first match with a window of surrounding items. The
//! dashboard uses this to power `/`, `n`, `N`, and to re-locate a
//! pinned job that has drifted out of the local buffer.
//!
//! Design notes:
//!
//! - **Race-free by construction.** Each walk operates on a consistent
//!   iterator over the SkipMap / SkipSet — the client's cursor
//!   position never has to survive a round-trip. The client anchors
//!   by *job id*, not by numeric position, and the server resolves the
//!   id at scan time.
//! - **`Locate` direction.** The dashboard's row-sticky follow uses
//!   `Locate` to ask "where is this id now?" without walking further.
//!   Returns `None` when the id is no longer in the index (job
//!   completed / requeued / removed) so the client can unpin.
//! - **Anchor-not-found fallback.** For `Forward`, an unknown anchor
//!   falls back to a walk from position 0. For `Backward`, from the
//!   last position. For `Locate`, returns `None`. This makes searches
//!   robust to the anchor job being processed between key press and
//!   server receipt.

use tokio::task;

use super::keys::make_job_key;
use super::store::Store;
use super::types::{Job, StoreError};

/// Direction of a `find_*` walk.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FindDirection {
    /// Skip the anchor, walk forward, return first match.
    Forward,
    /// Skip the anchor, walk backward, return first match.
    Backward,
    /// Return the anchor's current position without walking. Query is
    /// ignored. Falls through to `None` when the anchor is not found.
    Locate,
}

/// Anchor of a `list_window_*` request.
///
/// - `Around(id)`: follow a specific job id (search / pin).
/// - `Offset(pos)`: numeric depth anchor for depth-based navigation
///   (arrow keys, page down). Clamped to `max(0, total - limit)`
///   when `pos >= total` so the client always gets tail items under
///   drain instead of an empty window.
/// - `Head` / `Tail`: natural start / end anchors.
#[derive(Debug, Clone)]
pub enum WindowAnchor {
    Around(String),
    Offset(usize),
    Head,
    Tail,
}

/// Fallback behaviour for `list_window_*` when an `Around` anchor id
/// is not present in the list at scan time. See `api::admin::Fallback`
/// for the wire-facing counterpart.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WindowFallback {
    Head,
    Tail,
    Nowhere,
}

/// A window returned by `list_window_*`. `first_position` is the
/// 0-based position of `items[0]` in the underlying sorted list;
/// `total` is the list's current length. `resolved_anchor` is
/// `Some(id)` when the requested `Around` anchor was found (the
/// window is centered on it), and `None` when the fallback was
/// exercised.
#[derive(Debug)]
pub struct WindowOutcome {
    pub items: Vec<Job>,
    pub first_position: usize,
    pub total: usize,
    pub resolved_anchor: Option<String>,
}

/// A match returned by `find_*`. `position` is the item's absolute
/// index within the underlying in-memory index at scan time.
/// `window_offset` + `window_items` describe a page of surrounding
/// items so the dashboard can render the match's neighbourhood
/// without a separate `Subscribe` round-trip.
#[derive(Debug)]
pub struct FindOutcome {
    pub matched_position: usize,
    pub window_offset: usize,
    pub window_items: Vec<Job>,
}

/// Test a metadata-hydrated `Job` against a queue/type predicate.
/// Empty vectors mean "no constraint on this dimension." Values are
/// matched as substrings (case-sensitive) so `audit` matches
/// `audit.create`. Multiple values inside a vector are OR'd; different
/// fields are AND'd.
fn matches_query(queues: &[String], types: &[String], job: &Job) -> bool {
    if !queues.is_empty() && !queues.iter().any(|q| job.queue.contains(q)) {
        return false;
    }
    if !types.is_empty() && !types.iter().any(|t| job.job_type.contains(t)) {
        return false;
    }
    true
}

/// Center a window of `limit` items around `matched_position`, clamped
/// to `[0, total)`. Returns the `window_offset` to hand to the caller.
fn window_offset_around(matched_position: usize, limit: usize, total: usize) -> usize {
    let half = limit / 2;
    let raw = matched_position.saturating_sub(half);
    // Prefer to keep `limit` items in view: if the raw offset would
    // leave the window straddling the tail, slide it up so items fill
    // the window when possible.
    let max_offset = total.saturating_sub(limit);
    raw.min(max_offset)
}

impl Store {
    /// Search the in-memory `Dispatch` for the first job matching
    /// `queues` and `types` per `direction`, starting from `anchor_id`
    /// (or the natural start/end when `None`). See module docs.
    pub async fn find_ready(
        &self,
        anchor_id: Option<String>,
        direction: FindDirection,
        queues: Vec<String>,
        types: Vec<String>,
        limit: usize,
    ) -> Result<Option<FindOutcome>, StoreError> {
        let dispatch = self.dispatch.clone();
        let ks = self.ks.clone();

        task::spawn_blocking(move || {
            // Materialise the index into a Vec of ids so we can index
            // by position for anchor lookup, walk in either direction,
            // and build the window without re-walking. This is one
            // allocation of `n * (u16 + String)` — acceptable for the
            // Ready index (ephemeral, typically small).
            let entries: Vec<(u16, String)> = dispatch.iter().collect();
            walk_and_window(&ks, entries, anchor_id, direction, &queues, &types, limit)
        })
        .await?
    }

    /// Search the in-memory `InFlightIndex`. See `find_ready`.
    pub async fn find_in_flight(
        &self,
        anchor_id: Option<String>,
        direction: FindDirection,
        queues: Vec<String>,
        types: Vec<String>,
        limit: usize,
    ) -> Result<Option<FindOutcome>, StoreError> {
        let in_flight_index = self.in_flight_index.clone();
        let ks = self.ks.clone();

        task::spawn_blocking(move || {
            let entries: Vec<(u64, String)> = in_flight_index.iter().collect();
            walk_and_window(&ks, entries, anchor_id, direction, &queues, &types, limit)
        })
        .await?
    }

    /// Search the in-memory `ScheduledIndex`. See `find_ready`.
    pub async fn find_scheduled(
        &self,
        anchor_id: Option<String>,
        direction: FindDirection,
        queues: Vec<String>,
        types: Vec<String>,
        limit: usize,
    ) -> Result<Option<FindOutcome>, StoreError> {
        let scheduled_index = self.scheduled_index.clone();
        let ks = self.ks.clone();

        task::spawn_blocking(move || {
            let entries: Vec<(u64, String)> = scheduled_index.iter().collect();
            walk_and_window(&ks, entries, anchor_id, direction, &queues, &types, limit)
        })
        .await?
    }

    /// Return a window of jobs from the `Dispatch` anchored per
    /// `anchor`. See `WindowAnchor` for anchor semantics and
    /// `WindowFallback` for the "anchor not found" fallback.
    pub async fn list_window_ready(
        &self,
        anchor: WindowAnchor,
        fallback: WindowFallback,
        limit: usize,
    ) -> Result<WindowOutcome, StoreError> {
        let dispatch = self.dispatch.clone();
        let ks = self.ks.clone();

        task::spawn_blocking(move || {
            let entries: Vec<(u16, String)> = dispatch.iter().collect();
            resolve_window(&ks, entries, anchor, fallback, limit)
        })
        .await?
    }

    /// Same shape as `list_window_ready` but over the `InFlightIndex`.
    pub async fn list_window_in_flight(
        &self,
        anchor: WindowAnchor,
        fallback: WindowFallback,
        limit: usize,
    ) -> Result<WindowOutcome, StoreError> {
        let in_flight_index = self.in_flight_index.clone();
        let ks = self.ks.clone();

        task::spawn_blocking(move || {
            let entries: Vec<(u64, String)> = in_flight_index.iter().collect();
            resolve_window(&ks, entries, anchor, fallback, limit)
        })
        .await?
    }

    /// Same shape as `list_window_ready` but over the `ScheduledIndex`.
    pub async fn list_window_scheduled(
        &self,
        anchor: WindowAnchor,
        fallback: WindowFallback,
        limit: usize,
    ) -> Result<WindowOutcome, StoreError> {
        let scheduled_index = self.scheduled_index.clone();
        let ks = self.ks.clone();

        task::spawn_blocking(move || {
            let entries: Vec<(u64, String)> = scheduled_index.iter().collect();
            resolve_window(&ks, entries, anchor, fallback, limit)
        })
        .await?
    }
}

/// Shared window-resolution body used by all three `list_window_*`
/// methods. Given a materialised list of `(sort_key, job_id)` tuples,
/// resolves the anchor to a position, computes a window centered on
/// it (or clamped to the head/tail per `fallback`), and hydrates the
/// job records inside that window.
fn resolve_window<K>(
    ks: &super::store::Keyspaces,
    entries: Vec<(K, String)>,
    anchor: WindowAnchor,
    fallback: WindowFallback,
    limit: usize,
) -> Result<WindowOutcome, StoreError> {
    let total = entries.len();
    if total == 0 {
        return Ok(WindowOutcome {
            items: Vec::new(),
            first_position: 0,
            total: 0,
            resolved_anchor: None,
        });
    }

    // Phase 1: resolve the anchor to a first_position for the window.
    //
    // `Around` centers the window on the anchor id.
    // `Offset` uses the raw numeric offset as the window start,
    //   clamped to the valid range.
    // `Head` / `Tail` pin the window to the natural ends.
    let (first_position, resolved_anchor, center_on_anchor) = match &anchor {
        WindowAnchor::Head => (0, None, false),
        WindowAnchor::Tail => (total.saturating_sub(limit), None, false),
        WindowAnchor::Offset(offset) => {
            let max_start = total.saturating_sub(limit);
            ((*offset).min(max_start), None, false)
        }
        WindowAnchor::Around(id) => match entries.iter().position(|(_, jid)| jid == id) {
            Some(pos) => (pos, Some(id.clone()), true),
            None => match fallback {
                WindowFallback::Head => (0, None, false),
                WindowFallback::Tail => (total.saturating_sub(limit), None, false),
                WindowFallback::Nowhere => {
                    return Ok(WindowOutcome {
                        items: Vec::new(),
                        first_position: 0,
                        total,
                        resolved_anchor: None,
                    });
                }
            },
        },
    };

    // For `Around`, center the window on the resolved position; for
    // `Head`/`Tail`/`Offset`, the first_position we just computed IS
    // the window start.
    let first_position = if center_on_anchor {
        let half = limit / 2;
        let raw_start = first_position.saturating_sub(half);
        let max_start = total.saturating_sub(limit);
        raw_start.min(max_start)
    } else {
        first_position
    };
    let end = (first_position + limit).min(total);

    // Phase 3: hydrate.
    let mut items = Vec::with_capacity(end - first_position);
    for (_, id) in entries.iter().take(end).skip(first_position) {
        if let Some(job) = load_job(ks, id)? {
            items.push(job);
        }
    }

    Ok(WindowOutcome {
        items,
        first_position,
        total,
        resolved_anchor,
    })
}

/// Shared walk-and-window body over an already-materialised list of
/// `(sort_key, job_id)` tuples. Generic over the sort key type so the
/// three lists can share this implementation.
fn walk_and_window<K>(
    ks: &super::store::Keyspaces,
    entries: Vec<(K, String)>,
    anchor_id: Option<String>,
    direction: FindDirection,
    queues: &[String],
    types: &[String],
    limit: usize,
) -> Result<Option<FindOutcome>, StoreError> {
    let n = entries.len();
    if n == 0 {
        return Ok(None);
    }

    // Resolve anchor to a position (if any).
    let anchor_position = match &anchor_id {
        Some(id) => entries.iter().position(|(_, jid)| jid == id),
        None => None,
    };

    // Phase 1: find the matched position.
    let matched_position = match direction {
        FindDirection::Locate => match anchor_position {
            Some(pos) => Some(pos),
            None => return Ok(None),
        },
        FindDirection::Forward => {
            let start = match anchor_position {
                Some(pos) => pos + 1,
                // Anchor was requested but not found — fall back to
                // walking from the start.
                None => 0,
            };
            let mut found = None;
            for i in start..n {
                let (_, id) = &entries[i];
                if let Some(job) = load_job(ks, id)? {
                    if matches_query(queues, types, &job) {
                        found = Some(i);
                        break;
                    }
                }
            }
            found
        }
        FindDirection::Backward => {
            let start = match anchor_position {
                Some(pos) if pos > 0 => pos - 1,
                Some(_) => return Ok(None), // pos = 0, nothing before it
                // Anchor not found — fall back to walking from the end.
                None => n - 1,
            };
            let mut found = None;
            for i in (0..=start).rev() {
                let (_, id) = &entries[i];
                if let Some(job) = load_job(ks, id)? {
                    if matches_query(queues, types, &job) {
                        found = Some(i);
                        break;
                    }
                }
            }
            found
        }
    };

    // Phase 2: build a centered window around the match.
    let Some(matched_position) = matched_position else {
        return Ok(None);
    };
    let window_offset = window_offset_around(matched_position, limit, n);
    let mut window_items = Vec::with_capacity(limit);
    for i in window_offset..(window_offset + limit).min(n) {
        let (_, id) = &entries[i];
        if let Some(job) = load_job(ks, id)? {
            window_items.push(job);
        }
    }

    Ok(Some(FindOutcome {
        matched_position,
        window_offset,
        window_items,
    }))
}

/// Hydrate a job from the `jobs` keyspace by id. Returns `Ok(None)`
/// when the job is not on disk (which can happen if the index
/// contains an id that has already been removed — the caller just
/// skips the entry).
fn load_job(ks: &super::store::Keyspaces, id: &str) -> Result<Option<Job>, StoreError> {
    let job_key = make_job_key(id);
    match ks.data.get(&job_key)? {
        Some(bytes) => Ok(Some(rmp_serde::from_slice(&bytes)?)),
        None => Ok(None),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::options::EnqueueOptions;
    use crate::store::test_support::test_store;
    use crate::time::now_millis;

    /// Enqueue three ready jobs across two queues and two types so we
    /// have a stable filter/order fixture:
    ///
    /// - `j_a` priority 10, queue=q1, type=t1
    /// - `j_b` priority 20, queue=q2, type=t1
    /// - `j_c` priority 30, queue=q1, type=t2
    async fn fixture_three_ready() -> (crate::store::Store, [String; 3]) {
        let store = test_store();
        let now = now_millis();
        let a = store
            .enqueue(
                now,
                EnqueueOptions::new("t1", "q1", serde_json::json!(null)).priority(10),
            )
            .await
            .unwrap()
            .into_job();
        let b = store
            .enqueue(
                now,
                EnqueueOptions::new("t1", "q2", serde_json::json!(null)).priority(20),
            )
            .await
            .unwrap()
            .into_job();
        let c = store
            .enqueue(
                now,
                EnqueueOptions::new("t2", "q1", serde_json::json!(null)).priority(30),
            )
            .await
            .unwrap()
            .into_job();
        (store, [a.id, b.id, c.id])
    }

    #[tokio::test]
    async fn forward_from_start_returns_first_matching_queue() {
        let (store, [_a, b, _c]) = fixture_three_ready().await;
        let outcome = store
            .find_ready(
                None,
                FindDirection::Forward,
                vec!["q2".into()],
                Vec::new(),
                10,
            )
            .await
            .unwrap()
            .expect("should find a match");
        assert_eq!(outcome.matched_position, 1);
        assert_eq!(
            outcome.window_items[outcome.matched_position - outcome.window_offset].id,
            b
        );
    }

    #[tokio::test]
    async fn forward_skips_anchor_and_finds_next_match() {
        let (store, [a, _b, c]) = fixture_three_ready().await;
        // Anchor on j_a with query type=t2 should return j_c (index 2).
        let outcome = store
            .find_ready(
                Some(a),
                FindDirection::Forward,
                Vec::new(),
                vec!["t2".into()],
                10,
            )
            .await
            .unwrap()
            .expect("should find j_c");
        assert_eq!(outcome.matched_position, 2);
        assert_eq!(
            outcome.window_items[outcome.matched_position - outcome.window_offset].id,
            c
        );
    }

    #[tokio::test]
    async fn forward_returns_none_when_no_match() {
        let (store, _) = fixture_three_ready().await;
        let outcome = store
            .find_ready(
                None,
                FindDirection::Forward,
                vec!["no-such-queue".into()],
                Vec::new(),
                10,
            )
            .await
            .unwrap();
        assert!(outcome.is_none());
    }

    #[tokio::test]
    async fn forward_with_unknown_anchor_falls_back_to_start() {
        let (store, [a, _b, _c]) = fixture_three_ready().await;
        // Unknown anchor + queue=q1 should match j_a at position 0.
        let outcome = store
            .find_ready(
                Some("nonexistent".into()),
                FindDirection::Forward,
                vec!["q1".into()],
                Vec::new(),
                10,
            )
            .await
            .unwrap()
            .expect("should fall back and match j_a");
        assert_eq!(outcome.matched_position, 0);
        assert_eq!(outcome.window_items[0].id, a);
    }

    #[tokio::test]
    async fn backward_from_anchor_finds_earlier_match() {
        let (store, [_a, b, c]) = fixture_three_ready().await;
        // Backward from j_c with type=t1 — the closest t1 walking
        // backward from position 2 is j_b at position 1.
        let outcome = store
            .find_ready(
                Some(c),
                FindDirection::Backward,
                Vec::new(),
                vec!["t1".into()],
                10,
            )
            .await
            .unwrap()
            .expect("should find a t1 match before j_c");
        assert_eq!(outcome.matched_position, 1);
        let local = outcome.matched_position - outcome.window_offset;
        assert_eq!(outcome.window_items[local].id, b);
    }

    #[tokio::test]
    async fn substring_type_match_finds_prefixed_type() {
        // audit.create, audit.delete, payment.process across three
        // queues. Type: "audit" should match the first two.
        let store = test_store();
        let now = now_millis();
        let a = store
            .enqueue(
                now,
                EnqueueOptions::new("audit.create", "emails", serde_json::json!(null)).priority(10),
            )
            .await
            .unwrap()
            .into_job()
            .id;
        let _b = store
            .enqueue(
                now,
                EnqueueOptions::new("audit.delete", "billing", serde_json::json!(null))
                    .priority(20),
            )
            .await
            .unwrap()
            .into_job()
            .id;
        let _c = store
            .enqueue(
                now,
                EnqueueOptions::new("payment.process", "emails", serde_json::json!(null))
                    .priority(30),
            )
            .await
            .unwrap()
            .into_job()
            .id;

        let outcome = store
            .find_ready(
                None,
                FindDirection::Forward,
                Vec::new(),
                vec!["audit".into()],
                10,
            )
            .await
            .unwrap()
            .expect("substring `audit` should hit audit.create at position 0");
        assert_eq!(outcome.matched_position, 0);
        assert_eq!(outcome.window_items[0].id, a);
    }

    #[tokio::test]
    async fn substring_and_across_type_and_queue() {
        // Type: audit, Queue: emails — only the first job matches both.
        let store = test_store();
        let now = now_millis();
        let a = store
            .enqueue(
                now,
                EnqueueOptions::new("audit.create", "emails", serde_json::json!(null)).priority(10),
            )
            .await
            .unwrap()
            .into_job()
            .id;
        // audit.delete on billing — matches type but not queue.
        let _b = store
            .enqueue(
                now,
                EnqueueOptions::new("audit.delete", "billing", serde_json::json!(null))
                    .priority(20),
            )
            .await
            .unwrap()
            .into_job()
            .id;
        // payment.process on emails — matches queue but not type.
        let _c = store
            .enqueue(
                now,
                EnqueueOptions::new("payment.process", "emails", serde_json::json!(null))
                    .priority(30),
            )
            .await
            .unwrap()
            .into_job()
            .id;

        let outcome = store
            .find_ready(
                None,
                FindDirection::Forward,
                vec!["emails".into()],
                vec!["audit".into()],
                10,
            )
            .await
            .unwrap()
            .expect("only audit.create on emails matches both");
        assert_eq!(outcome.matched_position, 0);
        assert_eq!(outcome.window_items[0].id, a);
    }

    #[tokio::test]
    async fn locate_returns_current_position_of_anchor() {
        let (store, [_a, b, _c]) = fixture_three_ready().await;
        let outcome = store
            .find_ready(
                Some(b.clone()),
                FindDirection::Locate,
                // Query is ignored under Locate.
                vec!["irrelevant".into()],
                Vec::new(),
                10,
            )
            .await
            .unwrap()
            .expect("should locate j_b");
        assert_eq!(outcome.matched_position, 1);
        assert_eq!(
            outcome.window_items[outcome.matched_position - outcome.window_offset].id,
            b
        );
    }

    #[tokio::test]
    async fn locate_returns_none_when_anchor_gone() {
        let (store, _) = fixture_three_ready().await;
        let outcome = store
            .find_ready(
                Some("does-not-exist".into()),
                FindDirection::Locate,
                Vec::new(),
                Vec::new(),
                10,
            )
            .await
            .unwrap();
        assert!(outcome.is_none());
    }

    #[tokio::test]
    async fn window_is_centered_on_match() {
        let store = test_store();
        let now = now_millis();
        let mut ids = Vec::new();
        for i in 0..10u16 {
            let job = store
                .enqueue(
                    now,
                    EnqueueOptions::new("t1", "q1", serde_json::json!(null)).priority(i),
                )
                .await
                .unwrap()
                .into_job();
            ids.push(job.id);
        }
        // Anchor at position 0, forward, query is trivial (matches all).
        // First match after position 0 is position 1. Window limit 4 —
        // centered gives offset = 1 - 2 = 0, items 0..4.
        let outcome = store
            .find_ready(
                Some(ids[0].clone()),
                FindDirection::Forward,
                Vec::new(),
                Vec::new(),
                4,
            )
            .await
            .unwrap()
            .expect("should find next job");
        assert_eq!(outcome.matched_position, 1);
        assert_eq!(outcome.window_offset, 0);
        assert_eq!(outcome.window_items.len(), 4);
    }
}
