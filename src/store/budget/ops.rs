// Copyright (c) 2026 Chris Corbyn <chris@zizq.io>
// Licensed under the Business Source License 1.1. See LICENSE file for details.

//! Budget CRUD against the data keyspace.
//!
//! Lives in a submodule of `budget/` so the key builder stays
//! package-private to the budget subsystem, as the cron module does.
//!
//! Four write verbs differ only in what they do to a budget that
//! already exists:
//!
//! | Operation        | Absent    | Exists                 |
//! |------------------|-----------|------------------------|
//! | `create_budget`  | create    | `Conflict`             |
//! | `put_budget`     | create    | replace policy whole   |
//! | `patch_budget`   | `None`    | merge named fields     |
//! | `delete_budget`  | `false`   | delete                 |
//!
//! `create_budget` exists because `put`/`patch` inherently overwrite
//! whatever an operator adjusted. An application declaring "this budget
//! should exist" on every boot needs a call that will not clobber an
//! allocation someone tightened during an incident.

use std::ops::Bound;

use fjall::Readable;
use tokio::task;

use super::super::keys::RecordKind;
use super::super::options::PatchBudgetOptions;
use super::super::store::Store;
use super::super::types::StoreError;
use super::{Budget, BudgetStrategy};

impl Store {
    /// Create a budget, failing if the key is taken.
    ///
    /// Returns [`StoreError::Conflict`] if a budget already exists,
    /// leaving the stored policy untouched.
    pub async fn create_budget(
        &self,
        key: &str,
        allocation: u32,
        strategy: BudgetStrategy,
        now: u64,
    ) -> Result<Budget, StoreError> {
        let ks = self.ks.clone();
        let key = key.to_string();

        task::spawn_blocking(move || -> Result<Budget, StoreError> {
            // Validate before taking the write lock.
            let budget = Budget::new(allocation, strategy, now)?;
            let budget_key = make_budget_key(&key);

            let mut tx = ks.write_tx();

            // Checked under the single-writer lock, so no concurrent
            // creator can slip between the read and the insert.
            if ks.data.get(&budget_key)?.is_some() {
                return Err(StoreError::Conflict(format!(
                    "budget '{key}' already exists"
                )));
            }

            tx.insert(&ks.data, &budget_key, &rmp_serde::to_vec_named(&budget)?);
            ks.commit(tx, ks.default_commit_mode)?;

            Ok(budget)
        })
        .await?
    }

    /// Create a budget, or replace an existing one's policy.
    ///
    /// `created_at` is preserved when the budget already exists — a
    /// replace changes the policy, not the budget's identity.
    pub async fn put_budget(
        &self,
        key: &str,
        allocation: u32,
        strategy: BudgetStrategy,
        now: u64,
    ) -> Result<Budget, StoreError> {
        let ks = self.ks.clone();
        let key = key.to_string();

        task::spawn_blocking(move || -> Result<Budget, StoreError> {
            let mut budget = Budget::new(allocation, strategy, now)?;
            let budget_key = make_budget_key(&key);

            let mut tx = ks.write_tx();

            if let Some(bytes) = ks.data.get(&budget_key)? {
                let existing: Budget = rmp_serde::from_slice(&bytes)?;
                budget.created_at = existing.created_at;
            }

            tx.insert(&ks.data, &budget_key, &rmp_serde::to_vec_named(&budget)?);
            ks.commit(tx, ks.default_commit_mode)?;

            Ok(budget)
        })
        .await?
    }

    /// Update named fields of an existing budget's policy.
    ///
    /// Absent fields are left alone. Returns `None` if the budget does
    /// not exist — unlike `put_budget`, a patch has nothing to merge
    /// into and does not create one.
    pub async fn patch_budget(
        &self,
        key: &str,
        opts: PatchBudgetOptions,
        now: u64,
    ) -> Result<Option<Budget>, StoreError> {
        let ks = self.ks.clone();
        let key = key.to_string();

        task::spawn_blocking(move || -> Result<Option<Budget>, StoreError> {
            let budget_key = make_budget_key(&key);

            let mut tx = ks.write_tx();

            let existing: Budget = match ks.data.get(&budget_key)? {
                Some(bytes) => rmp_serde::from_slice(&bytes)?,
                None => return Ok(None),
            };

            // Re-validated through `Budget::new`, since a patch can
            // reach a combination neither the stored policy nor the
            // patch alone would have been rejected for.
            let mut budget = Budget::new(
                opts.allocation.unwrap_or(existing.allocation),
                opts.strategy.unwrap_or(existing.strategy),
                now,
            )?;
            budget.created_at = existing.created_at;

            tx.insert(&ks.data, &budget_key, &rmp_serde::to_vec_named(&budget)?);
            ks.commit(tx, ks.default_commit_mode)?;

            Ok(Some(budget))
        })
        .await?
    }

    /// Load a budget's policy, or `None` if it does not exist.
    pub async fn get_budget(&self, key: &str) -> Result<Option<Budget>, StoreError> {
        let ks = self.ks.clone();
        let key = key.to_string();

        task::spawn_blocking(move || -> Result<Option<Budget>, StoreError> {
            let snapshot = ks.db.read_tx();

            match snapshot.get(&ks.data, make_budget_key(&key))? {
                Some(bytes) => Ok(Some(rmp_serde::from_slice(&bytes)?)),
                None => Ok(None),
            }
        })
        .await?
    }

    /// List every budget with its policy, in lexicographic key order.
    ///
    /// A plain range scan, unlike `list_cron_groups` — budget keys have
    /// nothing nested beneath them, so there is nothing to step over.
    ///
    /// The policy comes back with the key because the scan has already
    /// read it; discarding it here would only force the caller into a
    /// read per budget. A budget's policy is a handful of scalars, so
    /// the whole list stays small at any sane number of budgets.
    pub async fn list_budgets(&self) -> Result<Vec<(String, Budget)>, StoreError> {
        let ks = self.ks.clone();

        task::spawn_blocking(move || -> Result<Vec<(String, Budget)>, StoreError> {
            let snapshot = ks.db.read_tx();

            // Every budget key is `B\0{key}`, so `[B\0, B\1)` is exactly
            // the set of them.
            let start = vec![RecordKind::Budget as u8, 0];
            let end = vec![RecordKind::Budget as u8, 1];

            snapshot
                .range::<Vec<u8>, _>(&ks.data, (Bound::Included(start), Bound::Excluded(end)))
                .map(|guard| {
                    let (key, value) = guard.into_inner()?;
                    // Skip the `B\0` prefix to recover the budget key.
                    let key = String::from_utf8(key[2..].to_vec()).map_err(|e| {
                        StoreError::Corruption(format!("budget key is not valid UTF-8: {e}"))
                    })?;
                    Ok((key, rmp_serde::from_slice(&value)?))
                })
                .collect()
        })
        .await?
    }

    /// Delete a budget.
    ///
    /// Returns `true` if it existed, `false` otherwise.
    ///
    /// Deleting a budget that jobs still reference will be rejected
    /// once cost accounting lands — there is nothing tracking those
    /// references yet, so this currently deletes unconditionally.
    pub async fn delete_budget(&self, key: &str) -> Result<bool, StoreError> {
        let ks = self.ks.clone();
        let key = key.to_string();

        task::spawn_blocking(move || -> Result<bool, StoreError> {
            let budget_key = make_budget_key(&key);

            let mut tx = ks.write_tx();

            if ks.data.get(&budget_key)?.is_none() {
                return Ok(false);
            }

            tx.remove(&ks.data, &budget_key);
            ks.commit(tx, ks.default_commit_mode)?;

            Ok(true)
        })
        .await?
    }
}

/// Build a budget key: `B\0{key}`.
fn make_budget_key(key: &str) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(2 + key.len());
    bytes.push(RecordKind::Budget as u8);
    bytes.push(0);
    bytes.extend_from_slice(key.as_bytes());
    bytes
}

#[cfg(test)]
mod tests {
    use super::super::super::options::PatchBudgetOptions;
    use super::super::super::test_support::test_store;
    use super::super::BudgetStrategy;
    use crate::store::StoreError;

    const NOW: u64 = 1_700_000_000_000;

    fn patch(allocation: Option<u32>, strategy: Option<BudgetStrategy>) -> PatchBudgetOptions {
        PatchBudgetOptions {
            allocation,
            strategy,
        }
    }

    #[tokio::test]
    async fn create_then_get_round_trips() {
        let store = test_store();

        let created = store
            .create_budget("stripe", 100, BudgetStrategy::WhileInFlight, NOW)
            .await
            .unwrap();
        assert_eq!(created.allocation, 100);

        let loaded = store.get_budget("stripe").await.unwrap().unwrap();
        assert_eq!(loaded.allocation, 100);
        assert_eq!(loaded.strategy, BudgetStrategy::WhileInFlight);
        assert_eq!(loaded.created_at, NOW);
    }

    #[tokio::test]
    async fn get_returns_none_for_a_missing_budget() {
        let store = test_store();
        assert!(store.get_budget("absent").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn create_rejects_an_existing_key_without_touching_it() {
        let store = test_store();
        store
            .create_budget("stripe", 100, BudgetStrategy::WhileInFlight, NOW)
            .await
            .unwrap();

        let result = store
            .create_budget("stripe", 5, BudgetStrategy::WhileInFlight, NOW + 1_000)
            .await;
        assert!(matches!(result, Err(StoreError::Conflict(_))));

        // The operator's policy survived the second caller.
        let loaded = store.get_budget("stripe").await.unwrap().unwrap();
        assert_eq!(loaded.allocation, 100);
    }

    #[tokio::test]
    async fn create_validates_the_policy() {
        let store = test_store();

        let result = store
            .create_budget("stripe", 0, BudgetStrategy::WhileInFlight, NOW)
            .await;
        assert!(matches!(result, Err(StoreError::InvalidOperation(_))));
        assert!(store.get_budget("stripe").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn put_creates_when_absent() {
        let store = test_store();

        let budget = store
            .put_budget(
                "stripe",
                50,
                BudgetStrategy::TimeBased {
                    duration_ms: 60_000,
                },
                NOW,
            )
            .await
            .unwrap();

        assert_eq!(budget.allocation, 50);
        assert_eq!(budget.created_at, NOW);
        assert_eq!(budget.updated_at, NOW);
    }

    #[tokio::test]
    async fn put_replaces_the_policy_but_keeps_created_at() {
        let store = test_store();
        store
            .create_budget("stripe", 100, BudgetStrategy::WhileInFlight, NOW)
            .await
            .unwrap();

        let replaced = store
            .put_budget(
                "stripe",
                10,
                BudgetStrategy::TimeBased { duration_ms: 1_000 },
                NOW + 5_000,
            )
            .await
            .unwrap();

        assert_eq!(replaced.allocation, 10);
        assert_eq!(
            replaced.strategy,
            BudgetStrategy::TimeBased { duration_ms: 1_000 }
        );
        // Identity is unchanged; only the policy moved.
        assert_eq!(replaced.created_at, NOW);
        assert_eq!(replaced.updated_at, NOW + 5_000);
    }

    #[tokio::test]
    async fn patch_changes_only_the_named_field() {
        let store = test_store();
        store
            .create_budget(
                "stripe",
                100,
                BudgetStrategy::TimeBased {
                    duration_ms: 60_000,
                },
                NOW,
            )
            .await
            .unwrap();

        let patched = store
            .patch_budget("stripe", patch(Some(25), None), NOW + 5_000)
            .await
            .unwrap()
            .unwrap();

        assert_eq!(patched.allocation, 25);
        // Untouched.
        assert_eq!(
            patched.strategy,
            BudgetStrategy::TimeBased {
                duration_ms: 60_000
            }
        );
        assert_eq!(patched.created_at, NOW);
        assert_eq!(patched.updated_at, NOW + 5_000);
    }

    #[tokio::test]
    async fn patch_can_change_the_strategy_alone() {
        let store = test_store();
        store
            .create_budget("stripe", 100, BudgetStrategy::WhileInFlight, NOW)
            .await
            .unwrap();

        let patched = store
            .patch_budget(
                "stripe",
                patch(
                    None,
                    Some(BudgetStrategy::TimeBased {
                        duration_ms: 30_000,
                    }),
                ),
                NOW + 1,
            )
            .await
            .unwrap()
            .unwrap();

        assert_eq!(patched.allocation, 100);
        assert_eq!(
            patched.strategy,
            BudgetStrategy::TimeBased {
                duration_ms: 30_000
            }
        );
    }

    #[tokio::test]
    async fn patch_returns_none_for_a_missing_budget() {
        let store = test_store();

        let result = store
            .patch_budget("absent", patch(Some(10), None), NOW)
            .await
            .unwrap();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn patch_validates_the_merged_policy() {
        let store = test_store();
        store
            .create_budget("stripe", 100, BudgetStrategy::WhileInFlight, NOW)
            .await
            .unwrap();

        let result = store
            .patch_budget("stripe", patch(Some(0), None), NOW + 1)
            .await;
        assert!(matches!(result, Err(StoreError::InvalidOperation(_))));

        // Rejected patches leave the stored policy alone.
        let loaded = store.get_budget("stripe").await.unwrap().unwrap();
        assert_eq!(loaded.allocation, 100);
    }

    #[tokio::test]
    async fn delete_removes_the_budget() {
        let store = test_store();
        store
            .create_budget("stripe", 100, BudgetStrategy::WhileInFlight, NOW)
            .await
            .unwrap();

        assert!(store.delete_budget("stripe").await.unwrap());
        assert!(store.get_budget("stripe").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn delete_reports_a_missing_budget() {
        let store = test_store();
        assert!(!store.delete_budget("absent").await.unwrap());
    }

    #[tokio::test]
    async fn list_returns_keys_in_order() {
        let store = test_store();
        for key in ["stripe", "ses", "algolia"] {
            store
                .create_budget(key, 10, BudgetStrategy::WhileInFlight, NOW)
                .await
                .unwrap();
        }

        let keys: Vec<String> = store
            .list_budgets()
            .await
            .unwrap()
            .into_iter()
            .map(|(key, _)| key)
            .collect();
        assert_eq!(keys, ["algolia", "ses", "stripe"]);
    }

    /// The listing scan has already read each record, so it hands the
    /// policy back rather than making the caller re-read it.
    #[tokio::test]
    async fn list_returns_each_policy_alongside_its_key() {
        let store = test_store();
        store
            .create_budget(
                "stripe",
                100,
                BudgetStrategy::TimeBased {
                    duration_ms: 60_000,
                },
                NOW,
            )
            .await
            .unwrap();

        let listed = store.list_budgets().await.unwrap();

        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].0, "stripe");
        assert_eq!(listed[0].1.allocation, 100);
        assert_eq!(
            listed[0].1.strategy,
            BudgetStrategy::TimeBased {
                duration_ms: 60_000
            }
        );
        assert_eq!(listed[0].1.created_at, NOW);
    }

    #[tokio::test]
    async fn list_is_empty_when_there_are_no_budgets() {
        let store = test_store();
        assert!(store.list_budgets().await.unwrap().is_empty());
    }

    /// Budget keys sit next to cron, job and payload records in the
    /// same keyspace, so the scan has to stop at its own prefix.
    #[tokio::test]
    async fn list_ignores_records_of_other_kinds() {
        use crate::store::options::{CronEntryOptions, EnqueueOptions, ReplaceCronGroupOptions};

        let store = test_store();
        store
            .create_budget("stripe", 10, BudgetStrategy::WhileInFlight, NOW)
            .await
            .unwrap();

        store
            .enqueue(NOW, EnqueueOptions::new("test", "q", serde_json::json!({})))
            .await
            .unwrap();

        store
            .replace_cron_group(
                "nightly",
                ReplaceCronGroupOptions {
                    paused: None,
                    timezone: None,
                    entries: vec![CronEntryOptions {
                        name: "cleanup".to_string(),
                        expression: "0 0 * * *".to_string(),
                        timezone: None,
                        paused: None,
                        job: EnqueueOptions::new("cleanup", "q", serde_json::json!({})),
                    }],
                },
                NOW,
            )
            .await
            .unwrap();

        let keys: Vec<String> = store
            .list_budgets()
            .await
            .unwrap()
            .into_iter()
            .map(|(key, _)| key)
            .collect();
        assert_eq!(keys, ["stripe"]);
    }
}
