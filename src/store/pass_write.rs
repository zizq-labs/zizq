// Copyright (c) 2026 Chris Corbyn <chris@zizq.io>
// Licensed under the Business Source License 1.1. See LICENSE file for details.

//! What a writer pass writes through.
//!
//! The writer is the only thing that writes, so a pass needs no
//! transaction's isolation, only to read its own writes. [`PassWrite`] is
//! the handful of operations the enqueue, take and completion paths use,
//! over either a fjall transaction or a plain [`BatchWrite`]: a write
//! batch on the database beneath it, with an overlay of the pass's own
//! writes for reads. A batch doesn't insert every write twice, first into
//! a transaction's own buffer and again at commit.

use std::collections::HashMap;

use fjall::{Readable, SingleWriterTxKeyspace, Slice, UserKey, UserValue};

use super::budget::BudgetPlan;
use super::enqueue::{PreparedEnqueue, plan_op_budgets};
use super::store::Keyspaces;
use super::types::StoreError;

/// The writes a pass makes, and the reads that must see them.
pub(in crate::store) trait PassWrite {
    fn get<K: AsRef<[u8]>>(
        &self,
        keyspace: &SingleWriterTxKeyspace,
        key: K,
    ) -> Result<Option<Slice>, StoreError>;

    fn insert<K: Into<UserKey>, V: Into<UserValue>>(
        &mut self,
        keyspace: &SingleWriterTxKeyspace,
        key: K,
        value: V,
    );

    fn remove<K: Into<UserKey>>(&mut self, keyspace: &SingleWriterTxKeyspace, key: K);

    /// Remove a key written once and never overwritten, which lets the
    /// tombstone be dropped early.
    fn remove_weak<K: Into<UserKey>>(&mut self, keyspace: &SingleWriterTxKeyspace, key: K);

    /// Replace a key's value with `f` of it (`None` removes it),
    /// returning the previous value.
    fn fetch_update<K, F>(
        &mut self,
        keyspace: &SingleWriterTxKeyspace,
        key: K,
        f: F,
    ) -> Result<Option<Slice>, StoreError>
    where
        K: Into<UserKey>,
        F: FnOnce(Option<&UserValue>) -> Option<UserValue>;

    /// Resolve the budgets an enqueue request binds, which scans the
    /// budget records. Only a transaction can: a batch pass never carries
    /// an enqueue that binds a budget.
    fn plan_enqueue_budgets(
        &self,
        ks: &Keyspaces,
        op: &[PreparedEnqueue],
    ) -> Result<BudgetPlan, StoreError>;
}

impl PassWrite for fjall::SingleWriterWriteTx<'_> {
    fn get<K: AsRef<[u8]>>(
        &self,
        keyspace: &SingleWriterTxKeyspace,
        key: K,
    ) -> Result<Option<Slice>, StoreError> {
        Ok(Readable::get(self, keyspace, key)?)
    }

    fn insert<K: Into<UserKey>, V: Into<UserValue>>(
        &mut self,
        keyspace: &SingleWriterTxKeyspace,
        key: K,
        value: V,
    ) {
        fjall::SingleWriterWriteTx::insert(self, keyspace, key, value);
    }

    fn remove<K: Into<UserKey>>(&mut self, keyspace: &SingleWriterTxKeyspace, key: K) {
        fjall::SingleWriterWriteTx::remove(self, keyspace, key);
    }

    fn remove_weak<K: Into<UserKey>>(&mut self, keyspace: &SingleWriterTxKeyspace, key: K) {
        fjall::SingleWriterWriteTx::remove_weak(self, keyspace, key);
    }

    fn fetch_update<K, F>(
        &mut self,
        keyspace: &SingleWriterTxKeyspace,
        key: K,
        f: F,
    ) -> Result<Option<Slice>, StoreError>
    where
        K: Into<UserKey>,
        F: FnOnce(Option<&UserValue>) -> Option<UserValue>,
    {
        Ok(fjall::SingleWriterWriteTx::fetch_update(
            self, keyspace, key, f,
        )?)
    }

    fn plan_enqueue_budgets(
        &self,
        ks: &Keyspaces,
        op: &[PreparedEnqueue],
    ) -> Result<BudgetPlan, StoreError> {
        plan_op_budgets(self, ks, op)
    }
}

/// A pass written as a plain write batch.
///
/// Reads see the pass's own writes through an overlay, and otherwise read
/// the database directly: nothing but the writer writes, so the database
/// holds exactly the state before the pass. A batch also takes none of the
/// transaction lock, which is why that has to hold.
pub(in crate::store) struct BatchWrite<'a> {
    ks: &'a Keyspaces,
    batch: fjall::OwnedWriteBatch,
    /// The pass's writes so far, by keyspace (data or index) and key.
    /// `None` is a removal.
    overlay: HashMap<(bool, Slice), Option<Slice>>,
}

impl<'a> BatchWrite<'a> {
    pub(in crate::store) fn new(ks: &'a Keyspaces) -> Self {
        Self {
            ks,
            batch: ks.db.inner().batch(),
            overlay: HashMap::new(),
        }
    }

    /// Whether `keyspace` is the data keyspace rather than the index.
    fn is_data(&self, keyspace: &SingleWriterTxKeyspace) -> bool {
        std::ptr::eq(keyspace, &self.ks.data)
    }

    pub(in crate::store) fn into_batch(self) -> fjall::OwnedWriteBatch {
        self.batch
    }
}

impl PassWrite for BatchWrite<'_> {
    fn get<K: AsRef<[u8]>>(
        &self,
        keyspace: &SingleWriterTxKeyspace,
        key: K,
    ) -> Result<Option<Slice>, StoreError> {
        let key = key.as_ref();
        if let Some(written) = self
            .overlay
            .get(&(self.is_data(keyspace), Slice::from(key)))
        {
            return Ok(written.clone());
        }
        Ok(keyspace.inner().get(key)?)
    }

    fn insert<K: Into<UserKey>, V: Into<UserValue>>(
        &mut self,
        keyspace: &SingleWriterTxKeyspace,
        key: K,
        value: V,
    ) {
        let (key, value) = (key.into(), value.into());
        self.batch
            .insert(keyspace.inner(), key.clone(), value.clone());
        self.overlay
            .insert((self.is_data(keyspace), key), Some(value));
    }

    fn remove<K: Into<UserKey>>(&mut self, keyspace: &SingleWriterTxKeyspace, key: K) {
        let key = key.into();
        self.batch.remove(keyspace.inner(), key.clone());
        self.overlay.insert((self.is_data(keyspace), key), None);
    }

    fn remove_weak<K: Into<UserKey>>(&mut self, keyspace: &SingleWriterTxKeyspace, key: K) {
        let key = key.into();
        self.batch.remove_weak(keyspace.inner(), key.clone());
        self.overlay.insert((self.is_data(keyspace), key), None);
    }

    fn fetch_update<K, F>(
        &mut self,
        keyspace: &SingleWriterTxKeyspace,
        key: K,
        f: F,
    ) -> Result<Option<Slice>, StoreError>
    where
        K: Into<UserKey>,
        F: FnOnce(Option<&UserValue>) -> Option<UserValue>,
    {
        let key = key.into();
        let previous = self.get(keyspace, &key)?;
        match f(previous.as_ref()) {
            Some(value) => self.insert(keyspace, key, value),
            None if previous.is_some() => self.remove(keyspace, key),
            None => {}
        }
        Ok(previous)
    }

    fn plan_enqueue_budgets(
        &self,
        _ks: &Keyspaces,
        op: &[PreparedEnqueue],
    ) -> Result<BudgetPlan, StoreError> {
        if op.iter().any(PreparedEnqueue::binds_budgets) {
            return Err(StoreError::Internal(
                "a batch pass can't resolve budgets".into(),
            ));
        }
        Ok(BudgetPlan::Proceed(Vec::new()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::test_support::test_store;

    /// Reads in a batch pass see the pass's own writes and removals, and
    /// otherwise what is stored, and nothing is stored until it commits.
    #[test]
    fn a_batch_pass_reads_its_own_writes() {
        let store = test_store();
        let ks = &store.ks;
        let mut tx = ks.write_tx();
        fjall::SingleWriterWriteTx::insert(&mut tx, &ks.data, "stored", "before");
        fjall::SingleWriterWriteTx::insert(&mut tx, &ks.data, "removed", "before");
        tx.commit().unwrap();

        let mut pass = BatchWrite::new(ks);
        pass.insert(&ks.data, "new", "written");
        pass.remove(&ks.data, "removed");
        assert_eq!(
            pass.get(&ks.data, "new").unwrap().as_deref(),
            Some(b"written".as_slice())
        );
        assert_eq!(pass.get(&ks.data, "removed").unwrap(), None);
        assert_eq!(
            pass.get(&ks.data, "stored").unwrap().as_deref(),
            Some(b"before".as_slice())
        );
        // Keyspaces are kept apart.
        assert_eq!(pass.get(&ks.index, "new").unwrap(), None);

        let previous = pass
            .fetch_update(&ks.data, "stored", |v| {
                assert_eq!(v.map(|v| &**v), Some(b"before".as_slice()));
                Some(Slice::from(b"after".as_slice()))
            })
            .unwrap();
        assert_eq!(previous.as_deref(), Some(b"before".as_slice()));
        assert_eq!(
            pass.get(&ks.data, "stored").unwrap().as_deref(),
            Some(b"after".as_slice())
        );

        assert!(ks.data.get("new").unwrap().is_none());
        pass.into_batch().commit().unwrap();
        assert_eq!(
            ks.data.get("new").unwrap().as_deref(),
            Some(b"written".as_slice())
        );
        assert!(ks.data.get("removed").unwrap().is_none());
        assert_eq!(
            ks.data.get("stored").unwrap().as_deref(),
            Some(b"after".as_slice())
        );
    }
}
