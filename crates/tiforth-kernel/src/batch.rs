use std::collections::HashSet;
use std::fmt;
use std::ops::Deref;
use std::sync::Arc;

use broken_pipeline::traits::arrow::Batch as ArrowBatch;

use crate::admission::AdmissionConsumer;
use crate::error::TiforthError;
use crate::runtime::{BatchOrigin, RuntimeEvent, RuntimeEventRecorder};

#[derive(Clone)]
pub struct OwnershipToken {
    inner: Arc<OwnershipTokenInner>,
}

struct OwnershipTokenInner {
    id: u64,
    consumer: Arc<dyn AdmissionConsumer>,
}

impl Drop for OwnershipTokenInner {
    fn drop(&mut self) {
        self.consumer.local_untrack_live_claim(self.id);
    }
}

impl OwnershipToken {
    fn id(&self) -> u64 {
        self.inner.id
    }

    pub(crate) fn new(id: u64, consumer: Arc<dyn AdmissionConsumer>) -> Self {
        consumer.local_track_live_claim(id);
        Self {
            inner: Arc::new(OwnershipTokenInner { id, consumer }),
        }
    }
}

#[derive(Clone)]
pub struct TiforthBatch {
    inner: Arc<TiforthBatchInner>,
}

struct TiforthBatchInner {
    batch: ArrowBatch,
    batch_id: u64,
    origin: BatchOrigin,
    claims: Vec<OwnershipToken>,
    events: RuntimeEventRecorder,
}

impl Drop for TiforthBatchInner {
    fn drop(&mut self) {
        let claim_count = unique_claim_count(&self.claims);
        if claim_count == 0 {
            return;
        }

        self.events.record(RuntimeEvent::BatchReleased {
            batch_id: self.batch_id,
            origin: self.origin.clone(),
            claim_count,
        });
    }
}

impl TiforthBatch {
    pub(crate) fn new(
        batch: ArrowBatch,
        batch_id: u64,
        origin: BatchOrigin,
        claims: Vec<OwnershipToken>,
        events: RuntimeEventRecorder,
    ) -> Result<Self, TiforthError> {
        Ok(Self {
            inner: Arc::new(TiforthBatchInner {
                batch,
                batch_id,
                origin,
                claims,
                events,
            }),
        })
    }

    pub fn from_arrow(batch: ArrowBatch) -> Self {
        Self {
            inner: Arc::new(TiforthBatchInner {
                batch: Arc::clone(&batch),
                batch_id: 0,
                origin: BatchOrigin::local("local-input"),
                claims: empty_claims(),
                events: RuntimeEventRecorder::default(),
            }),
        }
    }

    pub fn batch(&self) -> &ArrowBatch {
        &self.inner.batch
    }

    pub fn batch_id(&self) -> u64 {
        self.inner.batch_id
    }

    pub fn origin(&self) -> &BatchOrigin {
        &self.inner.origin
    }

    pub fn claim_count(&self) -> usize {
        unique_claim_count(&self.inner.claims)
    }

    pub(crate) fn claims(&self) -> &[OwnershipToken] {
        &self.inner.claims
    }
}

impl fmt::Debug for TiforthBatch {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TiforthBatch")
            .field("batch_id", &self.batch_id())
            .field("origin", self.origin())
            .field("num_rows", &self.num_rows())
            .field("num_columns", &self.num_columns())
            .field("claim_count", &self.claim_count())
            .finish()
    }
}

impl Deref for TiforthBatch {
    type Target = ArrowBatch;

    fn deref(&self) -> &Self::Target {
        self.batch()
    }
}

pub(crate) fn empty_claims() -> Vec<OwnershipToken> {
    Vec::new()
}

pub(crate) fn append_unique_claims(
    dst: &mut Vec<OwnershipToken>,
    claims: impl IntoIterator<Item = OwnershipToken>,
) {
    let mut ids = HashSet::new();
    for claim in dst.iter() {
        ids.insert(claim.id());
    }
    for claim in claims {
        if ids.insert(claim.id()) {
            dst.push(claim);
        }
    }
}

fn unique_claim_count(claims: &[OwnershipToken]) -> usize {
    let mut ids = HashSet::new();
    for claim in claims {
        ids.insert(claim.id());
    }
    ids.len()
}
