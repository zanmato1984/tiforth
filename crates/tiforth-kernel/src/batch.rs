use std::collections::HashSet;
use std::fmt;
use std::ops::Deref;
use std::sync::Arc;

use broken_pipeline::traits::arrow::Batch as ArrowBatch;

use crate::admission::AdmissionConsumer;
use crate::error::TiforthError;
use crate::runtime::{BatchOrigin, RuntimeEvent, RuntimeEventRecorder};

#[derive(Clone)]
pub struct BatchClaim {
    inner: Arc<BatchClaimInner>,
}

struct BatchClaimInner {
    id: u64,
    consumer: Arc<dyn AdmissionConsumer>,
}

impl Drop for BatchClaimInner {
    fn drop(&mut self) {
        self.consumer.local_untrack_live_claim(self.id);
    }
}

impl BatchClaim {
    pub fn id(&self) -> u64 {
        self.inner.id
    }

    pub fn local_try_shrink(&self, bytes: usize) -> Result<(), TiforthError> {
        self.inner.consumer.shrink(bytes)
    }

    pub fn local_try_release(&self) -> Result<(), TiforthError> {
        self.inner.consumer.release()
    }

    pub(crate) fn new(id: u64, consumer: Arc<dyn AdmissionConsumer>) -> Self {
        consumer.local_track_live_claim(id);
        Self {
            inner: Arc::new(BatchClaimInner { id, consumer }),
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
    column_claims: Vec<Vec<BatchClaim>>,
    events: RuntimeEventRecorder,
}

impl Drop for TiforthBatchInner {
    fn drop(&mut self) {
        let claim_count = unique_claim_count(&self.column_claims);
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
        column_claims: Vec<Vec<BatchClaim>>,
        events: RuntimeEventRecorder,
    ) -> Result<Self, TiforthError> {
        validate_column_claims(batch.num_columns(), &column_claims)?;
        Ok(Self {
            inner: Arc::new(TiforthBatchInner {
                batch,
                batch_id,
                origin,
                column_claims,
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
                column_claims: empty_column_claims(batch.num_columns()),
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
        unique_claim_count(&self.inner.column_claims)
    }

    pub(crate) fn column_claims(&self) -> &[Vec<BatchClaim>] {
        &self.inner.column_claims
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

pub(crate) fn empty_column_claims(columns: usize) -> Vec<Vec<BatchClaim>> {
    vec![Vec::new(); columns]
}

fn validate_column_claims(
    columns: usize,
    column_claims: &[Vec<BatchClaim>],
) -> Result<(), TiforthError> {
    if column_claims.len() == columns {
        Ok(())
    } else {
        Err(TiforthError::OwnershipContractViolation {
            detail: format!(
                "expected {columns} column-claim entries, found {}",
                column_claims.len()
            ),
        })
    }
}

fn unique_claim_count(column_claims: &[Vec<BatchClaim>]) -> usize {
    let mut ids = HashSet::new();
    for claims in column_claims {
        for claim in claims {
            ids.insert(claim.id());
        }
    }
    ids.len()
}
