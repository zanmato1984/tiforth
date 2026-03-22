use std::collections::HashSet;
use std::sync::{Arc, Mutex};

use broken_pipeline::traits::arrow::Batch as ArrowBatch;

use crate::admission::AdmissionConsumer;
use crate::error::TiforthError;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BatchOrigin {
    pub query: String,
    pub stage: String,
    pub operator: String,
}

impl BatchOrigin {
    pub fn local(operator: impl Into<String>) -> Self {
        let operator = operator.into();
        Self {
            query: "local-query".into(),
            stage: operator.clone(),
            operator,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RuntimeEvent {
    BatchEmitted {
        batch_id: u64,
        origin: BatchOrigin,
        claim_count: usize,
    },
    BatchHandedOff {
        batch_id: u64,
        to_operator: String,
        claim_count: usize,
    },
    BatchReleased {
        batch_id: u64,
        origin: BatchOrigin,
        claim_count: usize,
    },
    TerminalFinished,
    TerminalCancelled,
    TerminalError {
        message: String,
    },
}

#[derive(Clone, Default)]
pub struct RuntimeEventRecorder {
    events: Arc<Mutex<Vec<RuntimeEvent>>>,
}

impl RuntimeEventRecorder {
    pub fn record(&self, event: RuntimeEvent) {
        self.events
            .lock()
            .expect("runtime events mutex poisoned")
            .push(event);
    }

    pub fn events(&self) -> Vec<RuntimeEvent> {
        self.events
            .lock()
            .expect("runtime events mutex poisoned")
            .clone()
    }
}

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
pub struct GovernedBatch {
    inner: Arc<GovernedBatchInner>,
}

struct GovernedBatchInner {
    batch: ArrowBatch,
    batch_id: u64,
    origin: BatchOrigin,
    column_claims: Vec<Vec<BatchClaim>>,
    events: RuntimeEventRecorder,
}

impl Drop for GovernedBatchInner {
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

impl GovernedBatch {
    pub(crate) fn new(
        batch: ArrowBatch,
        batch_id: u64,
        origin: BatchOrigin,
        column_claims: Vec<Vec<BatchClaim>>,
        events: RuntimeEventRecorder,
    ) -> Result<Self, TiforthError> {
        validate_column_claims(batch.num_columns(), &column_claims)?;
        Ok(Self {
            inner: Arc::new(GovernedBatchInner {
                batch,
                batch_id,
                origin,
                column_claims,
                events,
            }),
        })
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
