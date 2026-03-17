use std::collections::{HashMap, HashSet};
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc, Mutex,
};

use broken_pipeline::traits::arrow::Batch;

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
        self.consumer.release();
    }
}

impl BatchClaim {
    pub fn id(&self) -> u64 {
        self.inner.id
    }

    pub fn local_try_shrink(&self, bytes: usize) -> Result<(), TiforthError> {
        Err(TiforthError::OwnershipContractViolation {
            detail: format!(
                "attempted to shrink {bytes} bytes from the consumer for live claim {} while that claim still referenced it",
                self.id()
            ),
        })
    }

    pub fn local_try_release(&self) -> Result<(), TiforthError> {
        Err(TiforthError::OwnershipContractViolation {
            detail: format!(
                "attempted to release the consumer for live claim {} while that claim still referenced it",
                self.id()
            ),
        })
    }

    fn new(id: u64, consumer: Arc<dyn AdmissionConsumer>) -> Self {
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
    batch: Batch,
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
        batch: Batch,
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

    pub(crate) fn ungoverned(batch: Batch) -> Self {
        Self {
            inner: Arc::new(GovernedBatchInner {
                batch: Arc::clone(&batch),
                batch_id: 0,
                origin: BatchOrigin::local("ungoverned"),
                column_claims: empty_column_claims(batch.num_columns()),
                events: RuntimeEventRecorder::default(),
            }),
        }
    }

    pub fn batch(&self) -> &Batch {
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

pub(crate) struct BatchTracker {
    events: RuntimeEventRecorder,
    next_batch_id: AtomicU64,
    next_claim_id: AtomicU64,
    pending: Mutex<HashMap<usize, PendingBatch>>,
}

struct PendingBatch {
    batch_id: u64,
    origin: BatchOrigin,
    column_claims: Vec<Vec<BatchClaim>>,
}

impl BatchTracker {
    pub(crate) fn new(events: RuntimeEventRecorder) -> Self {
        Self {
            events,
            next_batch_id: AtomicU64::new(1),
            next_claim_id: AtomicU64::new(1),
            pending: Mutex::new(HashMap::new()),
        }
    }

    pub(crate) fn events(&self) -> Vec<RuntimeEvent> {
        self.events.events()
    }

    pub(crate) fn record(&self, event: RuntimeEvent) {
        self.events.record(event);
    }

    pub(crate) fn new_claim(&self, consumer: Arc<dyn AdmissionConsumer>) -> BatchClaim {
        let id = self.next_claim_id.fetch_add(1, Ordering::Relaxed);
        BatchClaim::new(id, consumer)
    }

    pub(crate) fn emit_ungoverned_batch(
        &self,
        batch: Batch,
        operator_name: &str,
    ) -> Result<(), TiforthError> {
        let columns = batch.num_columns();
        self.emit_batch(
            batch,
            BatchOrigin::local(operator_name),
            empty_column_claims(columns),
        )
    }

    pub(crate) fn emit_batch(
        &self,
        batch: Batch,
        origin: BatchOrigin,
        column_claims: Vec<Vec<BatchClaim>>,
    ) -> Result<(), TiforthError> {
        validate_column_claims(batch.num_columns(), &column_claims)?;
        let batch_id = self.next_batch_id.fetch_add(1, Ordering::Relaxed);
        let claim_count = unique_claim_count(&column_claims);
        let key = batch_key(&batch);
        let pending = PendingBatch {
            batch_id,
            origin: origin.clone(),
            column_claims,
        };
        if self
            .pending
            .lock()
            .expect("batch tracker mutex poisoned")
            .insert(key, pending)
            .is_some()
        {
            return Err(TiforthError::OwnershipContractViolation {
                detail: format!(
                    "attempted to emit an already-tracked batch from {}",
                    origin.operator
                ),
            });
        }

        self.events.record(RuntimeEvent::BatchEmitted {
            batch_id,
            origin,
            claim_count,
        });
        Ok(())
    }

    pub(crate) fn adopt_batch(
        &self,
        batch: Batch,
        receiver: &str,
    ) -> Result<GovernedBatch, TiforthError> {
        let key = batch_key(&batch);
        let pending = self
            .pending
            .lock()
            .expect("batch tracker mutex poisoned")
            .remove(&key)
            .ok_or_else(|| TiforthError::OwnershipContractViolation {
                detail: format!("{receiver} received an untracked batch handoff"),
            })?;
        let claim_count = unique_claim_count(&pending.column_claims);
        self.events.record(RuntimeEvent::BatchHandedOff {
            batch_id: pending.batch_id,
            to_operator: receiver.into(),
            claim_count,
        });
        GovernedBatch::new(
            batch,
            pending.batch_id,
            pending.origin,
            pending.column_claims,
            self.events.clone(),
        )
    }
}

fn batch_key(batch: &Batch) -> usize {
    Arc::as_ptr(batch) as usize
}

fn empty_column_claims(columns: usize) -> Vec<Vec<BatchClaim>> {
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
