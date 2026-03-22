use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc, Mutex,
};

use crate::admission::{AdmissionConsumer, AdmissionController, RecordingAdmissionController};
use crate::batch::{OwnershipToken, TiforthBatch};
use crate::error::TiforthError;
use crate::snapshot::LocalExecutionSnapshot;
use crate::ArrowBatch;

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
pub(crate) struct RuntimeEventRecorder {
    events: Arc<Mutex<Vec<RuntimeEvent>>>,
}

impl RuntimeEventRecorder {
    pub(crate) fn record(&self, event: RuntimeEvent) {
        self.events
            .lock()
            .expect("runtime events mutex poisoned")
            .push(event);
    }

    pub(crate) fn events(&self) -> Vec<RuntimeEvent> {
        self.events
            .lock()
            .expect("runtime events mutex poisoned")
            .clone()
    }
}

#[derive(Clone)]
pub struct RuntimeContext {
    admission: Arc<dyn AdmissionController>,
    events: RuntimeEventRecorder,
    next_batch_id: Arc<AtomicU64>,
    next_claim_id: Arc<AtomicU64>,
}

impl RuntimeContext {
    pub fn new(admission: Arc<dyn AdmissionController>) -> Self {
        Self {
            admission,
            events: RuntimeEventRecorder::default(),
            next_batch_id: Arc::new(AtomicU64::new(1)),
            next_claim_id: Arc::new(AtomicU64::new(1)),
        }
    }

    pub(crate) fn admission(&self) -> &dyn AdmissionController {
        self.admission.as_ref()
    }

    pub fn runtime_events(&self) -> Vec<RuntimeEvent> {
        self.events.events()
    }

    pub fn local_snapshot(
        &self,
        admission: &RecordingAdmissionController,
    ) -> LocalExecutionSnapshot {
        LocalExecutionSnapshot::capture(admission, self.runtime_events())
    }

    pub fn new_token(&self, consumer: Arc<dyn AdmissionConsumer>) -> OwnershipToken {
        let id = self.next_claim_id.fetch_add(1, Ordering::Relaxed);
        OwnershipToken::new(id, consumer)
    }

    pub fn record_terminal_finished(&self) {
        self.events.record(RuntimeEvent::TerminalFinished);
    }

    pub fn record_terminal_cancelled(&self) {
        self.events.record(RuntimeEvent::TerminalCancelled);
    }

    pub fn record_terminal_error(&self, message: impl Into<String>) {
        self.events.record(RuntimeEvent::TerminalError {
            message: message.into(),
        });
    }

    pub(crate) fn emit_source_batch(
        &self,
        operator_name: &str,
        batch: ArrowBatch,
        claims: Vec<OwnershipToken>,
    ) -> Result<TiforthBatch, TiforthError> {
        self.emit_batch(BatchOrigin::local(operator_name), batch, claims)
    }

    pub(crate) fn emit_pipe_batch(
        &self,
        operator_name: &str,
        batch: ArrowBatch,
        claims: Vec<OwnershipToken>,
    ) -> Result<TiforthBatch, TiforthError> {
        self.emit_batch(BatchOrigin::local(operator_name), batch, claims)
    }

    pub(crate) fn record_handoff(&self, batch: &TiforthBatch, receiver: &str) {
        self.events.record(RuntimeEvent::BatchHandedOff {
            batch_id: batch.batch_id(),
            to_operator: receiver.into(),
            claim_count: batch.claim_count(),
        });
    }

    fn emit_batch(
        &self,
        origin: BatchOrigin,
        batch: ArrowBatch,
        claims: Vec<OwnershipToken>,
    ) -> Result<TiforthBatch, TiforthError> {
        let batch_id = self.next_batch_id.fetch_add(1, Ordering::Relaxed);
        let batch =
            TiforthBatch::new(batch, batch_id, origin.clone(), claims, self.events.clone())?;
        self.events.record(RuntimeEvent::BatchEmitted {
            batch_id,
            origin,
            claim_count: batch.claim_count(),
        });
        Ok(batch)
    }
}
