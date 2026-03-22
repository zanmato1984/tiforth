use std::collections::VecDeque;
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc, Mutex,
};

use arrow_schema::ArrowError;
use broken_pipeline::{
    OpOutput, PipeOperator, SinkOperator, SourceOperator, TaskContext, ThreadId,
};

use crate::admission::{AdmissionController, RecordingAdmissionController};
use crate::error::TiforthError;
use crate::filter::{filter_governed_batch, FilterPredicate};
use crate::handoff::{
    empty_column_claims, BatchClaim, BatchOrigin, GovernedBatch, RuntimeEvent, RuntimeEventRecorder,
};
use crate::projection::{project_governed_batch, ProjectionExpr};
use crate::snapshot::LocalExecutionSnapshot;
use crate::{Batch, TiforthTypes};

struct SourceInput {
    batch: Batch,
    column_claims: Vec<Vec<BatchClaim>>,
}

impl SourceInput {
    fn plain(batch: Batch) -> Self {
        Self {
            column_claims: empty_column_claims(batch.num_columns()),
            batch,
        }
    }
}

struct ExchangeState {
    queue: VecDeque<GovernedBatch>,
    pending: Option<GovernedBatch>,
}

#[derive(Clone)]
pub struct ProjectionRuntimeContext {
    admission: Arc<dyn AdmissionController>,
    events: RuntimeEventRecorder,
    next_batch_id: Arc<AtomicU64>,
    next_claim_id: Arc<AtomicU64>,
}

impl ProjectionRuntimeContext {
    pub fn new(admission: Arc<dyn AdmissionController>) -> Self {
        Self {
            admission,
            events: RuntimeEventRecorder::default(),
            next_batch_id: Arc::new(AtomicU64::new(1)),
            next_claim_id: Arc::new(AtomicU64::new(1)),
        }
    }

    pub fn admission(&self) -> &dyn AdmissionController {
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

    pub fn new_claim(&self, consumer: Arc<dyn crate::admission::AdmissionConsumer>) -> BatchClaim {
        let id = self.next_claim_id.fetch_add(1, Ordering::Relaxed);
        BatchClaim::new(id, consumer)
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

    fn emit_source_batch(
        &self,
        operator_name: &str,
        batch: Batch,
        column_claims: Vec<Vec<BatchClaim>>,
    ) -> Result<GovernedBatch, TiforthError> {
        self.emit_batch(BatchOrigin::local(operator_name), batch, column_claims)
    }

    fn emit_batch(
        &self,
        origin: BatchOrigin,
        batch: Batch,
        column_claims: Vec<Vec<BatchClaim>>,
    ) -> Result<GovernedBatch, TiforthError> {
        let batch_id = self.next_batch_id.fetch_add(1, Ordering::Relaxed);
        let governed = GovernedBatch::new(
            batch,
            batch_id,
            origin.clone(),
            column_claims,
            self.events.clone(),
        )?;
        self.events.record(RuntimeEvent::BatchEmitted {
            batch_id,
            origin,
            claim_count: governed.claim_count(),
        });
        Ok(governed)
    }

    fn emit_pipe_batch(
        &self,
        operator_name: &str,
        batch: Batch,
        column_claims: Vec<Vec<BatchClaim>>,
    ) -> Result<GovernedBatch, TiforthError> {
        self.emit_batch(BatchOrigin::local(operator_name), batch, column_claims)
    }

    fn record_handoff(&self, batch: &GovernedBatch, receiver: &str) {
        self.events.record(RuntimeEvent::BatchHandedOff {
            batch_id: batch.batch_id(),
            to_operator: receiver.into(),
            claim_count: batch.claim_count(),
        });
    }
}

pub struct StaticRecordBatchSource {
    name: String,
    batches: Mutex<VecDeque<SourceInput>>,
}

impl StaticRecordBatchSource {
    pub fn new(name: impl Into<String>, batches: Vec<Batch>) -> Self {
        Self {
            name: name.into(),
            batches: Mutex::new(batches.into_iter().map(SourceInput::plain).collect()),
        }
    }

    pub fn with_claims(
        name: impl Into<String>,
        batches: Vec<(Batch, Vec<Vec<crate::handoff::BatchClaim>>)>,
    ) -> Self {
        Self {
            name: name.into(),
            batches: Mutex::new(
                batches
                    .into_iter()
                    .map(|(batch, column_claims)| SourceInput {
                        batch,
                        column_claims,
                    })
                    .collect(),
            ),
        }
    }
}

impl SourceOperator<TiforthTypes> for StaticRecordBatchSource {
    fn name(&self) -> &str {
        &self.name
    }

    fn source(
        &self,
        ctx: &TaskContext<TiforthTypes>,
        _thread_id: ThreadId,
    ) -> Result<OpOutput<GovernedBatch>, ArrowError> {
        let mut batches = self.batches.lock().expect("source batches mutex poisoned");
        match batches.pop_front() {
            Some(batch) if batches.is_empty() => {
                let batch = ctx
                    .context()
                    .emit_source_batch(self.name(), batch.batch, batch.column_claims)
                    .map_err(ArrowError::from)?;
                Ok(OpOutput::Finished(Some(batch)))
            }
            Some(batch) => {
                let batch = ctx
                    .context()
                    .emit_source_batch(self.name(), batch.batch, batch.column_claims)
                    .map_err(ArrowError::from)?;
                Ok(OpOutput::SourcePipeHasMore(batch))
            }
            None => Ok(OpOutput::Finished(None)),
        }
    }
}

pub struct ProjectionPipe {
    name: String,
    projections: Vec<ProjectionExpr>,
}

impl ProjectionPipe {
    pub fn new(name: impl Into<String>, projections: Vec<ProjectionExpr>) -> Self {
        Self {
            name: name.into(),
            projections,
        }
    }
}

impl PipeOperator<TiforthTypes> for ProjectionPipe {
    fn name(&self) -> &str {
        &self.name
    }

    fn pipe(
        &self,
        ctx: &TaskContext<TiforthTypes>,
        _thread_id: ThreadId,
        input: Option<GovernedBatch>,
    ) -> Result<OpOutput<GovernedBatch>, ArrowError> {
        let batch = input.ok_or_else(|| ArrowError::from(TiforthError::InvalidPipeInput))?;
        let runtime = ctx.context();
        runtime.record_handoff(&batch, self.name());
        let (output, column_claims) = project_governed_batch(
            &batch,
            &self.projections,
            runtime.admission(),
            self.name(),
            &|consumer| runtime.new_claim(consumer),
        )
        .map_err(ArrowError::from)?;
        let output = runtime
            .emit_pipe_batch(self.name(), Arc::clone(&output), column_claims)
            .map_err(ArrowError::from)?;
        Ok(OpOutput::PipeEven(output))
    }
}

pub struct FilterPipe {
    name: String,
    predicate: FilterPredicate,
}

impl FilterPipe {
    pub fn new(name: impl Into<String>, predicate: FilterPredicate) -> Self {
        Self {
            name: name.into(),
            predicate,
        }
    }
}

impl PipeOperator<TiforthTypes> for FilterPipe {
    fn name(&self) -> &str {
        &self.name
    }

    fn pipe(
        &self,
        ctx: &TaskContext<TiforthTypes>,
        _thread_id: ThreadId,
        input: Option<GovernedBatch>,
    ) -> Result<OpOutput<GovernedBatch>, ArrowError> {
        let batch = input.ok_or_else(|| ArrowError::from(TiforthError::InvalidPipeInput))?;
        let runtime = ctx.context();
        runtime.record_handoff(&batch, self.name());
        let (output, column_claims) = filter_governed_batch(
            &batch,
            &self.predicate,
            runtime.admission(),
            self.name(),
            &|consumer| runtime.new_claim(consumer),
        )
        .map_err(ArrowError::from)?;
        let output = runtime
            .emit_pipe_batch(self.name(), Arc::clone(&output), column_claims)
            .map_err(ArrowError::from)?;
        Ok(OpOutput::PipeEven(output))
    }
}

pub struct ExchangePipe {
    name: String,
    capacity: usize,
    state: Mutex<ExchangeState>,
}

impl ExchangePipe {
    pub fn new(name: impl Into<String>, capacity: usize) -> Self {
        assert!(capacity > 0, "exchange capacity must be greater than zero");
        Self {
            name: name.into(),
            capacity,
            state: Mutex::new(ExchangeState {
                queue: VecDeque::with_capacity(capacity),
                pending: None,
            }),
        }
    }

    fn blocked(ctx: &TaskContext<TiforthTypes>) -> Result<OpOutput<GovernedBatch>, ArrowError> {
        Ok(OpOutput::Blocked(ctx.make_resumer()?))
    }
}

impl PipeOperator<TiforthTypes> for ExchangePipe {
    fn name(&self) -> &str {
        &self.name
    }

    fn pipe(
        &self,
        ctx: &TaskContext<TiforthTypes>,
        _thread_id: ThreadId,
        input: Option<GovernedBatch>,
    ) -> Result<OpOutput<GovernedBatch>, ArrowError> {
        let mut state = self.state.lock().expect("exchange state mutex poisoned");

        if let Some(batch) = input {
            if state.pending.is_some() {
                return Err(ArrowError::from(TiforthError::OwnershipContractViolation {
                    detail: format!(
                        "{} received new input while a blocked input was still pending",
                        self.name()
                    ),
                }));
            }
            ctx.context().record_handoff(&batch, self.name());
            if state.queue.len() >= self.capacity {
                state.pending = Some(batch);
                return Self::blocked(ctx);
            }
            state.queue.push_back(batch);
            return Ok(OpOutput::PipeSinkNeedsMore);
        }

        if let Some(outgoing) = state.queue.pop_front() {
            if state.pending.is_some() && state.queue.len() < self.capacity {
                let pending = state
                    .pending
                    .take()
                    .expect("pending exchange input should exist");
                state.queue.push_back(pending);
            }
            return Ok(OpOutput::SourcePipeHasMore(outgoing));
        }

        if let Some(pending) = state.pending.take() {
            return Ok(OpOutput::SourcePipeHasMore(pending));
        }

        Ok(OpOutput::PipeSinkNeedsMore)
    }

    fn has_drain(&self) -> bool {
        true
    }

    fn drain(
        &self,
        ctx: &TaskContext<TiforthTypes>,
        thread_id: ThreadId,
    ) -> Result<OpOutput<GovernedBatch>, ArrowError> {
        match self.pipe(ctx, thread_id, None)? {
            OpOutput::PipeSinkNeedsMore => Ok(OpOutput::Finished(None)),
            other => Ok(other),
        }
    }
}

pub struct CollectSink {
    name: String,
    batches: Mutex<Vec<GovernedBatch>>,
}

impl CollectSink {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            batches: Mutex::new(Vec::new()),
        }
    }

    pub fn batches(&self) -> Vec<GovernedBatch> {
        self.batches
            .lock()
            .expect("sink batches mutex poisoned")
            .clone()
    }
}

impl SinkOperator<TiforthTypes> for CollectSink {
    fn name(&self) -> &str {
        &self.name
    }

    fn sink(
        &self,
        ctx: &TaskContext<TiforthTypes>,
        _thread_id: ThreadId,
        input: Option<GovernedBatch>,
    ) -> Result<OpOutput<GovernedBatch>, ArrowError> {
        if let Some(batch) = input {
            ctx.context().record_handoff(&batch, self.name());
            self.batches
                .lock()
                .expect("sink batches mutex poisoned")
                .push(batch);
        }
        Ok(OpOutput::PipeSinkNeedsMore)
    }
}
