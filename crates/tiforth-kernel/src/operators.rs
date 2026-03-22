use std::collections::VecDeque;
use std::sync::Mutex;

use arrow_schema::ArrowError;
use broken_pipeline::{
    OpOutput, PipeOperator, SinkOperator, SourceOperator, TaskContext, ThreadId,
};

use crate::batch::{empty_claims, OwnershipToken, TiforthBatch};
use crate::error::TiforthError;
use crate::filter::{filter_batch, FilterPredicate};
use crate::projection::{project_batch, ProjectionExpr};
use crate::{ArrowBatch, TiforthTypes};

struct SourceInput {
    batch: ArrowBatch,
    claims: Vec<OwnershipToken>,
}

impl SourceInput {
    fn plain(batch: ArrowBatch) -> Self {
        Self {
            claims: empty_claims(),
            batch,
        }
    }
}

struct ExchangeState {
    queue: VecDeque<TiforthBatch>,
    pending: Option<TiforthBatch>,
}

pub struct StaticRecordBatchSource {
    name: String,
    batches: Mutex<VecDeque<SourceInput>>,
}

impl StaticRecordBatchSource {
    pub fn new(name: impl Into<String>, batches: Vec<ArrowBatch>) -> Self {
        Self {
            name: name.into(),
            batches: Mutex::new(batches.into_iter().map(SourceInput::plain).collect()),
        }
    }

    pub fn with_claims(
        name: impl Into<String>,
        batches: Vec<(ArrowBatch, Vec<crate::batch::OwnershipToken>)>,
    ) -> Self {
        Self {
            name: name.into(),
            batches: Mutex::new(
                batches
                    .into_iter()
                    .map(|(batch, claims)| SourceInput { batch, claims })
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
    ) -> Result<OpOutput<TiforthBatch>, ArrowError> {
        let mut batches = self.batches.lock().expect("source batches mutex poisoned");
        match batches.pop_front() {
            Some(batch) if batches.is_empty() => {
                let batch = ctx
                    .context()
                    .emit_source_batch(self.name(), batch.batch, batch.claims)
                    .map_err(ArrowError::from)?;
                Ok(OpOutput::Finished(Some(batch)))
            }
            Some(batch) => {
                let batch = ctx
                    .context()
                    .emit_source_batch(self.name(), batch.batch, batch.claims)
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
        input: Option<TiforthBatch>,
    ) -> Result<OpOutput<TiforthBatch>, ArrowError> {
        let batch = input.ok_or_else(|| ArrowError::from(TiforthError::InvalidPipeInput))?;
        let runtime = ctx.context();
        runtime.record_handoff(&batch, self.name());
        let output = project_batch(runtime, self.name(), &batch, &self.projections)
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
        input: Option<TiforthBatch>,
    ) -> Result<OpOutput<TiforthBatch>, ArrowError> {
        let batch = input.ok_or_else(|| ArrowError::from(TiforthError::InvalidPipeInput))?;
        let runtime = ctx.context();
        runtime.record_handoff(&batch, self.name());
        let output = filter_batch(runtime, self.name(), &batch, &self.predicate)
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

    fn blocked(ctx: &TaskContext<TiforthTypes>) -> Result<OpOutput<TiforthBatch>, ArrowError> {
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
        input: Option<TiforthBatch>,
    ) -> Result<OpOutput<TiforthBatch>, ArrowError> {
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
    ) -> Result<OpOutput<TiforthBatch>, ArrowError> {
        match self.pipe(ctx, thread_id, None)? {
            OpOutput::PipeSinkNeedsMore => Ok(OpOutput::Finished(None)),
            other => Ok(other),
        }
    }
}

pub struct CollectSink {
    name: String,
    batches: Mutex<Vec<TiforthBatch>>,
}

impl CollectSink {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            batches: Mutex::new(Vec::new()),
        }
    }

    pub fn batches(&self) -> Vec<TiforthBatch> {
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
        input: Option<TiforthBatch>,
    ) -> Result<OpOutput<TiforthBatch>, ArrowError> {
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
