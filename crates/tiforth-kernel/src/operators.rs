use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use arrow_schema::ArrowError;
use broken_pipeline::traits::arrow::{ArrowTypes, Batch};
use broken_pipeline::{
    OpOutput, PipeOperator, SinkOperator, SourceOperator, TaskContext, ThreadId,
};

use crate::admission::{AdmissionController, NoopAdmissionController};
use crate::error::TiforthError;
use crate::handoff::{BatchTracker, GovernedBatch, RuntimeEvent, RuntimeEventRecorder};
use crate::projection::{project_batch, project_governed_batch, ProjectionExpr};

enum SourceBatch {
    Plain(Batch),
    Claimed {
        batch: Batch,
        column_claims: Vec<Vec<crate::handoff::BatchClaim>>,
    },
}

#[derive(Clone)]
pub struct ProjectionRuntimeContext {
    admission: Arc<dyn AdmissionController>,
    tracker: Arc<BatchTracker>,
}

impl ProjectionRuntimeContext {
    pub fn new(admission: Arc<dyn AdmissionController>) -> Self {
        let events = RuntimeEventRecorder::default();
        Self {
            admission,
            tracker: Arc::new(BatchTracker::new(events)),
        }
    }

    pub fn admission(&self) -> &Arc<dyn AdmissionController> {
        &self.admission
    }

    pub fn runtime_events(&self) -> Vec<RuntimeEvent> {
        self.tracker.events()
    }

    pub fn new_claim(
        &self,
        consumer: Arc<dyn crate::admission::AdmissionConsumer>,
    ) -> crate::handoff::BatchClaim {
        self.tracker.new_claim(consumer)
    }

    pub fn record_terminal_finished(&self) {
        self.tracker.record(RuntimeEvent::TerminalFinished);
    }

    pub fn record_terminal_cancelled(&self) {
        self.tracker.record(RuntimeEvent::TerminalCancelled);
    }

    pub fn record_terminal_error(&self, message: impl Into<String>) {
        self.tracker.record(RuntimeEvent::TerminalError {
            message: message.into(),
        });
    }

    fn emit_source_batch(
        &self,
        operator_name: &str,
        batch: SourceBatch,
    ) -> Result<Batch, TiforthError> {
        match batch {
            SourceBatch::Plain(batch) => {
                self.tracker
                    .emit_ungoverned_batch(Arc::clone(&batch), operator_name)?;
                Ok(batch)
            }
            SourceBatch::Claimed {
                batch,
                column_claims,
            } => {
                self.tracker.emit_batch(
                    Arc::clone(&batch),
                    crate::handoff::BatchOrigin::local(operator_name),
                    column_claims,
                )?;
                Ok(batch)
            }
        }
    }

    fn adopt_batch(&self, batch: Batch, receiver: &str) -> Result<GovernedBatch, TiforthError> {
        self.tracker.adopt_batch(batch, receiver)
    }

    fn emit_projected_batch(
        &self,
        operator_name: &str,
        batch: Batch,
        column_claims: Vec<Vec<crate::handoff::BatchClaim>>,
    ) -> Result<(), TiforthError> {
        self.tracker.emit_batch(
            batch,
            crate::handoff::BatchOrigin::local(operator_name),
            column_claims,
        )
    }
}

pub struct StaticRecordBatchSource {
    name: String,
    batches: Mutex<VecDeque<SourceBatch>>,
}

impl StaticRecordBatchSource {
    pub fn new(name: impl Into<String>, batches: Vec<Batch>) -> Self {
        Self {
            name: name.into(),
            batches: Mutex::new(batches.into_iter().map(SourceBatch::Plain).collect()),
        }
    }

    pub fn new_claimed(
        name: impl Into<String>,
        batches: Vec<(Batch, Vec<Vec<crate::handoff::BatchClaim>>)>,
    ) -> Self {
        Self {
            name: name.into(),
            batches: Mutex::new(
                batches
                    .into_iter()
                    .map(|(batch, column_claims)| SourceBatch::Claimed {
                        batch,
                        column_claims,
                    })
                    .collect(),
            ),
        }
    }
}

impl SourceOperator<ArrowTypes> for StaticRecordBatchSource {
    fn name(&self) -> &str {
        &self.name
    }

    fn source(
        &self,
        ctx: &TaskContext<ArrowTypes>,
        _thread_id: ThreadId,
    ) -> Result<OpOutput<Batch>, ArrowError> {
        let mut batches = self.batches.lock().expect("source batches mutex poisoned");
        let runtime = ctx.context_as::<ProjectionRuntimeContext>();
        match batches.pop_front() {
            Some(batch) if batches.is_empty() => {
                let batch = match runtime {
                    Some(runtime) => runtime.emit_source_batch(self.name(), batch),
                    None => match batch {
                        SourceBatch::Plain(batch) => Ok(batch),
                        SourceBatch::Claimed { .. } => {
                            Err(TiforthError::OwnershipContractViolation {
                                detail: format!(
                                    "{} requires ProjectionRuntimeContext for claimed source batches",
                                    self.name()
                                ),
                            })
                        }
                    },
                }
                .map_err(ArrowError::from)?;
                Ok(OpOutput::Finished(Some(batch)))
            }
            Some(batch) => {
                let batch = match runtime {
                    Some(runtime) => runtime.emit_source_batch(self.name(), batch),
                    None => match batch {
                        SourceBatch::Plain(batch) => Ok(batch),
                        SourceBatch::Claimed { .. } => {
                            Err(TiforthError::OwnershipContractViolation {
                                detail: format!(
                                    "{} requires ProjectionRuntimeContext for claimed source batches",
                                    self.name()
                                ),
                            })
                        }
                    },
                }
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

impl PipeOperator<ArrowTypes> for ProjectionPipe {
    fn name(&self) -> &str {
        &self.name
    }

    fn pipe(
        &self,
        ctx: &TaskContext<ArrowTypes>,
        _thread_id: ThreadId,
        input: Option<Batch>,
    ) -> Result<OpOutput<Batch>, ArrowError> {
        let batch = input.ok_or_else(|| ArrowError::from(TiforthError::InvalidPipeInput))?;
        if let Some(runtime) = ctx.context_as::<ProjectionRuntimeContext>() {
            let input = runtime
                .adopt_batch(batch, self.name())
                .map_err(ArrowError::from)?;
            let (output, column_claims) = project_governed_batch(
                &input,
                &self.projections,
                runtime.admission().as_ref(),
                self.name(),
                &|consumer| runtime.new_claim(consumer),
            )
            .map_err(ArrowError::from)?;
            runtime
                .emit_projected_batch(self.name(), Arc::clone(&output), column_claims)
                .map_err(ArrowError::from)?;
            Ok(OpOutput::PipeEven(output))
        } else {
            let admission: Arc<dyn AdmissionController> = Arc::new(NoopAdmissionController);
            let output = project_batch(
                batch.as_ref(),
                &self.projections,
                admission.as_ref(),
                self.name(),
            )
            .map_err(ArrowError::from)?;
            Ok(OpOutput::PipeEven(output))
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

impl SinkOperator<ArrowTypes> for CollectSink {
    fn name(&self) -> &str {
        &self.name
    }

    fn sink(
        &self,
        ctx: &TaskContext<ArrowTypes>,
        _thread_id: ThreadId,
        input: Option<Batch>,
    ) -> Result<OpOutput<Batch>, ArrowError> {
        if let Some(batch) = input {
            let batch = if let Some(runtime) = ctx.context_as::<ProjectionRuntimeContext>() {
                runtime
                    .adopt_batch(batch, self.name())
                    .map_err(ArrowError::from)?
            } else {
                GovernedBatch::ungoverned(batch)
            };
            self.batches
                .lock()
                .expect("sink batches mutex poisoned")
                .push(batch);
        }
        Ok(OpOutput::PipeSinkNeedsMore)
    }
}
