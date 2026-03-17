use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use arrow_schema::ArrowError;
use broken_pipeline::traits::arrow::{ArrowTypes, Batch};
use broken_pipeline::{
    OpOutput, PipeOperator, SinkOperator, SourceOperator, TaskContext, ThreadId,
};

use crate::admission::{AdmissionController, NoopAdmissionController};
use crate::error::TiforthError;
use crate::projection::{project_batch, ProjectionExpr};

#[derive(Clone)]
pub struct ProjectionRuntimeContext {
    admission: Arc<dyn AdmissionController>,
}

impl ProjectionRuntimeContext {
    pub fn new(admission: Arc<dyn AdmissionController>) -> Self {
        Self { admission }
    }

    pub fn admission(&self) -> &Arc<dyn AdmissionController> {
        &self.admission
    }
}

pub struct StaticRecordBatchSource {
    name: String,
    batches: Mutex<VecDeque<Batch>>,
}

impl StaticRecordBatchSource {
    pub fn new(name: impl Into<String>, batches: Vec<Batch>) -> Self {
        Self {
            name: name.into(),
            batches: Mutex::new(batches.into_iter().collect()),
        }
    }
}

impl SourceOperator<ArrowTypes> for StaticRecordBatchSource {
    fn name(&self) -> &str {
        &self.name
    }

    fn source(
        &self,
        _ctx: &TaskContext<ArrowTypes>,
        _thread_id: ThreadId,
    ) -> Result<OpOutput<Batch>, ArrowError> {
        let mut batches = self.batches.lock().expect("source batches mutex poisoned");
        match batches.pop_front() {
            Some(batch) if batches.is_empty() => Ok(OpOutput::Finished(Some(batch))),
            Some(batch) => Ok(OpOutput::SourcePipeHasMore(batch)),
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
        let admission: Arc<dyn AdmissionController> = ctx
            .context_as::<ProjectionRuntimeContext>()
            .map(|runtime| Arc::clone(runtime.admission()))
            .unwrap_or_else(|| Arc::new(NoopAdmissionController));
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

pub struct CollectSink {
    name: String,
    batches: Mutex<Vec<Batch>>,
}

impl CollectSink {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            batches: Mutex::new(Vec::new()),
        }
    }

    pub fn batches(&self) -> Vec<Batch> {
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
        _ctx: &TaskContext<ArrowTypes>,
        _thread_id: ThreadId,
        input: Option<Batch>,
    ) -> Result<OpOutput<Batch>, ArrowError> {
        if let Some(batch) = input {
            self.batches
                .lock()
                .expect("sink batches mutex poisoned")
                .push(batch);
        }
        Ok(OpOutput::PipeSinkNeedsMore)
    }
}
