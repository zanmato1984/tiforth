pub mod admission;
pub mod batch;
pub mod collation;
pub mod error;
pub mod expr;
pub mod filter;
pub mod operators;
pub mod projection;
pub mod runtime;
pub mod snapshot;

use arrow_schema::ArrowError;
use broken_pipeline::PipelineTypes;
use broken_pipeline_schedule::{ScheduleError, ScheduleTypes};

pub use admission::{
    AdmissionConsumer, AdmissionController, AdmissionEvent, ConsumerKind, ConsumerSpec,
    NoopAdmissionController, RecordingAdmissionController,
};
pub use batch::{BatchClaim, TiforthBatch};
pub use broken_pipeline::traits::arrow::Batch as ArrowBatch;
pub use collation::{
    collation_eq_column_literal, collation_lt_column_literal, order_by_column_asc_indices,
    CollationRef,
};
pub use error::TiforthError;
pub use expr::Expr;
pub use filter::{filter_batch, FilterPredicate};
pub use operators::{
    CollectSink, ExchangePipe, FilterPipe, ProjectionPipe, StaticRecordBatchSource,
};
pub use projection::{project_batch, ProjectionExpr};
pub use runtime::{BatchOrigin, RuntimeContext, RuntimeEvent};
pub use snapshot::{
    AdmissionFixtureEvent, FixtureBatchOrigin, LocalExecutionFixture, LocalExecutionSnapshot,
    RuntimeFixtureEvent,
};

#[derive(Clone, Copy, Debug, Default)]
pub struct TiforthTypes;

impl PipelineTypes for TiforthTypes {
    type Batch = TiforthBatch;
    type Error = ArrowError;
    type Context = RuntimeContext;
}

impl ScheduleTypes for TiforthTypes {
    fn from_schedule_error(error: ScheduleError) -> Self::Error {
        ArrowError::ComputeError(error.to_string())
    }
}
