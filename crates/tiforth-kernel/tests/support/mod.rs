use std::sync::Arc;

use arrow_array::RecordBatch;
use tiforth_kernel::admission::{AdmissionController, RecordingAdmissionController};
use tiforth_kernel::filter::FilterPredicate;
use tiforth_kernel::projection::ProjectionExpr;
use tiforth_kernel::{ArrowBatch, RuntimeContext, TiforthBatch, TiforthError};

#[allow(dead_code)]
pub fn project_batch(
    input: &RecordBatch,
    projections: &[ProjectionExpr],
    controller: &RecordingAdmissionController,
    operator_name: &str,
) -> Result<ArrowBatch, TiforthError> {
    let runtime_admission: Arc<dyn AdmissionController> = Arc::new(controller.clone());
    let runtime = RuntimeContext::new(runtime_admission);
    let input = TiforthBatch::from_arrow(Arc::new(input.clone()));
    let output =
        tiforth_kernel::projection::project_batch(&runtime, operator_name, &input, projections)?;
    Ok(Arc::clone(output.batch()))
}

#[allow(dead_code)]
pub fn filter_batch(
    input: &RecordBatch,
    predicate: &FilterPredicate,
    controller: &RecordingAdmissionController,
    operator_name: &str,
) -> Result<ArrowBatch, TiforthError> {
    let runtime_admission: Arc<dyn AdmissionController> = Arc::new(controller.clone());
    let runtime = RuntimeContext::new(runtime_admission);
    let input = TiforthBatch::from_arrow(Arc::new(input.clone()));
    let output = tiforth_kernel::filter::filter_batch(&runtime, operator_name, &input, predicate)?;
    Ok(Arc::clone(output.batch()))
}
