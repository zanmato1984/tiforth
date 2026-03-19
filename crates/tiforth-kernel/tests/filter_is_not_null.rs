use std::any::Any;
use std::sync::Arc;

use arrow_array::{Array, ArrayRef, Int32Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use broken_pipeline::{
    compile, PipeOperator, Pipeline, PipelineChannel, SinkOperator, SourceOperator,
};
use broken_pipeline_schedule::SequentialCoroScheduler;
use tiforth_kernel::admission::{AdmissionController, RecordingAdmissionController};
use tiforth_kernel::operators::{
    CollectSink, FilterPipe, ProjectionRuntimeContext, StaticRecordBatchSource,
};
use tiforth_kernel::{ArrowTypes, Batch, FilterPredicate};

#[test]
fn filter_pipe_keeps_all_rows_when_predicate_column_has_no_nulls() {
    let input = make_single_int32_batch(vec![Some(1), Some(2), Some(3)], false);
    let admission = Arc::new(RecordingAdmissionController::unbounded());
    let runtime_admission: Arc<dyn AdmissionController> = admission.clone();
    let runtime_context = ProjectionRuntimeContext::new(runtime_admission);
    let sink = Arc::new(CollectSink::new("Sink"));

    let status = run_pipeline(
        Arc::new(StaticRecordBatchSource::new(
            "Source",
            vec![Arc::clone(&input)],
        )),
        Arc::new(FilterPipe::new(
            "Filter",
            FilterPredicate::is_not_null_column(0),
        )),
        Arc::clone(&sink),
        runtime_context,
    )
    .unwrap();

    assert!(status.is_finished());
    let outputs = sink.batches();
    assert_eq!(outputs.len(), 1);
    let output = outputs[0].batch();
    assert_eq!(output.num_rows(), 3);
    assert_eq!(
        collect_int32(output.column(0)),
        vec![Some(1), Some(2), Some(3)]
    );
}

#[test]
fn filter_pipe_drops_all_rows_when_predicate_column_is_all_null() {
    let input = make_single_int32_batch(vec![None, None, None], true);
    let admission = Arc::new(RecordingAdmissionController::unbounded());
    let runtime_admission: Arc<dyn AdmissionController> = admission.clone();
    let runtime_context = ProjectionRuntimeContext::new(runtime_admission);
    let sink = Arc::new(CollectSink::new("Sink"));

    let status = run_pipeline(
        Arc::new(StaticRecordBatchSource::new(
            "Source",
            vec![Arc::clone(&input)],
        )),
        Arc::new(FilterPipe::new(
            "Filter",
            FilterPredicate::is_not_null_column(0),
        )),
        Arc::clone(&sink),
        runtime_context,
    )
    .unwrap();

    assert!(status.is_finished());
    let outputs = sink.batches();
    assert_eq!(outputs.len(), 1);
    let output = outputs[0].batch();
    assert_eq!(output.num_rows(), 0);
    assert_eq!(output.schema().fields(), input.schema().fields());
}

#[test]
fn filter_pipe_preserves_schema_order_and_values_for_mixed_keep_drop() {
    let input = make_two_int32_batch(
        vec![Some(1), None, Some(3), None],
        true,
        vec![Some(10), Some(20), Some(30), Some(40)],
        false,
    );
    let admission = Arc::new(RecordingAdmissionController::unbounded());
    let runtime_admission: Arc<dyn AdmissionController> = admission.clone();
    let runtime_context = ProjectionRuntimeContext::new(runtime_admission);
    let sink = Arc::new(CollectSink::new("Sink"));

    let status = run_pipeline(
        Arc::new(StaticRecordBatchSource::new(
            "Source",
            vec![Arc::clone(&input)],
        )),
        Arc::new(FilterPipe::new(
            "Filter",
            FilterPredicate::is_not_null_column(0),
        )),
        Arc::clone(&sink),
        runtime_context,
    )
    .unwrap();

    assert!(status.is_finished());
    let outputs = sink.batches();
    assert_eq!(outputs.len(), 1);
    let output = outputs[0].batch();
    assert_eq!(output.schema().field(0).name(), "a");
    assert_eq!(output.schema().field(1).name(), "b");
    assert_eq!(output.schema().fields(), input.schema().fields());
    assert_eq!(output.num_rows(), 2);
    assert_eq!(collect_int32(output.column(0)), vec![Some(1), Some(3)]);
    assert_eq!(collect_int32(output.column(1)), vec![Some(10), Some(30)]);
}

#[test]
fn filter_pipe_reports_missing_column_error() {
    let input = make_single_int32_batch(vec![Some(1), Some(2), Some(3)], false);
    let admission = Arc::new(RecordingAdmissionController::unbounded());
    let runtime_admission: Arc<dyn AdmissionController> = admission.clone();
    let runtime_context = ProjectionRuntimeContext::new(runtime_admission);
    let sink = Arc::new(CollectSink::new("Sink"));

    let error = run_pipeline(
        Arc::new(StaticRecordBatchSource::new(
            "Source",
            vec![Arc::clone(&input)],
        )),
        Arc::new(FilterPipe::new(
            "Filter",
            FilterPredicate::is_not_null_column(1),
        )),
        Arc::clone(&sink),
        runtime_context,
    )
    .expect_err("missing-column filter should fail");

    assert!(error
        .to_string()
        .contains("missing input column at index 1"));
    assert!(sink.batches().is_empty());
}

#[test]
fn filter_pipe_reports_unsupported_predicate_type_error() {
    let input = make_utf8_batch(vec![Some("x"), Some("y"), Some("z")], false);
    let admission = Arc::new(RecordingAdmissionController::unbounded());
    let runtime_admission: Arc<dyn AdmissionController> = admission.clone();
    let runtime_context = ProjectionRuntimeContext::new(runtime_admission);
    let sink = Arc::new(CollectSink::new("Sink"));

    let error = run_pipeline(
        Arc::new(StaticRecordBatchSource::new(
            "Source",
            vec![Arc::clone(&input)],
        )),
        Arc::new(FilterPipe::new(
            "Filter",
            FilterPredicate::is_not_null_column(0),
        )),
        Arc::clone(&sink),
        runtime_context,
    )
    .expect_err("utf8 predicate input should fail");

    assert!(error
        .to_string()
        .contains("unsupported data type: expected Int32 or Date32 predicate input at column 0, got Utf8"));
    assert!(sink.batches().is_empty());
}

fn run_pipeline(
    source: Arc<dyn SourceOperator<ArrowTypes>>,
    pipe: Arc<dyn PipeOperator<ArrowTypes>>,
    sink: Arc<CollectSink>,
    runtime_context: ProjectionRuntimeContext,
) -> broken_pipeline_schedule::Result<broken_pipeline::TaskStatus> {
    let sink_op: Arc<dyn SinkOperator<ArrowTypes>> = sink;

    let pipeline = Pipeline::<ArrowTypes>::new(
        "FilterPipeline",
        vec![PipelineChannel::new(source, vec![pipe])],
        sink_op,
    );

    let pipe_runtime = compile(&pipeline, 1).pipelinexes()[0].pipe_exec();
    let scheduler = SequentialCoroScheduler::default();
    let context: Arc<dyn Any + Send + Sync> = Arc::new(runtime_context);
    let task_context = scheduler.make_task_context(Some(context));
    let handle = scheduler.schedule_task_group(pipe_runtime.task_group(), task_context);
    scheduler.wait_task_group(handle)
}

fn make_single_int32_batch(values: Vec<Option<i32>>, nullable: bool) -> Batch {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "a",
        DataType::Int32,
        nullable,
    )]));
    let values: ArrayRef = Arc::new(Int32Array::from(values));
    Arc::new(RecordBatch::try_new(schema, vec![values]).unwrap())
}

fn make_two_int32_batch(
    lhs_values: Vec<Option<i32>>,
    lhs_nullable: bool,
    rhs_values: Vec<Option<i32>>,
    rhs_nullable: bool,
) -> Batch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("a", DataType::Int32, lhs_nullable),
        Field::new("b", DataType::Int32, rhs_nullable),
    ]));
    let lhs: ArrayRef = Arc::new(Int32Array::from(lhs_values));
    let rhs: ArrayRef = Arc::new(Int32Array::from(rhs_values));
    Arc::new(RecordBatch::try_new(schema, vec![lhs, rhs]).unwrap())
}

fn make_utf8_batch(values: Vec<Option<&str>>, nullable: bool) -> Batch {
    let schema = Arc::new(Schema::new(vec![Field::new("s", DataType::Utf8, nullable)]));
    let values: ArrayRef = Arc::new(StringArray::from(values));
    Arc::new(RecordBatch::try_new(schema, vec![values]).unwrap())
}

fn collect_int32(array: &ArrayRef) -> Vec<Option<i32>> {
    let values = array
        .as_any()
        .downcast_ref::<Int32Array>()
        .expect("expected Int32Array output");
    (0..values.len())
        .map(|index| {
            values
                .is_null(index)
                .then_some(())
                .map_or(Some(values.value(index)), |_| None)
        })
        .collect()
}
