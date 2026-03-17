use std::any::Any;
use std::sync::Arc;

use arrow_array::{Array, ArrayRef, Int32Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use broken_pipeline::{
    compile, PipeOperator, Pipeline, PipelineChannel, SinkOperator, SourceOperator,
};
use broken_pipeline_schedule::SequentialCoroScheduler;
use tiforth_kernel::admission::{AdmissionEvent, ConsumerKind, RecordingAdmissionController};
use tiforth_kernel::expr::Expr;
use tiforth_kernel::operators::{
    CollectSink, ProjectionPipe, ProjectionRuntimeContext, StaticRecordBatchSource,
};
use tiforth_kernel::projection::{project_batch, ProjectionExpr};
use tiforth_kernel::{ArrowTypes, Batch};

#[test]
fn projection_pipe_runs_end_to_end_with_scheduler() {
    let input = make_batch(vec![Some(1), Some(2), Some(3)], false);
    let admission = Arc::new(RecordingAdmissionController::unbounded());
    let sink = Arc::new(CollectSink::new("Sink"));

    let status = run_pipeline(
        Arc::clone(&input),
        Arc::clone(&sink),
        Arc::clone(&admission),
    )
    .unwrap();
    assert!(status.is_finished());

    let outputs = sink.batches();
    assert_eq!(outputs.len(), 1);
    let output = &outputs[0];
    assert_eq!(output.schema().field(0).name(), "a_copy");
    assert_eq!(output.schema().field(1).name(), "a_plus_one");
    assert_eq!(
        collect_int32(output.column(0)),
        vec![Some(1), Some(2), Some(3)]
    );
    assert_eq!(
        collect_int32(output.column(1)),
        vec![Some(2), Some(3), Some(4)]
    );

    assert_eq!(
        admission.events(),
        vec![
            AdmissionEvent::Open {
                name: "Projection:a_plus_one".into(),
                kind: ConsumerKind::ProjectionOutput,
                spillable: false,
            },
            AdmissionEvent::TryReserve {
                name: "Projection:a_plus_one".into(),
                bytes: 13,
            },
            AdmissionEvent::Shrink {
                name: "Projection:a_plus_one".into(),
                bytes: 1,
            },
            AdmissionEvent::Release {
                name: "Projection:a_plus_one".into(),
                bytes: 12,
            },
        ]
    );
}

#[test]
fn add_projection_propagates_nulls() {
    let input = make_batch(vec![Some(1), None, Some(3)], true);
    let admission = RecordingAdmissionController::unbounded();
    let output = project_batch(
        input.as_ref(),
        &[ProjectionExpr::new(
            "a_plus_one",
            Expr::add(Expr::column(0), Expr::literal(Some(1))),
        )],
        &admission,
        "Projection",
    )
    .unwrap();

    assert_eq!(
        collect_int32(output.column(0)),
        vec![Some(2), None, Some(4)]
    );
}

#[test]
fn admission_denial_fails_before_projection_output_is_collected() {
    let input = make_batch(vec![Some(1), Some(2), Some(3)], false);
    let admission = Arc::new(RecordingAdmissionController::with_limit(0));
    let sink = Arc::new(CollectSink::new("Sink"));

    let error = run_pipeline(
        Arc::clone(&input),
        Arc::clone(&sink),
        Arc::clone(&admission),
    )
    .expect_err("admission-limited run should fail");

    assert!(error.to_string().contains("admission denied"));
    assert!(sink.batches().is_empty());
    assert_eq!(
        admission.events(),
        vec![AdmissionEvent::Open {
            name: "Projection:a_plus_one".into(),
            kind: ConsumerKind::ProjectionOutput,
            spillable: false,
        },]
    );
}

fn run_pipeline(
    input: Batch,
    sink: Arc<CollectSink>,
    admission: Arc<RecordingAdmissionController>,
) -> broken_pipeline_schedule::Result<broken_pipeline::TaskStatus> {
    let source = Arc::new(StaticRecordBatchSource::new("Source", vec![input]));
    let pipe = Arc::new(ProjectionPipe::new(
        "Projection",
        vec![
            ProjectionExpr::new("a_copy", Expr::column(0)),
            ProjectionExpr::new(
                "a_plus_one",
                Expr::add(Expr::column(0), Expr::literal(Some(1))),
            ),
        ],
    ));

    let source_op: Arc<dyn SourceOperator<ArrowTypes>> = source;
    let pipe_op: Arc<dyn PipeOperator<ArrowTypes>> = pipe;
    let sink_op: Arc<dyn SinkOperator<ArrowTypes>> = sink;

    let pipeline = Pipeline::<ArrowTypes>::new(
        "ProjectionPipeline",
        vec![PipelineChannel::new(source_op, vec![pipe_op])],
        sink_op,
    );

    let runtime = compile(&pipeline, 1).pipelinexes()[0].pipe_exec();
    let scheduler = SequentialCoroScheduler::default();
    let context: Arc<dyn Any + Send + Sync> = Arc::new(ProjectionRuntimeContext::new(admission));
    let task_context = scheduler.make_task_context(Some(context));
    let handle = scheduler.schedule_task_group(runtime.task_group(), task_context);
    scheduler.wait_task_group(handle)
}

fn make_batch(values: Vec<Option<i32>>, nullable: bool) -> Batch {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "a",
        DataType::Int32,
        nullable,
    )]));
    let values: ArrayRef = Arc::new(Int32Array::from(values));
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
