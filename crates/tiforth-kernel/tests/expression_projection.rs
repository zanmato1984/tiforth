use std::any::Any;
use std::sync::Arc;

use arrow_array::{Array, ArrayRef, Int32Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use broken_pipeline::{
    compile, PipeOperator, Pipeline, PipelineChannel, SinkOperator, SourceOperator,
};
use broken_pipeline_schedule::SequentialCoroScheduler;
use tiforth_kernel::admission::{
    AdmissionController, AdmissionEvent, ConsumerKind, RecordingAdmissionController,
};
use tiforth_kernel::expr::Expr;
use tiforth_kernel::handoff::RuntimeEvent;
use tiforth_kernel::operators::{
    CollectSink, ProjectionPipe, ProjectionRuntimeContext, StaticRecordBatchSource,
};
use tiforth_kernel::projection::{project_batch, ProjectionExpr};
use tiforth_kernel::{ArrowTypes, Batch};

#[test]
fn projection_pipe_runs_end_to_end_with_scheduler() {
    let input = make_batch(vec![Some(1), Some(2), Some(3)], false);
    let admission = Arc::new(RecordingAdmissionController::unbounded());
    let runtime_admission: Arc<dyn AdmissionController> = admission.clone();
    let runtime_context = ProjectionRuntimeContext::new(runtime_admission);
    let sink = Arc::new(CollectSink::new("Sink"));

    let status = run_pipeline(
        Arc::new(StaticRecordBatchSource::new(
            "Source",
            vec![Arc::clone(&input)],
        )),
        projection_pipe(),
        Arc::clone(&sink),
        runtime_context.clone(),
    )
    .unwrap();
    assert!(status.is_finished());

    let outputs = sink.batches();
    assert_eq!(outputs.len(), 1);
    let output = &outputs[0];
    assert_eq!(output.batch().schema().field(0).name(), "a_copy");
    assert_eq!(output.batch().schema().field(1).name(), "a_plus_one");
    assert_eq!(
        collect_int32(output.batch().column(0)),
        vec![Some(1), Some(2), Some(3)]
    );
    assert_eq!(
        collect_int32(output.batch().column(1)),
        vec![Some(2), Some(3), Some(4)]
    );
    assert_eq!(output.claim_count(), 1);

    assert_eq!(
        admission.events(),
        vec![
            AdmissionEvent::ConsumerOpened {
                name: "Projection:a_plus_one".into(),
                kind: ConsumerKind::ProjectionOutput,
                spillable: false,
            },
            AdmissionEvent::ReserveAdmitted {
                name: "Projection:a_plus_one".into(),
                bytes: 13,
            },
            AdmissionEvent::ConsumerShrunk {
                name: "Projection:a_plus_one".into(),
                bytes: 1,
            },
        ]
    );
    assert_eq!(
        runtime_context.runtime_events(),
        vec![
            RuntimeEvent::BatchEmitted {
                batch_id: 1,
                origin: tiforth_kernel::BatchOrigin::local("Source"),
                claim_count: 0,
            },
            RuntimeEvent::BatchHandedOff {
                batch_id: 1,
                to_operator: "Projection".into(),
                claim_count: 0,
            },
            RuntimeEvent::BatchEmitted {
                batch_id: 2,
                origin: tiforth_kernel::BatchOrigin::local("Projection"),
                claim_count: 1,
            },
            RuntimeEvent::BatchHandedOff {
                batch_id: 2,
                to_operator: "Sink".into(),
                claim_count: 1,
            },
        ]
    );

    drop(outputs);
    drop(sink);
    runtime_context.record_terminal_finished();

    assert_eq!(
        admission.events(),
        vec![
            AdmissionEvent::ConsumerOpened {
                name: "Projection:a_plus_one".into(),
                kind: ConsumerKind::ProjectionOutput,
                spillable: false,
            },
            AdmissionEvent::ReserveAdmitted {
                name: "Projection:a_plus_one".into(),
                bytes: 13,
            },
            AdmissionEvent::ConsumerShrunk {
                name: "Projection:a_plus_one".into(),
                bytes: 1,
            },
            AdmissionEvent::ConsumerReleased {
                name: "Projection:a_plus_one".into(),
                bytes: 12,
            },
        ]
    );
    assert_eq!(
        runtime_context.runtime_events(),
        vec![
            RuntimeEvent::BatchEmitted {
                batch_id: 1,
                origin: tiforth_kernel::BatchOrigin::local("Source"),
                claim_count: 0,
            },
            RuntimeEvent::BatchHandedOff {
                batch_id: 1,
                to_operator: "Projection".into(),
                claim_count: 0,
            },
            RuntimeEvent::BatchEmitted {
                batch_id: 2,
                origin: tiforth_kernel::BatchOrigin::local("Projection"),
                claim_count: 1,
            },
            RuntimeEvent::BatchHandedOff {
                batch_id: 2,
                to_operator: "Sink".into(),
                claim_count: 1,
            },
            RuntimeEvent::BatchReleased {
                batch_id: 2,
                origin: tiforth_kernel::BatchOrigin::local("Projection"),
                claim_count: 1,
            },
            RuntimeEvent::TerminalFinished,
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
    let runtime_admission: Arc<dyn AdmissionController> = admission.clone();
    let runtime_context = ProjectionRuntimeContext::new(runtime_admission);
    let sink = Arc::new(CollectSink::new("Sink"));

    let error = run_pipeline(
        Arc::new(StaticRecordBatchSource::new(
            "Source",
            vec![Arc::clone(&input)],
        )),
        projection_pipe(),
        Arc::clone(&sink),
        runtime_context.clone(),
    )
    .expect_err("admission-limited run should fail");

    assert!(error.to_string().contains("admission denied"));
    assert!(sink.batches().is_empty());
    runtime_context.record_terminal_error(error.to_string());
    assert_eq!(
        admission.events(),
        vec![
            AdmissionEvent::ConsumerOpened {
                name: "Projection:a_plus_one".into(),
                kind: ConsumerKind::ProjectionOutput,
                spillable: false,
            },
            AdmissionEvent::ReserveDenied {
                name: "Projection:a_plus_one".into(),
                bytes: 13,
                limit: 0,
            },
        ]
    );
    assert_eq!(
        runtime_context.runtime_events(),
        vec![
            RuntimeEvent::BatchEmitted {
                batch_id: 1,
                origin: tiforth_kernel::BatchOrigin::local("Source"),
                claim_count: 0,
            },
            RuntimeEvent::BatchHandedOff {
                batch_id: 1,
                to_operator: "Projection".into(),
                claim_count: 0,
            },
            RuntimeEvent::TerminalError {
                message: error.to_string(),
            },
        ]
    );
}

#[test]
fn direct_projection_forwards_source_claim_without_new_projection_consumer() {
    let input = make_batch(vec![Some(1), Some(2), Some(3)], false);
    let admission = Arc::new(RecordingAdmissionController::unbounded());
    let runtime_admission: Arc<dyn AdmissionController> = admission.clone();
    let runtime_context = ProjectionRuntimeContext::new(runtime_admission);
    let sink = Arc::new(CollectSink::new("Sink"));
    let source_consumer = admission.open(tiforth_kernel::ConsumerSpec::new(
        "Source:a",
        ConsumerKind::SourceInput,
        false,
    ));
    source_consumer.try_reserve(12).unwrap();
    let claim = runtime_context.new_claim(source_consumer);
    let source = Arc::new(StaticRecordBatchSource::new_claimed(
        "Source",
        vec![(Arc::clone(&input), vec![vec![claim]])],
    ));
    let pipe = Arc::new(ProjectionPipe::new(
        "Projection",
        vec![ProjectionExpr::new("a_copy", Expr::column(0))],
    ));

    let status = run_pipeline(source, pipe, Arc::clone(&sink), runtime_context.clone()).unwrap();
    assert!(status.is_finished());

    let outputs = sink.batches();
    assert_eq!(outputs.len(), 1);
    assert_eq!(outputs[0].claim_count(), 1);
    assert_eq!(
        collect_int32(outputs[0].batch().column(0)),
        vec![Some(1), Some(2), Some(3)]
    );

    assert_eq!(
        admission.events(),
        vec![
            AdmissionEvent::ConsumerOpened {
                name: "Source:a".into(),
                kind: ConsumerKind::SourceInput,
                spillable: false,
            },
            AdmissionEvent::ReserveAdmitted {
                name: "Source:a".into(),
                bytes: 12,
            },
        ]
    );

    drop(outputs);
    drop(sink);
    runtime_context.record_terminal_finished();

    assert_eq!(
        admission.events(),
        vec![
            AdmissionEvent::ConsumerOpened {
                name: "Source:a".into(),
                kind: ConsumerKind::SourceInput,
                spillable: false,
            },
            AdmissionEvent::ReserveAdmitted {
                name: "Source:a".into(),
                bytes: 12,
            },
            AdmissionEvent::ConsumerReleased {
                name: "Source:a".into(),
                bytes: 12,
            },
        ]
    );
}

fn run_pipeline(
    source: Arc<dyn SourceOperator<ArrowTypes>>,
    pipe: Arc<dyn PipeOperator<ArrowTypes>>,
    sink: Arc<CollectSink>,
    runtime_context: ProjectionRuntimeContext,
) -> broken_pipeline_schedule::Result<broken_pipeline::TaskStatus> {
    let sink_op: Arc<dyn SinkOperator<ArrowTypes>> = sink;

    let pipeline = Pipeline::<ArrowTypes>::new(
        "ProjectionPipeline",
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

fn projection_pipe() -> Arc<dyn PipeOperator<ArrowTypes>> {
    Arc::new(ProjectionPipe::new(
        "Projection",
        vec![
            ProjectionExpr::new("a_copy", Expr::column(0)),
            ProjectionExpr::new(
                "a_plus_one",
                Expr::add(Expr::column(0), Expr::literal(Some(1))),
            ),
        ],
    ))
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
