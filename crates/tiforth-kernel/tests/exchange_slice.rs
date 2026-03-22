use std::any::Any;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use arrow_array::{Array, ArrayRef, Int32Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use broken_pipeline::{
    compile, Awaiter, PipeOperator, Pipeline, PipelineChannel, Resumer, SharedAwaiter,
    SharedResumer, SinkOperator, SourceOperator, TaskContext, TaskStatus,
};
use broken_pipeline_schedule::SequentialCoroScheduler;
use tiforth_kernel::admission::{
    AdmissionController, AdmissionEvent, ConsumerKind, ConsumerSpec, RecordingAdmissionController,
};
use tiforth_kernel::expr::Expr;
use tiforth_kernel::operators::{
    CollectSink, ExchangePipe, ProjectionPipe, StaticRecordBatchSource,
};
use tiforth_kernel::projection::ProjectionExpr;
use tiforth_kernel::{ArrowBatch, RuntimeContext, TiforthTypes};

#[test]
fn exchange_passthrough_single_batch_preserves_schema_and_values() {
    let input = make_batch(vec![Some(1), Some(2), Some(3)], false);
    let admission = Arc::new(RecordingAdmissionController::unbounded());
    let runtime_admission: Arc<dyn AdmissionController> = admission.clone();
    let runtime_context = RuntimeContext::new(runtime_admission);
    let sink = Arc::new(CollectSink::new("Sink"));

    let status = run_pipeline(
        Arc::new(StaticRecordBatchSource::new(
            "Source",
            vec![Arc::clone(&input)],
        )),
        vec![
            projection_copy_pipe(),
            Arc::new(ExchangePipe::new("Exchange", 4)),
        ],
        Arc::clone(&sink),
        runtime_context,
    )
    .expect("exchange single-batch pipeline should succeed");

    assert!(status.is_finished());
    let outputs = sink.batches();
    assert_eq!(outputs.len(), 1);
    assert_eq!(outputs[0].batch().schema().field(0).name(), "a_copy");
    assert_eq!(
        collect_int32(outputs[0].batch().column(0)),
        vec![Some(1), Some(2), Some(3)]
    );
}

#[test]
fn exchange_passthrough_multi_batch_preserves_fifo_order() {
    let first = make_batch(vec![Some(1), Some(2)], false);
    let second = make_batch(vec![Some(3), Some(4)], false);
    let admission = Arc::new(RecordingAdmissionController::unbounded());
    let runtime_admission: Arc<dyn AdmissionController> = admission.clone();
    let runtime_context = RuntimeContext::new(runtime_admission);
    let sink = Arc::new(CollectSink::new("Sink"));

    let status = run_pipeline(
        Arc::new(StaticRecordBatchSource::new(
            "Source",
            vec![Arc::clone(&first), Arc::clone(&second)],
        )),
        vec![
            projection_copy_pipe(),
            Arc::new(ExchangePipe::new("Exchange", 4)),
        ],
        Arc::clone(&sink),
        runtime_context,
    )
    .expect("exchange multi-batch pipeline should succeed");

    assert!(status.is_finished());
    let outputs = sink.batches();
    assert_eq!(outputs.len(), 2);
    assert_eq!(
        collect_int32(outputs[0].batch().column(0)),
        vec![Some(1), Some(2)]
    );
    assert_eq!(
        collect_int32(outputs[1].batch().column(0)),
        vec![Some(3), Some(4)]
    );
}

#[test]
fn exchange_bounded_queue_can_block_then_resume() {
    let first = make_batch(vec![Some(1)], false);
    let second = make_batch(vec![Some(2)], false);
    let admission = Arc::new(RecordingAdmissionController::unbounded());
    let runtime_admission: Arc<dyn AdmissionController> = admission.clone();
    let runtime_context = RuntimeContext::new(runtime_admission);
    let sink = Arc::new(CollectSink::new("Sink"));

    let sink_op: Arc<dyn SinkOperator<TiforthTypes>> = sink.clone();
    let pipeline = Pipeline::<TiforthTypes>::new(
        "ExchangeBlockedResume",
        vec![PipelineChannel::new(
            Arc::new(StaticRecordBatchSource::new(
                "Source",
                vec![Arc::clone(&first), Arc::clone(&second)],
            )),
            vec![
                projection_copy_pipe(),
                Arc::new(ExchangePipe::new("Exchange", 1)),
            ],
        )],
        sink_op,
    );
    let pipe_runtime = compile(&pipeline, 1).pipelinexes()[0].pipe_exec();
    let task_context = test_task_context(runtime_context);

    let first_step = pipe_runtime
        .step(&task_context, 0)
        .expect("first exchange step should succeed");
    assert!(first_step.is_continue());

    let blocked = pipe_runtime
        .step(&task_context, 0)
        .expect("second exchange step should reach blocked state");
    assert!(blocked.is_blocked());
    let resumers = awaiter_resumers(blocked.awaiter().expect("missing blocked awaiter"));
    assert_eq!(resumers.len(), 1);
    resumers[0].resume();

    let mut saw_finished = false;
    for _ in 0..8 {
        let status = pipe_runtime
            .step(&task_context, 0)
            .expect("resumed exchange step should succeed");
        if status.is_blocked() {
            for resumer in awaiter_resumers(status.awaiter().expect("missing blocked awaiter")) {
                resumer.resume();
            }
            continue;
        }
        if status.is_finished() {
            saw_finished = true;
            break;
        }
    }

    assert!(
        saw_finished,
        "exchange pipeline did not reach finished status"
    );
    let outputs = sink.batches();
    assert_eq!(outputs.len(), 2);
    assert_eq!(collect_int32(outputs[0].batch().column(0)), vec![Some(1)]);
    assert_eq!(collect_int32(outputs[1].batch().column(0)), vec![Some(2)]);
}

#[test]
fn exchange_reports_finished_only_after_drain_for_buffered_batches() {
    let first = make_batch(vec![Some(1)], false);
    let second = make_batch(vec![Some(2)], false);
    let admission = Arc::new(RecordingAdmissionController::unbounded());
    let runtime_admission: Arc<dyn AdmissionController> = admission.clone();
    let runtime_context = RuntimeContext::new(runtime_admission);
    let sink = Arc::new(CollectSink::new("Sink"));

    let sink_op: Arc<dyn SinkOperator<TiforthTypes>> = sink.clone();
    let pipeline = Pipeline::<TiforthTypes>::new(
        "ExchangeDrainFinish",
        vec![PipelineChannel::new(
            Arc::new(StaticRecordBatchSource::new(
                "Source",
                vec![Arc::clone(&first), Arc::clone(&second)],
            )),
            vec![
                projection_copy_pipe(),
                Arc::new(ExchangePipe::new("Exchange", 4)),
            ],
        )],
        sink_op,
    );
    let pipe_runtime = compile(&pipeline, 1).pipelinexes()[0].pipe_exec();
    let task_context = test_task_context(runtime_context);

    assert!(pipe_runtime.step(&task_context, 0).unwrap().is_continue());
    assert_eq!(sink.batches().len(), 0);

    assert!(pipe_runtime.step(&task_context, 0).unwrap().is_continue());
    assert_eq!(sink.batches().len(), 0);

    assert!(pipe_runtime.step(&task_context, 0).unwrap().is_continue());
    assert_eq!(sink.batches().len(), 1);

    assert!(pipe_runtime.step(&task_context, 0).unwrap().is_continue());
    assert_eq!(sink.batches().len(), 2);

    assert!(pipe_runtime.step(&task_context, 0).unwrap().is_finished());
    assert_eq!(sink.batches().len(), 2);
}

#[test]
fn exchange_cancellation_teardown_releases_buffered_source_claims() {
    let input = make_batch(vec![Some(1), Some(2), Some(3)], false);
    let admission = Arc::new(RecordingAdmissionController::unbounded());
    let runtime_admission: Arc<dyn AdmissionController> = admission.clone();
    let runtime_context = RuntimeContext::new(runtime_admission);
    let sink = Arc::new(CollectSink::new("Sink"));

    let source_consumer = admission.open(ConsumerSpec::new(
        "Source:a",
        ConsumerKind::SourceInput,
        false,
    ));
    source_consumer
        .try_reserve(12)
        .expect("source reserve should succeed");
    let source_claim = runtime_context.new_token(source_consumer);
    let source = Arc::new(StaticRecordBatchSource::with_claims(
        "Source",
        vec![(Arc::clone(&input), vec![source_claim])],
    ));

    let sink_op: Arc<dyn SinkOperator<TiforthTypes>> = sink.clone();
    let pipeline = Pipeline::<TiforthTypes>::new(
        "ExchangeCancelledRelease",
        vec![PipelineChannel::new(
            source,
            vec![
                projection_copy_pipe(),
                Arc::new(ExchangePipe::new("Exchange", 4)),
            ],
        )],
        sink_op,
    );
    let pipe_runtime = compile(&pipeline, 1).pipelinexes()[0].pipe_exec();
    let task_context = test_task_context(runtime_context.clone());

    assert!(pipe_runtime.step(&task_context, 0).unwrap().is_continue());
    assert_eq!(sink.batches().len(), 0);

    drop(pipe_runtime);
    drop(pipeline);
    drop(sink);
    runtime_context.record_terminal_cancelled();

    let events = admission.events();
    assert!(events.contains(&AdmissionEvent::ConsumerOpened {
        name: "Source:a".into(),
        kind: ConsumerKind::SourceInput,
        spillable: false,
    }));
    assert!(events.contains(&AdmissionEvent::ReserveAdmitted {
        name: "Source:a".into(),
        bytes: 12,
    }));
    assert!(events.contains(&AdmissionEvent::ConsumerReleased {
        name: "Source:a".into(),
        bytes: 12,
    }));
}

fn run_pipeline(
    source: Arc<dyn SourceOperator<TiforthTypes>>,
    pipes: Vec<Arc<dyn PipeOperator<TiforthTypes>>>,
    sink: Arc<CollectSink>,
    runtime_context: RuntimeContext,
) -> broken_pipeline::BpResult<TaskStatus, TiforthTypes> {
    let sink_op: Arc<dyn SinkOperator<TiforthTypes>> = sink;
    let pipeline = Pipeline::<TiforthTypes>::new(
        "ExchangePipeline",
        vec![PipelineChannel::new(source, pipes)],
        sink_op,
    );

    let pipe_runtime = compile(&pipeline, 1).pipelinexes()[0].pipe_exec();
    let scheduler = SequentialCoroScheduler::<TiforthTypes>::default();
    let task_context = scheduler.make_task_context(runtime_context);
    let handle = scheduler.schedule_task_group(pipe_runtime.task_group(), task_context);
    scheduler.wait_task_group(handle)
}

fn projection_copy_pipe() -> Arc<dyn PipeOperator<TiforthTypes>> {
    Arc::new(ProjectionPipe::new(
        "Projection",
        vec![ProjectionExpr::new("a_copy", Expr::column(0))],
    ))
}

fn make_batch(values: Vec<Option<i32>>, nullable: bool) -> ArrowBatch {
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

#[derive(Default)]
struct TestResumer {
    resumed: AtomicBool,
}

impl Resumer for TestResumer {
    fn resume(&self) {
        self.resumed.store(true, Ordering::SeqCst);
    }

    fn is_resumed(&self) -> bool {
        self.resumed.load(Ordering::SeqCst)
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

struct RecordedAwaiter {
    resumers: Vec<SharedResumer>,
}

impl RecordedAwaiter {
    fn new(resumers: Vec<SharedResumer>) -> Self {
        Self { resumers }
    }

    fn resumers(&self) -> &[SharedResumer] {
        &self.resumers
    }
}

impl Awaiter for RecordedAwaiter {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

fn test_task_context(runtime_context: RuntimeContext) -> TaskContext<TiforthTypes> {
    TaskContext::new(
        runtime_context,
        Arc::new(|| Ok(Arc::new(TestResumer::default()) as SharedResumer)),
        Arc::new(|resumers| Ok(Arc::new(RecordedAwaiter::new(resumers)) as SharedAwaiter)),
    )
}

fn awaiter_resumers(awaiter: &SharedAwaiter) -> Vec<SharedResumer> {
    awaiter
        .as_any()
        .downcast_ref::<RecordedAwaiter>()
        .expect("expected RecordedAwaiter")
        .resumers()
        .to_vec()
}
