use std::any::Any;
use std::sync::{Arc, Mutex};

use arrow_array::{Array, ArrayRef, Int32Array, RecordBatch};
use arrow_schema::{ArrowError, DataType, Field, Schema};
use broken_pipeline::{
    compile, OpOutput, PipeOperator, Pipeline, PipelineChannel, SinkOperator, SourceOperator,
    TaskContext, TaskStatus, ThreadId,
};
use broken_pipeline_schedule::SequentialCoroScheduler;
use tiforth_kernel::admission::{
    AdmissionController, AdmissionEvent, ConsumerKind, RecordingAdmissionController,
};
use tiforth_kernel::expr::Expr;
use tiforth_kernel::operators::{
    CollectSink, ProjectionPipe, ProjectionRuntimeContext, StaticRecordBatchSource,
};
use tiforth_kernel::projection::{project_batch, ProjectionExpr};
use tiforth_kernel::{ArrowTypes, Batch, LocalExecutionFixture};

const PROJECTION_COMPUTED_BEFORE_TERMINAL: &str = include_str!(
    "../../../tests/conformance/fixtures/local-execution/projection-computed-before-terminal.json",
);
const PROJECTION_COMPUTED_FINISHED: &str = include_str!(
    "../../../tests/conformance/fixtures/local-execution/projection-computed-finished.json",
);
const PROJECTION_NON_NULL_LITERAL_BEFORE_TERMINAL: &str = include_str!(
    "../../../tests/conformance/fixtures/local-execution/projection-non-null-literal-before-terminal.json",
);
const PROJECTION_NON_NULL_LITERAL_FINISHED: &str = include_str!(
    "../../../tests/conformance/fixtures/local-execution/projection-non-null-literal-finished.json",
);
const PROJECTION_NULL_LITERAL_BEFORE_TERMINAL: &str = include_str!(
    "../../../tests/conformance/fixtures/local-execution/projection-null-literal-before-terminal.json",
);
const PROJECTION_NULL_LITERAL_FINISHED: &str = include_str!(
    "../../../tests/conformance/fixtures/local-execution/projection-null-literal-finished.json",
);
const PROJECTION_DENIED: &str =
    include_str!("../../../tests/conformance/fixtures/local-execution/projection-denied.json",);
const PROJECTION_OVERFLOW: &str =
    include_str!("../../../tests/conformance/fixtures/local-execution/projection-overflow.json",);
const PROJECTION_PASSTHROUGH_BEFORE_TERMINAL: &str = include_str!(
    "../../../tests/conformance/fixtures/local-execution/projection-passthrough-before-terminal.json",
);
const PROJECTION_PASSTHROUGH_FINISHED: &str = include_str!(
    "../../../tests/conformance/fixtures/local-execution/projection-passthrough-finished.json",
);
const PROJECTION_PASSTHROUGH_OWNERSHIP_VIOLATION: &str = include_str!(
    "../../../tests/conformance/fixtures/local-execution/projection-passthrough-ownership-violation.json",
);
const PROJECTION_PASSTHROUGH_SHRINK_OWNERSHIP_VIOLATION: &str = include_str!(
    "../../../tests/conformance/fixtures/local-execution/projection-passthrough-shrink-ownership-violation.json",
);
const PROJECTION_UNTRACKED_HANDOFF_OWNERSHIP_VIOLATION: &str = include_str!(
    "../../../tests/conformance/fixtures/local-execution/projection-untracked-handoff-ownership-violation.json",
);
const PROJECTION_CLAIMED_SOURCE_RUNTIME_CONTEXT_OWNERSHIP_VIOLATION: &str = include_str!(
    "../../../tests/conformance/fixtures/local-execution/projection-claimed-source-runtime-context-ownership-violation.json",
);
const PROJECTION_MIXED_CLAIMS_BEFORE_TERMINAL: &str = include_str!(
    "../../../tests/conformance/fixtures/local-execution/projection-mixed-claims-before-terminal.json",
);
const PROJECTION_MIXED_CLAIMS_FINISHED: &str = include_str!(
    "../../../tests/conformance/fixtures/local-execution/projection-mixed-claims-finished.json",
);
const PROJECTION_MIXED_CLAIMS_CANCELLED: &str = include_str!(
    "../../../tests/conformance/fixtures/local-execution/projection-mixed-claims-cancelled.json",
);

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

    assert_fixture_json(
        "projection-computed-before-terminal",
        runtime_context
            .local_snapshot(admission.as_ref())
            .to_fixture(),
        PROJECTION_COMPUTED_BEFORE_TERMINAL,
    );

    drop(outputs);
    drop(sink);
    runtime_context.record_terminal_finished();

    assert_fixture_json(
        "projection-computed-finished",
        runtime_context
            .local_snapshot(admission.as_ref())
            .to_fixture(),
        PROJECTION_COMPUTED_FINISHED,
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
fn direct_literal_projection_materializes_non_null_int32() {
    let input = make_batch(vec![Some(1), Some(2), Some(3)], false);
    let admission = RecordingAdmissionController::unbounded();
    let output = project_batch(
        input.as_ref(),
        &[ProjectionExpr::new("literal_seven", Expr::literal(Some(7)))],
        &admission,
        "Projection",
    )
    .unwrap();

    assert_eq!(output.num_rows(), input.num_rows());
    assert_eq!(output.schema().field(0).name(), "literal_seven");
    assert!(!output.schema().field(0).is_nullable());
    assert_eq!(
        collect_int32(output.column(0)),
        vec![Some(7), Some(7), Some(7)]
    );
    assert_eq!(
        admission.events(),
        vec![
            AdmissionEvent::ConsumerOpened {
                name: "Projection:literal_seven".into(),
                kind: ConsumerKind::ProjectionOutput,
                spillable: false,
            },
            AdmissionEvent::ReserveAdmitted {
                name: "Projection:literal_seven".into(),
                bytes: 13,
            },
            AdmissionEvent::ConsumerShrunk {
                name: "Projection:literal_seven".into(),
                bytes: 1,
            },
            AdmissionEvent::ConsumerReleased {
                name: "Projection:literal_seven".into(),
                bytes: 12,
            },
        ]
    );
}

#[test]
fn non_null_literal_projection_carries_shrunk_claim_through_runtime_handoff() {
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
        Arc::new(ProjectionPipe::new(
            "Projection",
            vec![ProjectionExpr::new("literal_seven", Expr::literal(Some(7)))],
        )),
        Arc::clone(&sink),
        runtime_context.clone(),
    )
    .unwrap();
    assert!(status.is_finished());

    let outputs = sink.batches();
    assert_eq!(outputs.len(), 1);
    let output = &outputs[0];
    assert_eq!(output.batch().schema().field(0).name(), "literal_seven");
    assert!(!output.batch().schema().field(0).is_nullable());
    assert_eq!(collect_int32(output.batch().column(0)), vec![Some(7); 3]);
    assert_eq!(output.claim_count(), 1);

    assert_fixture_json(
        "projection-non-null-literal-before-terminal",
        runtime_context
            .local_snapshot(admission.as_ref())
            .to_fixture(),
        PROJECTION_NON_NULL_LITERAL_BEFORE_TERMINAL,
    );

    drop(outputs);
    drop(sink);
    runtime_context.record_terminal_finished();

    assert_fixture_json(
        "projection-non-null-literal-finished",
        runtime_context
            .local_snapshot(admission.as_ref())
            .to_fixture(),
        PROJECTION_NON_NULL_LITERAL_FINISHED,
    );
}

#[test]
fn direct_null_literal_projection_materializes_nullable_all_null_int32() {
    let input = make_batch(vec![Some(1), Some(2), Some(3)], false);
    let admission = RecordingAdmissionController::unbounded();
    let output = project_batch(
        input.as_ref(),
        &[ProjectionExpr::new("null_literal", Expr::literal(None))],
        &admission,
        "Projection",
    )
    .unwrap();

    assert_eq!(output.num_rows(), input.num_rows());
    assert_eq!(output.schema().field(0).name(), "null_literal");
    assert!(output.schema().field(0).is_nullable());
    assert_eq!(collect_int32(output.column(0)), vec![None, None, None]);
    assert_eq!(
        admission.events(),
        vec![
            AdmissionEvent::ConsumerOpened {
                name: "Projection:null_literal".into(),
                kind: ConsumerKind::ProjectionOutput,
                spillable: false,
            },
            AdmissionEvent::ReserveAdmitted {
                name: "Projection:null_literal".into(),
                bytes: 13,
            },
            AdmissionEvent::ConsumerReleased {
                name: "Projection:null_literal".into(),
                bytes: 13,
            },
        ]
    );
}

#[test]
fn null_literal_projection_preserves_full_claim_without_shrink() {
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
        Arc::new(ProjectionPipe::new(
            "Projection",
            vec![ProjectionExpr::new("null_literal", Expr::literal(None))],
        )),
        Arc::clone(&sink),
        runtime_context.clone(),
    )
    .unwrap();
    assert!(status.is_finished());

    let outputs = sink.batches();
    assert_eq!(outputs.len(), 1);
    let output = &outputs[0];
    assert_eq!(output.batch().schema().field(0).name(), "null_literal");
    assert!(output.batch().schema().field(0).is_nullable());
    assert_eq!(
        collect_int32(output.batch().column(0)),
        vec![None, None, None]
    );
    assert_eq!(output.claim_count(), 1);

    assert_fixture_json(
        "projection-null-literal-before-terminal",
        runtime_context
            .local_snapshot(admission.as_ref())
            .to_fixture(),
        PROJECTION_NULL_LITERAL_BEFORE_TERMINAL,
    );

    drop(outputs);
    drop(sink);
    runtime_context.record_terminal_finished();

    assert_fixture_json(
        "projection-null-literal-finished",
        runtime_context
            .local_snapshot(admission.as_ref())
            .to_fixture(),
        PROJECTION_NULL_LITERAL_FINISHED,
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

    assert_fixture_json(
        "projection-denied",
        runtime_context
            .local_snapshot(admission.as_ref())
            .to_fixture(),
        PROJECTION_DENIED,
    );
}

#[test]
fn add_projection_overflow_is_reported_before_projection_output_is_collected() {
    let input = make_batch(vec![Some(i32::MAX), Some(2), Some(3)], false);
    let admission = Arc::new(RecordingAdmissionController::unbounded());
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
    .expect_err("overflowing add projection should fail");

    assert!(error.to_string().contains("int32 overflow"));
    assert!(sink.batches().is_empty());
    runtime_context.record_terminal_error(error.to_string());

    assert_fixture_json(
        "projection-overflow",
        runtime_context
            .local_snapshot(admission.as_ref())
            .to_fixture(),
        PROJECTION_OVERFLOW,
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

    assert_fixture_json(
        "projection-passthrough-before-terminal",
        runtime_context
            .local_snapshot(admission.as_ref())
            .to_fixture(),
        PROJECTION_PASSTHROUGH_BEFORE_TERMINAL,
    );

    drop(outputs);
    drop(sink);
    runtime_context.record_terminal_finished();

    assert_fixture_json(
        "projection-passthrough-finished",
        runtime_context
            .local_snapshot(admission.as_ref())
            .to_fixture(),
        PROJECTION_PASSTHROUGH_FINISHED,
    );
}

#[test]
fn passthrough_consumer_release_violation_is_reported_after_sink_handoff() {
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
    let claim = runtime_context.new_claim(Arc::clone(&source_consumer));
    let source = Arc::new(StaticRecordBatchSource::new_claimed(
        "Source",
        vec![(Arc::clone(&input), vec![vec![claim]])],
    ));
    let pipe = Arc::new(ProjectionPipe::new(
        "Projection",
        vec![ProjectionExpr::new("a_copy", Expr::column(0))],
    ));

    drive_pipeline_until_sink_handoff(source, pipe, Arc::clone(&sink), runtime_context.clone())
        .unwrap();

    let outputs = sink.batches();
    assert_eq!(outputs.len(), 1);
    assert_eq!(outputs[0].claim_count(), 1);
    assert_eq!(
        collect_int32(outputs[0].batch().column(0)),
        vec![Some(1), Some(2), Some(3)]
    );

    let error = source_consumer
        .release()
        .expect_err("live forwarded consumer release should fail after sink handoff");
    assert!(error
        .to_string()
        .contains("attempted to release the consumer for live claim"));

    drop(outputs);
    drop(sink);
    runtime_context.record_terminal_error(error.to_string());

    assert_fixture_json(
        "projection-passthrough-ownership-violation",
        runtime_context
            .local_snapshot(admission.as_ref())
            .to_fixture(),
        PROJECTION_PASSTHROUGH_OWNERSHIP_VIOLATION,
    );
}

#[test]
fn passthrough_consumer_shrink_violation_is_reported_after_sink_handoff() {
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
    let claim = runtime_context.new_claim(Arc::clone(&source_consumer));
    let source = Arc::new(StaticRecordBatchSource::new_claimed(
        "Source",
        vec![(Arc::clone(&input), vec![vec![claim]])],
    ));
    let pipe = Arc::new(ProjectionPipe::new(
        "Projection",
        vec![ProjectionExpr::new("a_copy", Expr::column(0))],
    ));

    drive_pipeline_until_sink_handoff(source, pipe, Arc::clone(&sink), runtime_context.clone())
        .unwrap();

    let outputs = sink.batches();
    assert_eq!(outputs.len(), 1);
    assert_eq!(outputs[0].claim_count(), 1);
    assert_eq!(
        collect_int32(outputs[0].batch().column(0)),
        vec![Some(1), Some(2), Some(3)]
    );

    let error = source_consumer
        .shrink(1)
        .expect_err("live forwarded consumer shrink should fail after sink handoff");
    assert!(error
        .to_string()
        .contains("attempted to shrink 1 bytes from the consumer for live claim"));

    drop(outputs);
    drop(sink);
    runtime_context.record_terminal_error(error.to_string());

    assert_fixture_json(
        "projection-passthrough-shrink-ownership-violation",
        runtime_context
            .local_snapshot(admission.as_ref())
            .to_fixture(),
        PROJECTION_PASSTHROUGH_SHRINK_OWNERSHIP_VIOLATION,
    );
}

#[test]
fn claimed_source_requires_projection_runtime_context_before_source_emit() {
    let input = make_batch(vec![Some(1), Some(2), Some(3)], false);
    let admission = Arc::new(RecordingAdmissionController::unbounded());
    let runtime_admission: Arc<dyn AdmissionController> = admission.clone();
    let runtime_context = ProjectionRuntimeContext::new(runtime_admission);
    let source_consumer = admission.open(tiforth_kernel::ConsumerSpec::new(
        "ClaimedSource:a",
        ConsumerKind::SourceInput,
        false,
    ));
    source_consumer.try_reserve(12).unwrap();
    let claim = runtime_context.new_claim(source_consumer);
    let source = StaticRecordBatchSource::new_claimed(
        "ClaimedSource",
        vec![(Arc::clone(&input), vec![vec![claim]])],
    );
    let scheduler = SequentialCoroScheduler::default();
    let task_context = scheduler.make_task_context(None);

    let error = source
        .source(&task_context, 0)
        .err()
        .expect("claimed source should require ProjectionRuntimeContext");

    assert!(error
        .to_string()
        .contains("requires ProjectionRuntimeContext"));
    runtime_context.record_terminal_error(error.to_string());

    assert_fixture_json(
        "projection-claimed-source-runtime-context-ownership-violation",
        runtime_context
            .local_snapshot(admission.as_ref())
            .to_fixture(),
        PROJECTION_CLAIMED_SOURCE_RUNTIME_CONTEXT_OWNERSHIP_VIOLATION,
    );
}

#[test]
fn untracked_handoff_violation_is_reported_before_projection_adopts_batch() {
    let input = make_batch(vec![Some(1), Some(2), Some(3)], false);
    let admission = Arc::new(RecordingAdmissionController::unbounded());
    let runtime_admission: Arc<dyn AdmissionController> = admission.clone();
    let runtime_context = ProjectionRuntimeContext::new(runtime_admission);
    let sink = Arc::new(CollectSink::new("Sink"));

    let error = run_pipeline(
        Arc::new(UntrackedRecordBatchSource::new("UntrackedSource", input)),
        Arc::new(ProjectionPipe::new(
            "Projection",
            vec![ProjectionExpr::new("a_copy", Expr::column(0))],
        )),
        Arc::clone(&sink),
        runtime_context.clone(),
    )
    .expect_err("untracked handoff should fail before projection adopts the batch");

    assert!(error
        .to_string()
        .contains("received an untracked batch handoff"));
    assert!(sink.batches().is_empty());
    runtime_context.record_terminal_error(error.to_string());

    assert_fixture_json(
        "projection-untracked-handoff-ownership-violation",
        runtime_context
            .local_snapshot(admission.as_ref())
            .to_fixture(),
        PROJECTION_UNTRACKED_HANDOFF_OWNERSHIP_VIOLATION,
    );
}

#[test]
fn projection_output_can_carry_forwarded_and_computed_claims_together() {
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

    let status = run_pipeline(
        source,
        projection_pipe(),
        Arc::clone(&sink),
        runtime_context.clone(),
    )
    .unwrap();
    assert!(status.is_finished());

    let outputs = sink.batches();
    assert_eq!(outputs.len(), 1);
    let output = &outputs[0];
    assert_eq!(output.claim_count(), 2);
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

    assert_fixture_json(
        "projection-mixed-claims-before-terminal",
        runtime_context
            .local_snapshot(admission.as_ref())
            .to_fixture(),
        PROJECTION_MIXED_CLAIMS_BEFORE_TERMINAL,
    );

    drop(outputs);
    drop(sink);
    runtime_context.record_terminal_finished();

    assert_fixture_json(
        "projection-mixed-claims-finished",
        runtime_context
            .local_snapshot(admission.as_ref())
            .to_fixture(),
        PROJECTION_MIXED_CLAIMS_FINISHED,
    );
}

#[test]
fn projection_cancellation_can_capture_mixed_claim_teardown_after_sink_handoff() {
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

    drive_pipeline_until_sink_handoff(
        source,
        projection_pipe(),
        Arc::clone(&sink),
        runtime_context.clone(),
    )
    .unwrap();

    let outputs = sink.batches();
    assert_eq!(outputs.len(), 1);
    let output = &outputs[0];
    assert_eq!(output.claim_count(), 2);
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

    drop(outputs);
    drop(sink);
    runtime_context.record_terminal_cancelled();

    assert_fixture_json(
        "projection-mixed-claims-cancelled",
        runtime_context
            .local_snapshot(admission.as_ref())
            .to_fixture(),
        PROJECTION_MIXED_CLAIMS_CANCELLED,
    );
}

fn assert_fixture_json(name: &str, actual: LocalExecutionFixture, expected: &str) {
    let actual = serde_json::to_string_pretty(&actual)
        .expect("LocalExecutionFixture JSON serialization should succeed");

    assert_eq!(actual, expected.trim_end(), "fixture mismatch for {name}");
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

fn drive_pipeline_until_sink_handoff(
    source: Arc<dyn SourceOperator<ArrowTypes>>,
    pipe: Arc<dyn PipeOperator<ArrowTypes>>,
    sink: Arc<CollectSink>,
    runtime_context: ProjectionRuntimeContext,
) -> Result<(), ArrowError> {
    let sink_op: Arc<dyn SinkOperator<ArrowTypes>> = sink.clone();

    let pipeline = Pipeline::<ArrowTypes>::new(
        "ProjectionPipeline",
        vec![PipelineChannel::new(source, vec![pipe])],
        sink_op,
    );

    let pipe_runtime = compile(&pipeline, 1).pipelinexes()[0].pipe_exec();
    let scheduler = SequentialCoroScheduler::default();
    let context: Arc<dyn Any + Send + Sync> = Arc::new(runtime_context);
    let task_context = scheduler.make_task_context(Some(context));

    loop {
        if !sink.batches().is_empty() {
            return Ok(());
        }

        match pipe_runtime.step(&task_context, 0)? {
            TaskStatus::Continue | TaskStatus::Yield => {}
            TaskStatus::Blocked(_) => {
                panic!("projection sink-handoff driver does not expect blocked status")
            }
            TaskStatus::Finished => {
                panic!("projection sink-handoff driver finished before sink handoff")
            }
            TaskStatus::Cancelled => {
                panic!("projection sink-handoff driver should not see cancelled before teardown")
            }
        }
    }
}

struct UntrackedRecordBatchSource {
    name: String,
    batch: Mutex<Option<Batch>>,
}

impl UntrackedRecordBatchSource {
    fn new(name: impl Into<String>, batch: Batch) -> Self {
        Self {
            name: name.into(),
            batch: Mutex::new(Some(batch)),
        }
    }
}

impl SourceOperator<ArrowTypes> for UntrackedRecordBatchSource {
    fn name(&self) -> &str {
        &self.name
    }

    fn source(
        &self,
        _ctx: &TaskContext<ArrowTypes>,
        _thread_id: ThreadId,
    ) -> Result<OpOutput<Batch>, ArrowError> {
        Ok(OpOutput::Finished(
            self.batch
                .lock()
                .expect("untracked source batch mutex poisoned")
                .take(),
        ))
    }
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
