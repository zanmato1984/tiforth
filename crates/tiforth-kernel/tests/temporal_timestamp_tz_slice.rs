use std::sync::Arc;

mod support;

use arrow_array::{
    Array, ArrayRef, Int32Array, RecordBatch, TimestampMicrosecondArray, TimestampMillisecondArray,
};
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use support::{filter_batch, project_batch};
use tiforth_kernel::admission::RecordingAdmissionController;
use tiforth_kernel::expr::Expr;
use tiforth_kernel::filter::FilterPredicate;
use tiforth_kernel::projection::ProjectionExpr;

#[test]
fn timestamp_tz_us_column_passthrough_preserves_epoch_values_and_non_nullable_field() {
    let input = make_single_timestamp_tz_us_batch(
        vec![Some(0), Some(1_000_000), Some(2_000_000)],
        false,
        "+00:00",
    );
    let admission = RecordingAdmissionController::unbounded();

    let output = project_batch(
        &input,
        &[ProjectionExpr::new("ts_copy", Expr::column(0))],
        &admission,
        "Projection",
    )
    .expect("timestamp_tz(us) projection should succeed");

    assert_eq!(output.num_rows(), input.num_rows());
    assert_eq!(output.schema().field(0).name(), "ts_copy");
    assert_eq!(
        output.schema().field(0).data_type(),
        &timestamp_tz_us_type("+00:00")
    );
    assert!(!output.schema().field(0).is_nullable());
    assert_eq!(
        collect_timestamp_microseconds(output.column(0)),
        vec![Some(0), Some(1_000_000), Some(2_000_000)]
    );
}

#[test]
fn timestamp_tz_us_nullable_passthrough_preserves_null_positions() {
    let input = make_single_timestamp_tz_us_batch(
        vec![Some(0), None, Some(2_000_000), None],
        true,
        "+00:00",
    );
    let admission = RecordingAdmissionController::unbounded();

    let output = project_batch(
        &input,
        &[ProjectionExpr::new("ts_copy", Expr::column(0))],
        &admission,
        "Projection",
    )
    .expect("nullable timestamp_tz(us) projection should succeed");

    assert_eq!(output.num_rows(), input.num_rows());
    assert_eq!(
        output.schema().field(0).data_type(),
        &timestamp_tz_us_type("+00:00")
    );
    assert!(output.schema().field(0).is_nullable());
    assert_eq!(
        collect_timestamp_microseconds(output.column(0)),
        vec![Some(0), None, Some(2_000_000), None]
    );
}

#[test]
fn timestamp_tz_us_predicate_keeps_all_rows_when_no_nulls() {
    let input = make_single_timestamp_tz_us_batch(
        vec![Some(0), Some(1_000_000), Some(2_000_000)],
        false,
        "+00:00",
    );
    let admission = RecordingAdmissionController::unbounded();

    let output = filter_batch(
        &input,
        &FilterPredicate::is_not_null_column(0),
        &admission,
        "Filter",
    )
    .expect("timestamp_tz(us) filter should succeed");

    assert_eq!(output.num_rows(), input.num_rows());
    assert_eq!(
        collect_timestamp_microseconds(output.column(0)),
        vec![Some(0), Some(1_000_000), Some(2_000_000)]
    );
}

#[test]
fn timestamp_tz_us_predicate_drops_all_rows_when_all_values_are_null() {
    let input = make_single_timestamp_tz_us_batch(vec![None, None, None], true, "+00:00");
    let admission = RecordingAdmissionController::unbounded();

    let output = filter_batch(
        &input,
        &FilterPredicate::is_not_null_column(0),
        &admission,
        "Filter",
    )
    .expect("all-null timestamp_tz(us) filter should succeed");

    assert_eq!(output.num_rows(), 0);
    assert_eq!(output.schema().fields(), input.schema().fields());
}

#[test]
fn timestamp_tz_us_predicate_mixed_keep_drop_preserves_row_order_and_full_row_values() {
    let input = make_timestamp_tz_us_and_int32_batch(
        vec![Some(0), None, Some(2_000_000), None],
        true,
        vec![Some(10), Some(20), Some(30), Some(40)],
        false,
        "+00:00",
    );
    let admission = RecordingAdmissionController::unbounded();

    let output = filter_batch(
        &input,
        &FilterPredicate::is_not_null_column(0),
        &admission,
        "Filter",
    )
    .expect("mixed timestamp_tz(us) filter should succeed");

    assert_eq!(output.schema().fields(), input.schema().fields());
    assert_eq!(output.num_rows(), 2);
    assert_eq!(
        collect_timestamp_microseconds(output.column(0)),
        vec![Some(0), Some(2_000_000)]
    );
    assert_eq!(collect_int32(output.column(1)), vec![Some(10), Some(30)]);
}

#[test]
fn timestamp_tz_us_projection_reports_missing_column_error() {
    let input = make_single_timestamp_tz_us_batch(
        vec![Some(0), Some(1_000_000), Some(2_000_000)],
        false,
        "+00:00",
    );
    let admission = RecordingAdmissionController::unbounded();

    let error = project_batch(
        &input,
        &[ProjectionExpr::new("missing", Expr::column(1))],
        &admission,
        "Projection",
    )
    .expect_err("projection missing-column should fail");

    assert!(error
        .to_string()
        .contains("missing input column at index 1"));
}

#[test]
fn timestamp_tz_us_predicate_reports_missing_column_error() {
    let input = make_single_timestamp_tz_us_batch(
        vec![Some(0), Some(1_000_000), Some(2_000_000)],
        false,
        "+00:00",
    );
    let admission = RecordingAdmissionController::unbounded();

    let error = filter_batch(
        &input,
        &FilterPredicate::is_not_null_column(1),
        &admission,
        "Filter",
    )
    .expect_err("predicate missing-column should fail");

    assert!(error
        .to_string()
        .contains("missing input column at index 1"));
}

#[test]
fn unsupported_timestamp_without_timezone_reports_execution_errors_in_projection_and_predicate_paths(
) {
    let input = make_single_timestamp_microsecond_batch(
        vec![Some(0), Some(1_000_000), Some(2_000_000)],
        false,
    );
    let admission = RecordingAdmissionController::unbounded();

    let projection_error = project_batch(
        &input,
        &[ProjectionExpr::new("ts_copy", Expr::column(0))],
        &admission,
        "Projection",
    )
    .expect_err("timestamp without timezone projection should fail");
    assert!(projection_error.to_string().contains(
        "unsupported temporal expression input at column 0, got Timestamp(Microsecond, None)"
    ));
    assert!(projection_error.to_string().contains(
        "first temporal slices support Date32 and timezone-aware Timestamp(Microsecond, <tz>) only"
    ));

    let predicate_error = filter_batch(
        &input,
        &FilterPredicate::is_not_null_column(0),
        &admission,
        "Filter",
    )
    .expect_err("timestamp without timezone predicate input should fail");
    assert!(predicate_error.to_string().contains(
        "unsupported temporal predicate input at column 0, got Timestamp(Microsecond, None)"
    ));
    assert!(predicate_error.to_string().contains(
        "first temporal slices support Date32 and timezone-aware Timestamp(Microsecond, <tz>) only"
    ));
}

#[test]
fn unsupported_timestamp_unit_reports_execution_errors_in_projection_and_predicate_paths() {
    let input = make_single_timestamp_millisecond_tz_batch(
        vec![Some(0), Some(1_000), Some(2_000)],
        false,
        "+00:00",
    );
    let admission = RecordingAdmissionController::unbounded();

    let projection_error = project_batch(
        &input,
        &[ProjectionExpr::new("ts_copy", Expr::column(0))],
        &admission,
        "Projection",
    )
    .expect_err("timestamp_tz(ms) projection should fail in first timestamp_tz(us) slice");
    assert!(projection_error
        .to_string()
        .contains("unsupported temporal expression input at column 0, got Timestamp(Millisecond"));
    assert!(projection_error.to_string().contains(
        "first temporal slices support Date32 and timezone-aware Timestamp(Microsecond, <tz>) only"
    ));

    let predicate_error = filter_batch(
        &input,
        &FilterPredicate::is_not_null_column(0),
        &admission,
        "Filter",
    )
    .expect_err("timestamp_tz(ms) predicate input should fail in first timestamp_tz(us) slice");
    assert!(predicate_error
        .to_string()
        .contains("unsupported temporal predicate input at column 0, got Timestamp(Millisecond"));
    assert!(predicate_error.to_string().contains(
        "first temporal slices support Date32 and timezone-aware Timestamp(Microsecond, <tz>) only"
    ));
}

fn timestamp_tz_us_type(timezone: &str) -> DataType {
    DataType::Timestamp(TimeUnit::Microsecond, Some(timezone.into()))
}

fn make_single_timestamp_tz_us_batch(
    values: Vec<Option<i64>>,
    nullable: bool,
    timezone: &str,
) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "ts",
        timestamp_tz_us_type(timezone),
        nullable,
    )]));
    let values: ArrayRef =
        Arc::new(TimestampMicrosecondArray::from(values).with_timezone(timezone));
    RecordBatch::try_new(schema, vec![values]).expect("timestamp_tz(us) batch should build")
}

fn make_single_timestamp_microsecond_batch(
    values: Vec<Option<i64>>,
    nullable: bool,
) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "ts",
        DataType::Timestamp(TimeUnit::Microsecond, None),
        nullable,
    )]));
    let values: ArrayRef = Arc::new(TimestampMicrosecondArray::from(values));
    RecordBatch::try_new(schema, vec![values]).expect("timestamp(us) batch should build")
}

fn make_single_timestamp_millisecond_tz_batch(
    values: Vec<Option<i64>>,
    nullable: bool,
    timezone: &str,
) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "ts",
        DataType::Timestamp(TimeUnit::Millisecond, Some(timezone.into())),
        nullable,
    )]));
    let values: ArrayRef =
        Arc::new(TimestampMillisecondArray::from(values).with_timezone(timezone));
    RecordBatch::try_new(schema, vec![values]).expect("timestamp_tz(ms) batch should build")
}

fn make_timestamp_tz_us_and_int32_batch(
    timestamp_values: Vec<Option<i64>>,
    timestamp_nullable: bool,
    int_values: Vec<Option<i32>>,
    int_nullable: bool,
    timezone: &str,
) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("ts", timestamp_tz_us_type(timezone), timestamp_nullable),
        Field::new("x", DataType::Int32, int_nullable),
    ]));
    let timestamps: ArrayRef =
        Arc::new(TimestampMicrosecondArray::from(timestamp_values).with_timezone(timezone));
    let ints: ArrayRef = Arc::new(Int32Array::from(int_values));
    RecordBatch::try_new(schema, vec![timestamps, ints]).expect("mixed batch should build")
}

fn collect_timestamp_microseconds(array: &ArrayRef) -> Vec<Option<i64>> {
    let values = array
        .as_any()
        .downcast_ref::<TimestampMicrosecondArray>()
        .expect("expected TimestampMicrosecondArray output");
    (0..values.len())
        .map(|index| {
            values
                .is_null(index)
                .then_some(())
                .map_or(Some(values.value(index)), |_| None)
        })
        .collect()
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
