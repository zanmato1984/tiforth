use std::sync::Arc;

use arrow_array::{Array, ArrayRef, Date32Array, Int32Array, RecordBatch, TimestampSecondArray};
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use tiforth_kernel::admission::RecordingAdmissionController;
use tiforth_kernel::expr::Expr;
use tiforth_kernel::filter::{filter_batch, FilterPredicate};
use tiforth_kernel::projection::{project_batch, ProjectionExpr};

#[test]
fn date32_column_passthrough_preserves_day_values_and_non_nullable_field() {
    let input = make_single_date32_batch(vec![Some(0), Some(1), Some(2)], false);
    let admission = RecordingAdmissionController::unbounded();

    let output = project_batch(
        &input,
        &[ProjectionExpr::new("d_copy", Expr::column(0))],
        &admission,
        "Projection",
    )
    .expect("date32 projection should succeed");

    assert_eq!(output.num_rows(), input.num_rows());
    assert_eq!(output.schema().field(0).name(), "d_copy");
    assert_eq!(output.schema().field(0).data_type(), &DataType::Date32);
    assert!(!output.schema().field(0).is_nullable());
    assert_eq!(collect_date32(output.column(0)), vec![Some(0), Some(1), Some(2)]);
}

#[test]
fn date32_nullable_passthrough_preserves_null_positions() {
    let input = make_single_date32_batch(vec![Some(0), None, Some(2), None], true);
    let admission = RecordingAdmissionController::unbounded();

    let output = project_batch(
        &input,
        &[ProjectionExpr::new("d_copy", Expr::column(0))],
        &admission,
        "Projection",
    )
    .expect("nullable date32 projection should succeed");

    assert_eq!(output.num_rows(), input.num_rows());
    assert_eq!(output.schema().field(0).data_type(), &DataType::Date32);
    assert!(output.schema().field(0).is_nullable());
    assert_eq!(
        collect_date32(output.column(0)),
        vec![Some(0), None, Some(2), None]
    );
}

#[test]
fn date32_predicate_keeps_all_rows_when_no_nulls() {
    let input = make_single_date32_batch(vec![Some(0), Some(1), Some(2)], false);
    let admission = RecordingAdmissionController::unbounded();

    let output = filter_batch(
        &input,
        &FilterPredicate::is_not_null_column(0),
        &admission,
        "Filter",
    )
    .expect("date32 filter should succeed");

    assert_eq!(output.num_rows(), input.num_rows());
    assert_eq!(collect_date32(output.column(0)), vec![Some(0), Some(1), Some(2)]);
}

#[test]
fn date32_predicate_drops_all_rows_when_all_values_are_null() {
    let input = make_single_date32_batch(vec![None, None, None], true);
    let admission = RecordingAdmissionController::unbounded();

    let output = filter_batch(
        &input,
        &FilterPredicate::is_not_null_column(0),
        &admission,
        "Filter",
    )
    .expect("all-null date32 filter should succeed");

    assert_eq!(output.num_rows(), 0);
    assert_eq!(output.schema().fields(), input.schema().fields());
}

#[test]
fn date32_predicate_mixed_keep_drop_preserves_row_order_and_full_row_values() {
    let input = make_date32_and_int32_batch(
        vec![Some(0), None, Some(2), None],
        true,
        vec![Some(10), Some(20), Some(30), Some(40)],
        false,
    );
    let admission = RecordingAdmissionController::unbounded();

    let output = filter_batch(
        &input,
        &FilterPredicate::is_not_null_column(0),
        &admission,
        "Filter",
    )
    .expect("mixed date32 filter should succeed");

    assert_eq!(output.schema().fields(), input.schema().fields());
    assert_eq!(output.num_rows(), 2);
    assert_eq!(collect_date32(output.column(0)), vec![Some(0), Some(2)]);
    assert_eq!(collect_int32(output.column(1)), vec![Some(10), Some(30)]);
}

#[test]
fn date32_projection_reports_missing_column_error() {
    let input = make_single_date32_batch(vec![Some(0), Some(1), Some(2)], false);
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
fn date32_predicate_reports_missing_column_error() {
    let input = make_single_date32_batch(vec![Some(0), Some(1), Some(2)], false);
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
fn unsupported_temporal_family_reports_execution_errors_in_projection_and_predicate_paths() {
    let input = make_single_timestamp_second_batch(vec![Some(0), Some(1), Some(2)], false);
    let admission = RecordingAdmissionController::unbounded();

    let projection_error = project_batch(
        &input,
        &[ProjectionExpr::new("ts_copy", Expr::column(0))],
        &admission,
        "Projection",
    )
    .expect_err("timestamp projection should fail in first temporal slice");
    assert!(projection_error.to_string().contains(
        "unsupported temporal expression input at column 0, got Timestamp(Second, None); first temporal slice supports Date32 only"
    ));

    let predicate_error = filter_batch(
        &input,
        &FilterPredicate::is_not_null_column(0),
        &admission,
        "Filter",
    )
    .expect_err("timestamp predicate input should fail in first temporal slice");
    assert!(predicate_error.to_string().contains(
        "unsupported temporal predicate input at column 0, got Timestamp(Second, None); first temporal slice supports Date32 only"
    ));
}

fn make_single_date32_batch(values: Vec<Option<i32>>, nullable: bool) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "d",
        DataType::Date32,
        nullable,
    )]));
    let values: ArrayRef = Arc::new(Date32Array::from(values));
    RecordBatch::try_new(schema, vec![values]).expect("date32 batch should build")
}

fn make_date32_and_int32_batch(
    date_values: Vec<Option<i32>>,
    date_nullable: bool,
    int_values: Vec<Option<i32>>,
    int_nullable: bool,
) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("d", DataType::Date32, date_nullable),
        Field::new("x", DataType::Int32, int_nullable),
    ]));
    let dates: ArrayRef = Arc::new(Date32Array::from(date_values));
    let ints: ArrayRef = Arc::new(Int32Array::from(int_values));
    RecordBatch::try_new(schema, vec![dates, ints]).expect("mixed batch should build")
}

fn make_single_timestamp_second_batch(values: Vec<Option<i64>>, nullable: bool) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "ts",
        DataType::Timestamp(TimeUnit::Second, None),
        nullable,
    )]));
    let values: ArrayRef = Arc::new(TimestampSecondArray::from(values));
    RecordBatch::try_new(schema, vec![values]).expect("timestamp batch should build")
}

fn collect_date32(array: &ArrayRef) -> Vec<Option<i32>> {
    let values = array
        .as_any()
        .downcast_ref::<Date32Array>()
        .expect("expected Date32Array output");
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
