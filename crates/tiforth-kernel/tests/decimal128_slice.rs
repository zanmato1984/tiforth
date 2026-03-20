use std::sync::Arc;

use arrow_array::{new_empty_array, Array, ArrayRef, Decimal128Array, Int32Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use tiforth_kernel::admission::RecordingAdmissionController;
use tiforth_kernel::expr::Expr;
use tiforth_kernel::filter::{filter_batch, FilterPredicate};
use tiforth_kernel::projection::{project_batch, ProjectionExpr};

#[test]
fn decimal128_column_passthrough_preserves_values_and_metadata() {
    let input = make_single_decimal128_batch(vec![Some(101), Some(202), Some(303)], false, 10, 2);
    let admission = RecordingAdmissionController::unbounded();

    let output = project_batch(
        &input,
        &[ProjectionExpr::new("d_copy", Expr::column(0))],
        &admission,
        "Projection",
    )
    .expect("decimal128 projection should succeed");

    assert_eq!(output.num_rows(), input.num_rows());
    assert_eq!(output.schema().field(0).name(), "d_copy");
    assert_eq!(
        output.schema().field(0).data_type(),
        &DataType::Decimal128(10, 2)
    );
    assert!(!output.schema().field(0).is_nullable());
    assert_eq!(
        collect_decimal128(output.column(0)),
        vec![Some(101), Some(202), Some(303)]
    );
}

#[test]
fn decimal128_nullable_passthrough_preserves_null_positions() {
    let input = make_single_decimal128_batch(vec![Some(101), None, Some(303), None], true, 10, 2);
    let admission = RecordingAdmissionController::unbounded();

    let output = project_batch(
        &input,
        &[ProjectionExpr::new("d_copy", Expr::column(0))],
        &admission,
        "Projection",
    )
    .expect("nullable decimal128 projection should succeed");

    assert_eq!(output.num_rows(), input.num_rows());
    assert_eq!(
        output.schema().field(0).data_type(),
        &DataType::Decimal128(10, 2)
    );
    assert!(output.schema().field(0).is_nullable());
    assert_eq!(
        collect_decimal128(output.column(0)),
        vec![Some(101), None, Some(303), None]
    );
}

#[test]
fn decimal128_predicate_keeps_all_rows_when_no_nulls() {
    let input = make_single_decimal128_batch(vec![Some(101), Some(202), Some(303)], false, 10, 2);
    let admission = RecordingAdmissionController::unbounded();

    let output = filter_batch(
        &input,
        &FilterPredicate::is_not_null_column(0),
        &admission,
        "Filter",
    )
    .expect("decimal128 filter should succeed");

    assert_eq!(output.num_rows(), input.num_rows());
    assert_eq!(
        collect_decimal128(output.column(0)),
        vec![Some(101), Some(202), Some(303)]
    );
}

#[test]
fn decimal128_predicate_drops_all_rows_when_all_values_are_null() {
    let input = make_single_decimal128_batch(vec![None, None, None], true, 10, 2);
    let admission = RecordingAdmissionController::unbounded();

    let output = filter_batch(
        &input,
        &FilterPredicate::is_not_null_column(0),
        &admission,
        "Filter",
    )
    .expect("all-null decimal128 filter should succeed");

    assert_eq!(output.num_rows(), 0);
    assert_eq!(output.schema().fields(), input.schema().fields());
}

#[test]
fn decimal128_predicate_mixed_keep_drop_preserves_row_order_and_full_row_values() {
    let input = make_decimal128_and_int32_batch(
        vec![Some(101), None, Some(303), None],
        true,
        10,
        2,
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
    .expect("mixed decimal128 filter should succeed");

    assert_eq!(output.schema().fields(), input.schema().fields());
    assert_eq!(output.num_rows(), 2);
    assert_eq!(
        collect_decimal128(output.column(0)),
        vec![Some(101), Some(303)]
    );
    assert_eq!(collect_int32(output.column(1)), vec![Some(10), Some(30)]);
}

#[test]
fn decimal128_projection_reports_missing_column_error() {
    let input = make_single_decimal128_batch(vec![Some(101), Some(202), Some(303)], false, 10, 2);
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
fn decimal128_predicate_reports_missing_column_error() {
    let input = make_single_decimal128_batch(vec![Some(101), Some(202), Some(303)], false, 10, 2);
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
fn unsupported_decimal_family_reports_execution_errors_in_projection_and_predicate_paths() {
    let input = make_empty_decimal256_batch();
    let admission = RecordingAdmissionController::unbounded();

    let projection_error = project_batch(
        &input,
        &[ProjectionExpr::new("d256_copy", Expr::column(0))],
        &admission,
        "Projection",
    )
    .expect_err("decimal256 projection should fail in first decimal slice");
    assert!(projection_error
        .to_string()
        .contains("unsupported decimal expression input at column 0"));
    assert!(projection_error
        .to_string()
        .contains("first decimal slice supports Decimal128 only"));

    let predicate_error = filter_batch(
        &input,
        &FilterPredicate::is_not_null_column(0),
        &admission,
        "Filter",
    )
    .expect_err("decimal256 predicate input should fail in first decimal slice");
    assert!(predicate_error
        .to_string()
        .contains("unsupported decimal predicate input at column 0"));
    assert!(predicate_error
        .to_string()
        .contains("first decimal slice supports Decimal128 only"));
}

#[test]
fn invalid_decimal128_metadata_reports_execution_errors_in_projection_and_predicate_paths() {
    let input = make_empty_invalid_decimal128_batch();
    let admission = RecordingAdmissionController::unbounded();

    let projection_error = project_batch(
        &input,
        &[ProjectionExpr::new("bad_copy", Expr::column(0))],
        &admission,
        "Projection",
    )
    .expect_err("invalid decimal128 metadata should fail in projection");
    assert!(projection_error.to_string().contains(
        "invalid decimal128 expression input metadata at column 0: precision 10, scale 12; expected precision 1..=38 and scale 0..=precision"
    ));

    let predicate_error = filter_batch(
        &input,
        &FilterPredicate::is_not_null_column(0),
        &admission,
        "Filter",
    )
    .expect_err("invalid decimal128 metadata should fail in predicate");
    assert!(predicate_error.to_string().contains(
        "invalid decimal128 predicate input metadata at column 0: precision 10, scale 12; expected precision 1..=38 and scale 0..=precision"
    ));
}

fn make_single_decimal128_batch(
    values: Vec<Option<i128>>,
    nullable: bool,
    precision: u8,
    scale: i8,
) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "d",
        DataType::Decimal128(precision, scale),
        nullable,
    )]));
    let values = Decimal128Array::from(values)
        .with_precision_and_scale(precision, scale)
        .expect("decimal128 precision/scale should be valid");
    let values: ArrayRef = Arc::new(values);
    RecordBatch::try_new(schema, vec![values]).expect("decimal128 batch should build")
}

fn make_decimal128_and_int32_batch(
    decimal_values: Vec<Option<i128>>,
    decimal_nullable: bool,
    precision: u8,
    scale: i8,
    int_values: Vec<Option<i32>>,
    int_nullable: bool,
) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new(
            "d",
            DataType::Decimal128(precision, scale),
            decimal_nullable,
        ),
        Field::new("x", DataType::Int32, int_nullable),
    ]));
    let decimals = Decimal128Array::from(decimal_values)
        .with_precision_and_scale(precision, scale)
        .expect("decimal128 precision/scale should be valid");
    let decimals: ArrayRef = Arc::new(decimals);
    let ints: ArrayRef = Arc::new(Int32Array::from(int_values));
    RecordBatch::try_new(schema, vec![decimals, ints]).expect("mixed batch should build")
}

fn make_empty_decimal256_batch() -> RecordBatch {
    let data_type = DataType::Decimal256(40, 4);
    let schema = Arc::new(Schema::new(vec![Field::new(
        "d256",
        data_type.clone(),
        false,
    )]));
    let values = new_empty_array(&data_type);
    RecordBatch::try_new(schema, vec![values]).expect("decimal256 batch should build")
}

fn make_empty_invalid_decimal128_batch() -> RecordBatch {
    let data_type = DataType::Decimal128(10, 12);
    let schema = Arc::new(Schema::new(vec![Field::new(
        "d_bad",
        data_type.clone(),
        true,
    )]));
    let values = new_empty_array(&data_type);
    RecordBatch::try_new(schema, vec![values]).expect("invalid decimal metadata batch should build")
}

fn collect_decimal128(array: &ArrayRef) -> Vec<Option<i128>> {
    let values = array
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .expect("expected Decimal128Array output");
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
