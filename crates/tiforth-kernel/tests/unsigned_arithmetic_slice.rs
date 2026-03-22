use std::sync::Arc;

mod support;

use arrow_array::{Array, Int32Array, Int64Array, RecordBatch, UInt32Array, UInt64Array};
use arrow_schema::{DataType, Field, Schema};
use support::{filter_batch, project_batch};
use tiforth_kernel::admission::RecordingAdmissionController;
use tiforth_kernel::expr::Expr;
use tiforth_kernel::filter::FilterPredicate;
use tiforth_kernel::projection::ProjectionExpr;

#[test]
fn uint64_column_passthrough_preserves_values_and_nullability() {
    let input = make_single_uint64_batch(vec![Some(0), Some(7), Some(42)], false);
    let admission = RecordingAdmissionController::unbounded();

    let output = project_batch(
        &input,
        &[ProjectionExpr::new("u_copy", Expr::column(0))],
        &admission,
        "Projection",
    )
    .expect("uint64 projection should succeed");

    assert_eq!(output.num_rows(), input.num_rows());
    assert_eq!(output.schema().field(0).name(), "u_copy");
    assert_eq!(output.schema().field(0).data_type(), &DataType::UInt64);
    assert!(!output.schema().field(0).is_nullable());
    assert_eq!(
        collect_uint64(output.column(0)),
        vec![Some(0), Some(7), Some(42)]
    );
}

#[test]
fn uint64_literal_projection_repeats_stable_value() {
    let input = make_single_uint64_batch(vec![Some(0), Some(7), Some(42)], false);
    let admission = RecordingAdmissionController::unbounded();

    let output = project_batch(
        &input,
        &[ProjectionExpr::new("seven", Expr::literal_uint64(Some(7)))],
        &admission,
        "Projection",
    )
    .expect("uint64 literal projection should succeed");

    assert_eq!(output.num_rows(), input.num_rows());
    assert_eq!(output.schema().field(0).data_type(), &DataType::UInt64);
    assert!(!output.schema().field(0).is_nullable());
    assert_eq!(
        collect_uint64(output.column(0)),
        vec![Some(7), Some(7), Some(7)]
    );
}

#[test]
fn uint64_add_no_overflow_preserves_uint64_results() {
    let input = make_uint64_pair_batch(
        vec![Some(1), Some(3), Some(10)],
        false,
        vec![Some(2), Some(4), Some(20)],
        false,
    );
    let admission = RecordingAdmissionController::unbounded();

    let output = project_batch(
        &input,
        &[ProjectionExpr::new(
            "sum",
            Expr::add_uint64(Expr::column(0), Expr::column(1)),
        )],
        &admission,
        "Projection",
    )
    .expect("uint64 add projection should succeed");

    assert_eq!(output.num_rows(), input.num_rows());
    assert_eq!(output.schema().field(0).data_type(), &DataType::UInt64);
    assert!(!output.schema().field(0).is_nullable());
    assert_eq!(
        collect_uint64(output.column(0)),
        vec![Some(3), Some(7), Some(30)]
    );
}

#[test]
fn uint64_add_null_propagation_preserves_null_rows() {
    let input = make_uint64_pair_batch(
        vec![None, Some(2), Some(3)],
        true,
        vec![Some(1), None, Some(4)],
        true,
    );
    let admission = RecordingAdmissionController::unbounded();

    let output = project_batch(
        &input,
        &[ProjectionExpr::new(
            "sum",
            Expr::add_uint64(Expr::column(0), Expr::column(1)),
        )],
        &admission,
        "Projection",
    )
    .expect("nullable uint64 add projection should succeed");

    assert_eq!(output.num_rows(), input.num_rows());
    assert_eq!(output.schema().field(0).data_type(), &DataType::UInt64);
    assert!(output.schema().field(0).is_nullable());
    assert_eq!(collect_uint64(output.column(0)), vec![None, None, Some(7)]);
}

#[test]
fn uint64_add_overflow_reports_execution_error() {
    let input = make_uint64_pair_batch(vec![Some(u64::MAX)], false, vec![Some(1)], false);
    let admission = RecordingAdmissionController::unbounded();

    let error = project_batch(
        &input,
        &[ProjectionExpr::new(
            "sum",
            Expr::add_uint64(Expr::column(0), Expr::column(1)),
        )],
        &admission,
        "Projection",
    )
    .expect_err("uint64 overflow should fail");

    assert!(error
        .to_string()
        .contains("uint64 overflow in Projection:sum at row 0"));
}

#[test]
fn uint64_predicate_mixed_keep_drop_preserves_row_order_and_full_row_values() {
    let input = make_uint64_and_int32_batch(
        vec![None, Some(5), None, Some(9)],
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
    .expect("uint64 filter should succeed");

    assert_eq!(output.schema().fields(), input.schema().fields());
    assert_eq!(output.num_rows(), 2);
    assert_eq!(collect_uint64(output.column(0)), vec![Some(5), Some(9)]);
    assert_eq!(collect_int32(output.column(1)), vec![Some(20), Some(40)]);
}

#[test]
fn mixed_signed_and_unsigned_arithmetic_reports_execution_error() {
    let input = make_mixed_int64_uint64_batch(vec![Some(1)], false, vec![Some(1)], false);
    let admission = RecordingAdmissionController::unbounded();

    let error = project_batch(
        &input,
        &[ProjectionExpr::new(
            "sum",
            Expr::add_uint64(Expr::column(0), Expr::column(1)),
        )],
        &admission,
        "Projection",
    )
    .expect_err("mixed signed and unsigned arithmetic should fail");

    assert!(error
        .to_string()
        .contains("mixed signed and unsigned arithmetic is unsupported"));
    assert!(error.to_string().contains("got Int64"));
}

#[test]
fn unsupported_unsigned_family_reports_execution_errors_in_projection_and_predicate_paths() {
    let input = make_single_uint32_batch(vec![Some(1), Some(2), Some(3)], false);
    let admission = RecordingAdmissionController::unbounded();

    let projection_error = project_batch(
        &input,
        &[ProjectionExpr::new("u32_copy", Expr::column(0))],
        &admission,
        "Projection",
    )
    .expect_err("uint32 projection should fail in first unsigned slice");
    assert!(projection_error
        .to_string()
        .contains("unsupported unsigned expression input at column 0"));
    assert!(projection_error
        .to_string()
        .contains("first unsigned slice supports UInt64 only"));

    let predicate_error = filter_batch(
        &input,
        &FilterPredicate::is_not_null_column(0),
        &admission,
        "Filter",
    )
    .expect_err("uint32 predicate should fail in first unsigned slice");
    assert!(predicate_error
        .to_string()
        .contains("unsupported unsigned predicate input at column 0"));
    assert!(predicate_error
        .to_string()
        .contains("first unsigned slice supports UInt64 only"));
}

#[test]
fn uint64_projection_reports_missing_column_error() {
    let input = make_single_uint64_batch(vec![Some(0), Some(7), Some(42)], false);
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
fn uint64_predicate_reports_missing_column_error() {
    let input = make_single_uint64_batch(vec![Some(0), Some(7), Some(42)], false);
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

fn make_single_uint64_batch(values: Vec<Option<u64>>, nullable: bool) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "u",
        DataType::UInt64,
        nullable,
    )]));
    let array = UInt64Array::from(values);
    RecordBatch::try_new(schema, vec![Arc::new(array)]).expect("uint64 batch should build")
}

fn make_uint64_pair_batch(
    lhs_values: Vec<Option<u64>>,
    lhs_nullable: bool,
    rhs_values: Vec<Option<u64>>,
    rhs_nullable: bool,
) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("lhs", DataType::UInt64, lhs_nullable),
        Field::new("rhs", DataType::UInt64, rhs_nullable),
    ]));
    let lhs = UInt64Array::from(lhs_values);
    let rhs = UInt64Array::from(rhs_values);
    RecordBatch::try_new(schema, vec![Arc::new(lhs), Arc::new(rhs)])
        .expect("uint64 pair batch should build")
}

fn make_uint64_and_int32_batch(
    uint64_values: Vec<Option<u64>>,
    uint64_nullable: bool,
    int32_values: Vec<Option<i32>>,
    int32_nullable: bool,
) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("u", DataType::UInt64, uint64_nullable),
        Field::new("i", DataType::Int32, int32_nullable),
    ]));
    let uint64_array = UInt64Array::from(uint64_values);
    let int32_array = Int32Array::from(int32_values);
    RecordBatch::try_new(schema, vec![Arc::new(uint64_array), Arc::new(int32_array)])
        .expect("uint64/int32 batch should build")
}

fn make_mixed_int64_uint64_batch(
    int64_values: Vec<Option<i64>>,
    int64_nullable: bool,
    uint64_values: Vec<Option<u64>>,
    uint64_nullable: bool,
) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("s", DataType::Int64, int64_nullable),
        Field::new("u", DataType::UInt64, uint64_nullable),
    ]));
    let int64_array = Int64Array::from(int64_values);
    let uint64_array = UInt64Array::from(uint64_values);
    RecordBatch::try_new(schema, vec![Arc::new(int64_array), Arc::new(uint64_array)])
        .expect("mixed int64/uint64 batch should build")
}

fn make_single_uint32_batch(values: Vec<Option<u32>>, nullable: bool) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "u32",
        DataType::UInt32,
        nullable,
    )]));
    let array = UInt32Array::from(values);
    RecordBatch::try_new(schema, vec![Arc::new(array)]).expect("uint32 batch should build")
}

fn collect_uint64(array: &dyn Array) -> Vec<Option<u64>> {
    let uint64 = array
        .as_any()
        .downcast_ref::<UInt64Array>()
        .expect("array should be UInt64");
    (0..uint64.len())
        .map(|index| {
            if uint64.is_null(index) {
                None
            } else {
                Some(uint64.value(index))
            }
        })
        .collect()
}

fn collect_int32(array: &dyn Array) -> Vec<Option<i32>> {
    let int32 = array
        .as_any()
        .downcast_ref::<Int32Array>()
        .expect("array should be Int32");
    (0..int32.len())
        .map(|index| {
            if int32.is_null(index) {
                None
            } else {
                Some(int32.value(index))
            }
        })
        .collect()
}
