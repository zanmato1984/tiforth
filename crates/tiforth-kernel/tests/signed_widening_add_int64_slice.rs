use std::sync::Arc;

mod support;

use arrow_array::{Array, Int32Array, Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use support::project_batch;
use tiforth_kernel::admission::RecordingAdmissionController;
use tiforth_kernel::expr::Expr;
use tiforth_kernel::projection::ProjectionExpr;

#[test]
fn int64_column_passthrough_preserves_values_and_nullability() {
    let input = make_single_int64_batch(vec![Some(-7), Some(0), Some(42)], false);
    let admission = RecordingAdmissionController::unbounded();

    let output = project_batch(
        &input,
        &[ProjectionExpr::new("s64_copy", Expr::column(0))],
        &admission,
        "Projection",
    )
    .expect("int64 projection should succeed");

    assert_eq!(output.num_rows(), input.num_rows());
    assert_eq!(output.schema().field(0).name(), "s64_copy");
    assert_eq!(output.schema().field(0).data_type(), &DataType::Int64);
    assert!(!output.schema().field(0).is_nullable());
    assert_eq!(
        collect_int64(output.column(0)),
        vec![Some(-7), Some(0), Some(42)]
    );
}

#[test]
fn int64_add_no_overflow_preserves_int64_results() {
    let input = make_int64_pair_batch(
        vec![Some(1), Some(-3), Some(10)],
        false,
        vec![Some(2), Some(4), Some(20)],
        false,
    );
    let admission = RecordingAdmissionController::unbounded();

    let output = project_batch(
        &input,
        &[ProjectionExpr::new(
            "sum",
            Expr::add_int64(Expr::column(0), Expr::column(1)),
        )],
        &admission,
        "Projection",
    )
    .expect("int64 add projection should succeed");

    assert_eq!(output.num_rows(), input.num_rows());
    assert_eq!(output.schema().field(0).data_type(), &DataType::Int64);
    assert!(!output.schema().field(0).is_nullable());
    assert_eq!(
        collect_int64(output.column(0)),
        vec![Some(3), Some(1), Some(30)]
    );
}

#[test]
fn int64_add_null_propagation_preserves_null_rows() {
    let input = make_int64_pair_batch(
        vec![None, Some(2), Some(-3)],
        true,
        vec![Some(1), None, Some(4)],
        true,
    );
    let admission = RecordingAdmissionController::unbounded();

    let output = project_batch(
        &input,
        &[ProjectionExpr::new(
            "sum",
            Expr::add_int64(Expr::column(0), Expr::column(1)),
        )],
        &admission,
        "Projection",
    )
    .expect("nullable int64 add projection should succeed");

    assert_eq!(output.num_rows(), input.num_rows());
    assert_eq!(output.schema().field(0).data_type(), &DataType::Int64);
    assert!(output.schema().field(0).is_nullable());
    assert_eq!(collect_int64(output.column(0)), vec![None, None, Some(1)]);
}

#[test]
fn signed_widening_int32_plus_int64_returns_int64_results() {
    let input = make_int32_int64_pair_batch(
        vec![Some(1), Some(-3), Some(100)],
        false,
        vec![Some(2), Some(4), Some(200)],
        false,
    );
    let admission = RecordingAdmissionController::unbounded();

    let output = project_batch(
        &input,
        &[ProjectionExpr::new(
            "sum",
            Expr::add_int64(Expr::column(0), Expr::column(1)),
        )],
        &admission,
        "Projection",
    )
    .expect("signed widening int32 + int64 projection should succeed");

    assert_eq!(output.num_rows(), input.num_rows());
    assert_eq!(output.schema().field(0).data_type(), &DataType::Int64);
    assert!(!output.schema().field(0).is_nullable());
    assert_eq!(
        collect_int64(output.column(0)),
        vec![Some(3), Some(1), Some(300)]
    );
}

#[test]
fn signed_widening_int64_plus_int32_returns_int64_results() {
    let input = make_int64_int32_pair_batch(
        vec![Some(2), Some(4), Some(200)],
        false,
        vec![Some(1), Some(-3), Some(100)],
        false,
    );
    let admission = RecordingAdmissionController::unbounded();

    let output = project_batch(
        &input,
        &[ProjectionExpr::new(
            "sum",
            Expr::add_int64(Expr::column(0), Expr::column(1)),
        )],
        &admission,
        "Projection",
    )
    .expect("signed widening int64 + int32 projection should succeed");

    assert_eq!(output.num_rows(), input.num_rows());
    assert_eq!(output.schema().field(0).data_type(), &DataType::Int64);
    assert!(!output.schema().field(0).is_nullable());
    assert_eq!(
        collect_int64(output.column(0)),
        vec![Some(3), Some(1), Some(300)]
    );
}

#[test]
fn int64_add_overflow_reports_execution_error() {
    let input = make_int64_pair_batch(vec![Some(i64::MAX)], false, vec![Some(1)], false);
    let admission = RecordingAdmissionController::unbounded();

    let error = project_batch(
        &input,
        &[ProjectionExpr::new(
            "sum",
            Expr::add_int64(Expr::column(0), Expr::column(1)),
        )],
        &admission,
        "Projection",
    )
    .expect_err("int64 overflow should fail");

    assert!(error
        .to_string()
        .contains("int64 overflow in Projection:sum at row 0"));
}

#[test]
fn signed_widening_int32_plus_int64_overflow_reports_execution_error() {
    let input = make_int32_int64_pair_batch(vec![Some(1)], false, vec![Some(i64::MAX)], false);
    let admission = RecordingAdmissionController::unbounded();

    let error = project_batch(
        &input,
        &[ProjectionExpr::new(
            "sum",
            Expr::add_int64(Expr::column(0), Expr::column(1)),
        )],
        &admission,
        "Projection",
    )
    .expect_err("signed widening overflow should fail");

    assert!(error
        .to_string()
        .contains("int64 overflow in Projection:sum at row 0"));
}

#[test]
fn int64_projection_reports_missing_column_error() {
    let input = make_single_int64_batch(vec![Some(-7), Some(0), Some(42)], false);
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

fn make_single_int64_batch(values: Vec<Option<i64>>, nullable: bool) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "s64",
        DataType::Int64,
        nullable,
    )]));
    let array = Int64Array::from(values);
    RecordBatch::try_new(schema, vec![Arc::new(array)]).expect("int64 batch should build")
}

fn make_int64_pair_batch(
    lhs_values: Vec<Option<i64>>,
    lhs_nullable: bool,
    rhs_values: Vec<Option<i64>>,
    rhs_nullable: bool,
) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("lhs", DataType::Int64, lhs_nullable),
        Field::new("rhs", DataType::Int64, rhs_nullable),
    ]));
    let lhs = Int64Array::from(lhs_values);
    let rhs = Int64Array::from(rhs_values);
    RecordBatch::try_new(schema, vec![Arc::new(lhs), Arc::new(rhs)])
        .expect("int64 pair batch should build")
}

fn make_int32_int64_pair_batch(
    lhs_values: Vec<Option<i32>>,
    lhs_nullable: bool,
    rhs_values: Vec<Option<i64>>,
    rhs_nullable: bool,
) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("lhs32", DataType::Int32, lhs_nullable),
        Field::new("rhs64", DataType::Int64, rhs_nullable),
    ]));
    let lhs = Int32Array::from(lhs_values);
    let rhs = Int64Array::from(rhs_values);
    RecordBatch::try_new(schema, vec![Arc::new(lhs), Arc::new(rhs)])
        .expect("int32/int64 pair batch should build")
}

fn make_int64_int32_pair_batch(
    lhs_values: Vec<Option<i64>>,
    lhs_nullable: bool,
    rhs_values: Vec<Option<i32>>,
    rhs_nullable: bool,
) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("lhs64", DataType::Int64, lhs_nullable),
        Field::new("rhs32", DataType::Int32, rhs_nullable),
    ]));
    let lhs = Int64Array::from(lhs_values);
    let rhs = Int32Array::from(rhs_values);
    RecordBatch::try_new(schema, vec![Arc::new(lhs), Arc::new(rhs)])
        .expect("int64/int32 pair batch should build")
}

fn collect_int64(array: &dyn Array) -> Vec<Option<i64>> {
    let int64 = array
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("array should be Int64");
    (0..int64.len())
        .map(|index| {
            if int64.is_null(index) {
                None
            } else {
                Some(int64.value(index))
            }
        })
        .collect()
}
