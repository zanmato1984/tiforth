use std::sync::Arc;

mod support;

use arrow_array::{Array, ArrayRef, Float64Array, Int32Array, Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use support::project_batch;
use tiforth_kernel::admission::RecordingAdmissionController;
use tiforth_kernel::expr::Expr;
use tiforth_kernel::projection::ProjectionExpr;

#[test]
fn float64_column_passthrough_preserves_values_and_nullability() {
    let input = make_single_float64_batch(vec![Some(-1.5), Some(0.0), Some(2.25)], false);
    let admission = RecordingAdmissionController::unbounded();

    let output = project_batch(
        &input,
        &[ProjectionExpr::new("f_copy", Expr::column(0))],
        &admission,
        "Projection",
    )
    .expect("float64 projection should succeed");

    assert_eq!(output.num_rows(), input.num_rows());
    assert_eq!(output.schema().field(0).name(), "f_copy");
    assert_eq!(output.schema().field(0).data_type(), &DataType::Float64);
    assert!(!output.schema().field(0).is_nullable());
    assert_eq!(
        collect_float64_tokens(output.column(0)),
        vec![
            Some("-1.5".to_string()),
            Some("0.0".to_string()),
            Some("2.25".to_string())
        ]
    );
}

#[test]
fn float64_add_basic_returns_float64_results() {
    let input = make_float64_pair_batch(
        vec![Some(1.25), Some(-0.5), Some(10.0)],
        false,
        vec![Some(2.0), Some(0.25), Some(-3.5)],
        false,
    );
    let admission = RecordingAdmissionController::unbounded();

    let output = project_batch(
        &input,
        &[ProjectionExpr::new(
            "sum",
            Expr::add_float64(Expr::column(0), Expr::column(1)),
        )],
        &admission,
        "Projection",
    )
    .expect("float64 add projection should succeed");

    assert_eq!(output.num_rows(), input.num_rows());
    assert_eq!(output.schema().field(0).data_type(), &DataType::Float64);
    assert!(!output.schema().field(0).is_nullable());
    assert_eq!(
        collect_float64_tokens(output.column(0)),
        vec![
            Some("3.25".to_string()),
            Some("-0.25".to_string()),
            Some("6.5".to_string())
        ]
    );
}

#[test]
fn float64_add_null_propagation_preserves_null_rows() {
    let input = make_float64_pair_batch(
        vec![None, Some(2.5), Some(-3.0)],
        true,
        vec![Some(1.0), None, Some(4.5)],
        true,
    );
    let admission = RecordingAdmissionController::unbounded();

    let output = project_batch(
        &input,
        &[ProjectionExpr::new(
            "sum",
            Expr::add_float64(Expr::column(0), Expr::column(1)),
        )],
        &admission,
        "Projection",
    )
    .expect("nullable float64 add projection should succeed");

    assert_eq!(output.num_rows(), input.num_rows());
    assert_eq!(output.schema().field(0).data_type(), &DataType::Float64);
    assert!(output.schema().field(0).is_nullable());
    assert_eq!(
        collect_float64_tokens(output.column(0)),
        vec![None, None, Some("1.5".to_string())]
    );
}

#[test]
fn float64_add_special_values_preserves_canonical_row_outcomes() {
    let input = make_float64_pair_batch(
        vec![
            Some(f64::INFINITY),
            Some(f64::NEG_INFINITY),
            Some(f64::NAN),
            Some(-0.0),
        ],
        false,
        vec![Some(1.0), Some(f64::INFINITY), Some(2.0), Some(-0.0)],
        false,
    );
    let admission = RecordingAdmissionController::unbounded();

    let output = project_batch(
        &input,
        &[ProjectionExpr::new(
            "sum",
            Expr::add_float64(Expr::column(0), Expr::column(1)),
        )],
        &admission,
        "Projection",
    )
    .expect("float64 special-value add projection should succeed");

    assert_eq!(output.num_rows(), input.num_rows());
    assert_eq!(
        collect_float64_tokens(output.column(0)),
        vec![
            Some("Infinity".to_string()),
            Some("NaN".to_string()),
            Some("NaN".to_string()),
            Some("-0.0".to_string()),
        ]
    );
}

#[test]
fn widening_int32_plus_float64_returns_float64_results() {
    let input = make_int32_float64_pair_batch(
        vec![Some(1), Some(-3), Some(100)],
        false,
        vec![Some(2.5), Some(4.25), Some(0.5)],
        false,
    );
    let admission = RecordingAdmissionController::unbounded();

    let output = project_batch(
        &input,
        &[ProjectionExpr::new(
            "sum",
            Expr::add_float64(Expr::column(0), Expr::column(1)),
        )],
        &admission,
        "Projection",
    )
    .expect("widening int32 + float64 projection should succeed");

    assert_eq!(output.schema().field(0).data_type(), &DataType::Float64);
    assert!(!output.schema().field(0).is_nullable());
    assert_eq!(
        collect_float64_tokens(output.column(0)),
        vec![
            Some("3.5".to_string()),
            Some("1.25".to_string()),
            Some("100.5".to_string())
        ]
    );
}

#[test]
fn widening_int64_plus_float64_returns_float64_results() {
    let input = make_int64_float64_pair_batch(
        vec![Some(2), Some(4), Some(200)],
        false,
        vec![Some(1.5), Some(-3.25), Some(0.75)],
        false,
    );
    let admission = RecordingAdmissionController::unbounded();

    let output = project_batch(
        &input,
        &[ProjectionExpr::new(
            "sum",
            Expr::add_float64(Expr::column(0), Expr::column(1)),
        )],
        &admission,
        "Projection",
    )
    .expect("widening int64 + float64 projection should succeed");

    assert_eq!(output.schema().field(0).data_type(), &DataType::Float64);
    assert!(!output.schema().field(0).is_nullable());
    assert_eq!(
        collect_float64_tokens(output.column(0)),
        vec![
            Some("3.5".to_string()),
            Some("0.75".to_string()),
            Some("200.75".to_string())
        ]
    );
}

#[test]
fn widening_float64_plus_int32_returns_float64_results() {
    let input = make_float64_int32_pair_batch(
        vec![Some(2.5), Some(4.25), Some(0.5)],
        false,
        vec![Some(1), Some(-3), Some(100)],
        false,
    );
    let admission = RecordingAdmissionController::unbounded();

    let output = project_batch(
        &input,
        &[ProjectionExpr::new(
            "sum",
            Expr::add_float64(Expr::column(0), Expr::column(1)),
        )],
        &admission,
        "Projection",
    )
    .expect("widening float64 + int32 projection should succeed");

    assert_eq!(output.schema().field(0).data_type(), &DataType::Float64);
    assert!(!output.schema().field(0).is_nullable());
    assert_eq!(
        collect_float64_tokens(output.column(0)),
        vec![
            Some("3.5".to_string()),
            Some("1.25".to_string()),
            Some("100.5".to_string())
        ]
    );
}

#[test]
fn widening_float64_plus_int64_returns_float64_results() {
    let input = make_float64_int64_pair_batch(
        vec![Some(1.5), Some(-3.25), Some(0.75)],
        false,
        vec![Some(2), Some(4), Some(200)],
        false,
    );
    let admission = RecordingAdmissionController::unbounded();

    let output = project_batch(
        &input,
        &[ProjectionExpr::new(
            "sum",
            Expr::add_float64(Expr::column(0), Expr::column(1)),
        )],
        &admission,
        "Projection",
    )
    .expect("widening float64 + int64 projection should succeed");

    assert_eq!(output.schema().field(0).data_type(), &DataType::Float64);
    assert!(!output.schema().field(0).is_nullable());
    assert_eq!(
        collect_float64_tokens(output.column(0)),
        vec![
            Some("3.5".to_string()),
            Some("0.75".to_string()),
            Some("200.75".to_string())
        ]
    );
}

#[test]
fn float64_projection_reports_missing_column_error() {
    let input = make_single_float64_batch(vec![Some(-1.5), Some(0.0), Some(2.25)], false);
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
fn float64_add_reports_missing_rhs_column_error() {
    let input = make_single_float64_batch(vec![Some(-1.5), Some(0.0), Some(2.25)], false);
    let admission = RecordingAdmissionController::unbounded();

    let error = project_batch(
        &input,
        &[ProjectionExpr::new(
            "sum",
            Expr::add_float64(Expr::column(0), Expr::column(1)),
        )],
        &admission,
        "Projection",
    )
    .expect_err("add missing-column should fail");

    assert!(error
        .to_string()
        .contains("missing input column at index 1"));
}

fn make_single_float64_batch(values: Vec<Option<f64>>, nullable: bool) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "f",
        DataType::Float64,
        nullable,
    )]));
    let array: ArrayRef = Arc::new(Float64Array::from(values));
    RecordBatch::try_new(schema, vec![array]).expect("float64 batch should build")
}

fn make_float64_pair_batch(
    lhs_values: Vec<Option<f64>>,
    lhs_nullable: bool,
    rhs_values: Vec<Option<f64>>,
    rhs_nullable: bool,
) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("lhs", DataType::Float64, lhs_nullable),
        Field::new("rhs", DataType::Float64, rhs_nullable),
    ]));
    let lhs: ArrayRef = Arc::new(Float64Array::from(lhs_values));
    let rhs: ArrayRef = Arc::new(Float64Array::from(rhs_values));
    RecordBatch::try_new(schema, vec![lhs, rhs]).expect("float64 pair batch should build")
}

fn make_int32_float64_pair_batch(
    lhs_values: Vec<Option<i32>>,
    lhs_nullable: bool,
    rhs_values: Vec<Option<f64>>,
    rhs_nullable: bool,
) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("lhs", DataType::Int32, lhs_nullable),
        Field::new("rhs", DataType::Float64, rhs_nullable),
    ]));
    let lhs: ArrayRef = Arc::new(Int32Array::from(lhs_values));
    let rhs: ArrayRef = Arc::new(Float64Array::from(rhs_values));
    RecordBatch::try_new(schema, vec![lhs, rhs]).expect("int32/float64 pair batch should build")
}

fn make_int64_float64_pair_batch(
    lhs_values: Vec<Option<i64>>,
    lhs_nullable: bool,
    rhs_values: Vec<Option<f64>>,
    rhs_nullable: bool,
) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("lhs", DataType::Int64, lhs_nullable),
        Field::new("rhs", DataType::Float64, rhs_nullable),
    ]));
    let lhs: ArrayRef = Arc::new(Int64Array::from(lhs_values));
    let rhs: ArrayRef = Arc::new(Float64Array::from(rhs_values));
    RecordBatch::try_new(schema, vec![lhs, rhs]).expect("int64/float64 pair batch should build")
}

fn make_float64_int32_pair_batch(
    lhs_values: Vec<Option<f64>>,
    lhs_nullable: bool,
    rhs_values: Vec<Option<i32>>,
    rhs_nullable: bool,
) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("lhs", DataType::Float64, lhs_nullable),
        Field::new("rhs", DataType::Int32, rhs_nullable),
    ]));
    let lhs: ArrayRef = Arc::new(Float64Array::from(lhs_values));
    let rhs: ArrayRef = Arc::new(Int32Array::from(rhs_values));
    RecordBatch::try_new(schema, vec![lhs, rhs]).expect("float64/int32 pair batch should build")
}

fn make_float64_int64_pair_batch(
    lhs_values: Vec<Option<f64>>,
    lhs_nullable: bool,
    rhs_values: Vec<Option<i64>>,
    rhs_nullable: bool,
) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("lhs", DataType::Float64, lhs_nullable),
        Field::new("rhs", DataType::Int64, rhs_nullable),
    ]));
    let lhs: ArrayRef = Arc::new(Float64Array::from(lhs_values));
    let rhs: ArrayRef = Arc::new(Int64Array::from(rhs_values));
    RecordBatch::try_new(schema, vec![lhs, rhs]).expect("float64/int64 pair batch should build")
}

fn collect_float64_tokens(array: &ArrayRef) -> Vec<Option<String>> {
    let values = array
        .as_any()
        .downcast_ref::<Float64Array>()
        .expect("expected Float64Array output");
    (0..values.len())
        .map(|index| {
            values
                .is_null(index)
                .then_some(())
                .map_or(Some(normalize_float64(values.value(index))), |_| None)
        })
        .collect()
}

fn normalize_float64(value: f64) -> String {
    if value.is_nan() {
        "NaN".to_string()
    } else if value == f64::NEG_INFINITY {
        "-Infinity".to_string()
    } else if value == f64::INFINITY {
        "Infinity".to_string()
    } else if value == 0.0 && value.is_sign_negative() {
        "-0.0".to_string()
    } else if value == 0.0 {
        "0.0".to_string()
    } else {
        value.to_string()
    }
}
