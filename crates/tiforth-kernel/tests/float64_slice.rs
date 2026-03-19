use std::sync::Arc;

use arrow_array::{Array, ArrayRef, Float32Array, Float64Array, Int32Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use tiforth_kernel::admission::RecordingAdmissionController;
use tiforth_kernel::expr::Expr;
use tiforth_kernel::filter::{filter_batch, FilterPredicate};
use tiforth_kernel::projection::{project_batch, ProjectionExpr};

#[test]
fn float64_column_passthrough_preserves_finite_values_and_non_nullable_field() {
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
fn float64_special_value_passthrough_preserves_infinities_nan_and_signed_zero() {
    let input = make_single_float64_batch(
        vec![
            Some(f64::NEG_INFINITY),
            Some(-0.0),
            Some(0.0),
            Some(f64::INFINITY),
            Some(f64::NAN),
        ],
        false,
    );
    let admission = RecordingAdmissionController::unbounded();

    let output = project_batch(
        &input,
        &[ProjectionExpr::new("f_copy", Expr::column(0))],
        &admission,
        "Projection",
    )
    .expect("float64 special-value projection should succeed");

    assert_eq!(output.num_rows(), input.num_rows());
    assert_eq!(
        collect_float64_tokens(output.column(0)),
        vec![
            Some("-Infinity".to_string()),
            Some("-0.0".to_string()),
            Some("0.0".to_string()),
            Some("Infinity".to_string()),
            Some("NaN".to_string()),
        ]
    );
}

#[test]
fn float64_nullable_passthrough_preserves_null_positions() {
    let input = make_single_float64_batch(
        vec![None, Some(f64::NEG_INFINITY), None, Some(f64::NAN), Some(1.0)],
        true,
    );
    let admission = RecordingAdmissionController::unbounded();

    let output = project_batch(
        &input,
        &[ProjectionExpr::new("f_copy", Expr::column(0))],
        &admission,
        "Projection",
    )
    .expect("nullable float64 projection should succeed");

    assert_eq!(output.num_rows(), input.num_rows());
    assert_eq!(output.schema().field(0).data_type(), &DataType::Float64);
    assert!(output.schema().field(0).is_nullable());
    assert_eq!(
        collect_float64_tokens(output.column(0)),
        vec![
            None,
            Some("-Infinity".to_string()),
            None,
            Some("NaN".to_string()),
            Some("1".to_string()),
        ]
    );
}

#[test]
fn float64_predicate_keeps_all_rows_for_non_null_special_values() {
    let input = make_single_float64_batch(
        vec![
            Some(f64::NEG_INFINITY),
            Some(-0.0),
            Some(0.0),
            Some(f64::INFINITY),
            Some(f64::NAN),
        ],
        false,
    );
    let admission = RecordingAdmissionController::unbounded();

    let output = filter_batch(
        &input,
        &FilterPredicate::is_not_null_column(0),
        &admission,
        "Filter",
    )
    .expect("float64 filter should succeed");

    assert_eq!(output.num_rows(), input.num_rows());
    assert_eq!(
        collect_float64_tokens(output.column(0)),
        vec![
            Some("-Infinity".to_string()),
            Some("-0.0".to_string()),
            Some("0.0".to_string()),
            Some("Infinity".to_string()),
            Some("NaN".to_string()),
        ]
    );
}

#[test]
fn float64_predicate_mixed_keep_drop_preserves_row_order_and_full_row_values() {
    let input = make_float64_and_int32_batch(
        vec![None, Some(f64::NEG_INFINITY), None, Some(f64::NAN), Some(1.0)],
        true,
        vec![Some(10), Some(20), Some(30), Some(40), Some(50)],
        false,
    );
    let admission = RecordingAdmissionController::unbounded();

    let output = filter_batch(
        &input,
        &FilterPredicate::is_not_null_column(0),
        &admission,
        "Filter",
    )
    .expect("mixed float64 filter should succeed");

    assert_eq!(output.schema().fields(), input.schema().fields());
    assert_eq!(output.num_rows(), 3);
    assert_eq!(
        collect_float64_tokens(output.column(0)),
        vec![
            Some("-Infinity".to_string()),
            Some("NaN".to_string()),
            Some("1".to_string()),
        ]
    );
    assert_eq!(collect_int32(output.column(1)), vec![Some(20), Some(40), Some(50)]);
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
fn float64_predicate_reports_missing_column_error() {
    let input = make_single_float64_batch(vec![Some(-1.5), Some(0.0), Some(2.25)], false);
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
fn unsupported_floating_family_reports_execution_errors_in_projection_and_predicate_paths() {
    let input = make_single_float32_batch(vec![Some(-1.5), Some(0.0), Some(2.25)], false);
    let admission = RecordingAdmissionController::unbounded();

    let projection_error = project_batch(
        &input,
        &[ProjectionExpr::new("f32_copy", Expr::column(0))],
        &admission,
        "Projection",
    )
    .expect_err("float32 projection should fail in first float slice");
    assert!(projection_error
        .to_string()
        .contains("unsupported floating expression input at column 0"));
    assert!(projection_error
        .to_string()
        .contains("first float slice supports Float64 only"));

    let predicate_error = filter_batch(
        &input,
        &FilterPredicate::is_not_null_column(0),
        &admission,
        "Filter",
    )
    .expect_err("float32 predicate input should fail in first float slice");
    assert!(predicate_error
        .to_string()
        .contains("unsupported floating predicate input at column 0"));
    assert!(predicate_error
        .to_string()
        .contains("first float slice supports Float64 only"));
}

fn make_single_float64_batch(values: Vec<Option<f64>>, nullable: bool) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "f",
        DataType::Float64,
        nullable,
    )]));
    let values: ArrayRef = Arc::new(Float64Array::from(values));
    RecordBatch::try_new(schema, vec![values]).expect("float64 batch should build")
}

fn make_float64_and_int32_batch(
    float_values: Vec<Option<f64>>,
    float_nullable: bool,
    int_values: Vec<Option<i32>>,
    int_nullable: bool,
) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("f", DataType::Float64, float_nullable),
        Field::new("x", DataType::Int32, int_nullable),
    ]));
    let floats: ArrayRef = Arc::new(Float64Array::from(float_values));
    let ints: ArrayRef = Arc::new(Int32Array::from(int_values));
    RecordBatch::try_new(schema, vec![floats, ints]).expect("mixed batch should build")
}

fn make_single_float32_batch(values: Vec<Option<f32>>, nullable: bool) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "f32",
        DataType::Float32,
        nullable,
    )]));
    let values: ArrayRef = Arc::new(Float32Array::from(values));
    RecordBatch::try_new(schema, vec![values]).expect("float32 batch should build")
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
