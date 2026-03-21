use std::sync::Arc;

use arrow_array::{Array, ArrayRef, BooleanArray, Int32Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use tiforth_kernel::admission::RecordingAdmissionController;
use tiforth_kernel::collation::{
    collation_eq_column_literal, collation_lt_column_literal, order_by_column_asc_indices,
};
use tiforth_kernel::expr::Expr;
use tiforth_kernel::filter::{filter_batch, FilterPredicate};
use tiforth_kernel::projection::{project_batch, ProjectionExpr};

#[test]
fn utf8_column_passthrough_binary_preserves_values_and_non_nullable_field() {
    let input = make_utf8_batch(vec![Some("Alpha"), Some("alpha"), Some("beta")], false);
    let admission = RecordingAdmissionController::unbounded();

    let output = project_batch(
        &input,
        &[ProjectionExpr::new("s_copy", Expr::column(0))],
        &admission,
        "Projection",
    )
    .expect("utf8 projection should succeed");

    assert_eq!(output.num_rows(), input.num_rows());
    assert_eq!(output.schema().field(0).name(), "s_copy");
    assert_eq!(output.schema().field(0).data_type(), &DataType::Utf8);
    assert!(!output.schema().field(0).is_nullable());
    assert_eq!(
        collect_utf8(output.column(0)),
        vec!["Alpha", "alpha", "beta"]
    );
}

#[test]
fn utf8_column_passthrough_unicode_ci_preserves_values_and_nullability() {
    let input = make_utf8_batch(vec![Some("Alpha"), None, Some("beta")], true);
    let admission = RecordingAdmissionController::unbounded();

    let output = project_batch(
        &input,
        &[ProjectionExpr::new("s_copy", Expr::column(0))],
        &admission,
        "Projection",
    )
    .expect("nullable utf8 projection should succeed");

    assert_eq!(output.num_rows(), input.num_rows());
    assert_eq!(output.schema().field(0).data_type(), &DataType::Utf8);
    assert!(output.schema().field(0).is_nullable());
    assert_eq!(
        collect_utf8_optional(output.column(0)),
        vec![Some("Alpha".to_string()), None, Some("beta".to_string()),]
    );
}

#[test]
fn utf8_predicate_all_kept_keeps_every_row() {
    let input = make_utf8_batch(vec![Some("Alpha"), Some("alpha"), Some("beta")], false);
    let admission = RecordingAdmissionController::unbounded();

    let output = filter_batch(
        &input,
        &FilterPredicate::is_not_null_column(0),
        &admission,
        "Filter",
    )
    .expect("utf8 is-not-null filter should succeed");

    assert_eq!(output.num_rows(), input.num_rows());
    assert_eq!(
        collect_utf8(output.column(0)),
        vec!["Alpha", "alpha", "beta"]
    );
}

#[test]
fn utf8_predicate_mixed_keep_drop_preserves_row_order() {
    let input = make_utf8_and_int32_batch(
        vec![Some("Alpha"), None, Some("beta")],
        true,
        vec![Some(10), Some(20), Some(30)],
        false,
    );
    let admission = RecordingAdmissionController::unbounded();

    let output = filter_batch(
        &input,
        &FilterPredicate::is_not_null_column(0),
        &admission,
        "Filter",
    )
    .expect("mixed utf8 is-not-null filter should succeed");

    assert_eq!(output.num_rows(), 2);
    assert_eq!(collect_utf8(output.column(0)), vec!["Alpha", "beta"]);
    assert_eq!(collect_int32(output.column(1)), vec![Some(10), Some(30)]);
}

#[test]
fn utf8_binary_equality_case_sensitive_returns_expected_tokens() {
    let input = make_utf8_batch(vec![Some("Alpha"), Some("alpha"), Some("beta")], false);

    let output = collation_eq_column_literal(&input, 0, Some("alpha"), "binary")
        .expect("binary collation_eq should succeed");

    assert_eq!(
        collect_bool(&output),
        vec![Some(false), Some(true), Some(false)]
    );
}

#[test]
fn utf8_unicode_ci_equality_case_insensitive_returns_expected_tokens() {
    let input = make_utf8_batch(vec![Some("Alpha"), Some("alpha"), Some("beta")], false);

    let output = collation_eq_column_literal(&input, 0, Some("alpha"), "unicode_ci")
        .expect("unicode_ci collation_eq should succeed");

    assert_eq!(
        collect_bool(&output),
        vec![Some(true), Some(true), Some(false)]
    );
}

#[test]
fn utf8_binary_less_than_uses_bytewise_ordering() {
    let input = make_utf8_batch(vec![Some("Alpha"), Some("alpha"), Some("beta")], false);

    let output = collation_lt_column_literal(&input, 0, Some("beta"), "binary")
        .expect("binary collation_lt should succeed");

    assert_eq!(
        collect_bool(&output),
        vec![Some(true), Some(true), Some(false)]
    );
}

#[test]
fn utf8_unicode_ci_ordering_normalization_uses_folded_keys_with_tie_breakers() {
    let input = make_utf8_batch(vec![Some("b"), Some("A"), Some("a"), Some("B")], false);

    let row_indices = order_by_column_asc_indices(&input, 0, "unicode_ci")
        .expect("unicode_ci order-by indices should succeed");

    assert_eq!(
        collect_utf8_by_indices(input.column(0), &row_indices),
        vec!["A", "a", "B", "b"]
    );
}

#[test]
fn missing_column_reports_execution_error() {
    let input = make_utf8_batch(vec![Some("Alpha"), Some("alpha"), Some("beta")], false);
    let admission = RecordingAdmissionController::unbounded();

    let error = filter_batch(
        &input,
        &FilterPredicate::is_not_null_column(1),
        &admission,
        "Filter",
    )
    .expect_err("missing-column filter should fail");

    assert!(error
        .to_string()
        .contains("missing input column at index 1"));
}

#[test]
fn unknown_collation_reports_execution_error() {
    let input = make_utf8_batch(vec![Some("Alpha"), Some("alpha"), Some("beta")], false);

    let error = collation_eq_column_literal(&input, 0, Some("alpha"), "unknown-collation")
        .expect_err("unknown collation should fail");

    assert!(error
        .to_string()
        .contains("unsupported collation_ref unknown-collation"));
    assert!(error.to_string().contains("binary and unicode_ci only"));
}

#[test]
fn unsupported_collation_type_reports_execution_error() {
    let input = make_int32_batch(vec![Some(1), Some(2), Some(3)], false);

    let error = collation_eq_column_literal(&input, 0, Some("alpha"), "binary")
        .expect_err("non-utf8 collation comparison should fail");

    assert!(error
        .to_string()
        .contains("unsupported collation comparison input at column 0, got Int32"));
}

fn make_utf8_batch(values: Vec<Option<&str>>, nullable: bool) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![Field::new("s", DataType::Utf8, nullable)]));
    let values: ArrayRef = Arc::new(StringArray::from(values));
    RecordBatch::try_new(schema, vec![values]).expect("utf8 batch should build")
}

fn make_utf8_and_int32_batch(
    utf8_values: Vec<Option<&str>>,
    utf8_nullable: bool,
    int_values: Vec<Option<i32>>,
    int_nullable: bool,
) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("s", DataType::Utf8, utf8_nullable),
        Field::new("x", DataType::Int32, int_nullable),
    ]));
    let strings: ArrayRef = Arc::new(StringArray::from(utf8_values));
    let ints: ArrayRef = Arc::new(Int32Array::from(int_values));
    RecordBatch::try_new(schema, vec![strings, ints]).expect("mixed batch should build")
}

fn make_int32_batch(values: Vec<Option<i32>>, nullable: bool) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "x",
        DataType::Int32,
        nullable,
    )]));
    let values: ArrayRef = Arc::new(Int32Array::from(values));
    RecordBatch::try_new(schema, vec![values]).expect("int32 batch should build")
}

fn collect_utf8(array: &ArrayRef) -> Vec<String> {
    let values = array
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("expected Utf8 output");
    (0..values.len())
        .map(|index| values.value(index).to_string())
        .collect()
}

fn collect_utf8_optional(array: &ArrayRef) -> Vec<Option<String>> {
    let values = array
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("expected Utf8 output");
    (0..values.len())
        .map(|index| {
            values
                .is_null(index)
                .then_some(())
                .map_or(Some(values.value(index).to_string()), |_| None)
        })
        .collect()
}

fn collect_utf8_by_indices(array: &ArrayRef, row_indices: &[usize]) -> Vec<String> {
    let values = array
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("expected Utf8 output");
    row_indices
        .iter()
        .map(|index| values.value(*index).to_string())
        .collect()
}

fn collect_int32(array: &ArrayRef) -> Vec<Option<i32>> {
    let values = array
        .as_any()
        .downcast_ref::<Int32Array>()
        .expect("expected Int32 output");
    (0..values.len())
        .map(|index| {
            values
                .is_null(index)
                .then_some(())
                .map_or(Some(values.value(index)), |_| None)
        })
        .collect()
}

fn collect_bool(array: &BooleanArray) -> Vec<Option<bool>> {
    (0..array.len())
        .map(|index| {
            array
                .is_null(index)
                .then_some(())
                .map_or(Some(array.value(index)), |_| None)
        })
        .collect()
}
