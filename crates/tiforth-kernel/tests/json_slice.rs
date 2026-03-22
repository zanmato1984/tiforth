use std::sync::Arc;

mod support;

use arrow_array::{Array, ArrayRef, Int32Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use serde_json::Value;
use support::{filter_batch, project_batch};
use tiforth_kernel::admission::RecordingAdmissionController;
use tiforth_kernel::expr::Expr;
use tiforth_kernel::filter::FilterPredicate;
use tiforth_kernel::projection::ProjectionExpr;

#[test]
fn json_column_passthrough_preserves_tokens_and_non_nullable_field() {
    let input = make_json_batch(vec![Some("{\"a\":1}"), Some("[1,2]"), Some("true")], false);
    let admission = RecordingAdmissionController::unbounded();

    let output = project_batch(
        &input,
        &[ProjectionExpr::new("j_copy", Expr::column(0))],
        &admission,
        "Projection",
    )
    .expect("json projection should succeed");

    assert_eq!(output.num_rows(), input.num_rows());
    assert_eq!(output.schema().field(0).name(), "j_copy");
    assert_eq!(output.schema().field(0).data_type(), &DataType::Utf8);
    assert!(!output.schema().field(0).is_nullable());
    assert_eq!(
        collect_json_tokens(output.column(0)),
        vec![
            Some("{\"a\":1}".to_string()),
            Some("[1,2]".to_string()),
            Some("true".to_string()),
        ]
    );
}

#[test]
fn json_nullable_passthrough_preserves_sql_null_and_json_literal_null() {
    let input = make_json_batch(
        vec![
            Some("{\"a\":1}"),
            None,
            Some("null"),
            Some("{\"b\":2,\"a\":1}"),
        ],
        true,
    );
    let admission = RecordingAdmissionController::unbounded();

    let output = project_batch(
        &input,
        &[ProjectionExpr::new("j_copy", Expr::column(0))],
        &admission,
        "Projection",
    )
    .expect("nullable json projection should succeed");

    assert_eq!(output.num_rows(), input.num_rows());
    assert_eq!(output.schema().field(0).data_type(), &DataType::Utf8);
    assert!(output.schema().field(0).is_nullable());
    assert_eq!(
        collect_json_tokens(output.column(0)),
        vec![
            Some("{\"a\":1}".to_string()),
            None,
            Some("null".to_string()),
            Some("{\"b\":2,\"a\":1}".to_string()),
        ]
    );
}

#[test]
fn json_object_canonicalization_supports_order_insensitive_comparison_tokens() {
    let input = make_json_batch(
        vec![Some("{\"a\":1,\"b\":2}"), Some("{\"b\":2,\"a\":1}")],
        false,
    );
    let admission = RecordingAdmissionController::unbounded();

    let output = project_batch(
        &input,
        &[ProjectionExpr::new("j_copy", Expr::column(0))],
        &admission,
        "Projection",
    )
    .expect("json object projection should succeed");

    let tokens = collect_json_tokens(output.column(0));
    let canonical_tokens: Vec<String> = tokens
        .iter()
        .map(|token| canonicalize_json_token(token.as_deref().expect("non-null token")))
        .collect();

    assert_eq!(
        canonical_tokens,
        vec![
            "{\"a\":1,\"b\":2}".to_string(),
            "{\"a\":1,\"b\":2}".to_string()
        ]
    );
}

#[test]
fn json_array_order_preserved_keeps_distinct_value_tokens() {
    let input = make_json_batch(vec![Some("[1,2]"), Some("[2,1]")], false);
    let admission = RecordingAdmissionController::unbounded();

    let output = project_batch(
        &input,
        &[ProjectionExpr::new("j_copy", Expr::column(0))],
        &admission,
        "Projection",
    )
    .expect("json array-order projection should succeed");

    let tokens = collect_json_tokens(output.column(0));
    assert_eq!(
        tokens,
        vec![Some("[1,2]".to_string()), Some("[2,1]".to_string())]
    );
    assert_ne!(
        canonicalize_json_token(tokens[0].as_deref().expect("non-null token")),
        canonicalize_json_token(tokens[1].as_deref().expect("non-null token"))
    );
}

#[test]
fn json_predicate_all_kept_includes_json_literal_null() {
    let input = make_json_batch(
        vec![Some("{\"a\":1}"), Some("null"), Some("[1,2]"), Some("true")],
        false,
    );
    let admission = RecordingAdmissionController::unbounded();

    let output = filter_batch(
        &input,
        &FilterPredicate::is_not_null_column(0),
        &admission,
        "Filter",
    )
    .expect("json is-not-null filter should keep all non-sql-null rows");

    assert_eq!(output.num_rows(), input.num_rows());
    assert_eq!(
        collect_json_tokens(output.column(0)),
        vec![
            Some("{\"a\":1}".to_string()),
            Some("null".to_string()),
            Some("[1,2]".to_string()),
            Some("true".to_string()),
        ]
    );
}

#[test]
fn json_predicate_all_dropped_for_all_sql_null() {
    let input = make_json_batch(vec![None, None, None], true);
    let admission = RecordingAdmissionController::unbounded();

    let output = filter_batch(
        &input,
        &FilterPredicate::is_not_null_column(0),
        &admission,
        "Filter",
    )
    .expect("json is-not-null filter should drop sql-null rows");

    assert_eq!(output.num_rows(), 0);
}

#[test]
fn json_predicate_mixed_keep_drop_preserves_row_order_and_full_row_values() {
    let input = make_json_and_int32_batch(
        vec![Some("{\"a\":1}"), None, Some("null"), Some("[1,2]")],
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
    .expect("mixed json is-not-null filter should succeed");

    assert_eq!(output.schema().fields(), input.schema().fields());
    assert_eq!(output.num_rows(), 3);
    assert_eq!(
        collect_json_tokens(output.column(0)),
        vec![
            Some("{\"a\":1}".to_string()),
            Some("null".to_string()),
            Some("[1,2]".to_string()),
        ]
    );
    assert_eq!(
        collect_int32(output.column(1)),
        vec![Some(10), Some(30), Some(40)]
    );
}

#[test]
fn json_projection_reports_missing_column_error() {
    let input = make_json_batch(vec![Some("{\"a\":1}"), Some("[1,2]"), Some("true")], false);
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
fn json_predicate_reports_missing_column_error() {
    let input = make_json_batch(vec![Some("{\"a\":1}"), Some("[1,2]"), Some("true")], false);
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

fn make_json_batch(tokens: Vec<Option<&str>>, nullable: bool) -> RecordBatch {
    // The first local JSON checkpoint carries canonical JSON value tokens in Utf8 arrays.
    let schema = Arc::new(Schema::new(vec![Field::new("j", DataType::Utf8, nullable)]));
    let values: ArrayRef = Arc::new(StringArray::from(tokens));
    RecordBatch::try_new(schema, vec![values]).expect("json token batch should build")
}

fn make_json_and_int32_batch(
    json_tokens: Vec<Option<&str>>,
    json_nullable: bool,
    ints: Vec<Option<i32>>,
    int_nullable: bool,
) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("j", DataType::Utf8, json_nullable),
        Field::new("x", DataType::Int32, int_nullable),
    ]));
    let json_values: ArrayRef = Arc::new(StringArray::from(json_tokens));
    let int_values: ArrayRef = Arc::new(Int32Array::from(ints));
    RecordBatch::try_new(schema, vec![json_values, int_values]).expect("mixed batch should build")
}

fn collect_json_tokens(array: &ArrayRef) -> Vec<Option<String>> {
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

fn canonicalize_json_token(token: &str) -> String {
    let value: Value = serde_json::from_str(token).expect("token should parse as json");
    render_canonical_json(&value)
}

fn render_canonical_json(value: &Value) -> String {
    match value {
        Value::Null => "null".to_string(),
        Value::Bool(value) => value.to_string(),
        Value::Number(value) => value.to_string(),
        Value::String(value) => serde_json::to_string(value).expect("string should serialize"),
        Value::Array(values) => {
            let rendered = values
                .iter()
                .map(render_canonical_json)
                .collect::<Vec<_>>()
                .join(",");
            format!("[{rendered}]")
        }
        Value::Object(values) => {
            let mut entries: Vec<_> = values.iter().collect();
            entries.sort_by(|left, right| left.0.cmp(right.0));
            let rendered = entries
                .into_iter()
                .map(|(key, value)| {
                    format!(
                        "{}:{}",
                        serde_json::to_string(key).expect("key should serialize"),
                        render_canonical_json(value)
                    )
                })
                .collect::<Vec<_>>()
                .join(",");
            format!("{{{rendered}}}")
        }
    }
}
