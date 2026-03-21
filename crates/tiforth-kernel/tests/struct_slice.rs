use std::sync::Arc;

use arrow_array::builder::{Int32Builder, ListBuilder, StructBuilder};
use arrow_array::{Array, ArrayRef, Int32Array, RecordBatch, StructArray};
use arrow_schema::{DataType, Field, Schema};
use tiforth_kernel::admission::RecordingAdmissionController;
use tiforth_kernel::expr::Expr;
use tiforth_kernel::projection::{project_batch, ProjectionExpr};

#[test]
fn struct_column_passthrough_preserves_field_order_values_and_non_nullable_field() {
    let input = make_single_struct_batch(vec![
        Some((1, Some(2))),
        Some((3, Some(4))),
        Some((5, Some(6))),
    ]);
    let admission = RecordingAdmissionController::unbounded();

    let output = project_batch(
        &input,
        &[ProjectionExpr::new("s_copy", Expr::column(0))],
        &admission,
        "Projection",
    )
    .expect("struct projection should succeed");

    assert_eq!(output.num_rows(), input.num_rows());
    assert_eq!(output.schema().field(0).name(), "s_copy");
    assert_eq!(
        output.schema().field(0).data_type(),
        &expected_struct_type()
    );
    assert!(!output.schema().field(0).is_nullable());
    assert_eq!(
        collect_struct_rows(output.column(0)),
        vec![Some((1, Some(2))), Some((3, Some(4))), Some((5, Some(6)))]
    );
}

#[test]
fn struct_nullable_passthrough_preserves_top_level_null_positions() {
    let input = make_single_struct_batch(vec![Some((1, None)), None, Some((2, Some(3)))]);
    let admission = RecordingAdmissionController::unbounded();

    let output = project_batch(
        &input,
        &[ProjectionExpr::new("s_copy", Expr::column(0))],
        &admission,
        "Projection",
    )
    .expect("nullable struct projection should succeed");

    assert_eq!(output.num_rows(), input.num_rows());
    assert_eq!(
        output.schema().field(0).data_type(),
        &expected_struct_type()
    );
    assert!(output.schema().field(0).is_nullable());
    assert_eq!(
        collect_struct_rows(output.column(0)),
        vec![Some((1, None)), None, Some((2, Some(3)))]
    );
}

#[test]
fn struct_child_null_preservation_keeps_row_valid() {
    let input =
        make_single_struct_batch(vec![Some((1, None)), Some((2, Some(3))), Some((4, None))]);
    let admission = RecordingAdmissionController::unbounded();

    let output = project_batch(
        &input,
        &[ProjectionExpr::new("s_copy", Expr::column(0))],
        &admission,
        "Projection",
    )
    .expect("struct child-null projection should succeed");

    assert_eq!(
        collect_struct_rows(output.column(0)),
        vec![Some((1, None)), Some((2, Some(3))), Some((4, None))]
    );
}

#[test]
fn struct_projection_reports_missing_column_error() {
    let input = make_single_struct_batch(vec![Some((1, Some(2))), Some((3, Some(4)))]);
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
fn unsupported_nested_family_projection_reports_execution_error() {
    let input = make_single_list_batch();
    let admission = RecordingAdmissionController::unbounded();

    let error = project_batch(
        &input,
        &[ProjectionExpr::new("list_copy", Expr::column(0))],
        &admission,
        "Projection",
    )
    .expect_err("list projection should fail outside the current nested checkpoints");

    assert!(error
        .to_string()
        .contains("unsupported nested expression input at column 0"));
    assert!(error.to_string().contains(
        "struct<a:int32, b:int32?>, map<int32, int32?>, and dense_union<i:int32, n:int32?> only"
    ));
}

fn make_single_struct_batch(rows: Vec<Option<(i32, Option<i32>)>>) -> RecordBatch {
    let mut builder = StructBuilder::from_fields(struct_fields(), rows.len());

    for row in rows {
        match row {
            Some((a, b)) => {
                builder
                    .field_builder::<Int32Builder>(0)
                    .expect("field 0 should be int32")
                    .append_value(a);
                match b {
                    Some(value) => builder
                        .field_builder::<Int32Builder>(1)
                        .expect("field 1 should be int32")
                        .append_value(value),
                    None => builder
                        .field_builder::<Int32Builder>(1)
                        .expect("field 1 should be int32")
                        .append_null(),
                }
                builder.append(true);
            }
            None => {
                builder
                    .field_builder::<Int32Builder>(0)
                    .expect("field 0 should be int32")
                    .append_null();
                builder
                    .field_builder::<Int32Builder>(1)
                    .expect("field 1 should be int32")
                    .append_null();
                builder.append(false);
            }
        }
    }

    let struct_array = builder.finish();
    let schema = Arc::new(Schema::new(vec![Field::new(
        "s",
        struct_array.data_type().clone(),
        struct_array.null_count() > 0,
    )]));
    RecordBatch::try_new(schema, vec![Arc::new(struct_array) as ArrayRef])
        .expect("struct batch should be valid")
}

fn make_single_list_batch() -> RecordBatch {
    let mut builder = ListBuilder::new(Int32Builder::new());

    builder.values().append_value(1);
    builder.values().append_value(2);
    builder.append(true);

    builder.values().append_value(3);
    builder.append(true);

    let list_array = builder.finish();
    let schema = Arc::new(Schema::new(vec![Field::new(
        "l",
        list_array.data_type().clone(),
        list_array.null_count() > 0,
    )]));
    RecordBatch::try_new(schema, vec![Arc::new(list_array) as ArrayRef])
        .expect("list batch should be valid")
}

fn collect_struct_rows(array: &ArrayRef) -> Vec<Option<(i32, Option<i32>)>> {
    let values = array
        .as_any()
        .downcast_ref::<StructArray>()
        .expect("array should be struct");
    let a_values = values
        .column(0)
        .as_any()
        .downcast_ref::<Int32Array>()
        .expect("field a should be int32");
    let b_values = values
        .column(1)
        .as_any()
        .downcast_ref::<Int32Array>()
        .expect("field b should be int32");

    (0..values.len())
        .map(|index| {
            if values.is_null(index) {
                None
            } else {
                Some((
                    a_values.value(index),
                    (!b_values.is_null(index)).then(|| b_values.value(index)),
                ))
            }
        })
        .collect()
}

fn expected_struct_type() -> DataType {
    DataType::Struct(struct_fields().into())
}

fn struct_fields() -> Vec<Field> {
    vec![
        Field::new("a", DataType::Int32, false),
        Field::new("b", DataType::Int32, true),
    ]
}
