use std::sync::Arc;

use arrow_array::builder::{Int32Builder, ListBuilder, MapBuilder};
use arrow_array::{Array, ArrayRef, Int32Array, MapArray, RecordBatch};
use arrow_schema::{Field, Schema};
use tiforth_kernel::admission::RecordingAdmissionController;
use tiforth_kernel::expr::Expr;
use tiforth_kernel::projection::{project_batch, ProjectionExpr};

#[test]
fn map_column_passthrough_preserves_entry_order_values_and_non_nullable_field() {
    let input = make_single_map_batch(vec![
        Some(vec![(1, Some(2)), (3, Some(4))]),
        Some(vec![(5, Some(6))]),
        Some(vec![]),
    ]);
    let admission = RecordingAdmissionController::unbounded();

    let output = project_batch(
        &input,
        &[ProjectionExpr::new("m_copy", Expr::column(0))],
        &admission,
        "Projection",
    )
    .expect("map projection should succeed");

    assert_eq!(output.num_rows(), input.num_rows());
    assert_eq!(output.schema().field(0).name(), "m_copy");
    assert_eq!(
        output.schema().field(0).data_type(),
        input.schema().field(0).data_type()
    );
    assert!(!output.schema().field(0).is_nullable());
    assert_eq!(
        collect_map_rows(output.column(0)),
        vec![
            Some(vec![(1, Some(2)), (3, Some(4))]),
            Some(vec![(5, Some(6))]),
            Some(vec![]),
        ]
    );
}

#[test]
fn map_nullable_passthrough_preserves_top_level_null_positions() {
    let input = make_single_map_batch(vec![Some(vec![(1, None)]), None, Some(vec![(2, Some(3))])]);
    let admission = RecordingAdmissionController::unbounded();

    let output = project_batch(
        &input,
        &[ProjectionExpr::new("m_copy", Expr::column(0))],
        &admission,
        "Projection",
    )
    .expect("nullable map projection should succeed");

    assert_eq!(output.num_rows(), input.num_rows());
    assert_eq!(
        output.schema().field(0).data_type(),
        input.schema().field(0).data_type()
    );
    assert!(output.schema().field(0).is_nullable());
    assert_eq!(
        collect_map_rows(output.column(0)),
        vec![Some(vec![(1, None)]), None, Some(vec![(2, Some(3))])]
    );
}

#[test]
fn map_value_null_preservation_keeps_row_valid() {
    let input = make_single_map_batch(vec![
        Some(vec![(1, None), (2, Some(3))]),
        Some(vec![]),
        Some(vec![(4, None)]),
    ]);
    let admission = RecordingAdmissionController::unbounded();

    let output = project_batch(
        &input,
        &[ProjectionExpr::new("m_copy", Expr::column(0))],
        &admission,
        "Projection",
    )
    .expect("map value-null projection should succeed");

    assert_eq!(
        collect_map_rows(output.column(0)),
        vec![
            Some(vec![(1, None), (2, Some(3))]),
            Some(vec![]),
            Some(vec![(4, None)]),
        ]
    );
}

#[test]
fn map_projection_reports_missing_column_error() {
    let input = make_single_map_batch(vec![Some(vec![(1, Some(2))]), Some(vec![(3, Some(4))])]);
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

fn make_single_map_batch(rows: Vec<Option<Vec<(i32, Option<i32>)>>>) -> RecordBatch {
    let mut builder = MapBuilder::new(None, Int32Builder::new(), Int32Builder::new());

    for row in rows {
        match row {
            Some(entries) => {
                for (key, value) in entries {
                    builder.keys().append_value(key);
                    match value {
                        Some(value) => builder.values().append_value(value),
                        None => builder.values().append_null(),
                    }
                }
                builder.append(true).expect("map row should be valid");
            }
            None => builder.append(false).expect("null map row should be valid"),
        }
    }

    let map_array = builder.finish();
    let schema = Arc::new(Schema::new(vec![Field::new(
        "m",
        map_array.data_type().clone(),
        map_array.null_count() > 0,
    )]));
    RecordBatch::try_new(schema, vec![Arc::new(map_array) as ArrayRef])
        .expect("map batch should be valid")
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

fn collect_map_rows(array: &ArrayRef) -> Vec<Option<Vec<(i32, Option<i32>)>>> {
    let values = array
        .as_any()
        .downcast_ref::<MapArray>()
        .expect("array should be map");
    let keys = values
        .keys()
        .as_any()
        .downcast_ref::<Int32Array>()
        .expect("map keys should be int32");
    let map_values = values
        .values()
        .as_any()
        .downcast_ref::<Int32Array>()
        .expect("map values should be int32");

    (0..values.len())
        .map(|index| {
            if values.is_null(index) {
                None
            } else {
                let start = values.value_offsets()[index] as usize;
                let end = values.value_offsets()[index + 1] as usize;
                Some(
                    (start..end)
                        .map(|entry_index| {
                            (
                                keys.value(entry_index),
                                (!map_values.is_null(entry_index))
                                    .then(|| map_values.value(entry_index)),
                            )
                        })
                        .collect(),
                )
            }
        })
        .collect()
}
