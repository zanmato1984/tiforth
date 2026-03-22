use std::sync::Arc;

mod support;

use arrow_array::{Array, ArrayRef, Int32Array, RecordBatch, UnionArray};
use arrow_buffer::Buffer;
use arrow_schema::{DataType, Field, Schema};
use support::project_batch;
use tiforth_kernel::admission::RecordingAdmissionController;
use tiforth_kernel::expr::Expr;
use tiforth_kernel::projection::ProjectionExpr;

#[derive(Debug, Clone, PartialEq, Eq)]
enum UnionRow {
    I(i32),
    N(Option<i32>),
}

#[test]
fn union_column_passthrough_preserves_variant_order_values_and_non_nullable_field() {
    let input =
        make_single_dense_union_batch(vec![UnionRow::I(1), UnionRow::N(Some(2)), UnionRow::I(3)]);
    let admission = RecordingAdmissionController::unbounded();

    let output = project_batch(
        &input,
        &[ProjectionExpr::new("u_copy", Expr::column(0))],
        &admission,
        "Projection",
    )
    .expect("union projection should succeed");

    assert_eq!(output.num_rows(), input.num_rows());
    assert_eq!(output.schema().field(0).name(), "u_copy");
    assert_eq!(
        output.schema().field(0).data_type(),
        input.schema().field(0).data_type()
    );
    assert!(!output.schema().field(0).is_nullable());
    assert_eq!(
        collect_union_rows(output.column(0)),
        vec![UnionRow::I(1), UnionRow::N(Some(2)), UnionRow::I(3)]
    );
}

#[test]
fn union_variant_switch_preserves_dense_offsets_and_payloads() {
    let input = make_single_dense_union_batch(vec![
        UnionRow::I(1),
        UnionRow::N(Some(2)),
        UnionRow::I(3),
        UnionRow::N(Some(4)),
    ]);
    let admission = RecordingAdmissionController::unbounded();

    let output = project_batch(
        &input,
        &[ProjectionExpr::new("u_copy", Expr::column(0))],
        &admission,
        "Projection",
    )
    .expect("union variant-switch projection should succeed");

    let output_union = output
        .column(0)
        .as_any()
        .downcast_ref::<UnionArray>()
        .expect("output column should be union");
    assert_eq!(output_union.type_ids().as_ref(), &[0, 1, 0, 1]);
    assert_eq!(
        output_union
            .offsets()
            .expect("dense union should have offsets")
            .as_ref(),
        &[0, 0, 1, 1]
    );
    assert_eq!(
        collect_union_rows(output.column(0)),
        vec![
            UnionRow::I(1),
            UnionRow::N(Some(2)),
            UnionRow::I(3),
            UnionRow::N(Some(4)),
        ]
    );
}

#[test]
fn union_variant_null_preservation_keeps_row_non_null() {
    let input = make_single_dense_union_batch(vec![
        UnionRow::N(None),
        UnionRow::I(4),
        UnionRow::N(Some(5)),
    ]);
    let admission = RecordingAdmissionController::unbounded();

    let output = project_batch(
        &input,
        &[ProjectionExpr::new("u_copy", Expr::column(0))],
        &admission,
        "Projection",
    )
    .expect("nullable union-variant projection should succeed");

    assert_eq!(
        collect_union_rows(output.column(0)),
        vec![UnionRow::N(None), UnionRow::I(4), UnionRow::N(Some(5))]
    );
}

#[test]
fn union_projection_reports_missing_column_error() {
    let input = make_single_dense_union_batch(vec![UnionRow::I(1), UnionRow::N(Some(2))]);
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
fn unsupported_union_mode_projection_reports_execution_error() {
    let input = make_single_sparse_union_batch();
    let admission = RecordingAdmissionController::unbounded();

    let error = project_batch(
        &input,
        &[ProjectionExpr::new("u_copy", Expr::column(0))],
        &admission,
        "Projection",
    )
    .expect_err("sparse union projection should fail outside the current nested checkpoints");

    assert!(error
        .to_string()
        .contains("unsupported nested expression input at column 0"));
    assert!(error.to_string().contains(
        "struct<a:int32, b:int32?>, map<int32, int32?>, and dense_union<i:int32, n:int32?> only"
    ));
}

fn make_single_dense_union_batch(rows: Vec<UnionRow>) -> RecordBatch {
    let mut type_ids = Vec::with_capacity(rows.len());
    let mut value_offsets = Vec::with_capacity(rows.len());
    let mut ints = Vec::new();
    let mut nullable_ints = Vec::new();

    for row in rows {
        match row {
            UnionRow::I(value) => {
                type_ids.push(0_i8);
                value_offsets.push(ints.len() as i32);
                ints.push(value);
            }
            UnionRow::N(value) => {
                type_ids.push(1_i8);
                value_offsets.push(nullable_ints.len() as i32);
                nullable_ints.push(value);
            }
        }
    }

    let union_array = UnionArray::try_new(
        &[0_i8, 1_i8],
        Buffer::from_slice_ref(type_ids.as_slice()),
        Some(Buffer::from_slice_ref(value_offsets.as_slice())),
        vec![
            (
                Field::new("i", DataType::Int32, false),
                Arc::new(Int32Array::from(ints)) as ArrayRef,
            ),
            (
                Field::new("n", DataType::Int32, true),
                Arc::new(Int32Array::from(nullable_ints)) as ArrayRef,
            ),
        ],
    )
    .expect("dense union batch should be valid");

    let schema = Arc::new(Schema::new(vec![Field::new(
        "u",
        union_array.data_type().clone(),
        false,
    )]));
    RecordBatch::try_new(schema, vec![Arc::new(union_array) as ArrayRef])
        .expect("union batch should be valid")
}

fn make_single_sparse_union_batch() -> RecordBatch {
    let union_array = UnionArray::try_new(
        &[0_i8, 1_i8],
        Buffer::from_slice_ref([0_i8, 1_i8, 0_i8]),
        None,
        vec![
            (
                Field::new("i", DataType::Int32, false),
                Arc::new(Int32Array::from(vec![1, 0, 3])) as ArrayRef,
            ),
            (
                Field::new("n", DataType::Int32, true),
                Arc::new(Int32Array::from(vec![Some(0), Some(2), Some(0)])) as ArrayRef,
            ),
        ],
    )
    .expect("sparse union batch should be valid");

    let schema = Arc::new(Schema::new(vec![Field::new(
        "u",
        union_array.data_type().clone(),
        false,
    )]));
    RecordBatch::try_new(schema, vec![Arc::new(union_array) as ArrayRef])
        .expect("sparse union batch should be valid")
}

fn collect_union_rows(array: &ArrayRef) -> Vec<UnionRow> {
    let union = array
        .as_any()
        .downcast_ref::<UnionArray>()
        .expect("array should be union");

    (0..union.len())
        .map(|index| {
            let slot = union.value(index);
            let slot = slot
                .as_any()
                .downcast_ref::<Int32Array>()
                .expect("union slot should be int32");

            match union.type_id(index) {
                0 => UnionRow::I(slot.value(0)),
                1 => UnionRow::N((!slot.is_null(0)).then(|| slot.value(0))),
                type_id => panic!("unexpected union type id: {type_id}"),
            }
        })
        .collect()
}
