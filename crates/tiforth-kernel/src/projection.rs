use std::sync::Arc;

use arrow_array::builder::Int32Builder;
use arrow_array::{Array, ArrayRef, Int32Array, RecordBatch};
use arrow_schema::Schema;

use broken_pipeline::traits::arrow::Batch;

use crate::admission::{AdmissionController, ConsumerKind, ConsumerSpec};
use crate::error::TiforthError;
use crate::expr::Expr;

#[derive(Clone, Debug)]
pub struct ProjectionExpr {
    pub name: String,
    pub expr: Expr,
}

impl ProjectionExpr {
    pub fn new(name: impl Into<String>, expr: Expr) -> Self {
        Self {
            name: name.into(),
            expr,
        }
    }
}

enum EvalValue {
    Array(ArrayRef),
    Scalar(Option<i32>),
}

pub fn project_batch(
    batch: &RecordBatch,
    projections: &[ProjectionExpr],
    controller: &dyn AdmissionController,
    operator_name: &str,
) -> Result<Batch, TiforthError> {
    let schema = Arc::new(Schema::new(
        projections
            .iter()
            .map(|projection| {
                projection
                    .expr
                    .field(batch.schema().as_ref(), &projection.name)
            })
            .collect::<Result<Vec<_>, _>>()?,
    ));
    let arrays = projections
        .iter()
        .map(|projection| evaluate_projection(projection, batch, controller, operator_name))
        .collect::<Result<Vec<_>, _>>()?;

    Ok(Arc::new(RecordBatch::try_new(schema, arrays)?))
}

fn evaluate_projection(
    projection: &ProjectionExpr,
    batch: &RecordBatch,
    controller: &dyn AdmissionController,
    operator_name: &str,
) -> Result<ArrayRef, TiforthError> {
    match evaluate_value(
        &projection.expr,
        batch,
        controller,
        operator_name,
        &projection.name,
    )? {
        EvalValue::Array(array) => Ok(array),
        EvalValue::Scalar(value) => materialize_scalar(
            value,
            batch.num_rows(),
            controller,
            operator_name,
            &projection.name,
        ),
    }
}

fn evaluate_value(
    expr: &Expr,
    batch: &RecordBatch,
    controller: &dyn AdmissionController,
    operator_name: &str,
    output_name: &str,
) -> Result<EvalValue, TiforthError> {
    match expr {
        Expr::Column(index) => batch
            .columns()
            .get(*index)
            .cloned()
            .map(EvalValue::Array)
            .ok_or(TiforthError::MissingColumn { index: *index }),
        Expr::Literal(value) => Ok(EvalValue::Scalar(*value)),
        Expr::Add(lhs, rhs) => {
            let lhs = evaluate_value(lhs, batch, controller, operator_name, output_name)?;
            let rhs = evaluate_value(rhs, batch, controller, operator_name, output_name)?;
            Ok(EvalValue::Array(materialize_add(
                &lhs,
                &rhs,
                batch.num_rows(),
                controller,
                operator_name,
                output_name,
            )?))
        }
    }
}

fn materialize_scalar(
    value: Option<i32>,
    rows: usize,
    controller: &dyn AdmissionController,
    operator_name: &str,
    output_name: &str,
) -> Result<ArrayRef, TiforthError> {
    with_admitted_int32_array(rows, controller, operator_name, output_name, |builder| {
        for _ in 0..rows {
            match value {
                Some(value) => builder.append_value(value),
                None => builder.append_null(),
            }
        }
        Ok(())
    })
}

fn materialize_add(
    lhs: &EvalValue,
    rhs: &EvalValue,
    rows: usize,
    controller: &dyn AdmissionController,
    operator_name: &str,
    output_name: &str,
) -> Result<ArrayRef, TiforthError> {
    with_admitted_int32_array(rows, controller, operator_name, output_name, |builder| {
        for row in 0..rows {
            match (int32_value(lhs, row)?, int32_value(rhs, row)?) {
                (Some(lhs), Some(rhs)) => {
                    builder.append_value(lhs.checked_add(rhs).ok_or_else(|| {
                        TiforthError::Message(format!(
                            "int32 overflow in {operator_name}:{output_name} at row {row}"
                        ))
                    })?)
                }
                _ => builder.append_null(),
            }
        }
        Ok(())
    })
}

fn with_admitted_int32_array<F>(
    rows: usize,
    controller: &dyn AdmissionController,
    operator_name: &str,
    output_name: &str,
    build: F,
) -> Result<ArrayRef, TiforthError>
where
    F: FnOnce(&mut Int32Builder) -> Result<(), TiforthError>,
{
    let consumer = controller.open(ConsumerSpec::new(
        format!("{operator_name}:{output_name}"),
        ConsumerKind::ProjectionOutput,
        false,
    ));
    let estimated = estimate_int32_array_bytes(rows);
    consumer.try_reserve(estimated)?;

    let mut builder = Int32Builder::with_capacity(rows);
    if let Err(error) = build(&mut builder) {
        consumer.release();
        return Err(error);
    }

    let array = builder.finish();
    let actual = actual_int32_array_bytes(&array);
    if estimated > actual {
        consumer.shrink(estimated - actual);
    }
    consumer.release();
    Ok(Arc::new(array))
}

fn int32_value(value: &EvalValue, row: usize) -> Result<Option<i32>, TiforthError> {
    match value {
        EvalValue::Scalar(value) => Ok(*value),
        EvalValue::Array(array) => {
            let int32 = array.as_any().downcast_ref::<Int32Array>().ok_or_else(|| {
                TiforthError::UnsupportedDataType {
                    detail: format!(
                        "expected Int32 expression input, got {:?}",
                        array.data_type()
                    ),
                }
            })?;
            if int32.is_null(row) {
                Ok(None)
            } else {
                Ok(Some(int32.value(row)))
            }
        }
    }
}

fn estimate_int32_array_bytes(rows: usize) -> usize {
    rows * std::mem::size_of::<i32>() + rows.div_ceil(8)
}

fn actual_int32_array_bytes(array: &Int32Array) -> usize {
    let mut bytes = array.len() * std::mem::size_of::<i32>();
    if array.null_count() > 0 {
        bytes += array.len().div_ceil(8);
    }
    bytes
}
