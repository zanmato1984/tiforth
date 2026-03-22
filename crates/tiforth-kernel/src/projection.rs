use std::sync::Arc;

use arrow_array::builder::{Int32Builder, UInt64Builder};
use arrow_array::{Array, ArrayRef, Int32Array, RecordBatch, UInt64Array};
use arrow_schema::Schema;

use crate::admission::{AdmissionController, ConsumerKind, ConsumerSpec};
use crate::batch::{append_unique_claims, OwnershipToken, TiforthBatch};
use crate::error::TiforthError;
use crate::expr::Expr;
use crate::runtime::RuntimeContext;
use crate::ArrowBatch;

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
    Int32Scalar(Option<i32>),
    UInt64Scalar(Option<u64>),
}

pub fn project_batch(
    runtime: &RuntimeContext,
    operator_name: &str,
    input: &TiforthBatch,
    projections: &[ProjectionExpr],
) -> Result<TiforthBatch, TiforthError> {
    let (output, claims) = project_output(
        input,
        projections,
        runtime.admission(),
        operator_name,
        &|consumer| runtime.new_token(consumer),
    )?;
    runtime.emit_pipe_batch(operator_name, output, claims)
}

fn project_output(
    input: &TiforthBatch,
    projections: &[ProjectionExpr],
    controller: &dyn AdmissionController,
    operator_name: &str,
    claim_factory: &dyn Fn(Arc<dyn crate::admission::AdmissionConsumer>) -> OwnershipToken,
) -> Result<(ArrowBatch, Vec<OwnershipToken>), TiforthError> {
    let schema = Arc::new(Schema::new(
        projections
            .iter()
            .map(|projection| {
                projection
                    .expr
                    .field(input.batch().schema().as_ref(), &projection.name)
            })
            .collect::<Result<Vec<_>, _>>()?,
    ));

    let mut arrays = Vec::with_capacity(projections.len());
    let mut claims = Vec::new();
    for projection in projections {
        let (array, projection_claims) = evaluate_governed_projection(
            projection,
            input,
            controller,
            operator_name,
            claim_factory,
        )?;
        arrays.push(array);
        append_unique_claims(&mut claims, projection_claims);
    }

    let batch = Arc::new(RecordBatch::try_new(schema, arrays)?);
    Ok((batch, claims))
}

fn evaluate_governed_projection(
    projection: &ProjectionExpr,
    input: &TiforthBatch,
    controller: &dyn AdmissionController,
    operator_name: &str,
    claim_factory: &dyn Fn(Arc<dyn crate::admission::AdmissionConsumer>) -> OwnershipToken,
) -> Result<(ArrayRef, Vec<OwnershipToken>), TiforthError> {
    match &projection.expr {
        Expr::Column(index) => {
            let array = input
                .batch()
                .columns()
                .get(*index)
                .cloned()
                .ok_or(TiforthError::MissingColumn { index: *index })?;
            Ok((array, input.claims().to_vec()))
        }
        Expr::LiteralInt32(value) => materialize_int32_scalar_with_claim(
            *value,
            input.batch().num_rows(),
            controller,
            operator_name,
            &projection.name,
            claim_factory,
        ),
        Expr::LiteralUInt64(value) => materialize_uint64_scalar_with_claim(
            *value,
            input.batch().num_rows(),
            controller,
            operator_name,
            &projection.name,
            claim_factory,
        ),
        Expr::AddInt32(lhs, rhs) => {
            let lhs = evaluate_value(
                lhs,
                input.batch().as_ref(),
                controller,
                operator_name,
                &projection.name,
            )?;
            let rhs = evaluate_value(
                rhs,
                input.batch().as_ref(),
                controller,
                operator_name,
                &projection.name,
            )?;
            materialize_int32_add_with_claim(
                &lhs,
                &rhs,
                input.batch().num_rows(),
                controller,
                operator_name,
                &projection.name,
                claim_factory,
            )
        }
        Expr::AddUInt64(lhs, rhs) => {
            let lhs = evaluate_value(
                lhs,
                input.batch().as_ref(),
                controller,
                operator_name,
                &projection.name,
            )?;
            let rhs = evaluate_value(
                rhs,
                input.batch().as_ref(),
                controller,
                operator_name,
                &projection.name,
            )?;
            materialize_uint64_add_with_claim(
                &lhs,
                &rhs,
                input.batch().num_rows(),
                controller,
                operator_name,
                &projection.name,
                claim_factory,
            )
        }
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
        Expr::LiteralInt32(value) => Ok(EvalValue::Int32Scalar(*value)),
        Expr::LiteralUInt64(value) => Ok(EvalValue::UInt64Scalar(*value)),
        Expr::AddInt32(lhs, rhs) => {
            let lhs = evaluate_value(lhs, batch, controller, operator_name, output_name)?;
            let rhs = evaluate_value(rhs, batch, controller, operator_name, output_name)?;
            Ok(EvalValue::Array(materialize_int32_add(
                &lhs,
                &rhs,
                batch.num_rows(),
                controller,
                operator_name,
                output_name,
            )?))
        }
        Expr::AddUInt64(lhs, rhs) => {
            let lhs = evaluate_value(lhs, batch, controller, operator_name, output_name)?;
            let rhs = evaluate_value(rhs, batch, controller, operator_name, output_name)?;
            Ok(EvalValue::Array(materialize_uint64_add(
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

fn materialize_int32_scalar_with_claim(
    value: Option<i32>,
    rows: usize,
    controller: &dyn AdmissionController,
    operator_name: &str,
    output_name: &str,
    claim_factory: &dyn Fn(Arc<dyn crate::admission::AdmissionConsumer>) -> OwnershipToken,
) -> Result<(ArrayRef, Vec<OwnershipToken>), TiforthError> {
    with_claimed_int32_array(
        rows,
        controller,
        operator_name,
        output_name,
        claim_factory,
        |builder| {
            for _ in 0..rows {
                match value {
                    Some(value) => builder.append_value(value),
                    None => builder.append_null(),
                }
            }
            Ok(())
        },
    )
}

fn materialize_uint64_scalar_with_claim(
    value: Option<u64>,
    rows: usize,
    controller: &dyn AdmissionController,
    operator_name: &str,
    output_name: &str,
    claim_factory: &dyn Fn(Arc<dyn crate::admission::AdmissionConsumer>) -> OwnershipToken,
) -> Result<(ArrayRef, Vec<OwnershipToken>), TiforthError> {
    with_claimed_uint64_array(
        rows,
        controller,
        operator_name,
        output_name,
        claim_factory,
        |builder| {
            for _ in 0..rows {
                match value {
                    Some(value) => builder.append_value(value),
                    None => builder.append_null(),
                }
            }
            Ok(())
        },
    )
}

fn materialize_int32_add(
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

fn materialize_int32_add_with_claim(
    lhs: &EvalValue,
    rhs: &EvalValue,
    rows: usize,
    controller: &dyn AdmissionController,
    operator_name: &str,
    output_name: &str,
    claim_factory: &dyn Fn(Arc<dyn crate::admission::AdmissionConsumer>) -> OwnershipToken,
) -> Result<(ArrayRef, Vec<OwnershipToken>), TiforthError> {
    with_claimed_int32_array(
        rows,
        controller,
        operator_name,
        output_name,
        claim_factory,
        |builder| {
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
        },
    )
}

fn materialize_uint64_add(
    lhs: &EvalValue,
    rhs: &EvalValue,
    rows: usize,
    controller: &dyn AdmissionController,
    operator_name: &str,
    output_name: &str,
) -> Result<ArrayRef, TiforthError> {
    with_admitted_uint64_array(rows, controller, operator_name, output_name, |builder| {
        for row in 0..rows {
            match (uint64_value(lhs, row)?, uint64_value(rhs, row)?) {
                (Some(lhs), Some(rhs)) => {
                    builder.append_value(lhs.checked_add(rhs).ok_or_else(|| {
                        TiforthError::Message(format!(
                            "uint64 overflow in {operator_name}:{output_name} at row {row}"
                        ))
                    })?)
                }
                _ => builder.append_null(),
            }
        }
        Ok(())
    })
}

fn materialize_uint64_add_with_claim(
    lhs: &EvalValue,
    rhs: &EvalValue,
    rows: usize,
    controller: &dyn AdmissionController,
    operator_name: &str,
    output_name: &str,
    claim_factory: &dyn Fn(Arc<dyn crate::admission::AdmissionConsumer>) -> OwnershipToken,
) -> Result<(ArrayRef, Vec<OwnershipToken>), TiforthError> {
    with_claimed_uint64_array(
        rows,
        controller,
        operator_name,
        output_name,
        claim_factory,
        |builder| {
            for row in 0..rows {
                match (uint64_value(lhs, row)?, uint64_value(rhs, row)?) {
                    (Some(lhs), Some(rhs)) => {
                        builder.append_value(lhs.checked_add(rhs).ok_or_else(|| {
                            TiforthError::Message(format!(
                                "uint64 overflow in {operator_name}:{output_name} at row {row}"
                            ))
                        })?)
                    }
                    _ => builder.append_null(),
                }
            }
            Ok(())
        },
    )
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
        consumer.release()?;
        return Err(error);
    }

    let array = builder.finish();
    let actual = actual_int32_array_bytes(&array);
    if estimated > actual {
        consumer.shrink(estimated - actual)?;
    }
    consumer.release()?;
    Ok(Arc::new(array))
}

fn with_claimed_int32_array<F>(
    rows: usize,
    controller: &dyn AdmissionController,
    operator_name: &str,
    output_name: &str,
    claim_factory: &dyn Fn(Arc<dyn crate::admission::AdmissionConsumer>) -> OwnershipToken,
    build: F,
) -> Result<(ArrayRef, Vec<OwnershipToken>), TiforthError>
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
        consumer.release()?;
        return Err(error);
    }

    let array = builder.finish();
    let actual = actual_int32_array_bytes(&array);
    if estimated > actual {
        consumer.shrink(estimated - actual)?;
    }

    let claim = claim_factory(consumer);
    Ok((Arc::new(array), vec![claim]))
}

fn with_admitted_uint64_array<F>(
    rows: usize,
    controller: &dyn AdmissionController,
    operator_name: &str,
    output_name: &str,
    build: F,
) -> Result<ArrayRef, TiforthError>
where
    F: FnOnce(&mut UInt64Builder) -> Result<(), TiforthError>,
{
    let consumer = controller.open(ConsumerSpec::new(
        format!("{operator_name}:{output_name}"),
        ConsumerKind::ProjectionOutput,
        false,
    ));
    let estimated = estimate_uint64_array_bytes(rows);
    consumer.try_reserve(estimated)?;

    let mut builder = UInt64Builder::with_capacity(rows);
    if let Err(error) = build(&mut builder) {
        consumer.release()?;
        return Err(error);
    }

    let array = builder.finish();
    let actual = actual_uint64_array_bytes(&array);
    if estimated > actual {
        consumer.shrink(estimated - actual)?;
    }
    consumer.release()?;
    Ok(Arc::new(array))
}

fn with_claimed_uint64_array<F>(
    rows: usize,
    controller: &dyn AdmissionController,
    operator_name: &str,
    output_name: &str,
    claim_factory: &dyn Fn(Arc<dyn crate::admission::AdmissionConsumer>) -> OwnershipToken,
    build: F,
) -> Result<(ArrayRef, Vec<OwnershipToken>), TiforthError>
where
    F: FnOnce(&mut UInt64Builder) -> Result<(), TiforthError>,
{
    let consumer = controller.open(ConsumerSpec::new(
        format!("{operator_name}:{output_name}"),
        ConsumerKind::ProjectionOutput,
        false,
    ));
    let estimated = estimate_uint64_array_bytes(rows);
    consumer.try_reserve(estimated)?;

    let mut builder = UInt64Builder::with_capacity(rows);
    if let Err(error) = build(&mut builder) {
        consumer.release()?;
        return Err(error);
    }

    let array = builder.finish();
    let actual = actual_uint64_array_bytes(&array);
    if estimated > actual {
        consumer.shrink(estimated - actual)?;
    }

    let claim = claim_factory(consumer);
    Ok((Arc::new(array), vec![claim]))
}

fn int32_value(value: &EvalValue, row: usize) -> Result<Option<i32>, TiforthError> {
    match value {
        EvalValue::Int32Scalar(value) => Ok(*value),
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
        EvalValue::UInt64Scalar(_) => Err(TiforthError::UnsupportedDataType {
            detail: "mixed signed and unsigned arithmetic is unsupported in the current checkpoint"
                .to_string(),
        }),
    }
}

fn uint64_value(value: &EvalValue, row: usize) -> Result<Option<u64>, TiforthError> {
    match value {
        EvalValue::UInt64Scalar(value) => Ok(*value),
        EvalValue::Array(array) => {
            let uint64 = array
                .as_any()
                .downcast_ref::<UInt64Array>()
                .ok_or_else(|| TiforthError::UnsupportedDataType {
                    detail: format!(
                        "expected UInt64 expression input, got {:?}",
                        array.data_type()
                    ),
                })?;
            if uint64.is_null(row) {
                Ok(None)
            } else {
                Ok(Some(uint64.value(row)))
            }
        }
        EvalValue::Int32Scalar(_) => Err(TiforthError::UnsupportedDataType {
            detail: "mixed signed and unsigned arithmetic is unsupported in the current checkpoint"
                .to_string(),
        }),
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

fn estimate_uint64_array_bytes(rows: usize) -> usize {
    rows * std::mem::size_of::<u64>() + rows.div_ceil(8)
}

fn actual_uint64_array_bytes(array: &UInt64Array) -> usize {
    let mut bytes = array.len() * std::mem::size_of::<u64>();
    if array.null_count() > 0 {
        bytes += array.len().div_ceil(8);
    }
    bytes
}
