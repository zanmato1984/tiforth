use std::sync::Arc;

use arrow_array::builder::BooleanBuilder;
use arrow_array::{Array, ArrayRef, BooleanArray, RecordBatch};
use arrow_schema::{DataType, TimeUnit};
use arrow_select::filter::filter_record_batch;

use crate::admission::{AdmissionConsumer, AdmissionController, ConsumerKind, ConsumerSpec};
use crate::batch::{BatchClaim, TiforthBatch};
use crate::error::TiforthError;
use crate::runtime::RuntimeContext;
use crate::Batch;

#[derive(Clone, Debug)]
pub enum FilterPredicate {
    IsNotNullColumn(usize),
}

impl FilterPredicate {
    pub fn is_not_null_column(index: usize) -> Self {
        Self::IsNotNullColumn(index)
    }
}

pub fn filter_batch(
    runtime: &RuntimeContext,
    operator_name: &str,
    input: &TiforthBatch,
    predicate: &FilterPredicate,
) -> Result<TiforthBatch, TiforthError> {
    let (output, column_claims) = filter_output(
        input,
        predicate,
        runtime.admission(),
        operator_name,
        &|consumer| runtime.new_claim(consumer),
    )?;
    runtime.emit_pipe_batch(operator_name, output, column_claims)
}

fn filter_output(
    input: &TiforthBatch,
    predicate: &FilterPredicate,
    controller: &dyn AdmissionController,
    operator_name: &str,
    claim_factory: &dyn Fn(Arc<dyn AdmissionConsumer>) -> BatchClaim,
) -> Result<(Batch, Vec<Vec<BatchClaim>>), TiforthError> {
    let selection = evaluate_selection(predicate, input.batch().as_ref())?;
    let consumers =
        reserve_filter_output_consumers(input.batch().as_ref(), controller, operator_name)?;
    let filtered =
        filter_record_batch(input.batch().as_ref(), &selection).map_err(TiforthError::from);
    let filtered = match filtered {
        Ok(filtered) => filtered,
        Err(error) => {
            release_consumers_best_effort(&consumers);
            return Err(error);
        }
    };

    let mut column_claims = Vec::with_capacity(filtered.num_columns());
    let filtered_columns = filtered.columns();
    for (index, filtered_array) in filtered_columns.iter().enumerate() {
        if let Err(error) = reconcile_filter_output_bytes(&consumers[index], filtered_array) {
            for consumer in consumers.iter().skip(index) {
                let _ = consumer.consumer.release();
            }
            return Err(error);
        }
        let claim = claim_factory(Arc::clone(&consumers[index].consumer));
        column_claims.push(vec![claim]);
    }

    Ok((Arc::new(filtered), column_claims))
}

struct ReservedConsumer {
    consumer: Arc<dyn AdmissionConsumer>,
    estimated_bytes: usize,
}

fn evaluate_selection(
    predicate: &FilterPredicate,
    batch: &RecordBatch,
) -> Result<BooleanArray, TiforthError> {
    match predicate {
        FilterPredicate::IsNotNullColumn(index) => {
            let column = batch
                .columns()
                .get(*index)
                .ok_or(TiforthError::MissingColumn { index: *index })?;
            validate_predicate_input_type(*index, column.data_type())?;

            let mut builder = BooleanBuilder::with_capacity(column.len());
            for row in 0..column.len() {
                builder.append_value(!column.is_null(row));
            }
            Ok(builder.finish())
        }
    }
}

fn validate_predicate_input_type(index: usize, data_type: &DataType) -> Result<(), TiforthError> {
    match data_type {
        DataType::Int32 | DataType::UInt64 | DataType::Utf8 | DataType::Date32 | DataType::Float64 => Ok(()),
        DataType::UInt8 | DataType::UInt16 | DataType::UInt32 => {
            Err(TiforthError::UnsupportedDataType {
                detail: format!(
                    "unsupported unsigned predicate input at column {index}, got {data_type:?}; first unsigned slice supports UInt64 only"
                ),
            })
        }
        DataType::Decimal128(precision, scale) => {
            validate_decimal128_metadata(index, *precision, *scale, "predicate")
        }
        DataType::Timestamp(_, _) => validate_timestamp_tz_us(index, data_type, "predicate"),
        DataType::Decimal256(_, _) => Err(TiforthError::UnsupportedDataType {
            detail: format!(
                "unsupported decimal predicate input at column {index}, got {data_type:?}; first decimal slice supports Decimal128 only"
            ),
        }),
        DataType::Float16 | DataType::Float32 => Err(TiforthError::UnsupportedDataType {
            detail: format!(
                "unsupported floating predicate input at column {index}, got {data_type:?}; first float slice supports Float64 only"
            ),
        }),
        DataType::Date64
        | DataType::Time32(_)
        | DataType::Time64(_)
        | DataType::Duration(_)
        | DataType::Interval(_) => Err(TiforthError::UnsupportedDataType {
            detail: format!(
                "unsupported temporal predicate input at column {index}, got {data_type:?}; first temporal slices support Date32 and timezone-aware Timestamp(Microsecond, <tz>) only"
            ),
        }),
        _ => Err(TiforthError::UnsupportedDataType {
            detail: format!(
                "expected Int32, UInt64, Utf8, Date32, Decimal128, Float64, or timezone-aware Timestamp(Microsecond, <tz>) predicate input at column {index}, got {data_type:?}"
            ),
        }),
    }
}

fn validate_timestamp_tz_us(
    index: usize,
    data_type: &DataType,
    surface: &str,
) -> Result<(), TiforthError> {
    match data_type {
        DataType::Timestamp(TimeUnit::Microsecond, Some(timezone)) if !timezone.is_empty() => {
            Ok(())
        }
        _ => Err(TiforthError::UnsupportedDataType {
            detail: format!(
                "unsupported temporal {surface} input at column {index}, got {data_type:?}; first temporal slices support Date32 and timezone-aware Timestamp(Microsecond, <tz>) only"
            ),
        }),
    }
}

fn validate_decimal128_metadata(
    index: usize,
    precision: u8,
    scale: i8,
    surface: &str,
) -> Result<(), TiforthError> {
    if !(1..=38).contains(&precision) || scale < 0 || scale > precision as i8 {
        return Err(TiforthError::UnsupportedDataType {
            detail: format!(
                "invalid decimal128 {surface} input metadata at column {index}: precision {precision}, scale {scale}; expected precision 1..=38 and scale 0..=precision"
            ),
        });
    }
    Ok(())
}

fn reserve_filter_output_consumers(
    batch: &RecordBatch,
    controller: &dyn AdmissionController,
    operator_name: &str,
) -> Result<Vec<ReservedConsumer>, TiforthError> {
    let mut consumers = Vec::with_capacity(batch.num_columns());
    for (index, field) in batch.schema().fields().iter().enumerate() {
        let consumer = controller.open(ConsumerSpec::new(
            format!("{operator_name}:{}", field.name()),
            ConsumerKind::FilterOutput,
            false,
        ));
        let estimated_bytes = estimate_filter_output_bytes(batch.column(index));
        if let Err(error) = consumer.try_reserve(estimated_bytes) {
            release_consumers_best_effort(&consumers);
            return Err(error);
        }
        consumers.push(ReservedConsumer {
            consumer,
            estimated_bytes,
        });
    }
    Ok(consumers)
}

fn reconcile_filter_output_bytes(
    consumer: &ReservedConsumer,
    output: &ArrayRef,
) -> Result<(), TiforthError> {
    let actual = output.get_buffer_memory_size();
    if consumer.estimated_bytes > actual {
        consumer
            .consumer
            .shrink(consumer.estimated_bytes - actual)?;
    }
    Ok(())
}

fn estimate_filter_output_bytes(input: &ArrayRef) -> usize {
    input.get_buffer_memory_size()
}

fn release_consumers_best_effort(consumers: &[ReservedConsumer]) {
    for consumer in consumers {
        let _ = consumer.consumer.release();
    }
}
