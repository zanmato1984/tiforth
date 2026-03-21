use std::cmp::Ordering;

use arrow_array::builder::BooleanBuilder;
use arrow_array::{Array, BooleanArray, RecordBatch, StringArray};
use arrow_schema::DataType;

use crate::error::TiforthError;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CollationRef {
    Binary,
    UnicodeCi,
}

impl CollationRef {
    pub fn parse(collation_ref: &str) -> Result<Self, TiforthError> {
        match collation_ref {
            "binary" => Ok(Self::Binary),
            "unicode_ci" => Ok(Self::UnicodeCi),
            _ => Err(TiforthError::UnsupportedDataType {
                detail: format!(
                    "unsupported collation_ref {collation_ref}; first collation slice supports binary and unicode_ci only"
                ),
            }),
        }
    }
}

pub fn collation_eq_column_literal(
    batch: &RecordBatch,
    index: usize,
    literal: Option<&str>,
    collation_ref: &str,
) -> Result<BooleanArray, TiforthError> {
    evaluate_collation_comparison(batch, index, literal, collation_ref, Ordering::is_eq)
}

pub fn collation_lt_column_literal(
    batch: &RecordBatch,
    index: usize,
    literal: Option<&str>,
    collation_ref: &str,
) -> Result<BooleanArray, TiforthError> {
    evaluate_collation_comparison(batch, index, literal, collation_ref, Ordering::is_lt)
}

pub fn order_by_column_asc_indices(
    batch: &RecordBatch,
    index: usize,
    collation_ref: &str,
) -> Result<Vec<usize>, TiforthError> {
    let collation = CollationRef::parse(collation_ref)?;
    let column = utf8_column(batch, index, "ordering")?;

    let mut indices = (0..column.len()).collect::<Vec<_>>();
    indices.sort_by(|lhs, rhs| compare_row_indices(column, *lhs, *rhs, collation));

    Ok(indices)
}

fn evaluate_collation_comparison(
    batch: &RecordBatch,
    index: usize,
    literal: Option<&str>,
    collation_ref: &str,
    comparison: impl Fn(Ordering) -> bool,
) -> Result<BooleanArray, TiforthError> {
    let collation = CollationRef::parse(collation_ref)?;
    let column = utf8_column(batch, index, "comparison")?;
    let mut builder = BooleanBuilder::with_capacity(column.len());

    for row in 0..column.len() {
        match literal {
            Some(rhs) if !column.is_null(row) => {
                let lhs = column.value(row);
                let ordering = compare_strings(lhs, rhs, collation);
                builder.append_value(comparison(ordering));
            }
            _ => builder.append_null(),
        }
    }

    Ok(builder.finish())
}

fn utf8_column<'a>(
    batch: &'a RecordBatch,
    index: usize,
    surface: &str,
) -> Result<&'a StringArray, TiforthError> {
    let column = batch
        .columns()
        .get(index)
        .ok_or(TiforthError::MissingColumn { index })?;
    if column.data_type() != &DataType::Utf8 {
        return Err(TiforthError::UnsupportedDataType {
            detail: format!(
                "unsupported collation {surface} input at column {index}, got {:?}; first collation slice supports Utf8 only",
                column.data_type()
            ),
        });
    }
    column.as_any().downcast_ref::<StringArray>().ok_or_else(|| {
        TiforthError::UnsupportedDataType {
            detail: format!(
                "unsupported collation {surface} input at column {index}, got {:?}; first collation slice supports Utf8 only",
                column.data_type()
            ),
        }
    })
}

fn compare_strings(lhs: &str, rhs: &str, collation: CollationRef) -> Ordering {
    match collation {
        CollationRef::Binary => lhs.as_bytes().cmp(rhs.as_bytes()),
        CollationRef::UnicodeCi => unicode_ci_key(lhs).cmp(&unicode_ci_key(rhs)),
    }
}

fn compare_row_indices(
    column: &StringArray,
    lhs_index: usize,
    rhs_index: usize,
    collation: CollationRef,
) -> Ordering {
    match (column.is_null(lhs_index), column.is_null(rhs_index)) {
        (true, true) => lhs_index.cmp(&rhs_index),
        (true, false) => Ordering::Less,
        (false, true) => Ordering::Greater,
        (false, false) => {
            let lhs = column.value(lhs_index);
            let rhs = column.value(rhs_index);
            compare_strings(lhs, rhs, collation)
                .then_with(|| lhs.as_bytes().cmp(rhs.as_bytes()))
                .then_with(|| lhs_index.cmp(&rhs_index))
        }
    }
}

fn unicode_ci_key(value: &str) -> String {
    value.to_lowercase()
}
