use arrow_schema::{DataType, Field, Schema, TimeUnit};

use crate::error::TiforthError;

#[derive(Clone, Debug)]
pub enum Expr {
    Column(usize),
    LiteralInt32(Option<i32>),
    LiteralUInt64(Option<u64>),
    AddInt32(Box<Expr>, Box<Expr>),
    AddUInt64(Box<Expr>, Box<Expr>),
}

impl Expr {
    pub fn column(index: usize) -> Self {
        Self::Column(index)
    }

    pub fn literal(value: Option<i32>) -> Self {
        Self::LiteralInt32(value)
    }

    pub fn add(lhs: Expr, rhs: Expr) -> Self {
        Self::AddInt32(Box::new(lhs), Box::new(rhs))
    }

    pub fn literal_uint64(value: Option<u64>) -> Self {
        Self::LiteralUInt64(value)
    }

    pub fn add_uint64(lhs: Expr, rhs: Expr) -> Self {
        Self::AddUInt64(Box::new(lhs), Box::new(rhs))
    }

    pub fn field(&self, input_schema: &Schema, name: &str) -> Result<Field, TiforthError> {
        match self {
            Self::Column(index) => {
                let field = input_schema
                    .fields()
                    .get(*index)
                    .ok_or(TiforthError::MissingColumn { index: *index })?;
                validate_column_input_type(*index, field.data_type())?;
                Ok(Field::new(
                    name,
                    field.data_type().clone(),
                    field.is_nullable(),
                ))
            }
            Self::LiteralInt32(value) => Ok(Field::new(name, DataType::Int32, value.is_none())),
            Self::LiteralUInt64(value) => Ok(Field::new(name, DataType::UInt64, value.is_none())),
            Self::AddInt32(lhs, rhs) => Ok(Field::new(
                name,
                DataType::Int32,
                lhs.int32_nullable(input_schema)? || rhs.int32_nullable(input_schema)?,
            )),
            Self::AddUInt64(lhs, rhs) => Ok(Field::new(
                name,
                DataType::UInt64,
                lhs.uint64_nullable(input_schema)? || rhs.uint64_nullable(input_schema)?,
            )),
        }
    }

    fn int32_nullable(&self, input_schema: &Schema) -> Result<bool, TiforthError> {
        match self {
            Self::Column(index) => {
                let field = input_schema
                    .fields()
                    .get(*index)
                    .ok_or(TiforthError::MissingColumn { index: *index })?;
                if field.data_type() != &DataType::Int32 {
                    return Err(TiforthError::UnsupportedDataType {
                        detail: format!(
                            "expected Int32 input for arithmetic at column {index}, got {:?}",
                            field.data_type()
                        ),
                    });
                }
                Ok(field.is_nullable())
            }
            Self::LiteralInt32(value) => Ok(value.is_none()),
            Self::LiteralUInt64(_) | Self::AddUInt64(_, _) => {
                Err(TiforthError::UnsupportedDataType {
                    detail:
                        "mixed signed and unsigned arithmetic is unsupported in the current checkpoint"
                            .to_string(),
                })
            }
            Self::AddInt32(lhs, rhs) => {
                Ok(lhs.int32_nullable(input_schema)? || rhs.int32_nullable(input_schema)?)
            }
        }
    }

    fn uint64_nullable(&self, input_schema: &Schema) -> Result<bool, TiforthError> {
        match self {
            Self::Column(index) => {
                let field = input_schema
                    .fields()
                    .get(*index)
                    .ok_or(TiforthError::MissingColumn { index: *index })?;
                validate_uint64_arithmetic_input(*index, field.data_type())?;
                Ok(field.is_nullable())
            }
            Self::LiteralInt32(_) | Self::AddInt32(_, _) => {
                Err(TiforthError::UnsupportedDataType {
                    detail:
                        "mixed signed and unsigned arithmetic is unsupported in the current checkpoint"
                            .to_string(),
                })
            }
            Self::LiteralUInt64(value) => Ok(value.is_none()),
            Self::AddUInt64(lhs, rhs) => {
                Ok(lhs.uint64_nullable(input_schema)? || rhs.uint64_nullable(input_schema)?)
            }
        }
    }
}

fn validate_column_input_type(index: usize, data_type: &DataType) -> Result<(), TiforthError> {
    match data_type {
        DataType::UInt64 => Ok(()),
        DataType::UInt8 | DataType::UInt16 | DataType::UInt32 => {
            Err(TiforthError::UnsupportedDataType {
                detail: format!(
                    "unsupported unsigned expression input at column {index}, got {data_type:?}; first unsigned slice supports UInt64 only"
                ),
            })
        }
        DataType::Decimal128(precision, scale) => {
            validate_decimal128_metadata(index, *precision, *scale, "expression")
        }
        DataType::Decimal256(_, _) => Err(TiforthError::UnsupportedDataType {
            detail: format!(
                "unsupported decimal expression input at column {index}, got {data_type:?}; first decimal slice supports Decimal128 only"
            ),
        }),
        DataType::Float16 | DataType::Float32 => Err(TiforthError::UnsupportedDataType {
            detail: format!(
                "unsupported floating expression input at column {index}, got {data_type:?}; first float slice supports Float64 only"
            ),
        }),
        DataType::Timestamp(_, _) => validate_timestamp_tz_us(index, data_type, "expression"),
        DataType::Date64
        | DataType::Time32(_)
        | DataType::Time64(_)
        | DataType::Duration(_)
        | DataType::Interval(_) => Err(TiforthError::UnsupportedDataType {
            detail: format!(
                "unsupported temporal expression input at column {index}, got {data_type:?}; first temporal slices support Date32 and timezone-aware Timestamp(Microsecond, <tz>) only"
            ),
        }),
        _ => Ok(()),
    }
}

fn validate_uint64_arithmetic_input(
    index: usize,
    data_type: &DataType,
) -> Result<(), TiforthError> {
    match data_type {
        DataType::UInt64 => Ok(()),
        DataType::UInt8 | DataType::UInt16 | DataType::UInt32 => {
            Err(TiforthError::UnsupportedDataType {
                detail: format!(
                    "unsupported unsigned arithmetic input at column {index}, got {data_type:?}; first unsigned slice supports UInt64 only"
                ),
            })
        }
        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => {
            Err(TiforthError::UnsupportedDataType {
                detail: format!(
                    "mixed signed and unsigned arithmetic is unsupported at column {index}; expected UInt64 input for unsigned arithmetic, got {data_type:?}"
                ),
            })
        }
        _ => Err(TiforthError::UnsupportedDataType {
            detail: format!(
                "expected UInt64 input for unsigned arithmetic at column {index}, got {data_type:?}"
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
