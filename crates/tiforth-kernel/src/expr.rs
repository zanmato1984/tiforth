use std::sync::Arc;

use arrow_schema::{DataType, Field, Fields, Schema, TimeUnit, UnionFields, UnionMode};

use crate::error::TiforthError;

#[derive(Clone, Debug)]
pub enum Expr {
    Column(usize),
    LiteralInt32(Option<i32>),
    LiteralUInt64(Option<u64>),
    AddInt32(Box<Expr>, Box<Expr>),
    AddInt64(Box<Expr>, Box<Expr>),
    AddFloat64(Box<Expr>, Box<Expr>),
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

    pub fn add_int64(lhs: Expr, rhs: Expr) -> Self {
        Self::AddInt64(Box::new(lhs), Box::new(rhs))
    }

    pub fn add_float64(lhs: Expr, rhs: Expr) -> Self {
        Self::AddFloat64(Box::new(lhs), Box::new(rhs))
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
            Self::AddInt64(lhs, rhs) => Ok(Field::new(
                name,
                DataType::Int64,
                lhs.int64_nullable(input_schema)? || rhs.int64_nullable(input_schema)?,
            )),
            Self::AddFloat64(lhs, rhs) => Ok(Field::new(
                name,
                DataType::Float64,
                lhs.float64_nullable(input_schema)? || rhs.float64_nullable(input_schema)?,
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
            Self::AddInt64(_, _) => Err(TiforthError::UnsupportedDataType {
                detail:
                    "add<int32> operands must resolve to Int32 in the current checkpoint; add<int64> is a separate follow-on slice"
                        .to_string(),
            }),
            Self::AddFloat64(_, _) => Err(TiforthError::UnsupportedDataType {
                detail:
                    "add<int32> operands must resolve to Int32 in the current checkpoint; add<float64> is a separate follow-on slice"
                        .to_string(),
            }),
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

    fn int64_nullable(&self, input_schema: &Schema) -> Result<bool, TiforthError> {
        match self {
            Self::Column(index) => {
                let field = input_schema
                    .fields()
                    .get(*index)
                    .ok_or(TiforthError::MissingColumn { index: *index })?;
                validate_int64_arithmetic_input(*index, field.data_type())?;
                Ok(field.is_nullable())
            }
            Self::LiteralInt32(_) => Err(TiforthError::UnsupportedDataType {
                detail:
                    "first signed-widening add<int64> slice keeps operand support column-backed; literal<int32> is unsupported"
                        .to_string(),
            }),
            Self::AddInt32(_, _) => Err(TiforthError::UnsupportedDataType {
                detail:
                    "first signed-widening add<int64> slice keeps add<int64> operands column-backed; nested add<int32> operands are unsupported"
                        .to_string(),
            }),
            Self::AddFloat64(_, _) => Err(TiforthError::UnsupportedDataType {
                detail:
                    "first signed-widening add<int64> slice keeps add<float64> as a separate follow-on checkpoint"
                        .to_string(),
            }),
            Self::LiteralUInt64(_) | Self::AddUInt64(_, _) => {
                Err(TiforthError::UnsupportedDataType {
                    detail:
                        "mixed signed and unsigned arithmetic is unsupported in the current checkpoint"
                            .to_string(),
                })
            }
            Self::AddInt64(lhs, rhs) => {
                Ok(lhs.int64_nullable(input_schema)? || rhs.int64_nullable(input_schema)?)
            }
        }
    }

    fn float64_nullable(&self, input_schema: &Schema) -> Result<bool, TiforthError> {
        match self {
            Self::Column(index) => {
                let field = input_schema
                    .fields()
                    .get(*index)
                    .ok_or(TiforthError::MissingColumn { index: *index })?;
                validate_float64_arithmetic_input(*index, field.data_type())?;
                Ok(field.is_nullable())
            }
            Self::LiteralInt32(_) => Err(TiforthError::UnsupportedDataType {
                detail:
                    "first widening add<float64> slice keeps operand support column-backed; literal<int32> is unsupported"
                        .to_string(),
            }),
            Self::LiteralUInt64(_) | Self::AddUInt64(_, _) => {
                Err(TiforthError::UnsupportedDataType {
                    detail:
                        "mixed signed and unsigned arithmetic is unsupported in the current checkpoint"
                            .to_string(),
                })
            }
            Self::AddInt32(_, _) => Err(TiforthError::UnsupportedDataType {
                detail:
                    "first widening add<float64> slice keeps add<float64> operands column-backed; nested add<int32> operands are unsupported"
                        .to_string(),
            }),
            Self::AddInt64(_, _) => Err(TiforthError::UnsupportedDataType {
                detail:
                    "first widening add<float64> slice keeps add<float64> operands column-backed; nested add<int64> operands are unsupported"
                        .to_string(),
            }),
            Self::AddFloat64(_, _) => Err(TiforthError::UnsupportedDataType {
                detail:
                    "first widening add<float64> slice keeps add<float64> operands column-backed; nested add<float64> operands are unsupported"
                        .to_string(),
            }),
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
            Self::LiteralInt32(_)
            | Self::AddInt32(_, _)
            | Self::AddInt64(_, _)
            | Self::AddFloat64(_, _) => Err(TiforthError::UnsupportedDataType {
                detail:
                    "mixed signed and unsigned arithmetic is unsupported in the current checkpoint"
                        .to_string(),
            }),
            Self::LiteralUInt64(value) => Ok(value.is_none()),
            Self::AddUInt64(lhs, rhs) => {
                Ok(lhs.uint64_nullable(input_schema)? || rhs.uint64_nullable(input_schema)?)
            }
        }
    }
}

fn validate_int64_arithmetic_input(index: usize, data_type: &DataType) -> Result<(), TiforthError> {
    match data_type {
        DataType::Int32 | DataType::Int64 => Ok(()),
        DataType::Int8 | DataType::Int16 => Err(TiforthError::UnsupportedDataType {
            detail: format!(
                "unsupported signed arithmetic input at column {index}, got {data_type:?}; first signed-widening slice supports Int32 and Int64 only"
            ),
        }),
        DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64 => {
            Err(TiforthError::UnsupportedDataType {
                detail: format!(
                    "mixed signed and unsigned arithmetic is unsupported at column {index}; expected Int32 or Int64 input for signed arithmetic, got {data_type:?}"
                ),
            })
        }
        _ => Err(TiforthError::UnsupportedDataType {
            detail: format!(
                "expected Int32 or Int64 input for signed arithmetic at column {index}, got {data_type:?}"
            ),
        }),
    }
}

fn validate_float64_arithmetic_input(
    index: usize,
    data_type: &DataType,
) -> Result<(), TiforthError> {
    match data_type {
        DataType::Int32 | DataType::Int64 | DataType::Float64 => Ok(()),
        DataType::Int8 | DataType::Int16 => Err(TiforthError::UnsupportedDataType {
            detail: format!(
                "unsupported signed arithmetic input at column {index}, got {data_type:?}; first widening add<float64> slice supports Int32, Int64, and Float64 only"
            ),
        }),
        DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64 => {
            Err(TiforthError::UnsupportedDataType {
                detail: format!(
                    "mixed signed and unsigned arithmetic is unsupported at column {index}; expected Int32, Int64, or Float64 input for widening float64 arithmetic, got {data_type:?}"
                ),
            })
        }
        DataType::Float16 | DataType::Float32 => Err(TiforthError::UnsupportedDataType {
            detail: format!(
                "unsupported floating arithmetic input at column {index}, got {data_type:?}; first widening add<float64> slice supports Float64 only"
            ),
        }),
        _ => Err(TiforthError::UnsupportedDataType {
            detail: format!(
                "expected Int32, Int64, or Float64 input for widening float64 arithmetic at column {index}, got {data_type:?}"
            ),
        }),
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
        DataType::Struct(fields) => validate_struct_passthrough_input(index, fields),
        DataType::Map(field, ordered) => {
            validate_map_passthrough_input(index, field.as_ref(), *ordered)
        }
        DataType::Union(fields, mode) => validate_union_passthrough_input(index, fields, *mode),
        DataType::List(_) | DataType::LargeList(_) | DataType::FixedSizeList(_, _) => {
            Err(unsupported_nested_expression_input(index, data_type))
        }
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

fn validate_struct_passthrough_input(index: usize, fields: &Fields) -> Result<(), TiforthError> {
    if fields.len() == 2
        && fields[0].name() == "a"
        && fields[0].data_type() == &DataType::Int32
        && !fields[0].is_nullable()
        && fields[1].name() == "b"
        && fields[1].data_type() == &DataType::Int32
        && fields[1].is_nullable()
    {
        Ok(())
    } else {
        Err(unsupported_nested_expression_input(
            index,
            &DataType::Struct(fields.clone()),
        ))
    }
}

fn validate_map_passthrough_input(
    index: usize,
    field: &Field,
    ordered: bool,
) -> Result<(), TiforthError> {
    match field.data_type() {
        DataType::Struct(fields)
            if field.name() == "entries"
                && !field.is_nullable()
                && fields.len() == 2
                && fields[0].name() == "keys"
                && fields[0].data_type() == &DataType::Int32
                && !fields[0].is_nullable()
                && fields[1].name() == "values"
                && fields[1].data_type() == &DataType::Int32
                && fields[1].is_nullable() =>
        {
            Ok(())
        }
        _ => Err(unsupported_nested_expression_input(
            index,
            &DataType::Map(Arc::new(field.clone()), ordered),
        )),
    }
}

fn validate_union_passthrough_input(
    index: usize,
    fields: &UnionFields,
    mode: UnionMode,
) -> Result<(), TiforthError> {
    let supports_i_variant = fields.iter().any(|(_, field)| {
        field.name() == "i" && field.data_type() == &DataType::Int32 && !field.is_nullable()
    });
    let supports_n_variant = fields.iter().any(|(_, field)| {
        field.name() == "n" && field.data_type() == &DataType::Int32 && field.is_nullable()
    });

    if mode == UnionMode::Dense && fields.len() == 2 && supports_i_variant && supports_n_variant {
        Ok(())
    } else {
        Err(unsupported_nested_expression_input(
            index,
            &DataType::Union(fields.clone(), mode),
        ))
    }
}

fn unsupported_nested_expression_input(index: usize, data_type: &DataType) -> TiforthError {
    TiforthError::UnsupportedDataType {
        detail: format!(
            "unsupported nested expression input at column {index}, got {data_type:?}; first executable nested slices support struct<a:int32, b:int32?>, map<int32, int32?>, and dense_union<i:int32, n:int32?> only"
        ),
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
