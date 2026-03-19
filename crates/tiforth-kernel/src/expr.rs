use arrow_schema::{DataType, Field, Schema};

use crate::error::TiforthError;

#[derive(Clone, Debug)]
pub enum Expr {
    Column(usize),
    Literal(Option<i32>),
    Add(Box<Expr>, Box<Expr>),
}

impl Expr {
    pub fn column(index: usize) -> Self {
        Self::Column(index)
    }

    pub fn literal(value: Option<i32>) -> Self {
        Self::Literal(value)
    }

    pub fn add(lhs: Expr, rhs: Expr) -> Self {
        Self::Add(Box::new(lhs), Box::new(rhs))
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
            Self::Literal(value) => Ok(Field::new(name, DataType::Int32, value.is_none())),
            Self::Add(lhs, rhs) => Ok(Field::new(
                name,
                DataType::Int32,
                lhs.int32_nullable(input_schema)? || rhs.int32_nullable(input_schema)?,
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
            Self::Literal(value) => Ok(value.is_none()),
            Self::Add(lhs, rhs) => {
                Ok(lhs.int32_nullable(input_schema)? || rhs.int32_nullable(input_schema)?)
            }
        }
    }
}

fn validate_column_input_type(index: usize, data_type: &DataType) -> Result<(), TiforthError> {
    match data_type {
        DataType::Date64
        | DataType::Time32(_)
        | DataType::Time64(_)
        | DataType::Timestamp(_, _)
        | DataType::Duration(_)
        | DataType::Interval(_) => Err(TiforthError::UnsupportedDataType {
            detail: format!(
                "unsupported temporal expression input at column {index}, got {data_type:?}; first temporal slice supports Date32 only"
            ),
        }),
        _ => Ok(()),
    }
}
