use std::fmt::{self, Display};

use arrow_schema::ArrowError;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TiforthError {
    AdmissionDenied {
        consumer: String,
        requested: usize,
        limit: usize,
    },
    MissingColumn {
        index: usize,
    },
    UnsupportedDataType {
        detail: String,
    },
    InvalidPipeInput,
    Message(String),
}

impl Display for TiforthError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::AdmissionDenied {
                consumer,
                requested,
                limit,
            } => write!(
                f,
                "admission denied for {consumer}: requested {requested} bytes with limit {limit}"
            ),
            Self::MissingColumn { index } => write!(f, "missing input column at index {index}"),
            Self::UnsupportedDataType { detail } => write!(f, "unsupported data type: {detail}"),
            Self::InvalidPipeInput => f.write_str("projection pipe expected an input batch"),
            Self::Message(message) => f.write_str(message),
        }
    }
}

impl std::error::Error for TiforthError {}

impl From<ArrowError> for TiforthError {
    fn from(value: ArrowError) -> Self {
        Self::Message(value.to_string())
    }
}

impl From<TiforthError> for ArrowError {
    fn from(value: TiforthError) -> Self {
        ArrowError::ComputeError(value.to_string())
    }
}
