mod engine;
pub mod slices;

pub use slices::{
    first_decimal128_slice, first_expression_slice, first_filter_is_not_null_slice,
    first_float64_ordering_slice, first_temporal_date32_slice, first_temporal_timestamp_tz_slice,
    first_union_slice, first_unsigned_arithmetic_slice,
};
