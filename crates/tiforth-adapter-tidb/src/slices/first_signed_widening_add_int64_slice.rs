use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::engine::{is_missing_column, SqlExecutionPlan};
pub use crate::engine::{
    EngineColumn, EngineExecutionError, EngineExecutionResult, ADAPTER as TIDB_ADAPTER,
    ENGINE as TIDB_ENGINE,
};

pub const FIRST_SIGNED_WIDENING_ADD_INT64_SLICE_ID: &str = "first-signed-widening-add-int64-slice";
pub const COMPARISON_MODE_ROW_ORDER_PRESERVED: &str = "row-order-preserved";

const FIRST_SIGNED_WIDENING_ADD_INT64_SLICE_SPEC_REFS: [&str; 5] = [
    "docs/design/first-signed-widening-add-int64-slice.md",
    "docs/spec/functions/numeric-add-family-completion.md",
    "docs/spec/type-system.md",
    "tests/conformance/first-signed-widening-add-int64-slice.md",
    "tests/differential/first-signed-widening-add-int64-slice.md",
];

const INT64_BASIC_INPUT_SQL: &str = concat!(
    "SELECT CAST(-7 AS SIGNED) AS s64 ",
    "UNION ALL ",
    "SELECT CAST(0 AS SIGNED) AS s64 ",
    "UNION ALL ",
    "SELECT CAST(42 AS SIGNED) AS s64"
);

const INT64_ADD_BASIC_INPUT_SQL: &str = concat!(
    "SELECT CAST(1 AS SIGNED) AS lhs, CAST(2 AS SIGNED) AS rhs ",
    "UNION ALL ",
    "SELECT CAST(-3 AS SIGNED) AS lhs, CAST(4 AS SIGNED) AS rhs ",
    "UNION ALL ",
    "SELECT CAST(10 AS SIGNED) AS lhs, CAST(20 AS SIGNED) AS rhs"
);

const INT64_ADD_NULLABLE_INPUT_SQL: &str = concat!(
    "SELECT CAST(NULL AS SIGNED) AS lhs, CAST(1 AS SIGNED) AS rhs ",
    "UNION ALL ",
    "SELECT CAST(2 AS SIGNED) AS lhs, CAST(NULL AS SIGNED) AS rhs ",
    "UNION ALL ",
    "SELECT CAST(-3 AS SIGNED) AS lhs, CAST(4 AS SIGNED) AS rhs"
);

const INT32_INT64_ADD_BASIC_INPUT_SQL: &str = concat!(
    "SELECT CAST(1 AS SIGNED) AS lhs32, CAST(2 AS SIGNED) AS rhs64 ",
    "UNION ALL ",
    "SELECT CAST(-3 AS SIGNED) AS lhs32, CAST(4 AS SIGNED) AS rhs64 ",
    "UNION ALL ",
    "SELECT CAST(100 AS SIGNED) AS lhs32, CAST(200 AS SIGNED) AS rhs64"
);

const INT64_INT32_ADD_BASIC_INPUT_SQL: &str = concat!(
    "SELECT CAST(2 AS SIGNED) AS lhs64, CAST(1 AS SIGNED) AS rhs32 ",
    "UNION ALL ",
    "SELECT CAST(4 AS SIGNED) AS lhs64, CAST(-3 AS SIGNED) AS rhs32 ",
    "UNION ALL ",
    "SELECT CAST(200 AS SIGNED) AS lhs64, CAST(100 AS SIGNED) AS rhs32"
);

const INT64_ADD_OVERFLOW_INPUT_SQL: &str = concat!(
    "SELECT CAST(9223372036854775807 AS SIGNED) AS lhs, ",
    "CAST(1 AS SIGNED) AS rhs"
);

const INT32_INT64_ADD_OVERFLOW_INPUT_SQL: &str = concat!(
    "SELECT CAST(1 AS SIGNED) AS lhs32, ",
    "CAST(9223372036854775807 AS SIGNED) AS rhs64"
);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AdapterRequest {
    pub slice_id: String,
    pub case_id: String,
    pub spec_refs: Vec<String>,
    pub input_ref: String,
    pub comparison_mode: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub projection_ref: Option<String>,
}

pub type TidbExecutionPlan = SqlExecutionPlan<AdapterRequest>;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CaseResult {
    pub slice_id: String,
    pub engine: String,
    pub adapter: String,
    pub case_id: String,
    pub spec_refs: Vec<String>,
    pub input_ref: String,
    pub comparison_mode: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub projection_ref: Option<String>,
    pub outcome: CaseOutcome,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "lowercase")]
pub enum CaseOutcome {
    Rows {
        schema: Vec<SchemaField>,
        rows: Vec<Vec<Value>>,
        row_count: usize,
    },
    Error {
        error_class: ErrorClass,
        #[serde(skip_serializing_if = "Option::is_none")]
        engine_code: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        engine_message: Option<String>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SchemaField {
    pub name: String,
    pub logical_type: String,
    pub nullable: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ErrorClass {
    MissingColumn,
    ArithmeticOverflow,
    AdapterUnavailable,
    EngineError,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AdapterRequestValidationError {
    UnsupportedSliceId(String),
    UnknownCaseId(String),
    MismatchedCaseDefinition {
        case_id: String,
        expected_input_ref: &'static str,
        actual_input_ref: String,
        expected_comparison_mode: &'static str,
        actual_comparison_mode: String,
        expected_projection_ref: Option<&'static str>,
        actual_projection_ref: Option<String>,
    },
    MismatchedSpecRefs {
        case_id: String,
        expected_spec_refs: Vec<String>,
        actual_spec_refs: Vec<String>,
    },
}

pub trait TidbRunner {
    fn run(&self, plan: &TidbExecutionPlan) -> Result<EngineExecutionResult, EngineExecutionError>;
}

#[derive(Debug, Default, Clone, Copy)]
pub struct TidbFirstSignedWideningAddInt64SliceAdapter;

impl TidbFirstSignedWideningAddInt64SliceAdapter {
    pub fn canonical_requests() -> Vec<AdapterRequest> {
        CASE_DEFINITIONS
            .iter()
            .copied()
            .map(CaseDefinition::canonical_request)
            .collect()
    }

    pub fn lower_request(
        request: &AdapterRequest,
    ) -> Result<TidbExecutionPlan, AdapterRequestValidationError> {
        let case = validate_request(request)?;

        Ok(SqlExecutionPlan::new(request.clone(), case.render_sql()))
    }

    pub fn execute<R: TidbRunner>(
        request: &AdapterRequest,
        runner: &R,
    ) -> Result<CaseResult, AdapterRequestValidationError> {
        let plan = Self::lower_request(request)?;

        let outcome = match runner.run(&plan) {
            Ok(result) => CaseOutcome::Rows {
                schema: result
                    .columns
                    .into_iter()
                    .map(|column| SchemaField {
                        name: column.name,
                        logical_type: normalize_logical_type(&column.engine_type),
                        nullable: column.nullable,
                    })
                    .collect(),
                row_count: result.rows.len(),
                rows: result.rows,
            },
            Err(error) => normalize_error(error),
        };

        Ok(CaseResult {
            slice_id: request.slice_id.clone(),
            engine: TIDB_ENGINE.to_string(),
            adapter: TIDB_ADAPTER.to_string(),
            case_id: request.case_id.clone(),
            spec_refs: request.spec_refs.clone(),
            input_ref: request.input_ref.clone(),
            comparison_mode: request.comparison_mode.clone(),
            projection_ref: request.projection_ref.clone(),
            outcome,
        })
    }
}

#[derive(Debug, Clone, Copy)]
struct CaseDefinition {
    case_id: &'static str,
    input_ref: &'static str,
    projection_ref: &'static str,
}

impl CaseDefinition {
    fn canonical_request(self) -> AdapterRequest {
        AdapterRequest {
            slice_id: FIRST_SIGNED_WIDENING_ADD_INT64_SLICE_ID.to_string(),
            case_id: self.case_id.to_string(),
            spec_refs: FIRST_SIGNED_WIDENING_ADD_INT64_SLICE_SPEC_REFS
                .iter()
                .map(|spec_ref| (*spec_ref).to_string())
                .collect(),
            input_ref: self.input_ref.to_string(),
            comparison_mode: COMPARISON_MODE_ROW_ORDER_PRESERVED.to_string(),
            projection_ref: Some(self.projection_ref.to_string()),
        }
    }

    fn render_sql(self) -> String {
        let input_sql = input_sql(self.input_ref);

        match self.projection_ref {
            "column-0" => {
                let column_name = projection_column_name(self.input_ref);
                format!(
                    "SELECT input_rows.{column_name} AS {column_name} FROM ({input_sql}) AS input_rows"
                )
            }
            "column-1" => format!(
                "SELECT input_rows.__missing_column_1 AS __missing_column_1 FROM ({input_sql}) AS input_rows"
            ),
            "add-int64-column-0-column-1" => {
                let add_expression = add_expression(self.input_ref);
                format!(
                    "SELECT CAST({add_expression} AS SIGNED) AS sum FROM ({input_sql}) AS input_rows"
                )
            }
            _ => unreachable!("validated projection refs should always be known"),
        }
    }
}

const CASE_DEFINITIONS: [CaseDefinition; 8] = [
    CaseDefinition {
        case_id: "int64-column-passthrough",
        input_ref: "first-int64-basic",
        projection_ref: "column-0",
    },
    CaseDefinition {
        case_id: "int64-add-basic",
        input_ref: "first-int64-add-basic",
        projection_ref: "add-int64-column-0-column-1",
    },
    CaseDefinition {
        case_id: "int64-add-null-propagation",
        input_ref: "first-int64-add-nullable",
        projection_ref: "add-int64-column-0-column-1",
    },
    CaseDefinition {
        case_id: "signed-widening-int32-plus-int64",
        input_ref: "first-int32-int64-add-basic",
        projection_ref: "add-int64-column-0-column-1",
    },
    CaseDefinition {
        case_id: "signed-widening-int64-plus-int32",
        input_ref: "first-int64-int32-add-basic",
        projection_ref: "add-int64-column-0-column-1",
    },
    CaseDefinition {
        case_id: "int64-add-overflow-error",
        input_ref: "first-int64-add-overflow",
        projection_ref: "add-int64-column-0-column-1",
    },
    CaseDefinition {
        case_id: "signed-widening-int32-plus-int64-overflow-error",
        input_ref: "first-int32-int64-add-overflow",
        projection_ref: "add-int64-column-0-column-1",
    },
    CaseDefinition {
        case_id: "int64-missing-column-error",
        input_ref: "first-int64-basic",
        projection_ref: "column-1",
    },
];

fn validate_request(
    request: &AdapterRequest,
) -> Result<CaseDefinition, AdapterRequestValidationError> {
    if request.slice_id != FIRST_SIGNED_WIDENING_ADD_INT64_SLICE_ID {
        return Err(AdapterRequestValidationError::UnsupportedSliceId(
            request.slice_id.clone(),
        ));
    }

    let case = CASE_DEFINITIONS
        .iter()
        .copied()
        .find(|candidate| candidate.case_id == request.case_id)
        .ok_or_else(|| AdapterRequestValidationError::UnknownCaseId(request.case_id.clone()))?;

    if request.input_ref != case.input_ref
        || request.comparison_mode != COMPARISON_MODE_ROW_ORDER_PRESERVED
        || request.projection_ref.as_deref() != Some(case.projection_ref)
    {
        return Err(AdapterRequestValidationError::MismatchedCaseDefinition {
            case_id: request.case_id.clone(),
            expected_input_ref: case.input_ref,
            actual_input_ref: request.input_ref.clone(),
            expected_comparison_mode: COMPARISON_MODE_ROW_ORDER_PRESERVED,
            actual_comparison_mode: request.comparison_mode.clone(),
            expected_projection_ref: Some(case.projection_ref),
            actual_projection_ref: request.projection_ref.clone(),
        });
    }

    let expected_spec_refs: Vec<String> = FIRST_SIGNED_WIDENING_ADD_INT64_SLICE_SPEC_REFS
        .iter()
        .map(|spec_ref| (*spec_ref).to_string())
        .collect();
    if request.spec_refs != expected_spec_refs {
        return Err(AdapterRequestValidationError::MismatchedSpecRefs {
            case_id: request.case_id.clone(),
            expected_spec_refs,
            actual_spec_refs: request.spec_refs.clone(),
        });
    }

    Ok(case)
}

fn input_sql(input_ref: &str) -> &'static str {
    match input_ref {
        "first-int64-basic" => INT64_BASIC_INPUT_SQL,
        "first-int64-add-basic" => INT64_ADD_BASIC_INPUT_SQL,
        "first-int64-add-nullable" => INT64_ADD_NULLABLE_INPUT_SQL,
        "first-int32-int64-add-basic" => INT32_INT64_ADD_BASIC_INPUT_SQL,
        "first-int64-int32-add-basic" => INT64_INT32_ADD_BASIC_INPUT_SQL,
        "first-int64-add-overflow" => INT64_ADD_OVERFLOW_INPUT_SQL,
        "first-int32-int64-add-overflow" => INT32_INT64_ADD_OVERFLOW_INPUT_SQL,
        _ => unreachable!("validated input refs should always be known"),
    }
}

fn projection_column_name(input_ref: &str) -> &'static str {
    match input_ref {
        "first-int64-basic" => "s64",
        _ => unreachable!("validated projection column refs should always be known"),
    }
}

fn add_expression(input_ref: &str) -> &'static str {
    match input_ref {
        "first-int64-add-basic" | "first-int64-add-nullable" | "first-int64-add-overflow" => {
            "input_rows.lhs + input_rows.rhs"
        }
        "first-int32-int64-add-basic" | "first-int32-int64-add-overflow" => {
            "CAST(input_rows.lhs32 AS SIGNED) + input_rows.rhs64"
        }
        "first-int64-int32-add-basic" => "input_rows.lhs64 + CAST(input_rows.rhs32 AS SIGNED)",
        _ => unreachable!("validated add refs should always be known"),
    }
}

fn normalize_error(error: EngineExecutionError) -> CaseOutcome {
    match error {
        EngineExecutionError::AdapterUnavailable { message } => CaseOutcome::Error {
            error_class: ErrorClass::AdapterUnavailable,
            engine_code: None,
            engine_message: message,
        },
        EngineExecutionError::EngineFailure { code, message } => {
            let error_class = if is_missing_column(code.as_deref(), &message) {
                ErrorClass::MissingColumn
            } else if is_arithmetic_overflow(code.as_deref(), &message) {
                ErrorClass::ArithmeticOverflow
            } else {
                ErrorClass::EngineError
            };

            CaseOutcome::Error {
                error_class,
                engine_code: code,
                engine_message: Some(message),
            }
        }
    }
}

fn is_arithmetic_overflow(engine_code: Option<&str>, engine_message: &str) -> bool {
    let normalized_message = engine_message.to_ascii_lowercase();

    engine_code == Some("1690")
        || ((normalized_message.contains("overflow")
            || normalized_message.contains("out of range"))
            && (normalized_message.contains("bigint")
                || normalized_message.contains("int64")
                || normalized_message.contains("signed")))
}

fn normalize_logical_type(engine_type: &str) -> String {
    let normalized = engine_type.trim().to_ascii_lowercase();
    let normalized = normalized.split('(').next().unwrap_or(&normalized).trim();

    match normalized {
        "int" | "integer" | "mediumint" | "smallint" | "tinyint" | "int32" => "int32".to_string(),
        "bigint" | "signed" | "signed integer" | "int64" => "int64".to_string(),
        other => other.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn canonical_requests_cover_all_documented_cases() {
        let requests = TidbFirstSignedWideningAddInt64SliceAdapter::canonical_requests();
        let case_ids: Vec<&str> = requests
            .iter()
            .map(|request| request.case_id.as_str())
            .collect();

        assert_eq!(requests.len(), 8);
        assert_eq!(
            case_ids,
            vec![
                "int64-column-passthrough",
                "int64-add-basic",
                "int64-add-null-propagation",
                "signed-widening-int32-plus-int64",
                "signed-widening-int64-plus-int32",
                "int64-add-overflow-error",
                "signed-widening-int32-plus-int64-overflow-error",
                "int64-missing-column-error",
            ]
        );
    }

    #[test]
    fn execute_rows_normalizes_schema_and_row_count() {
        let request = TidbFirstSignedWideningAddInt64SliceAdapter::canonical_requests()
            .into_iter()
            .find(|request| request.case_id == "signed-widening-int32-plus-int64")
            .unwrap();
        let runner = StubRunner::rows(
            vec![EngineColumn {
                name: "sum".to_string(),
                engine_type: "bigint".to_string(),
                nullable: false,
            }],
            vec![vec![json!(3)], vec![json!(1)], vec![json!(300)]],
        );

        let result =
            TidbFirstSignedWideningAddInt64SliceAdapter::execute(&request, &runner).unwrap();

        assert_eq!(
            result.outcome,
            CaseOutcome::Rows {
                schema: vec![SchemaField {
                    name: "sum".to_string(),
                    logical_type: "int64".to_string(),
                    nullable: false,
                }],
                rows: vec![vec![json!(3)], vec![json!(1)], vec![json!(300)]],
                row_count: 3,
            }
        );
    }

    #[test]
    fn execute_normalizes_arithmetic_overflow_errors() {
        let request = TidbFirstSignedWideningAddInt64SliceAdapter::canonical_requests()
            .into_iter()
            .find(|request| request.case_id == "int64-add-overflow-error")
            .unwrap();
        let runner = StubRunner::error(EngineExecutionError::EngineFailure {
            code: Some("1690".to_string()),
            message: "BIGINT value is out of range in '(input_rows.lhs + input_rows.rhs)'"
                .to_string(),
        });

        let result =
            TidbFirstSignedWideningAddInt64SliceAdapter::execute(&request, &runner).unwrap();

        assert_eq!(
            result.outcome,
            CaseOutcome::Error {
                error_class: ErrorClass::ArithmeticOverflow,
                engine_code: Some("1690".to_string()),
                engine_message: Some(
                    "BIGINT value is out of range in '(input_rows.lhs + input_rows.rhs)'"
                        .to_string()
                ),
            }
        );
    }

    #[test]
    fn lowering_rejects_requests_with_mismatched_projection_refs() {
        let mut request = TidbFirstSignedWideningAddInt64SliceAdapter::canonical_requests()
            .into_iter()
            .find(|request| request.case_id == "int64-add-basic")
            .unwrap();
        request.projection_ref = Some("column-0".to_string());

        let error =
            TidbFirstSignedWideningAddInt64SliceAdapter::lower_request(&request).unwrap_err();

        assert_eq!(
            error,
            AdapterRequestValidationError::MismatchedCaseDefinition {
                case_id: "int64-add-basic".to_string(),
                expected_input_ref: "first-int64-add-basic",
                actual_input_ref: "first-int64-add-basic".to_string(),
                expected_comparison_mode: COMPARISON_MODE_ROW_ORDER_PRESERVED,
                actual_comparison_mode: COMPARISON_MODE_ROW_ORDER_PRESERVED.to_string(),
                expected_projection_ref: Some("add-int64-column-0-column-1"),
                actual_projection_ref: Some("column-0".to_string()),
            }
        );
    }

    struct StubRunner {
        result: Result<EngineExecutionResult, EngineExecutionError>,
    }

    impl StubRunner {
        fn rows(columns: Vec<EngineColumn>, rows: Vec<Vec<Value>>) -> Self {
            Self {
                result: Ok(EngineExecutionResult { columns, rows }),
            }
        }

        fn error(error: EngineExecutionError) -> Self {
            Self { result: Err(error) }
        }
    }

    impl TidbRunner for StubRunner {
        fn run(
            &self,
            _plan: &TidbExecutionPlan,
        ) -> Result<EngineExecutionResult, EngineExecutionError> {
            self.result.clone()
        }
    }
}
