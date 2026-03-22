use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::engine::{is_missing_column, normalize_json_value, SqlExecutionPlan};
pub use crate::engine::{
    EngineColumn, EngineExecutionError, EngineExecutionResult, ADAPTER as TIDB_ADAPTER,
    ENGINE as TIDB_ENGINE,
};

pub const FIRST_JSON_SLICE_ID: &str = "first-json-slice";
const FIRST_JSON_SLICE_SPEC_REFS: [&str; 4] = [
    "docs/design/first-json-semantic-slice.md",
    "docs/spec/type-system.md",
    "tests/conformance/first-json-slice.md",
    "tests/differential/first-json-slice.md",
];

const JSON_BASIC_INPUT_SQL: &str = concat!(
    "SELECT CAST('{\\\"a\\\":1}' AS JSON) AS j ",
    "UNION ALL ",
    "SELECT CAST('[1,2]' AS JSON) AS j ",
    "UNION ALL ",
    "SELECT CAST('true' AS JSON) AS j"
);

const JSON_NULLABLE_INPUT_SQL: &str = concat!(
    "SELECT CAST('{\\\"a\\\":1}' AS JSON) AS j ",
    "UNION ALL ",
    "SELECT CAST(NULL AS JSON) AS j ",
    "UNION ALL ",
    "SELECT CAST('null' AS JSON) AS j ",
    "UNION ALL ",
    "SELECT CAST('{\\\"b\\\":2,\\\"a\\\":1}' AS JSON) AS j"
);

const JSON_ALL_NULL_INPUT_SQL: &str = concat!(
    "SELECT CAST(NULL AS JSON) AS j ",
    "UNION ALL ",
    "SELECT CAST(NULL AS JSON) AS j ",
    "UNION ALL ",
    "SELECT CAST(NULL AS JSON) AS j"
);

const JSON_ARRAY_ORDER_INPUT_SQL: &str = concat!(
    "SELECT CAST('[1,2]' AS JSON) AS j ",
    "UNION ALL ",
    "SELECT CAST('[2,1]' AS JSON) AS j"
);

const UTF8_JSON_TEXT_INPUT_SQL: &str = "SELECT CAST('{\"a\":1}' AS CHAR) AS s";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AdapterRequest {
    pub slice_id: String,
    pub case_id: String,
    pub spec_refs: Vec<String>,
    pub input_ref: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub projection_ref: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub filter_ref: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub comparison_ref: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cast_ref: Option<String>,
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
    #[serde(skip_serializing_if = "Option::is_none")]
    pub projection_ref: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub filter_ref: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub comparison_ref: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cast_ref: Option<String>,
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
    UnsupportedJsonComparison,
    UnsupportedJsonCast,
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
        expected_projection_ref: Option<&'static str>,
        actual_projection_ref: Option<String>,
        expected_filter_ref: Option<&'static str>,
        actual_filter_ref: Option<String>,
        expected_comparison_ref: Option<&'static str>,
        actual_comparison_ref: Option<String>,
        expected_cast_ref: Option<&'static str>,
        actual_cast_ref: Option<String>,
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
pub struct TidbFirstJsonSliceAdapter;

impl TidbFirstJsonSliceAdapter {
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
            Ok(result) => {
                let schema: Vec<SchemaField> = result
                    .columns
                    .into_iter()
                    .map(|column| SchemaField {
                        name: column.name,
                        logical_type: normalize_logical_type(&column.engine_type),
                        nullable: column.nullable,
                    })
                    .collect();
                let row_count = result.rows.len();
                let rows = normalize_rows_for_schema(result.rows, &schema);

                CaseOutcome::Rows {
                    schema,
                    rows,
                    row_count,
                }
            }
            Err(error) => normalize_error(error),
        };

        Ok(CaseResult {
            slice_id: request.slice_id.clone(),
            engine: TIDB_ENGINE.to_string(),
            adapter: TIDB_ADAPTER.to_string(),
            case_id: request.case_id.clone(),
            spec_refs: request.spec_refs.clone(),
            input_ref: request.input_ref.clone(),
            projection_ref: request.projection_ref.clone(),
            filter_ref: request.filter_ref.clone(),
            comparison_ref: request.comparison_ref.clone(),
            cast_ref: request.cast_ref.clone(),
            outcome,
        })
    }
}

#[derive(Debug, Clone, Copy)]
struct CaseDefinition {
    case_id: &'static str,
    input_ref: &'static str,
    projection_ref: Option<&'static str>,
    filter_ref: Option<&'static str>,
    comparison_ref: Option<&'static str>,
    cast_ref: Option<&'static str>,
}

impl CaseDefinition {
    fn canonical_request(self) -> AdapterRequest {
        AdapterRequest {
            slice_id: FIRST_JSON_SLICE_ID.to_string(),
            case_id: self.case_id.to_string(),
            spec_refs: FIRST_JSON_SLICE_SPEC_REFS
                .iter()
                .map(|spec_ref| (*spec_ref).to_string())
                .collect(),
            input_ref: self.input_ref.to_string(),
            projection_ref: self.projection_ref.map(str::to_string),
            filter_ref: self.filter_ref.map(str::to_string),
            comparison_ref: self.comparison_ref.map(str::to_string),
            cast_ref: self.cast_ref.map(str::to_string),
        }
    }

    fn render_sql(self) -> String {
        let input_sql = input_sql(self.input_ref);

        match (
            self.projection_ref,
            self.filter_ref,
            self.comparison_ref,
            self.cast_ref,
        ) {
            (Some("column-0"), None, None, None) => {
                let column_name = projection_column_name(self.input_ref);
                format!(
                    "SELECT input_rows.{column_name} AS {column_name} FROM ({input_sql}) AS input_rows"
                )
            }
            (None, Some(filter_ref), None, None) => {
                let filter_condition = filter_condition(self.input_ref, filter_ref);
                format!("SELECT * FROM ({input_sql}) AS input_rows WHERE {filter_condition}")
            }
            (None, None, Some("less-than-column-0-column-0"), None) => {
                format!(
                    "SELECT (input_rows.j < input_rows.j) AS cmp FROM ({input_sql}) AS input_rows"
                )
            }
            (None, None, None, Some("json-column-0-to-int32")) => {
                format!("SELECT CAST(input_rows.j AS SIGNED) AS cast_value FROM ({input_sql}) AS input_rows")
            }
            (None, None, None, Some("utf8-column-0-to-json")) => {
                format!("SELECT CAST(input_rows.s AS JSON) AS cast_value FROM ({input_sql}) AS input_rows")
            }
            _ => unreachable!("validated case definitions always set exactly one operation ref"),
        }
    }
}

const CASE_DEFINITIONS: [CaseDefinition; 11] = [
    CaseDefinition {
        case_id: "json-column-passthrough",
        input_ref: "first-json-basic",
        projection_ref: Some("column-0"),
        filter_ref: None,
        comparison_ref: None,
        cast_ref: None,
    },
    CaseDefinition {
        case_id: "json-column-null-preserve",
        input_ref: "first-json-nullable",
        projection_ref: Some("column-0"),
        filter_ref: None,
        comparison_ref: None,
        cast_ref: None,
    },
    CaseDefinition {
        case_id: "json-object-canonicalization",
        input_ref: "first-json-nullable",
        projection_ref: Some("column-0"),
        filter_ref: None,
        comparison_ref: None,
        cast_ref: None,
    },
    CaseDefinition {
        case_id: "json-array-order-preserved",
        input_ref: "first-json-array-order",
        projection_ref: Some("column-0"),
        filter_ref: None,
        comparison_ref: None,
        cast_ref: None,
    },
    CaseDefinition {
        case_id: "json-is-not-null-all-kept",
        input_ref: "first-json-basic",
        projection_ref: None,
        filter_ref: Some("is-not-null-column-0"),
        comparison_ref: None,
        cast_ref: None,
    },
    CaseDefinition {
        case_id: "json-is-not-null-all-dropped",
        input_ref: "first-json-all-null",
        projection_ref: None,
        filter_ref: Some("is-not-null-column-0"),
        comparison_ref: None,
        cast_ref: None,
    },
    CaseDefinition {
        case_id: "json-is-not-null-mixed-keep-drop",
        input_ref: "first-json-nullable",
        projection_ref: None,
        filter_ref: Some("is-not-null-column-0"),
        comparison_ref: None,
        cast_ref: None,
    },
    CaseDefinition {
        case_id: "json-missing-column-error",
        input_ref: "first-json-basic",
        projection_ref: None,
        filter_ref: Some("is-not-null-column-1"),
        comparison_ref: None,
        cast_ref: None,
    },
    CaseDefinition {
        case_id: "unsupported-json-ordering-comparison-error",
        input_ref: "first-json-basic",
        projection_ref: None,
        filter_ref: None,
        comparison_ref: Some("less-than-column-0-column-0"),
        cast_ref: None,
    },
    CaseDefinition {
        case_id: "unsupported-json-cast-error",
        input_ref: "first-json-basic",
        projection_ref: None,
        filter_ref: None,
        comparison_ref: None,
        cast_ref: Some("json-column-0-to-int32"),
    },
    CaseDefinition {
        case_id: "unsupported-cast-to-json-error",
        input_ref: "first-utf8-json-text",
        projection_ref: None,
        filter_ref: None,
        comparison_ref: None,
        cast_ref: Some("utf8-column-0-to-json"),
    },
];

fn validate_request(
    request: &AdapterRequest,
) -> Result<CaseDefinition, AdapterRequestValidationError> {
    if request.slice_id != FIRST_JSON_SLICE_ID {
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
        || request.projection_ref.as_deref() != case.projection_ref
        || request.filter_ref.as_deref() != case.filter_ref
        || request.comparison_ref.as_deref() != case.comparison_ref
        || request.cast_ref.as_deref() != case.cast_ref
    {
        return Err(AdapterRequestValidationError::MismatchedCaseDefinition {
            case_id: request.case_id.clone(),
            expected_input_ref: case.input_ref,
            actual_input_ref: request.input_ref.clone(),
            expected_projection_ref: case.projection_ref,
            actual_projection_ref: request.projection_ref.clone(),
            expected_filter_ref: case.filter_ref,
            actual_filter_ref: request.filter_ref.clone(),
            expected_comparison_ref: case.comparison_ref,
            actual_comparison_ref: request.comparison_ref.clone(),
            expected_cast_ref: case.cast_ref,
            actual_cast_ref: request.cast_ref.clone(),
        });
    }

    let expected_spec_refs: Vec<String> = FIRST_JSON_SLICE_SPEC_REFS
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
        "first-json-basic" => JSON_BASIC_INPUT_SQL,
        "first-json-nullable" => JSON_NULLABLE_INPUT_SQL,
        "first-json-all-null" => JSON_ALL_NULL_INPUT_SQL,
        "first-json-array-order" => JSON_ARRAY_ORDER_INPUT_SQL,
        "first-utf8-json-text" => UTF8_JSON_TEXT_INPUT_SQL,
        _ => unreachable!("validated input refs should always be known"),
    }
}

fn projection_column_name(input_ref: &str) -> &'static str {
    match input_ref {
        "first-json-basic"
        | "first-json-nullable"
        | "first-json-all-null"
        | "first-json-array-order" => "j",
        "first-utf8-json-text" => "s",
        _ => unreachable!("validated input refs should always be known"),
    }
}

fn filter_condition(input_ref: &str, filter_ref: &str) -> &'static str {
    match filter_ref {
        "is-not-null-column-0" => match input_ref {
            "first-json-basic"
            | "first-json-nullable"
            | "first-json-all-null"
            | "first-json-array-order" => "input_rows.j IS NOT NULL",
            "first-utf8-json-text" => "input_rows.s IS NOT NULL",
            _ => unreachable!("validated input refs should always be known"),
        },
        "is-not-null-column-1" => "input_rows.__missing_column_1 IS NOT NULL",
        _ => unreachable!("validated filter refs should always be known"),
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
            } else if is_unsupported_json_comparison(code.as_deref(), &message) {
                ErrorClass::UnsupportedJsonComparison
            } else if is_unsupported_json_cast(code.as_deref(), &message) {
                ErrorClass::UnsupportedJsonCast
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

fn is_unsupported_json_comparison(engine_code: Option<&str>, engine_message: &str) -> bool {
    let normalized_message = engine_message.to_ascii_lowercase();

    engine_code == Some("TFJ001")
        || normalized_message.contains("unsupported json comparison")
        || normalized_message.contains("json ordering")
}

fn is_unsupported_json_cast(engine_code: Option<&str>, engine_message: &str) -> bool {
    let normalized_message = engine_message.to_ascii_lowercase();

    engine_code == Some("TFJ002")
        || normalized_message.contains("unsupported json cast")
        || normalized_message.contains("cast to json")
        || normalized_message.contains("cast from json")
}

fn normalize_logical_type(engine_type: &str) -> String {
    let normalized = engine_type.trim().to_ascii_lowercase();
    let normalized_base = normalized.split('(').next().unwrap_or(&normalized).trim();

    if normalized_base.contains("json") {
        return "json".to_string();
    }

    match normalized_base {
        "char" | "varchar" | "text" => "utf8".to_string(),
        "int" | "integer" | "mediumint" | "smallint" | "tinyint" => "int32".to_string(),
        other => other.to_string(),
    }
}

fn normalize_rows_for_schema(rows: Vec<Vec<Value>>, schema: &[SchemaField]) -> Vec<Vec<Value>> {
    rows.into_iter()
        .map(|row| {
            row.into_iter()
                .enumerate()
                .map(|(index, value)| match schema.get(index) {
                    Some(field) if field.logical_type == "json" => normalize_json_value(value),
                    _ => value,
                })
                .collect()
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn canonical_requests_cover_all_documented_cases() {
        let requests = TidbFirstJsonSliceAdapter::canonical_requests();
        let case_ids: Vec<&str> = requests
            .iter()
            .map(|request| request.case_id.as_str())
            .collect();

        assert_eq!(requests.len(), 11);
        assert_eq!(
            case_ids,
            vec![
                "json-column-passthrough",
                "json-column-null-preserve",
                "json-object-canonicalization",
                "json-array-order-preserved",
                "json-is-not-null-all-kept",
                "json-is-not-null-all-dropped",
                "json-is-not-null-mixed-keep-drop",
                "json-missing-column-error",
                "unsupported-json-ordering-comparison-error",
                "unsupported-json-cast-error",
                "unsupported-cast-to-json-error",
            ]
        );

        for request in requests {
            let op_ref_count = request.projection_ref.iter().count()
                + request.filter_ref.iter().count()
                + request.comparison_ref.iter().count()
                + request.cast_ref.iter().count();
            assert_eq!(op_ref_count, 1);
        }
    }

    #[test]
    fn lowering_renders_tidb_sql_for_each_documented_case() {
        let requests = TidbFirstJsonSliceAdapter::canonical_requests();

        for request in requests {
            let plan = TidbFirstJsonSliceAdapter::lower_request(&request).unwrap();
            assert!(!plan.sql.is_empty());
        }

        let comparison_request = TidbFirstJsonSliceAdapter::canonical_requests()
            .into_iter()
            .find(|request| request.case_id == "unsupported-json-ordering-comparison-error")
            .unwrap();
        let comparison_plan =
            TidbFirstJsonSliceAdapter::lower_request(&comparison_request).unwrap();
        assert!(comparison_plan.sql.contains("input_rows.j < input_rows.j"));

        let cast_request = TidbFirstJsonSliceAdapter::canonical_requests()
            .into_iter()
            .find(|request| request.case_id == "unsupported-cast-to-json-error")
            .unwrap();
        let cast_plan = TidbFirstJsonSliceAdapter::lower_request(&cast_request).unwrap();
        assert!(cast_plan.sql.contains("CAST(input_rows.s AS JSON)"));
    }

    #[test]
    fn execute_rows_normalizes_json_schema_and_tokens() {
        let request = TidbFirstJsonSliceAdapter::canonical_requests()
            .into_iter()
            .find(|request| request.case_id == "json-object-canonicalization")
            .unwrap();
        let runner = StubRunner::rows(
            vec![EngineColumn {
                name: "j".to_string(),
                engine_type: "json".to_string(),
                nullable: true,
            }],
            vec![
                vec![json!("{\"b\":2,\"a\":1}")],
                vec![json!(null)],
                vec![json!({"a": 1, "b": 2})],
            ],
        );

        let result = TidbFirstJsonSliceAdapter::execute(&request, &runner).unwrap();

        assert_eq!(result.engine, TIDB_ENGINE);
        assert_eq!(result.adapter, TIDB_ADAPTER);
        assert_eq!(
            result.outcome,
            CaseOutcome::Rows {
                schema: vec![SchemaField {
                    name: "j".to_string(),
                    logical_type: "json".to_string(),
                    nullable: true,
                }],
                rows: vec![
                    vec![json!("{\"a\":1,\"b\":2}")],
                    vec![json!(null)],
                    vec![json!("{\"a\":1,\"b\":2}")],
                ],
                row_count: 3,
            }
        );
    }

    #[test]
    fn execute_normalizes_missing_column_and_unsupported_json_errors() {
        let missing_request = TidbFirstJsonSliceAdapter::canonical_requests()
            .into_iter()
            .find(|request| request.case_id == "json-missing-column-error")
            .unwrap();
        let missing_runner = StubRunner::error(EngineExecutionError::EngineFailure {
            code: Some("1054".to_string()),
            message: "Unknown column '__missing_column_1' in 'where clause'".to_string(),
        });
        let missing_result =
            TidbFirstJsonSliceAdapter::execute(&missing_request, &missing_runner).unwrap();
        assert_eq!(
            missing_result.outcome,
            CaseOutcome::Error {
                error_class: ErrorClass::MissingColumn,
                engine_code: Some("1054".to_string()),
                engine_message: Some(
                    "Unknown column '__missing_column_1' in 'where clause'".to_string()
                ),
            }
        );

        let comparison_request = TidbFirstJsonSliceAdapter::canonical_requests()
            .into_iter()
            .find(|request| request.case_id == "unsupported-json-ordering-comparison-error")
            .unwrap();
        let comparison_runner = StubRunner::error(EngineExecutionError::EngineFailure {
            code: Some("1105".to_string()),
            message:
                "unsupported json comparison: ordering probe is out of scope for first-json-slice"
                    .to_string(),
        });
        let comparison_result =
            TidbFirstJsonSliceAdapter::execute(&comparison_request, &comparison_runner).unwrap();
        assert_eq!(
            comparison_result.outcome,
            CaseOutcome::Error {
                error_class: ErrorClass::UnsupportedJsonComparison,
                engine_code: Some("1105".to_string()),
                engine_message: Some("unsupported json comparison: ordering probe is out of scope for first-json-slice".to_string()),
            }
        );

        let cast_request = TidbFirstJsonSliceAdapter::canonical_requests()
            .into_iter()
            .find(|request| request.case_id == "unsupported-json-cast-error")
            .unwrap();
        let cast_runner = StubRunner::error(EngineExecutionError::EngineFailure {
            code: Some("1105".to_string()),
            message: "unsupported json cast: explicit cast is out of scope for first-json-slice"
                .to_string(),
        });
        let cast_result = TidbFirstJsonSliceAdapter::execute(&cast_request, &cast_runner).unwrap();
        assert_eq!(
            cast_result.outcome,
            CaseOutcome::Error {
                error_class: ErrorClass::UnsupportedJsonCast,
                engine_code: Some("1105".to_string()),
                engine_message: Some(
                    "unsupported json cast: explicit cast is out of scope for first-json-slice"
                        .to_string()
                ),
            }
        );
    }

    #[test]
    fn lowering_rejects_requests_with_mismatched_operation_refs() {
        let mut request = TidbFirstJsonSliceAdapter::canonical_requests()
            .into_iter()
            .find(|request| request.case_id == "unsupported-json-cast-error")
            .unwrap();
        request.cast_ref = None;
        request.comparison_ref = Some("less-than-column-0-column-0".to_string());

        let error = TidbFirstJsonSliceAdapter::lower_request(&request).unwrap_err();

        assert_eq!(
            error,
            AdapterRequestValidationError::MismatchedCaseDefinition {
                case_id: "unsupported-json-cast-error".to_string(),
                expected_input_ref: "first-json-basic",
                actual_input_ref: "first-json-basic".to_string(),
                expected_projection_ref: None,
                actual_projection_ref: None,
                expected_filter_ref: None,
                actual_filter_ref: None,
                expected_comparison_ref: None,
                actual_comparison_ref: Some("less-than-column-0-column-0".to_string()),
                expected_cast_ref: Some("json-column-0-to-int32"),
                actual_cast_ref: None,
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
