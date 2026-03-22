use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::engine::{canonicalize_nested_value, is_missing_column, SqlExecutionPlan};
pub use crate::engine::{
    EngineColumn, EngineExecutionError, EngineExecutionResult, ADAPTER as TIDB_ADAPTER,
    ENGINE as TIDB_ENGINE,
};

pub const FIRST_STRUCT_SLICE_ID: &str = "first-struct-slice";
const FIRST_STRUCT_SLICE_SPEC_REFS: [&str; 4] = [
    "docs/design/first-struct-aware-handoff-slice.md",
    "docs/spec/type-system.md",
    "tests/conformance/first-struct-slice.md",
    "tests/differential/first-struct-slice.md",
];

const STRUCT_BASIC_INPUT_SQL: &str = concat!(
    "SELECT JSON_OBJECT('a', 1, 'b', 2) AS s ",
    "UNION ALL ",
    "SELECT JSON_OBJECT('a', 3, 'b', 4) AS s ",
    "UNION ALL ",
    "SELECT JSON_OBJECT('a', 5, 'b', 6) AS s"
);

const STRUCT_NULLABLE_INPUT_SQL: &str = concat!(
    "SELECT JSON_OBJECT('a', 1, 'b', NULL) AS s ",
    "UNION ALL ",
    "SELECT CAST(NULL AS JSON) AS s ",
    "UNION ALL ",
    "SELECT JSON_OBJECT('a', 2, 'b', 3) AS s"
);

const MAP_BASIC_INPUT_SQL: &str = concat!(
    "SELECT JSON_OBJECT('1', 2) AS m ",
    "UNION ALL ",
    "SELECT JSON_OBJECT('3', 4) AS m ",
    "UNION ALL ",
    "SELECT JSON_OBJECT('5', 6) AS m"
);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AdapterRequest {
    pub slice_id: String,
    pub case_id: String,
    pub spec_refs: Vec<String>,
    pub input_ref: String,
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
    UnsupportedNestedFamily,
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
pub struct TidbFirstStructSliceAdapter;

impl TidbFirstStructSliceAdapter {
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
                let rows = normalize_rows_for_schema(result.rows, &schema);

                CaseOutcome::Rows {
                    row_count: rows.len(),
                    rows,
                    schema,
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
            outcome,
        })
    }
}

#[derive(Debug, Clone, Copy)]
struct CaseDefinition {
    case_id: &'static str,
    input_ref: &'static str,
    projection_ref: Option<&'static str>,
}

impl CaseDefinition {
    fn canonical_request(self) -> AdapterRequest {
        AdapterRequest {
            slice_id: FIRST_STRUCT_SLICE_ID.to_string(),
            case_id: self.case_id.to_string(),
            spec_refs: FIRST_STRUCT_SLICE_SPEC_REFS
                .iter()
                .map(|spec_ref| (*spec_ref).to_string())
                .collect(),
            input_ref: self.input_ref.to_string(),
            projection_ref: self.projection_ref.map(str::to_string),
        }
    }

    fn render_sql(self) -> String {
        let input_sql = input_sql(self.input_ref);

        match self.projection_ref {
            Some("column-0") => {
                let column_name = projection_column_name(self.input_ref);
                format!(
                    "SELECT input_rows.{column_name} AS {column_name} FROM ({input_sql}) AS input_rows"
                )
            }
            Some("column-1") => {
                format!("SELECT input_rows.__missing_column_1 AS missing FROM ({input_sql}) AS input_rows")
            }
            _ => unreachable!("validated case definitions always set exactly one operation ref"),
        }
    }
}

const CASE_DEFINITIONS: [CaseDefinition; 5] = [
    CaseDefinition {
        case_id: "struct-column-passthrough",
        input_ref: "first-struct-basic",
        projection_ref: Some("column-0"),
    },
    CaseDefinition {
        case_id: "struct-column-null-preserve",
        input_ref: "first-struct-nullable",
        projection_ref: Some("column-0"),
    },
    CaseDefinition {
        case_id: "struct-child-null-preserve",
        input_ref: "first-struct-nullable",
        projection_ref: Some("column-0"),
    },
    CaseDefinition {
        case_id: "struct-missing-column-error",
        input_ref: "first-struct-basic",
        projection_ref: Some("column-1"),
    },
    CaseDefinition {
        case_id: "unsupported-nested-family-error",
        input_ref: "first-map-basic",
        projection_ref: Some("column-0"),
    },
];

fn validate_request(
    request: &AdapterRequest,
) -> Result<CaseDefinition, AdapterRequestValidationError> {
    if request.slice_id != FIRST_STRUCT_SLICE_ID {
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
    {
        return Err(AdapterRequestValidationError::MismatchedCaseDefinition {
            case_id: request.case_id.clone(),
            expected_input_ref: case.input_ref,
            actual_input_ref: request.input_ref.clone(),
            expected_projection_ref: case.projection_ref,
            actual_projection_ref: request.projection_ref.clone(),
        });
    }

    let expected_spec_refs: Vec<String> = FIRST_STRUCT_SLICE_SPEC_REFS
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
        "first-struct-basic" => STRUCT_BASIC_INPUT_SQL,
        "first-struct-nullable" => STRUCT_NULLABLE_INPUT_SQL,
        "first-map-basic" => MAP_BASIC_INPUT_SQL,
        _ => unreachable!("validated input refs should always be known"),
    }
}

fn projection_column_name(input_ref: &str) -> &'static str {
    match input_ref {
        "first-struct-basic" | "first-struct-nullable" => "s",
        "first-map-basic" => "m",
        _ => unreachable!("validated input refs should always be known"),
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
            } else if is_unsupported_nested_family(code.as_deref(), &message) {
                ErrorClass::UnsupportedNestedFamily
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

fn is_unsupported_nested_family(engine_code: Option<&str>, engine_message: &str) -> bool {
    let normalized_message = engine_message.to_ascii_lowercase();

    engine_code == Some("TFS001")
        || normalized_message.contains("unsupported nested family")
        || normalized_message.contains("unsupported nested expression input")
}

fn normalize_logical_type(engine_type: &str) -> String {
    let normalized = engine_type.trim().to_ascii_lowercase();

    if normalized.contains("struct") {
        return "struct<a:int32,b:int32?>".to_string();
    }

    if normalized.contains("map") {
        return "map<int32,int32?>".to_string();
    }

    match normalized.split('(').next().unwrap_or(&normalized).trim() {
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
                    Some(field) if field.logical_type.starts_with("struct<") => {
                        canonicalize_nested_value(value)
                    }
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
        let requests = TidbFirstStructSliceAdapter::canonical_requests();
        let case_ids: Vec<&str> = requests
            .iter()
            .map(|request| request.case_id.as_str())
            .collect();

        assert_eq!(requests.len(), 5);
        assert_eq!(
            case_ids,
            vec![
                "struct-column-passthrough",
                "struct-column-null-preserve",
                "struct-child-null-preserve",
                "struct-missing-column-error",
                "unsupported-nested-family-error",
            ]
        );

        for request in requests {
            assert!(request.projection_ref.is_some());
        }
    }

    #[test]
    fn lowering_renders_tidb_sql_for_documented_case_shapes() {
        let requests = TidbFirstStructSliceAdapter::canonical_requests();

        for request in &requests {
            let plan = TidbFirstStructSliceAdapter::lower_request(request).unwrap();
            assert!(!plan.sql.is_empty());
        }

        let missing_column_request = requests
            .iter()
            .find(|request| request.case_id == "struct-missing-column-error")
            .unwrap();
        let missing_column_plan =
            TidbFirstStructSliceAdapter::lower_request(missing_column_request).unwrap();
        assert!(missing_column_plan.sql.contains("__missing_column_1"));
    }

    #[test]
    fn execute_rows_normalizes_struct_schema_and_values() {
        let request = TidbFirstStructSliceAdapter::canonical_requests()
            .into_iter()
            .find(|request| request.case_id == "struct-child-null-preserve")
            .unwrap();
        let runner = StubRunner::rows(
            vec![EngineColumn {
                name: "s".to_string(),
                engine_type: "struct<a:int32,b:int32?>".to_string(),
                nullable: true,
            }],
            vec![
                vec![json!({"b": null, "a": 1})],
                vec![json!(null)],
                vec![json!({"b": 3, "a": 2})],
            ],
        );

        let result = TidbFirstStructSliceAdapter::execute(&request, &runner).unwrap();

        assert_eq!(result.engine, TIDB_ENGINE);
        assert_eq!(result.adapter, TIDB_ADAPTER);
        assert_eq!(
            result.outcome,
            CaseOutcome::Rows {
                schema: vec![SchemaField {
                    name: "s".to_string(),
                    logical_type: "struct<a:int32,b:int32?>".to_string(),
                    nullable: true,
                }],
                rows: vec![
                    vec![json!({"a": 1, "b": null})],
                    vec![json!(null)],
                    vec![json!({"a": 2, "b": 3})],
                ],
                row_count: 3,
            }
        );
    }

    #[test]
    fn execute_normalizes_struct_error_classes() {
        let missing_request = TidbFirstStructSliceAdapter::canonical_requests()
            .into_iter()
            .find(|request| request.case_id == "struct-missing-column-error")
            .unwrap();
        let missing_runner = StubRunner::error(EngineExecutionError::EngineFailure {
            code: Some("1054".to_string()),
            message: "Unknown column '__missing_column_1' in 'field list'".to_string(),
        });
        let missing_result =
            TidbFirstStructSliceAdapter::execute(&missing_request, &missing_runner).unwrap();
        assert_eq!(
            missing_result.outcome,
            CaseOutcome::Error {
                error_class: ErrorClass::MissingColumn,
                engine_code: Some("1054".to_string()),
                engine_message: Some(
                    "Unknown column '__missing_column_1' in 'field list'".to_string()
                ),
            }
        );

        let unsupported_request = TidbFirstStructSliceAdapter::canonical_requests()
            .into_iter()
            .find(|request| request.case_id == "unsupported-nested-family-error")
            .unwrap();
        let unsupported_runner = StubRunner::error(EngineExecutionError::EngineFailure {
            code: Some("1105".to_string()),
            message: "unsupported nested expression input at column 0".to_string(),
        });
        let unsupported_result =
            TidbFirstStructSliceAdapter::execute(&unsupported_request, &unsupported_runner)
                .unwrap();
        assert_eq!(
            unsupported_result.outcome,
            CaseOutcome::Error {
                error_class: ErrorClass::UnsupportedNestedFamily,
                engine_code: Some("1105".to_string()),
                engine_message: Some("unsupported nested expression input at column 0".to_string()),
            }
        );
    }

    #[test]
    fn lowering_rejects_requests_with_mismatched_projection_ref() {
        let mut request = TidbFirstStructSliceAdapter::canonical_requests()
            .into_iter()
            .find(|request| request.case_id == "struct-column-passthrough")
            .unwrap();
        request.projection_ref = Some("column-1".to_string());

        let error = TidbFirstStructSliceAdapter::lower_request(&request).unwrap_err();

        assert_eq!(
            error,
            AdapterRequestValidationError::MismatchedCaseDefinition {
                case_id: "struct-column-passthrough".to_string(),
                expected_input_ref: "first-struct-basic",
                actual_input_ref: "first-struct-basic".to_string(),
                expected_projection_ref: Some("column-0"),
                actual_projection_ref: Some("column-1".to_string()),
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
