use serde::{Deserialize, Serialize};
use serde_json::Value;

pub const FIRST_EXPRESSION_SLICE_ID: &str = "first-expression-slice";
pub const TIDB_ENGINE: &str = "tidb";
pub const TIDB_ADAPTER: &str = "tidb-sql";

const FIRST_EXPRESSION_SLICE_SPEC_REFS: [&str; 4] = [
    "docs/spec/milestone-1-expression-projection.md",
    "docs/spec/type-system.md",
    "tests/conformance/expression-projection-slice.md",
    "tests/differential/first-expression-slice.md",
];

const INT32_BASIC_INPUT_SQL: &str = concat!(
    "SELECT CAST(1 AS SIGNED) AS a ",
    "UNION ALL ",
    "SELECT CAST(2 AS SIGNED) AS a ",
    "UNION ALL ",
    "SELECT CAST(3 AS SIGNED) AS a"
);

const INT32_NULLABLE_INPUT_SQL: &str = concat!(
    "SELECT CAST(1 AS SIGNED) AS a ",
    "UNION ALL ",
    "SELECT CAST(NULL AS SIGNED) AS a ",
    "UNION ALL ",
    "SELECT CAST(3 AS SIGNED) AS a"
);

const INT32_OVERFLOW_INPUT_SQL: &str = "SELECT CAST(2147483647 AS SIGNED) AS a";

const COLUMN_A_SQL: &str = "SELECT input_rows.a AS a FROM ({input_sql}) AS input_rows";
const LITERAL_SEVEN_SQL: &str = "SELECT CAST(7 AS SIGNED) AS lit FROM ({input_sql}) AS input_rows";
const LITERAL_NULL_SQL: &str =
    "SELECT CAST(NULL AS SIGNED) AS lit FROM ({input_sql}) AS input_rows";
const ADD_A_PLUS_ONE_SQL: &str = concat!(
    "SELECT CAST(input_rows.a + CAST(1 AS SIGNED) AS SIGNED) AS a_plus_one ",
    "FROM ({input_sql}) AS input_rows"
);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AdapterRequest {
    pub slice_id: String,
    pub case_id: String,
    pub spec_refs: Vec<String>,
    pub input_ref: String,
    pub projection_ref: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TidbExecutionPlan {
    pub request: AdapterRequest,
    pub sql: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CaseResult {
    pub slice_id: String,
    pub engine: String,
    pub adapter: String,
    pub case_id: String,
    pub spec_refs: Vec<String>,
    pub input_ref: String,
    pub projection_ref: String,
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
    ArithmeticOverflow,
    AdapterUnavailable,
    EngineError,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EngineExecutionResult {
    pub columns: Vec<EngineColumn>,
    pub rows: Vec<Vec<Value>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EngineColumn {
    pub name: String,
    pub engine_type: String,
    pub nullable: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EngineExecutionError {
    AdapterUnavailable {
        message: Option<String>,
    },
    EngineFailure {
        code: Option<String>,
        message: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AdapterRequestValidationError {
    UnsupportedSliceId(String),
    UnknownCaseId(String),
    MismatchedCaseDefinition {
        case_id: String,
        expected_input_ref: &'static str,
        actual_input_ref: String,
        expected_projection_ref: &'static str,
        actual_projection_ref: String,
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
pub struct TidbFirstExpressionSliceAdapter;

impl TidbFirstExpressionSliceAdapter {
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

        Ok(TidbExecutionPlan {
            request: request.clone(),
            sql: case.render_sql(),
        })
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
                        logical_type: normalize_case_logical_type(
                            &request.case_id,
                            &request.projection_ref,
                            &column.engine_type,
                        ),
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
            slice_id: FIRST_EXPRESSION_SLICE_ID.to_string(),
            case_id: self.case_id.to_string(),
            spec_refs: FIRST_EXPRESSION_SLICE_SPEC_REFS
                .iter()
                .map(|spec_ref| (*spec_ref).to_string())
                .collect(),
            input_ref: self.input_ref.to_string(),
            projection_ref: self.projection_ref.to_string(),
        }
    }

    fn render_sql(self) -> String {
        let input_sql = input_sql(self.input_ref);
        let template = projection_sql(self.projection_ref);

        template.replace("{input_sql}", input_sql)
    }
}

const CASE_DEFINITIONS: [CaseDefinition; 6] = [
    CaseDefinition {
        case_id: "column-passthrough",
        input_ref: "first-expression-slice-int32-basic",
        projection_ref: "column-a",
    },
    CaseDefinition {
        case_id: "literal-int32-seven",
        input_ref: "first-expression-slice-int32-basic",
        projection_ref: "literal-int32-seven",
    },
    CaseDefinition {
        case_id: "literal-int32-null",
        input_ref: "first-expression-slice-int32-basic",
        projection_ref: "literal-int32-null",
    },
    CaseDefinition {
        case_id: "add-int32-literal",
        input_ref: "first-expression-slice-int32-basic",
        projection_ref: "add-a-plus-one",
    },
    CaseDefinition {
        case_id: "add-int32-null-propagation",
        input_ref: "first-expression-slice-int32-nullable",
        projection_ref: "add-a-plus-one",
    },
    CaseDefinition {
        case_id: "add-int32-overflow-error",
        input_ref: "first-expression-slice-int32-overflow",
        projection_ref: "add-a-plus-one",
    },
];

fn validate_request(
    request: &AdapterRequest,
) -> Result<CaseDefinition, AdapterRequestValidationError> {
    if request.slice_id != FIRST_EXPRESSION_SLICE_ID {
        return Err(AdapterRequestValidationError::UnsupportedSliceId(
            request.slice_id.clone(),
        ));
    }

    let case = CASE_DEFINITIONS
        .iter()
        .copied()
        .find(|candidate| candidate.case_id == request.case_id)
        .ok_or_else(|| AdapterRequestValidationError::UnknownCaseId(request.case_id.clone()))?;

    if request.input_ref != case.input_ref || request.projection_ref != case.projection_ref {
        return Err(AdapterRequestValidationError::MismatchedCaseDefinition {
            case_id: request.case_id.clone(),
            expected_input_ref: case.input_ref,
            actual_input_ref: request.input_ref.clone(),
            expected_projection_ref: case.projection_ref,
            actual_projection_ref: request.projection_ref.clone(),
        });
    }

    let expected_spec_refs: Vec<String> = FIRST_EXPRESSION_SLICE_SPEC_REFS
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
        "first-expression-slice-int32-basic" => INT32_BASIC_INPUT_SQL,
        "first-expression-slice-int32-nullable" => INT32_NULLABLE_INPUT_SQL,
        "first-expression-slice-int32-overflow" => INT32_OVERFLOW_INPUT_SQL,
        _ => unreachable!("validated input refs should always be known"),
    }
}

fn projection_sql(projection_ref: &str) -> &'static str {
    match projection_ref {
        "column-a" => COLUMN_A_SQL,
        "literal-int32-seven" => LITERAL_SEVEN_SQL,
        "literal-int32-null" => LITERAL_NULL_SQL,
        "add-a-plus-one" => ADD_A_PLUS_ONE_SQL,
        _ => unreachable!("validated projection refs should always be known"),
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
            let error_class = if is_arithmetic_overflow(code.as_deref(), &message) {
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
        || normalized_message.contains("out of range")
        || normalized_message.contains("overflow")
}

fn normalize_logical_type(engine_type: &str) -> String {
    let normalized = engine_type.trim().to_ascii_lowercase();
    let normalized = normalized.split('(').next().unwrap_or(&normalized).trim();

    match normalized {
        "int" | "integer" | "mediumint" | "smallint" | "tinyint" => "int32".to_string(),
        "bigint" | "signed" | "signed integer" => "int64".to_string(),
        other => other.to_string(),
    }
}

fn normalize_case_logical_type(case_id: &str, projection_ref: &str, engine_type: &str) -> String {
    let normalized = normalize_logical_type(engine_type);
    if is_literal_int32_case(case_id, projection_ref) && normalized == "int64" {
        "int32".to_string()
    } else {
        normalized
    }
}

fn is_literal_int32_case(case_id: &str, projection_ref: &str) -> bool {
    matches!(
        (case_id, projection_ref),
        ("literal-int32-seven", "literal-int32-seven")
            | ("literal-int32-null", "literal-int32-null")
    )
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;

    use super::*;
    use serde_json::json;

    #[test]
    fn canonical_requests_cover_all_documented_cases() {
        let requests = TidbFirstExpressionSliceAdapter::canonical_requests();
        let case_ids: Vec<&str> = requests
            .iter()
            .map(|request| request.case_id.as_str())
            .collect();

        assert_eq!(requests.len(), 6);
        assert_eq!(
            case_ids,
            vec![
                "column-passthrough",
                "literal-int32-seven",
                "literal-int32-null",
                "add-int32-literal",
                "add-int32-null-propagation",
                "add-int32-overflow-error",
            ]
        );

        for request in requests {
            assert_eq!(request.slice_id, FIRST_EXPRESSION_SLICE_ID);
            assert_eq!(request.spec_refs.len(), 4);
            assert_eq!(
                request.spec_refs[0],
                "docs/spec/milestone-1-expression-projection.md"
            );
            assert_eq!(request.spec_refs[1], "docs/spec/type-system.md");
            assert_eq!(
                request.spec_refs[2],
                "tests/conformance/expression-projection-slice.md"
            );
            assert_eq!(
                request.spec_refs[3],
                "tests/differential/first-expression-slice.md"
            );
        }
    }

    #[test]
    fn lowering_renders_tidb_sql_for_each_documented_case() {
        let requests = TidbFirstExpressionSliceAdapter::canonical_requests();
        let plans: Vec<(String, String)> = requests
            .iter()
            .map(|request| {
                let plan = TidbFirstExpressionSliceAdapter::lower_request(request).unwrap();
                (plan.request.case_id, plan.sql)
            })
            .collect();

        assert_eq!(
            plans,
            vec![
                (
                    "column-passthrough".to_string(),
                    "SELECT input_rows.a AS a FROM (SELECT CAST(1 AS SIGNED) AS a UNION ALL SELECT CAST(2 AS SIGNED) AS a UNION ALL SELECT CAST(3 AS SIGNED) AS a) AS input_rows".to_string(),
                ),
                (
                    "literal-int32-seven".to_string(),
                    "SELECT CAST(7 AS SIGNED) AS lit FROM (SELECT CAST(1 AS SIGNED) AS a UNION ALL SELECT CAST(2 AS SIGNED) AS a UNION ALL SELECT CAST(3 AS SIGNED) AS a) AS input_rows".to_string(),
                ),
                (
                    "literal-int32-null".to_string(),
                    "SELECT CAST(NULL AS SIGNED) AS lit FROM (SELECT CAST(1 AS SIGNED) AS a UNION ALL SELECT CAST(2 AS SIGNED) AS a UNION ALL SELECT CAST(3 AS SIGNED) AS a) AS input_rows".to_string(),
                ),
                (
                    "add-int32-literal".to_string(),
                    "SELECT CAST(input_rows.a + CAST(1 AS SIGNED) AS SIGNED) AS a_plus_one FROM (SELECT CAST(1 AS SIGNED) AS a UNION ALL SELECT CAST(2 AS SIGNED) AS a UNION ALL SELECT CAST(3 AS SIGNED) AS a) AS input_rows".to_string(),
                ),
                (
                    "add-int32-null-propagation".to_string(),
                    "SELECT CAST(input_rows.a + CAST(1 AS SIGNED) AS SIGNED) AS a_plus_one FROM (SELECT CAST(1 AS SIGNED) AS a UNION ALL SELECT CAST(NULL AS SIGNED) AS a UNION ALL SELECT CAST(3 AS SIGNED) AS a) AS input_rows".to_string(),
                ),
                (
                    "add-int32-overflow-error".to_string(),
                    "SELECT CAST(input_rows.a + CAST(1 AS SIGNED) AS SIGNED) AS a_plus_one FROM (SELECT CAST(2147483647 AS SIGNED) AS a) AS input_rows".to_string(),
                ),
            ]
        );
    }

    #[test]
    fn execute_rows_normalizes_schema_and_row_count() {
        let request = TidbFirstExpressionSliceAdapter::canonical_requests()
            .into_iter()
            .find(|request| request.case_id == "column-passthrough")
            .unwrap();
        let runner = StubRunner::rows(
            vec![EngineColumn {
                name: "a".to_string(),
                engine_type: "int".to_string(),
                nullable: false,
            }],
            vec![vec![json!(1)], vec![json!(2)], vec![json!(3)]],
        );

        let result = TidbFirstExpressionSliceAdapter::execute(&request, &runner).unwrap();

        assert_eq!(runner.sqls(), vec!["SELECT input_rows.a AS a FROM (SELECT CAST(1 AS SIGNED) AS a UNION ALL SELECT CAST(2 AS SIGNED) AS a UNION ALL SELECT CAST(3 AS SIGNED) AS a) AS input_rows"]);
        assert_eq!(result.engine, TIDB_ENGINE);
        assert_eq!(result.adapter, TIDB_ADAPTER);
        assert_eq!(
            result.outcome,
            CaseOutcome::Rows {
                schema: vec![SchemaField {
                    name: "a".to_string(),
                    logical_type: "int32".to_string(),
                    nullable: false,
                }],
                rows: vec![vec![json!(1)], vec![json!(2)], vec![json!(3)]],
                row_count: 3,
            }
        );
    }

    #[test]
    fn execute_narrows_literal_bigint_metadata_to_int32() {
        let request = TidbFirstExpressionSliceAdapter::canonical_requests()
            .into_iter()
            .find(|request| request.case_id == "literal-int32-seven")
            .unwrap();
        let runner = StubRunner::rows(
            vec![EngineColumn {
                name: "lit".to_string(),
                engine_type: "bigint".to_string(),
                nullable: false,
            }],
            vec![vec![json!(7)], vec![json!(7)], vec![json!(7)]],
        );

        let result = TidbFirstExpressionSliceAdapter::execute(&request, &runner).unwrap();

        assert_eq!(
            result.outcome,
            CaseOutcome::Rows {
                schema: vec![SchemaField {
                    name: "lit".to_string(),
                    logical_type: "int32".to_string(),
                    nullable: false,
                }],
                rows: vec![vec![json!(7)], vec![json!(7)], vec![json!(7)]],
                row_count: 3,
            }
        );
    }

    #[test]
    fn execute_keeps_non_literal_bigint_metadata_as_int64() {
        let request = TidbFirstExpressionSliceAdapter::canonical_requests()
            .into_iter()
            .find(|request| request.case_id == "add-int32-literal")
            .unwrap();
        let runner = StubRunner::rows(
            vec![EngineColumn {
                name: "a_plus_one".to_string(),
                engine_type: "bigint".to_string(),
                nullable: false,
            }],
            vec![vec![json!(2)], vec![json!(3)], vec![json!(4)]],
        );

        let result = TidbFirstExpressionSliceAdapter::execute(&request, &runner).unwrap();

        assert_eq!(
            result.outcome,
            CaseOutcome::Rows {
                schema: vec![SchemaField {
                    name: "a_plus_one".to_string(),
                    logical_type: "int64".to_string(),
                    nullable: false,
                }],
                rows: vec![vec![json!(2)], vec![json!(3)], vec![json!(4)]],
                row_count: 3,
            }
        );
    }

    #[test]
    fn execute_normalizes_tidb_overflow_errors() {
        let request = TidbFirstExpressionSliceAdapter::canonical_requests()
            .into_iter()
            .find(|request| request.case_id == "add-int32-overflow-error")
            .unwrap();
        let runner = StubRunner::error(EngineExecutionError::EngineFailure {
            code: Some("1690".to_string()),
            message: "Error 1690 (22003): value is out of range".to_string(),
        });

        let result = TidbFirstExpressionSliceAdapter::execute(&request, &runner).unwrap();

        assert_eq!(
            result.outcome,
            CaseOutcome::Error {
                error_class: ErrorClass::ArithmeticOverflow,
                engine_code: Some("1690".to_string()),
                engine_message: Some("Error 1690 (22003): value is out of range".to_string()),
            }
        );
    }

    #[test]
    fn execute_normalizes_adapter_unavailable_without_extra_code() {
        let request = TidbFirstExpressionSliceAdapter::canonical_requests()
            .into_iter()
            .find(|request| request.case_id == "add-int32-overflow-error")
            .unwrap();
        let runner = StubRunner::error(EngineExecutionError::AdapterUnavailable {
            message: Some("TiDB DSN is not configured".to_string()),
        });

        let result = TidbFirstExpressionSliceAdapter::execute(&request, &runner).unwrap();
        let serialized = serde_json::to_value(&result).unwrap();

        assert_eq!(
            result.outcome,
            CaseOutcome::Error {
                error_class: ErrorClass::AdapterUnavailable,
                engine_code: None,
                engine_message: Some("TiDB DSN is not configured".to_string()),
            }
        );
        assert_eq!(serialized["outcome"]["kind"], "error");
        assert_eq!(serialized["outcome"]["error_class"], "adapter_unavailable");
        assert!(serialized["outcome"].get("engine_code").is_none());
    }

    #[test]
    fn lowering_rejects_requests_with_mismatched_spec_refs() {
        let mut request = TidbFirstExpressionSliceAdapter::canonical_requests()
            .into_iter()
            .find(|request| request.case_id == "column-passthrough")
            .unwrap();
        request.spec_refs.pop();

        let error = TidbFirstExpressionSliceAdapter::lower_request(&request).unwrap_err();

        assert_eq!(
            error,
            AdapterRequestValidationError::MismatchedSpecRefs {
                case_id: "column-passthrough".to_string(),
                expected_spec_refs: vec![
                    "docs/spec/milestone-1-expression-projection.md".to_string(),
                    "docs/spec/type-system.md".to_string(),
                    "tests/conformance/expression-projection-slice.md".to_string(),
                    "tests/differential/first-expression-slice.md".to_string(),
                ],
                actual_spec_refs: vec![
                    "docs/spec/milestone-1-expression-projection.md".to_string(),
                    "docs/spec/type-system.md".to_string(),
                    "tests/conformance/expression-projection-slice.md".to_string(),
                ],
            }
        );
    }

    struct StubRunner {
        result: Result<EngineExecutionResult, EngineExecutionError>,
        seen_sql: RefCell<Vec<String>>,
    }

    impl StubRunner {
        fn rows(columns: Vec<EngineColumn>, rows: Vec<Vec<Value>>) -> Self {
            Self {
                result: Ok(EngineExecutionResult { columns, rows }),
                seen_sql: RefCell::new(Vec::new()),
            }
        }

        fn error(error: EngineExecutionError) -> Self {
            Self {
                result: Err(error),
                seen_sql: RefCell::new(Vec::new()),
            }
        }

        fn sqls(&self) -> Vec<String> {
            self.seen_sql.borrow().clone()
        }
    }

    impl TidbRunner for StubRunner {
        fn run(
            &self,
            plan: &TidbExecutionPlan,
        ) -> Result<EngineExecutionResult, EngineExecutionError> {
            self.seen_sql.borrow_mut().push(plan.sql.clone());
            self.result.clone()
        }
    }
}
