use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::engine::{is_missing_column, SqlExecutionPlan};
pub use crate::engine::{
    EngineColumn, EngineExecutionError, EngineExecutionResult, ADAPTER as TIDB_ADAPTER,
    ENGINE as TIDB_ENGINE,
};

pub const FIRST_FILTER_IS_NOT_NULL_SLICE_ID: &str = "first-filter-is-not-null-slice";
const FIRST_FILTER_IS_NOT_NULL_SLICE_SPEC_REFS: [&str; 4] = [
    "docs/spec/first-filter-is-not-null.md",
    "docs/spec/type-system.md",
    "tests/conformance/first-filter-is-not-null-slice.md",
    "tests/differential/first-filter-is-not-null-slice.md",
];

const INT32_BASIC_INPUT_SQL: &str = concat!(
    "SELECT CAST(1 AS SIGNED) AS a ",
    "UNION ALL ",
    "SELECT CAST(2 AS SIGNED) AS a ",
    "UNION ALL ",
    "SELECT CAST(3 AS SIGNED) AS a"
);

const INT32_ALL_NULL_INPUT_SQL: &str = concat!(
    "SELECT CAST(NULL AS SIGNED) AS a ",
    "UNION ALL ",
    "SELECT CAST(NULL AS SIGNED) AS a ",
    "UNION ALL ",
    "SELECT CAST(NULL AS SIGNED) AS a"
);

const INT32_MIXED_TWO_COLUMN_INPUT_SQL: &str = concat!(
    "SELECT CAST(1 AS SIGNED) AS a, CAST(10 AS SIGNED) AS b ",
    "UNION ALL ",
    "SELECT CAST(NULL AS SIGNED) AS a, CAST(20 AS SIGNED) AS b ",
    "UNION ALL ",
    "SELECT CAST(3 AS SIGNED) AS a, CAST(30 AS SIGNED) AS b ",
    "UNION ALL ",
    "SELECT CAST(NULL AS SIGNED) AS a, CAST(40 AS SIGNED) AS b"
);

const UTF8_BASIC_INPUT_SQL: &str = concat!(
    "SELECT CAST('x' AS CHAR) AS s ",
    "UNION ALL ",
    "SELECT CAST('y' AS CHAR) AS s ",
    "UNION ALL ",
    "SELECT CAST('z' AS CHAR) AS s"
);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AdapterRequest {
    pub slice_id: String,
    pub case_id: String,
    pub spec_refs: Vec<String>,
    pub input_ref: String,
    pub filter_ref: String,
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
    pub filter_ref: String,
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
    UnsupportedPredicateType,
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
        expected_filter_ref: &'static str,
        actual_filter_ref: String,
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
pub struct TidbFirstFilterIsNotNullSliceAdapter;

impl TidbFirstFilterIsNotNullSliceAdapter {
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
            filter_ref: request.filter_ref.clone(),
            outcome,
        })
    }
}

#[derive(Debug, Clone, Copy)]
struct CaseDefinition {
    case_id: &'static str,
    input_ref: &'static str,
    filter_ref: &'static str,
}

impl CaseDefinition {
    fn canonical_request(self) -> AdapterRequest {
        AdapterRequest {
            slice_id: FIRST_FILTER_IS_NOT_NULL_SLICE_ID.to_string(),
            case_id: self.case_id.to_string(),
            spec_refs: FIRST_FILTER_IS_NOT_NULL_SLICE_SPEC_REFS
                .iter()
                .map(|spec_ref| (*spec_ref).to_string())
                .collect(),
            input_ref: self.input_ref.to_string(),
            filter_ref: self.filter_ref.to_string(),
        }
    }

    fn render_sql(self) -> String {
        let input_sql = input_sql(self.input_ref);
        let filter_condition = filter_condition(self.input_ref, self.filter_ref);

        format!("SELECT * FROM ({input_sql}) AS input_rows WHERE {filter_condition}")
    }
}

const CASE_DEFINITIONS: [CaseDefinition; 5] = [
    CaseDefinition {
        case_id: "all-rows-kept",
        input_ref: "first-filter-is-not-null-int32-basic",
        filter_ref: "is-not-null-column-0",
    },
    CaseDefinition {
        case_id: "all-rows-dropped",
        input_ref: "first-filter-is-not-null-int32-all-null",
        filter_ref: "is-not-null-column-0",
    },
    CaseDefinition {
        case_id: "mixed-keep-drop",
        input_ref: "first-filter-is-not-null-int32-mixed-two-column",
        filter_ref: "is-not-null-column-0",
    },
    CaseDefinition {
        case_id: "missing-column-error",
        input_ref: "first-filter-is-not-null-int32-basic",
        filter_ref: "is-not-null-column-1",
    },
    CaseDefinition {
        case_id: "unsupported-predicate-type-error",
        input_ref: "first-filter-is-not-null-utf8-basic",
        filter_ref: "is-not-null-column-0",
    },
];

fn validate_request(
    request: &AdapterRequest,
) -> Result<CaseDefinition, AdapterRequestValidationError> {
    if request.slice_id != FIRST_FILTER_IS_NOT_NULL_SLICE_ID {
        return Err(AdapterRequestValidationError::UnsupportedSliceId(
            request.slice_id.clone(),
        ));
    }

    let case = CASE_DEFINITIONS
        .iter()
        .copied()
        .find(|candidate| candidate.case_id == request.case_id)
        .ok_or_else(|| AdapterRequestValidationError::UnknownCaseId(request.case_id.clone()))?;

    if request.input_ref != case.input_ref || request.filter_ref != case.filter_ref {
        return Err(AdapterRequestValidationError::MismatchedCaseDefinition {
            case_id: request.case_id.clone(),
            expected_input_ref: case.input_ref,
            actual_input_ref: request.input_ref.clone(),
            expected_filter_ref: case.filter_ref,
            actual_filter_ref: request.filter_ref.clone(),
        });
    }

    let expected_spec_refs: Vec<String> = FIRST_FILTER_IS_NOT_NULL_SLICE_SPEC_REFS
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
        "first-filter-is-not-null-int32-basic" => INT32_BASIC_INPUT_SQL,
        "first-filter-is-not-null-int32-all-null" => INT32_ALL_NULL_INPUT_SQL,
        "first-filter-is-not-null-int32-mixed-two-column" => INT32_MIXED_TWO_COLUMN_INPUT_SQL,
        "first-filter-is-not-null-utf8-basic" => UTF8_BASIC_INPUT_SQL,
        _ => unreachable!("validated input refs should always be known"),
    }
}

fn filter_condition(input_ref: &str, filter_ref: &str) -> &'static str {
    match filter_ref {
        "is-not-null-column-0" => match input_ref {
            "first-filter-is-not-null-int32-basic"
            | "first-filter-is-not-null-int32-all-null"
            | "first-filter-is-not-null-int32-mixed-two-column" => "input_rows.a IS NOT NULL",
            "first-filter-is-not-null-utf8-basic" => "input_rows.s IS NOT NULL",
            _ => unreachable!("validated input refs should always be known"),
        },
        "is-not-null-column-1" => match input_ref {
            "first-filter-is-not-null-int32-mixed-two-column" => "input_rows.b IS NOT NULL",
            _ => "input_rows.__missing_column_1 IS NOT NULL",
        },
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
            } else if is_unsupported_predicate_type(code.as_deref(), &message) {
                ErrorClass::UnsupportedPredicateType
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

fn is_unsupported_predicate_type(engine_code: Option<&str>, engine_message: &str) -> bool {
    let normalized_message = engine_message.to_ascii_lowercase();

    engine_code == Some("TF001")
        || normalized_message.contains("unsupported predicate type")
        || normalized_message.contains("int32 predicate")
}

fn normalize_logical_type(engine_type: &str) -> String {
    let normalized = engine_type.trim().to_ascii_lowercase();
    let normalized = normalized.split('(').next().unwrap_or(&normalized).trim();

    match normalized {
        "int" | "integer" | "mediumint" | "smallint" | "tinyint" => "int32".to_string(),
        "char" | "varchar" | "text" => "utf8".to_string(),
        other => other.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;

    use super::*;
    use serde_json::json;

    #[test]
    fn canonical_requests_cover_all_documented_cases() {
        let requests = TidbFirstFilterIsNotNullSliceAdapter::canonical_requests();
        let case_ids: Vec<&str> = requests
            .iter()
            .map(|request| request.case_id.as_str())
            .collect();

        assert_eq!(requests.len(), 5);
        assert_eq!(
            case_ids,
            vec![
                "all-rows-kept",
                "all-rows-dropped",
                "mixed-keep-drop",
                "missing-column-error",
                "unsupported-predicate-type-error",
            ]
        );

        for request in requests {
            assert_eq!(request.slice_id, FIRST_FILTER_IS_NOT_NULL_SLICE_ID);
            assert_eq!(request.spec_refs.len(), 4);
            assert_eq!(
                request.spec_refs[0],
                "docs/spec/first-filter-is-not-null.md"
            );
            assert_eq!(request.spec_refs[1], "docs/spec/type-system.md");
            assert_eq!(
                request.spec_refs[2],
                "tests/conformance/first-filter-is-not-null-slice.md"
            );
            assert_eq!(
                request.spec_refs[3],
                "tests/differential/first-filter-is-not-null-slice.md"
            );
        }
    }

    #[test]
    fn lowering_renders_tidb_sql_for_each_documented_case() {
        let requests = TidbFirstFilterIsNotNullSliceAdapter::canonical_requests();
        let plans: Vec<(String, String)> = requests
            .iter()
            .map(|request| {
                let plan = TidbFirstFilterIsNotNullSliceAdapter::lower_request(request).unwrap();
                (plan.request.case_id, plan.sql)
            })
            .collect();

        assert_eq!(
            plans,
            vec![
                (
                    "all-rows-kept".to_string(),
                    "SELECT * FROM (SELECT CAST(1 AS SIGNED) AS a UNION ALL SELECT CAST(2 AS SIGNED) AS a UNION ALL SELECT CAST(3 AS SIGNED) AS a) AS input_rows WHERE input_rows.a IS NOT NULL".to_string(),
                ),
                (
                    "all-rows-dropped".to_string(),
                    "SELECT * FROM (SELECT CAST(NULL AS SIGNED) AS a UNION ALL SELECT CAST(NULL AS SIGNED) AS a UNION ALL SELECT CAST(NULL AS SIGNED) AS a) AS input_rows WHERE input_rows.a IS NOT NULL".to_string(),
                ),
                (
                    "mixed-keep-drop".to_string(),
                    "SELECT * FROM (SELECT CAST(1 AS SIGNED) AS a, CAST(10 AS SIGNED) AS b UNION ALL SELECT CAST(NULL AS SIGNED) AS a, CAST(20 AS SIGNED) AS b UNION ALL SELECT CAST(3 AS SIGNED) AS a, CAST(30 AS SIGNED) AS b UNION ALL SELECT CAST(NULL AS SIGNED) AS a, CAST(40 AS SIGNED) AS b) AS input_rows WHERE input_rows.a IS NOT NULL".to_string(),
                ),
                (
                    "missing-column-error".to_string(),
                    "SELECT * FROM (SELECT CAST(1 AS SIGNED) AS a UNION ALL SELECT CAST(2 AS SIGNED) AS a UNION ALL SELECT CAST(3 AS SIGNED) AS a) AS input_rows WHERE input_rows.__missing_column_1 IS NOT NULL".to_string(),
                ),
                (
                    "unsupported-predicate-type-error".to_string(),
                    "SELECT * FROM (SELECT CAST('x' AS CHAR) AS s UNION ALL SELECT CAST('y' AS CHAR) AS s UNION ALL SELECT CAST('z' AS CHAR) AS s) AS input_rows WHERE input_rows.s IS NOT NULL".to_string(),
                ),
            ]
        );
    }

    #[test]
    fn execute_rows_normalizes_schema_and_row_count() {
        let request = TidbFirstFilterIsNotNullSliceAdapter::canonical_requests()
            .into_iter()
            .find(|request| request.case_id == "mixed-keep-drop")
            .unwrap();
        let runner = StubRunner::rows(
            vec![
                EngineColumn {
                    name: "a".to_string(),
                    engine_type: "int".to_string(),
                    nullable: true,
                },
                EngineColumn {
                    name: "b".to_string(),
                    engine_type: "int".to_string(),
                    nullable: false,
                },
            ],
            vec![vec![json!(1), json!(10)], vec![json!(3), json!(30)]],
        );

        let result = TidbFirstFilterIsNotNullSliceAdapter::execute(&request, &runner).unwrap();

        assert_eq!(runner.sqls(), vec!["SELECT * FROM (SELECT CAST(1 AS SIGNED) AS a, CAST(10 AS SIGNED) AS b UNION ALL SELECT CAST(NULL AS SIGNED) AS a, CAST(20 AS SIGNED) AS b UNION ALL SELECT CAST(3 AS SIGNED) AS a, CAST(30 AS SIGNED) AS b UNION ALL SELECT CAST(NULL AS SIGNED) AS a, CAST(40 AS SIGNED) AS b) AS input_rows WHERE input_rows.a IS NOT NULL"]);
        assert_eq!(result.engine, TIDB_ENGINE);
        assert_eq!(result.adapter, TIDB_ADAPTER);
        assert_eq!(
            result.outcome,
            CaseOutcome::Rows {
                schema: vec![
                    SchemaField {
                        name: "a".to_string(),
                        logical_type: "int32".to_string(),
                        nullable: true,
                    },
                    SchemaField {
                        name: "b".to_string(),
                        logical_type: "int32".to_string(),
                        nullable: false,
                    },
                ],
                rows: vec![vec![json!(1), json!(10)], vec![json!(3), json!(30)]],
                row_count: 2,
            }
        );
    }

    #[test]
    fn execute_normalizes_missing_column_errors() {
        let request = TidbFirstFilterIsNotNullSliceAdapter::canonical_requests()
            .into_iter()
            .find(|request| request.case_id == "missing-column-error")
            .unwrap();
        let runner = StubRunner::error(EngineExecutionError::EngineFailure {
            code: Some("1054".to_string()),
            message: "Unknown column '__missing_column_1' in 'where clause'".to_string(),
        });

        let result = TidbFirstFilterIsNotNullSliceAdapter::execute(&request, &runner).unwrap();

        assert_eq!(
            result.outcome,
            CaseOutcome::Error {
                error_class: ErrorClass::MissingColumn,
                engine_code: Some("1054".to_string()),
                engine_message: Some(
                    "Unknown column '__missing_column_1' in 'where clause'".to_string()
                ),
            }
        );
    }

    #[test]
    fn execute_normalizes_unsupported_predicate_type_errors() {
        let request = TidbFirstFilterIsNotNullSliceAdapter::canonical_requests()
            .into_iter()
            .find(|request| request.case_id == "unsupported-predicate-type-error")
            .unwrap();
        let runner = StubRunner::error(EngineExecutionError::EngineFailure {
            code: Some("1105".to_string()),
            message:
                "unsupported predicate type: utf8 input is out of scope for first-filter-is-not-null-slice"
                    .to_string(),
        });

        let result = TidbFirstFilterIsNotNullSliceAdapter::execute(&request, &runner).unwrap();

        assert_eq!(
            result.outcome,
            CaseOutcome::Error {
                error_class: ErrorClass::UnsupportedPredicateType,
                engine_code: Some("1105".to_string()),
                engine_message: Some(
                    "unsupported predicate type: utf8 input is out of scope for first-filter-is-not-null-slice"
                        .to_string()
                ),
            }
        );
    }

    #[test]
    fn execute_normalizes_adapter_unavailable_without_extra_code() {
        let request = TidbFirstFilterIsNotNullSliceAdapter::canonical_requests()
            .into_iter()
            .find(|request| request.case_id == "all-rows-kept")
            .unwrap();
        let runner = StubRunner::error(EngineExecutionError::AdapterUnavailable {
            message: Some("TiDB DSN is not configured".to_string()),
        });

        let result = TidbFirstFilterIsNotNullSliceAdapter::execute(&request, &runner).unwrap();
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
        let mut request = TidbFirstFilterIsNotNullSliceAdapter::canonical_requests()
            .into_iter()
            .find(|request| request.case_id == "all-rows-kept")
            .unwrap();
        request.spec_refs.pop();

        let error = TidbFirstFilterIsNotNullSliceAdapter::lower_request(&request).unwrap_err();

        assert_eq!(
            error,
            AdapterRequestValidationError::MismatchedSpecRefs {
                case_id: "all-rows-kept".to_string(),
                expected_spec_refs: vec![
                    "docs/spec/first-filter-is-not-null.md".to_string(),
                    "docs/spec/type-system.md".to_string(),
                    "tests/conformance/first-filter-is-not-null-slice.md".to_string(),
                    "tests/differential/first-filter-is-not-null-slice.md".to_string(),
                ],
                actual_spec_refs: vec![
                    "docs/spec/first-filter-is-not-null.md".to_string(),
                    "docs/spec/type-system.md".to_string(),
                    "tests/conformance/first-filter-is-not-null-slice.md".to_string(),
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
