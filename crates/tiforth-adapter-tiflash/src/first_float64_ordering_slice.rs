use serde::{Deserialize, Serialize};
use serde_json::Value;

pub const FIRST_FLOAT64_ORDERING_SLICE_ID: &str = "first-float64-ordering-slice";
pub const TIFLASH_ENGINE: &str = "tiflash";
pub const TIFLASH_ADAPTER: &str = "tiflash-sql";
pub const COMPARISON_MODE_ROW_ORDER_PRESERVED: &str = "row-order-preserved";
pub const COMPARISON_MODE_FLOAT64_MULTISET_CANONICAL: &str = "float64-multiset-canonical";

const FIRST_FLOAT64_ORDERING_SLICE_SPEC_REFS: [&str; 4] = [
    "docs/design/first-float64-ordering-slice.md",
    "docs/spec/type-system.md",
    "tests/conformance/first-float64-ordering-slice.md",
    "tests/differential/first-float64-ordering-slice.md",
];

const FLOAT64_BASIC_INPUT_SQL: &str = concat!(
    "SELECT CAST(-1.5 AS DOUBLE) AS f ",
    "UNION ALL ",
    "SELECT CAST(0.0 AS DOUBLE) AS f ",
    "UNION ALL ",
    "SELECT CAST(2.25 AS DOUBLE) AS f"
);

const FLOAT64_SPECIAL_VALUES_INPUT_SQL: &str = concat!(
    "SELECT CAST('-Infinity' AS DOUBLE) AS f ",
    "UNION ALL ",
    "SELECT CAST('-0.0' AS DOUBLE) AS f ",
    "UNION ALL ",
    "SELECT CAST('0.0' AS DOUBLE) AS f ",
    "UNION ALL ",
    "SELECT CAST('Infinity' AS DOUBLE) AS f ",
    "UNION ALL ",
    "SELECT CAST('NaN' AS DOUBLE) AS f"
);

const FLOAT64_NULLABLE_SPECIAL_VALUES_INPUT_SQL: &str = concat!(
    "SELECT CAST(NULL AS DOUBLE) AS f ",
    "UNION ALL ",
    "SELECT CAST('-Infinity' AS DOUBLE) AS f ",
    "UNION ALL ",
    "SELECT CAST(NULL AS DOUBLE) AS f ",
    "UNION ALL ",
    "SELECT CAST('NaN' AS DOUBLE) AS f ",
    "UNION ALL ",
    "SELECT CAST(1.0 AS DOUBLE) AS f"
);

const FLOAT64_ORDERING_SCRAMBLE_INPUT_SQL: &str = concat!(
    "SELECT CAST('NaN' AS DOUBLE) AS f ",
    "UNION ALL ",
    "SELECT CAST(1.0 AS DOUBLE) AS f ",
    "UNION ALL ",
    "SELECT CAST('-Infinity' AS DOUBLE) AS f ",
    "UNION ALL ",
    "SELECT CAST('Infinity' AS DOUBLE) AS f ",
    "UNION ALL ",
    "SELECT CAST(0.0 AS DOUBLE) AS f ",
    "UNION ALL ",
    "SELECT CAST('-0.0' AS DOUBLE) AS f"
);

const FLOAT32_BASIC_INPUT_SQL: &str = concat!(
    "SELECT CAST(1.5 AS FLOAT) AS f32 ",
    "UNION ALL ",
    "SELECT CAST(0.0 AS FLOAT) AS f32 ",
    "UNION ALL ",
    "SELECT CAST(-2.25 AS FLOAT) AS f32"
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
    #[serde(skip_serializing_if = "Option::is_none")]
    pub filter_ref: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TiflashExecutionPlan {
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
    pub comparison_mode: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub projection_ref: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub filter_ref: Option<String>,
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
    UnsupportedFloatingType,
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
        expected_comparison_mode: &'static str,
        actual_comparison_mode: String,
        expected_projection_ref: Option<&'static str>,
        actual_projection_ref: Option<String>,
        expected_filter_ref: Option<&'static str>,
        actual_filter_ref: Option<String>,
    },
    MismatchedSpecRefs {
        case_id: String,
        expected_spec_refs: Vec<String>,
        actual_spec_refs: Vec<String>,
    },
}

pub trait TiflashRunner {
    fn run(
        &self,
        plan: &TiflashExecutionPlan,
    ) -> Result<EngineExecutionResult, EngineExecutionError>;
}

#[derive(Debug, Default, Clone, Copy)]
pub struct TiflashFirstFloat64OrderingSliceAdapter;

impl TiflashFirstFloat64OrderingSliceAdapter {
    pub fn canonical_requests() -> Vec<AdapterRequest> {
        CASE_DEFINITIONS
            .iter()
            .copied()
            .map(CaseDefinition::canonical_request)
            .collect()
    }

    pub fn lower_request(
        request: &AdapterRequest,
    ) -> Result<TiflashExecutionPlan, AdapterRequestValidationError> {
        let case = validate_request(request)?;

        Ok(TiflashExecutionPlan {
            request: request.clone(),
            sql: case.render_sql(),
        })
    }

    pub fn execute<R: TiflashRunner>(
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
            engine: TIFLASH_ENGINE.to_string(),
            adapter: TIFLASH_ADAPTER.to_string(),
            case_id: request.case_id.clone(),
            spec_refs: request.spec_refs.clone(),
            input_ref: request.input_ref.clone(),
            comparison_mode: request.comparison_mode.clone(),
            projection_ref: request.projection_ref.clone(),
            filter_ref: request.filter_ref.clone(),
            outcome,
        })
    }
}

#[derive(Debug, Clone, Copy)]
struct CaseDefinition {
    case_id: &'static str,
    input_ref: &'static str,
    comparison_mode: &'static str,
    projection_ref: Option<&'static str>,
    filter_ref: Option<&'static str>,
}

impl CaseDefinition {
    fn canonical_request(self) -> AdapterRequest {
        AdapterRequest {
            slice_id: FIRST_FLOAT64_ORDERING_SLICE_ID.to_string(),
            case_id: self.case_id.to_string(),
            spec_refs: FIRST_FLOAT64_ORDERING_SLICE_SPEC_REFS
                .iter()
                .map(|spec_ref| (*spec_ref).to_string())
                .collect(),
            input_ref: self.input_ref.to_string(),
            comparison_mode: self.comparison_mode.to_string(),
            projection_ref: self.projection_ref.map(str::to_string),
            filter_ref: self.filter_ref.map(str::to_string),
        }
    }

    fn render_sql(self) -> String {
        let input_sql = input_sql(self.input_ref);

        match (self.projection_ref, self.filter_ref) {
            (Some("column-0"), None) => {
                let column_name = projection_column_name(self.input_ref);
                format!(
                    "SELECT input_rows.{column_name} AS {column_name} FROM ({input_sql}) AS input_rows"
                )
            }
            (None, Some(filter_ref)) => {
                let filter_condition = filter_condition(self.input_ref, filter_ref);
                format!("SELECT * FROM ({input_sql}) AS input_rows WHERE {filter_condition}")
            }
            _ => unreachable!("validated case definitions always set exactly one operation ref"),
        }
    }
}

const CASE_DEFINITIONS: [CaseDefinition; 7] = [
    CaseDefinition {
        case_id: "float64-column-passthrough",
        input_ref: "first-float64-basic",
        comparison_mode: COMPARISON_MODE_ROW_ORDER_PRESERVED,
        projection_ref: Some("column-0"),
        filter_ref: None,
    },
    CaseDefinition {
        case_id: "float64-special-values-passthrough",
        input_ref: "first-float64-special-values",
        comparison_mode: COMPARISON_MODE_ROW_ORDER_PRESERVED,
        projection_ref: Some("column-0"),
        filter_ref: None,
    },
    CaseDefinition {
        case_id: "float64-is-not-null-all-kept",
        input_ref: "first-float64-special-values",
        comparison_mode: COMPARISON_MODE_ROW_ORDER_PRESERVED,
        projection_ref: None,
        filter_ref: Some("is-not-null-column-0"),
    },
    CaseDefinition {
        case_id: "float64-is-not-null-mixed-keep-drop",
        input_ref: "first-float64-nullable-special-values",
        comparison_mode: COMPARISON_MODE_ROW_ORDER_PRESERVED,
        projection_ref: None,
        filter_ref: Some("is-not-null-column-0"),
    },
    CaseDefinition {
        case_id: "float64-canonical-ordering-normalization",
        input_ref: "first-float64-ordering-scramble",
        comparison_mode: COMPARISON_MODE_FLOAT64_MULTISET_CANONICAL,
        projection_ref: Some("column-0"),
        filter_ref: None,
    },
    CaseDefinition {
        case_id: "float64-missing-column-error",
        input_ref: "first-float64-basic",
        comparison_mode: COMPARISON_MODE_ROW_ORDER_PRESERVED,
        projection_ref: None,
        filter_ref: Some("is-not-null-column-1"),
    },
    CaseDefinition {
        case_id: "unsupported-floating-type-error",
        input_ref: "first-float32-basic",
        comparison_mode: COMPARISON_MODE_ROW_ORDER_PRESERVED,
        projection_ref: Some("column-0"),
        filter_ref: None,
    },
];

fn validate_request(
    request: &AdapterRequest,
) -> Result<CaseDefinition, AdapterRequestValidationError> {
    if request.slice_id != FIRST_FLOAT64_ORDERING_SLICE_ID {
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
        || request.comparison_mode != case.comparison_mode
        || request.projection_ref.as_deref() != case.projection_ref
        || request.filter_ref.as_deref() != case.filter_ref
    {
        return Err(AdapterRequestValidationError::MismatchedCaseDefinition {
            case_id: request.case_id.clone(),
            expected_input_ref: case.input_ref,
            actual_input_ref: request.input_ref.clone(),
            expected_comparison_mode: case.comparison_mode,
            actual_comparison_mode: request.comparison_mode.clone(),
            expected_projection_ref: case.projection_ref,
            actual_projection_ref: request.projection_ref.clone(),
            expected_filter_ref: case.filter_ref,
            actual_filter_ref: request.filter_ref.clone(),
        });
    }

    let expected_spec_refs: Vec<String> = FIRST_FLOAT64_ORDERING_SLICE_SPEC_REFS
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
        "first-float64-basic" => FLOAT64_BASIC_INPUT_SQL,
        "first-float64-special-values" => FLOAT64_SPECIAL_VALUES_INPUT_SQL,
        "first-float64-nullable-special-values" => FLOAT64_NULLABLE_SPECIAL_VALUES_INPUT_SQL,
        "first-float64-ordering-scramble" => FLOAT64_ORDERING_SCRAMBLE_INPUT_SQL,
        "first-float32-basic" => FLOAT32_BASIC_INPUT_SQL,
        _ => unreachable!("validated input refs should always be known"),
    }
}

fn projection_column_name(input_ref: &str) -> &'static str {
    match input_ref {
        "first-float64-basic"
        | "first-float64-special-values"
        | "first-float64-nullable-special-values"
        | "first-float64-ordering-scramble" => "f",
        "first-float32-basic" => "f32",
        _ => unreachable!("validated input refs should always be known"),
    }
}

fn filter_condition(input_ref: &str, filter_ref: &str) -> &'static str {
    match filter_ref {
        "is-not-null-column-0" => match input_ref {
            "first-float64-basic"
            | "first-float64-special-values"
            | "first-float64-nullable-special-values"
            | "first-float64-ordering-scramble" => "input_rows.f IS NOT NULL",
            "first-float32-basic" => "input_rows.f32 IS NOT NULL",
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
            } else if is_unsupported_floating_type(code.as_deref(), &message) {
                ErrorClass::UnsupportedFloatingType
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

fn is_missing_column(engine_code: Option<&str>, engine_message: &str) -> bool {
    let normalized_message = engine_message.to_ascii_lowercase();

    engine_code == Some("1054")
        || normalized_message.contains("unknown column")
        || normalized_message.contains("no such column")
}

fn is_unsupported_floating_type(engine_code: Option<&str>, engine_message: &str) -> bool {
    let normalized_message = engine_message.to_ascii_lowercase();

    engine_code == Some("1105")
        || normalized_message.contains("unsupported floating type")
        || (normalized_message.contains("float") && normalized_message.contains("out of scope"))
}

fn normalize_logical_type(engine_type: &str) -> String {
    let normalized = engine_type.trim().to_ascii_lowercase();
    let normalized = normalized.split('(').next().unwrap_or(&normalized).trim();

    match normalized {
        "double" | "double precision" | "float64" => "float64".to_string(),
        "float" | "real" | "float32" => "float32".to_string(),
        "int" | "integer" | "mediumint" | "smallint" | "tinyint" => "int32".to_string(),
        other => other.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn canonical_requests_cover_all_documented_cases() {
        let requests = TiflashFirstFloat64OrderingSliceAdapter::canonical_requests();
        let case_ids: Vec<&str> = requests
            .iter()
            .map(|request| request.case_id.as_str())
            .collect();

        assert_eq!(requests.len(), 7);
        assert_eq!(
            case_ids,
            vec![
                "float64-column-passthrough",
                "float64-special-values-passthrough",
                "float64-is-not-null-all-kept",
                "float64-is-not-null-mixed-keep-drop",
                "float64-canonical-ordering-normalization",
                "float64-missing-column-error",
                "unsupported-floating-type-error",
            ]
        );

        for request in requests {
            assert_eq!(request.slice_id, FIRST_FLOAT64_ORDERING_SLICE_ID);
            assert_eq!(request.spec_refs.len(), 4);
            assert_eq!(
                request.spec_refs[0],
                "docs/design/first-float64-ordering-slice.md"
            );
            assert_eq!(request.spec_refs[1], "docs/spec/type-system.md");
            assert_eq!(
                request.spec_refs[2],
                "tests/conformance/first-float64-ordering-slice.md"
            );
            assert_eq!(
                request.spec_refs[3],
                "tests/differential/first-float64-ordering-slice.md"
            );
            let op_ref_count =
                request.projection_ref.iter().count() + request.filter_ref.iter().count();
            assert_eq!(op_ref_count, 1);
            assert!(
                request.comparison_mode == COMPARISON_MODE_ROW_ORDER_PRESERVED
                    || request.comparison_mode == COMPARISON_MODE_FLOAT64_MULTISET_CANONICAL
            );
        }
    }

    #[test]
    fn lowering_renders_tiflash_sql_for_each_documented_case() {
        let requests = TiflashFirstFloat64OrderingSliceAdapter::canonical_requests();
        let plans: Vec<(String, String)> = requests
            .iter()
            .map(|request| {
                let plan = TiflashFirstFloat64OrderingSliceAdapter::lower_request(request).unwrap();
                (plan.request.case_id, plan.sql)
            })
            .collect();

        assert_eq!(
            plans,
            vec![
                (
                    "float64-column-passthrough".to_string(),
                    "SELECT input_rows.f AS f FROM (SELECT CAST(-1.5 AS DOUBLE) AS f UNION ALL SELECT CAST(0.0 AS DOUBLE) AS f UNION ALL SELECT CAST(2.25 AS DOUBLE) AS f) AS input_rows".to_string()
                ),
                (
                    "float64-special-values-passthrough".to_string(),
                    "SELECT input_rows.f AS f FROM (SELECT CAST('-Infinity' AS DOUBLE) AS f UNION ALL SELECT CAST('-0.0' AS DOUBLE) AS f UNION ALL SELECT CAST('0.0' AS DOUBLE) AS f UNION ALL SELECT CAST('Infinity' AS DOUBLE) AS f UNION ALL SELECT CAST('NaN' AS DOUBLE) AS f) AS input_rows".to_string()
                ),
                (
                    "float64-is-not-null-all-kept".to_string(),
                    "SELECT * FROM (SELECT CAST('-Infinity' AS DOUBLE) AS f UNION ALL SELECT CAST('-0.0' AS DOUBLE) AS f UNION ALL SELECT CAST('0.0' AS DOUBLE) AS f UNION ALL SELECT CAST('Infinity' AS DOUBLE) AS f UNION ALL SELECT CAST('NaN' AS DOUBLE) AS f) AS input_rows WHERE input_rows.f IS NOT NULL".to_string()
                ),
                (
                    "float64-is-not-null-mixed-keep-drop".to_string(),
                    "SELECT * FROM (SELECT CAST(NULL AS DOUBLE) AS f UNION ALL SELECT CAST('-Infinity' AS DOUBLE) AS f UNION ALL SELECT CAST(NULL AS DOUBLE) AS f UNION ALL SELECT CAST('NaN' AS DOUBLE) AS f UNION ALL SELECT CAST(1.0 AS DOUBLE) AS f) AS input_rows WHERE input_rows.f IS NOT NULL".to_string()
                ),
                (
                    "float64-canonical-ordering-normalization".to_string(),
                    "SELECT input_rows.f AS f FROM (SELECT CAST('NaN' AS DOUBLE) AS f UNION ALL SELECT CAST(1.0 AS DOUBLE) AS f UNION ALL SELECT CAST('-Infinity' AS DOUBLE) AS f UNION ALL SELECT CAST('Infinity' AS DOUBLE) AS f UNION ALL SELECT CAST(0.0 AS DOUBLE) AS f UNION ALL SELECT CAST('-0.0' AS DOUBLE) AS f) AS input_rows".to_string()
                ),
                (
                    "float64-missing-column-error".to_string(),
                    "SELECT * FROM (SELECT CAST(-1.5 AS DOUBLE) AS f UNION ALL SELECT CAST(0.0 AS DOUBLE) AS f UNION ALL SELECT CAST(2.25 AS DOUBLE) AS f) AS input_rows WHERE input_rows.__missing_column_1 IS NOT NULL".to_string()
                ),
                (
                    "unsupported-floating-type-error".to_string(),
                    "SELECT input_rows.f32 AS f32 FROM (SELECT CAST(1.5 AS FLOAT) AS f32 UNION ALL SELECT CAST(0.0 AS FLOAT) AS f32 UNION ALL SELECT CAST(-2.25 AS FLOAT) AS f32) AS input_rows".to_string()
                ),
            ]
        );
    }

    #[test]
    fn execute_rows_normalizes_schema_and_row_count() {
        let request = TiflashFirstFloat64OrderingSliceAdapter::canonical_requests()
            .into_iter()
            .find(|request| request.case_id == "float64-canonical-ordering-normalization")
            .unwrap();
        let runner = StubRunner::rows(
            vec![EngineColumn {
                name: "f".to_string(),
                engine_type: "double".to_string(),
                nullable: false,
            }],
            vec![
                vec![json!("NaN")],
                vec![json!("1.0")],
                vec![json!("-Infinity")],
                vec![json!("Infinity")],
                vec![json!("0.0")],
                vec![json!("-0.0")],
            ],
        );

        let result = TiflashFirstFloat64OrderingSliceAdapter::execute(&request, &runner).unwrap();

        assert_eq!(result.engine, TIFLASH_ENGINE);
        assert_eq!(result.adapter, TIFLASH_ADAPTER);
        assert_eq!(
            result.outcome,
            CaseOutcome::Rows {
                schema: vec![SchemaField {
                    name: "f".to_string(),
                    logical_type: "float64".to_string(),
                    nullable: false,
                }],
                rows: vec![
                    vec![json!("NaN")],
                    vec![json!("1.0")],
                    vec![json!("-Infinity")],
                    vec![json!("Infinity")],
                    vec![json!("0.0")],
                    vec![json!("-0.0")],
                ],
                row_count: 6,
            }
        );
        assert_eq!(
            result.comparison_mode,
            COMPARISON_MODE_FLOAT64_MULTISET_CANONICAL
        );
    }

    #[test]
    fn execute_normalizes_missing_column_errors() {
        let request = TiflashFirstFloat64OrderingSliceAdapter::canonical_requests()
            .into_iter()
            .find(|request| request.case_id == "float64-missing-column-error")
            .unwrap();
        let runner = StubRunner::error(EngineExecutionError::EngineFailure {
            code: Some("1054".to_string()),
            message: "Unknown column '__missing_column_1' in 'where clause'".to_string(),
        });

        let result = TiflashFirstFloat64OrderingSliceAdapter::execute(&request, &runner).unwrap();

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
    fn execute_normalizes_unsupported_floating_type_errors() {
        let request = TiflashFirstFloat64OrderingSliceAdapter::canonical_requests()
            .into_iter()
            .find(|request| request.case_id == "unsupported-floating-type-error")
            .unwrap();
        let runner = StubRunner::error(EngineExecutionError::EngineFailure {
            code: Some("1105".to_string()),
            message:
                "unsupported floating type: float32 input is out of scope for first-float64-ordering-slice"
                    .to_string(),
        });

        let result = TiflashFirstFloat64OrderingSliceAdapter::execute(&request, &runner).unwrap();

        assert_eq!(
            result.outcome,
            CaseOutcome::Error {
                error_class: ErrorClass::UnsupportedFloatingType,
                engine_code: Some("1105".to_string()),
                engine_message: Some(
                    "unsupported floating type: float32 input is out of scope for first-float64-ordering-slice"
                        .to_string()
                ),
            }
        );
    }

    #[test]
    fn lowering_rejects_requests_with_mismatched_operation_refs() {
        let mut request = TiflashFirstFloat64OrderingSliceAdapter::canonical_requests()
            .into_iter()
            .find(|request| request.case_id == "float64-column-passthrough")
            .unwrap();
        request.projection_ref = None;
        request.filter_ref = Some("is-not-null-column-0".to_string());

        let error = TiflashFirstFloat64OrderingSliceAdapter::lower_request(&request).unwrap_err();

        assert_eq!(
            error,
            AdapterRequestValidationError::MismatchedCaseDefinition {
                case_id: "float64-column-passthrough".to_string(),
                expected_input_ref: "first-float64-basic",
                actual_input_ref: "first-float64-basic".to_string(),
                expected_comparison_mode: COMPARISON_MODE_ROW_ORDER_PRESERVED,
                actual_comparison_mode: COMPARISON_MODE_ROW_ORDER_PRESERVED.to_string(),
                expected_projection_ref: Some("column-0"),
                actual_projection_ref: None,
                expected_filter_ref: None,
                actual_filter_ref: Some("is-not-null-column-0".to_string()),
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

    impl TiflashRunner for StubRunner {
        fn run(
            &self,
            _plan: &TiflashExecutionPlan,
        ) -> Result<EngineExecutionResult, EngineExecutionError> {
            self.result.clone()
        }
    }
}
