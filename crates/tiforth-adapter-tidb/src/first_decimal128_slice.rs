use serde::{Deserialize, Serialize};
use serde_json::Value;

pub const FIRST_DECIMAL128_SLICE_ID: &str = "first-decimal128-slice";
pub const TIDB_ENGINE: &str = "tidb";
pub const TIDB_ADAPTER: &str = "tidb-sql";

const FIRST_DECIMAL128_SLICE_SPEC_REFS: [&str; 4] = [
    "docs/design/first-decimal-semantic-slice.md",
    "docs/spec/type-system.md",
    "tests/conformance/first-decimal128-slice.md",
    "tests/differential/first-decimal128-slice.md",
];

const DECIMAL128_BASIC_INPUT_SQL: &str = concat!(
    "SELECT CAST('1.00' AS DECIMAL(10,2)) AS d ",
    "UNION ALL ",
    "SELECT CAST('2.50' AS DECIMAL(10,2)) AS d ",
    "UNION ALL ",
    "SELECT CAST('-3.75' AS DECIMAL(10,2)) AS d"
);

const DECIMAL128_NULLABLE_INPUT_SQL: &str = concat!(
    "SELECT CAST('1.00' AS DECIMAL(10,2)) AS d ",
    "UNION ALL ",
    "SELECT CAST(NULL AS DECIMAL(10,2)) AS d ",
    "UNION ALL ",
    "SELECT CAST('-3.75' AS DECIMAL(10,2)) AS d ",
    "UNION ALL ",
    "SELECT CAST(NULL AS DECIMAL(10,2)) AS d"
);

const DECIMAL128_ALL_NULL_INPUT_SQL: &str = concat!(
    "SELECT CAST(NULL AS DECIMAL(10,2)) AS d ",
    "UNION ALL ",
    "SELECT CAST(NULL AS DECIMAL(10,2)) AS d ",
    "UNION ALL ",
    "SELECT CAST(NULL AS DECIMAL(10,2)) AS d"
);

const DECIMAL256_BASIC_INPUT_SQL: &str = concat!(
    "SELECT CAST('1.0000' AS DECIMAL(40,4)) AS d256 ",
    "UNION ALL ",
    "SELECT CAST('2.5000' AS DECIMAL(40,4)) AS d256 ",
    "UNION ALL ",
    "SELECT CAST('-3.7500' AS DECIMAL(40,4)) AS d256"
);

const DECIMAL128_INVALID_SCALE_INPUT_SQL: &str = "SELECT CAST('1.00' AS DECIMAL(10,12)) AS d_bad";

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
    UnsupportedDecimalType,
    InvalidDecimalMetadata,
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

pub trait TidbRunner {
    fn run(&self, plan: &TidbExecutionPlan) -> Result<EngineExecutionResult, EngineExecutionError>;
}

#[derive(Debug, Default, Clone, Copy)]
pub struct TidbFirstDecimal128SliceAdapter;

impl TidbFirstDecimal128SliceAdapter {
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
    projection_ref: Option<&'static str>,
    filter_ref: Option<&'static str>,
}

impl CaseDefinition {
    fn canonical_request(self) -> AdapterRequest {
        AdapterRequest {
            slice_id: FIRST_DECIMAL128_SLICE_ID.to_string(),
            case_id: self.case_id.to_string(),
            spec_refs: FIRST_DECIMAL128_SLICE_SPEC_REFS
                .iter()
                .map(|spec_ref| (*spec_ref).to_string())
                .collect(),
            input_ref: self.input_ref.to_string(),
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

const CASE_DEFINITIONS: [CaseDefinition; 8] = [
    CaseDefinition {
        case_id: "decimal128-column-passthrough",
        input_ref: "first-decimal128-basic",
        projection_ref: Some("column-0"),
        filter_ref: None,
    },
    CaseDefinition {
        case_id: "decimal128-column-null-preserve",
        input_ref: "first-decimal128-nullable",
        projection_ref: Some("column-0"),
        filter_ref: None,
    },
    CaseDefinition {
        case_id: "decimal128-is-not-null-all-kept",
        input_ref: "first-decimal128-basic",
        projection_ref: None,
        filter_ref: Some("is-not-null-column-0"),
    },
    CaseDefinition {
        case_id: "decimal128-is-not-null-all-dropped",
        input_ref: "first-decimal128-all-null",
        projection_ref: None,
        filter_ref: Some("is-not-null-column-0"),
    },
    CaseDefinition {
        case_id: "decimal128-is-not-null-mixed-keep-drop",
        input_ref: "first-decimal128-nullable",
        projection_ref: None,
        filter_ref: Some("is-not-null-column-0"),
    },
    CaseDefinition {
        case_id: "decimal128-missing-column-error",
        input_ref: "first-decimal128-basic",
        projection_ref: None,
        filter_ref: Some("is-not-null-column-1"),
    },
    CaseDefinition {
        case_id: "unsupported-decimal-type-error",
        input_ref: "first-decimal256-basic",
        projection_ref: Some("column-0"),
        filter_ref: None,
    },
    CaseDefinition {
        case_id: "invalid-decimal-metadata-error",
        input_ref: "first-decimal128-invalid-scale",
        projection_ref: Some("column-0"),
        filter_ref: None,
    },
];

fn validate_request(
    request: &AdapterRequest,
) -> Result<CaseDefinition, AdapterRequestValidationError> {
    if request.slice_id != FIRST_DECIMAL128_SLICE_ID {
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
    {
        return Err(AdapterRequestValidationError::MismatchedCaseDefinition {
            case_id: request.case_id.clone(),
            expected_input_ref: case.input_ref,
            actual_input_ref: request.input_ref.clone(),
            expected_projection_ref: case.projection_ref,
            actual_projection_ref: request.projection_ref.clone(),
            expected_filter_ref: case.filter_ref,
            actual_filter_ref: request.filter_ref.clone(),
        });
    }

    let expected_spec_refs: Vec<String> = FIRST_DECIMAL128_SLICE_SPEC_REFS
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
        "first-decimal128-basic" => DECIMAL128_BASIC_INPUT_SQL,
        "first-decimal128-nullable" => DECIMAL128_NULLABLE_INPUT_SQL,
        "first-decimal128-all-null" => DECIMAL128_ALL_NULL_INPUT_SQL,
        "first-decimal256-basic" => DECIMAL256_BASIC_INPUT_SQL,
        "first-decimal128-invalid-scale" => DECIMAL128_INVALID_SCALE_INPUT_SQL,
        _ => unreachable!("validated input refs should always be known"),
    }
}

fn projection_column_name(input_ref: &str) -> &'static str {
    match input_ref {
        "first-decimal128-basic" | "first-decimal128-nullable" | "first-decimal128-all-null" => "d",
        "first-decimal256-basic" => "d256",
        "first-decimal128-invalid-scale" => "d_bad",
        _ => unreachable!("validated input refs should always be known"),
    }
}

fn filter_condition(input_ref: &str, filter_ref: &str) -> &'static str {
    match filter_ref {
        "is-not-null-column-0" => match input_ref {
            "first-decimal128-basic"
            | "first-decimal128-nullable"
            | "first-decimal128-all-null" => "input_rows.d IS NOT NULL",
            "first-decimal256-basic" => "input_rows.d256 IS NOT NULL",
            "first-decimal128-invalid-scale" => "input_rows.d_bad IS NOT NULL",
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
            } else if is_invalid_decimal_metadata(code.as_deref(), &message) {
                ErrorClass::InvalidDecimalMetadata
            } else if is_unsupported_decimal_type(code.as_deref(), &message) {
                ErrorClass::UnsupportedDecimalType
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

fn is_unsupported_decimal_type(engine_code: Option<&str>, engine_message: &str) -> bool {
    let normalized_message = engine_message.to_ascii_lowercase();

    normalized_message.contains("unsupported decimal type")
        || normalized_message.contains("decimal256")
        || (engine_code == Some("1105") && normalized_message.contains("out of scope"))
}

fn is_invalid_decimal_metadata(engine_code: Option<&str>, engine_message: &str) -> bool {
    let normalized_message = engine_message.to_ascii_lowercase();

    normalized_message.contains("invalid decimal metadata")
        || normalized_message.contains("scale") && normalized_message.contains("precision")
        || normalized_message.contains("too big scale")
        || (engine_code == Some("1105") && normalized_message.contains("decimal(10,12)"))
}

fn normalize_logical_type(engine_type: &str) -> String {
    let normalized = engine_type.trim().to_ascii_lowercase();
    if let Some((precision, scale)) = parse_decimal_type(&normalized) {
        if precision <= 38 {
            return format!("decimal128({precision},{scale})");
        }
        return format!("decimal256({precision},{scale})");
    }

    let base = normalized.split_whitespace().next().unwrap_or(&normalized);
    match base {
        "int" | "integer" | "mediumint" | "smallint" | "tinyint" => "int32".to_string(),
        other => other.to_string(),
    }
}

fn parse_decimal_type(normalized_type: &str) -> Option<(u32, u32)> {
    let base = normalized_type
        .split_whitespace()
        .next()
        .unwrap_or(normalized_type);
    let inside = base.strip_prefix("decimal(")?.strip_suffix(')')?;
    let mut parts = inside.split(',');
    let precision = parts.next()?.trim().parse().ok()?;
    let scale = parts.next()?.trim().parse().ok()?;
    if parts.next().is_some() {
        return None;
    }
    Some((precision, scale))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn canonical_requests_cover_all_documented_cases() {
        let requests = TidbFirstDecimal128SliceAdapter::canonical_requests();
        let case_ids: Vec<&str> = requests
            .iter()
            .map(|request| request.case_id.as_str())
            .collect();

        assert_eq!(requests.len(), 8);
        assert_eq!(
            case_ids,
            vec![
                "decimal128-column-passthrough",
                "decimal128-column-null-preserve",
                "decimal128-is-not-null-all-kept",
                "decimal128-is-not-null-all-dropped",
                "decimal128-is-not-null-mixed-keep-drop",
                "decimal128-missing-column-error",
                "unsupported-decimal-type-error",
                "invalid-decimal-metadata-error",
            ]
        );

        for request in requests {
            assert_eq!(request.slice_id, FIRST_DECIMAL128_SLICE_ID);
            assert_eq!(request.spec_refs.len(), 4);
            assert_eq!(
                request.spec_refs[0],
                "docs/design/first-decimal-semantic-slice.md"
            );
            let op_ref_count =
                request.projection_ref.iter().count() + request.filter_ref.iter().count();
            assert_eq!(op_ref_count, 1);
        }
    }

    #[test]
    fn lowering_renders_tidb_sql_for_each_documented_case() {
        let requests = TidbFirstDecimal128SliceAdapter::canonical_requests();
        let plans: Vec<(String, String)> = requests
            .iter()
            .map(|request| {
                let plan = TidbFirstDecimal128SliceAdapter::lower_request(request).unwrap();
                (plan.request.case_id, plan.sql)
            })
            .collect();

        assert_eq!(
            plans,
            vec![
                (
                    "decimal128-column-passthrough".to_string(),
                    "SELECT input_rows.d AS d FROM (SELECT CAST('1.00' AS DECIMAL(10,2)) AS d UNION ALL SELECT CAST('2.50' AS DECIMAL(10,2)) AS d UNION ALL SELECT CAST('-3.75' AS DECIMAL(10,2)) AS d) AS input_rows".to_string()
                ),
                (
                    "decimal128-column-null-preserve".to_string(),
                    "SELECT input_rows.d AS d FROM (SELECT CAST('1.00' AS DECIMAL(10,2)) AS d UNION ALL SELECT CAST(NULL AS DECIMAL(10,2)) AS d UNION ALL SELECT CAST('-3.75' AS DECIMAL(10,2)) AS d UNION ALL SELECT CAST(NULL AS DECIMAL(10,2)) AS d) AS input_rows".to_string()
                ),
                (
                    "decimal128-is-not-null-all-kept".to_string(),
                    "SELECT * FROM (SELECT CAST('1.00' AS DECIMAL(10,2)) AS d UNION ALL SELECT CAST('2.50' AS DECIMAL(10,2)) AS d UNION ALL SELECT CAST('-3.75' AS DECIMAL(10,2)) AS d) AS input_rows WHERE input_rows.d IS NOT NULL".to_string()
                ),
                (
                    "decimal128-is-not-null-all-dropped".to_string(),
                    "SELECT * FROM (SELECT CAST(NULL AS DECIMAL(10,2)) AS d UNION ALL SELECT CAST(NULL AS DECIMAL(10,2)) AS d UNION ALL SELECT CAST(NULL AS DECIMAL(10,2)) AS d) AS input_rows WHERE input_rows.d IS NOT NULL".to_string()
                ),
                (
                    "decimal128-is-not-null-mixed-keep-drop".to_string(),
                    "SELECT * FROM (SELECT CAST('1.00' AS DECIMAL(10,2)) AS d UNION ALL SELECT CAST(NULL AS DECIMAL(10,2)) AS d UNION ALL SELECT CAST('-3.75' AS DECIMAL(10,2)) AS d UNION ALL SELECT CAST(NULL AS DECIMAL(10,2)) AS d) AS input_rows WHERE input_rows.d IS NOT NULL".to_string()
                ),
                (
                    "decimal128-missing-column-error".to_string(),
                    "SELECT * FROM (SELECT CAST('1.00' AS DECIMAL(10,2)) AS d UNION ALL SELECT CAST('2.50' AS DECIMAL(10,2)) AS d UNION ALL SELECT CAST('-3.75' AS DECIMAL(10,2)) AS d) AS input_rows WHERE input_rows.__missing_column_1 IS NOT NULL".to_string()
                ),
                (
                    "unsupported-decimal-type-error".to_string(),
                    "SELECT input_rows.d256 AS d256 FROM (SELECT CAST('1.0000' AS DECIMAL(40,4)) AS d256 UNION ALL SELECT CAST('2.5000' AS DECIMAL(40,4)) AS d256 UNION ALL SELECT CAST('-3.7500' AS DECIMAL(40,4)) AS d256) AS input_rows".to_string()
                ),
                (
                    "invalid-decimal-metadata-error".to_string(),
                    "SELECT input_rows.d_bad AS d_bad FROM (SELECT CAST('1.00' AS DECIMAL(10,12)) AS d_bad) AS input_rows".to_string()
                ),
            ]
        );
    }

    #[test]
    fn execute_rows_normalizes_schema_and_row_count() {
        let request = TidbFirstDecimal128SliceAdapter::canonical_requests()
            .into_iter()
            .find(|request| request.case_id == "decimal128-is-not-null-mixed-keep-drop")
            .unwrap();
        let runner = StubRunner::rows(
            vec![EngineColumn {
                name: "d".to_string(),
                engine_type: "decimal(10,2)".to_string(),
                nullable: true,
            }],
            vec![vec![json!("1.00")], vec![json!("-3.75")]],
        );

        let result = TidbFirstDecimal128SliceAdapter::execute(&request, &runner).unwrap();

        assert_eq!(result.engine, TIDB_ENGINE);
        assert_eq!(result.adapter, TIDB_ADAPTER);
        assert_eq!(
            result.outcome,
            CaseOutcome::Rows {
                schema: vec![SchemaField {
                    name: "d".to_string(),
                    logical_type: "decimal128(10,2)".to_string(),
                    nullable: true,
                }],
                rows: vec![vec![json!("1.00")], vec![json!("-3.75")]],
                row_count: 2,
            }
        );
    }

    #[test]
    fn execute_normalizes_missing_column_errors() {
        let request = TidbFirstDecimal128SliceAdapter::canonical_requests()
            .into_iter()
            .find(|request| request.case_id == "decimal128-missing-column-error")
            .unwrap();
        let runner = StubRunner::error(EngineExecutionError::EngineFailure {
            code: Some("1054".to_string()),
            message: "Unknown column '__missing_column_1' in 'where clause'".to_string(),
        });

        let result = TidbFirstDecimal128SliceAdapter::execute(&request, &runner).unwrap();

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
    fn execute_normalizes_unsupported_decimal_type_errors() {
        let request = TidbFirstDecimal128SliceAdapter::canonical_requests()
            .into_iter()
            .find(|request| request.case_id == "unsupported-decimal-type-error")
            .unwrap();
        let runner = StubRunner::error(EngineExecutionError::EngineFailure {
            code: Some("1105".to_string()),
            message:
                "unsupported decimal type: decimal256 input is out of scope for first-decimal128-slice"
                    .to_string(),
        });

        let result = TidbFirstDecimal128SliceAdapter::execute(&request, &runner).unwrap();

        assert_eq!(
            result.outcome,
            CaseOutcome::Error {
                error_class: ErrorClass::UnsupportedDecimalType,
                engine_code: Some("1105".to_string()),
                engine_message: Some(
                    "unsupported decimal type: decimal256 input is out of scope for first-decimal128-slice"
                        .to_string()
                ),
            }
        );
    }

    #[test]
    fn execute_normalizes_invalid_decimal_metadata_errors() {
        let request = TidbFirstDecimal128SliceAdapter::canonical_requests()
            .into_iter()
            .find(|request| request.case_id == "invalid-decimal-metadata-error")
            .unwrap();
        let runner = StubRunner::error(EngineExecutionError::EngineFailure {
            code: Some("1105".to_string()),
            message: "invalid decimal metadata: decimal(10,12) has scale greater than precision"
                .to_string(),
        });

        let result = TidbFirstDecimal128SliceAdapter::execute(&request, &runner).unwrap();

        assert_eq!(
            result.outcome,
            CaseOutcome::Error {
                error_class: ErrorClass::InvalidDecimalMetadata,
                engine_code: Some("1105".to_string()),
                engine_message: Some(
                    "invalid decimal metadata: decimal(10,12) has scale greater than precision"
                        .to_string()
                ),
            }
        );
    }

    #[test]
    fn execute_normalizes_adapter_unavailable_without_extra_code() {
        let request = TidbFirstDecimal128SliceAdapter::canonical_requests()
            .into_iter()
            .find(|request| request.case_id == "decimal128-column-passthrough")
            .unwrap();
        let runner = StubRunner::error(EngineExecutionError::AdapterUnavailable {
            message: Some("TiDB DSN is not configured".to_string()),
        });

        let result = TidbFirstDecimal128SliceAdapter::execute(&request, &runner).unwrap();
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
    }

    #[test]
    fn lowering_rejects_requests_with_mismatched_operation_refs() {
        let mut request = TidbFirstDecimal128SliceAdapter::canonical_requests()
            .into_iter()
            .find(|request| request.case_id == "decimal128-column-passthrough")
            .unwrap();
        request.projection_ref = None;
        request.filter_ref = Some("is-not-null-column-0".to_string());

        let error = TidbFirstDecimal128SliceAdapter::lower_request(&request).unwrap_err();

        assert_eq!(
            error,
            AdapterRequestValidationError::MismatchedCaseDefinition {
                case_id: "decimal128-column-passthrough".to_string(),
                expected_input_ref: "first-decimal128-basic",
                actual_input_ref: "first-decimal128-basic".to_string(),
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

    impl TidbRunner for StubRunner {
        fn run(
            &self,
            _plan: &TidbExecutionPlan,
        ) -> Result<EngineExecutionResult, EngineExecutionError> {
            self.result.clone()
        }
    }
}
