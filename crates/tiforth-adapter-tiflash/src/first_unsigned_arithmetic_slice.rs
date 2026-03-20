use serde::{Deserialize, Serialize};
use serde_json::Value;

pub const FIRST_UNSIGNED_ARITHMETIC_SLICE_ID: &str = "first-unsigned-arithmetic-slice";
pub const TIFLASH_ENGINE: &str = "tiflash";
pub const TIFLASH_ADAPTER: &str = "tiflash-sql";
pub const COMPARISON_MODE_ROW_ORDER_PRESERVED: &str = "row-order-preserved";

const FIRST_UNSIGNED_ARITHMETIC_SLICE_SPEC_REFS: [&str; 4] = [
    "docs/design/first-unsigned-arithmetic-slice.md",
    "docs/spec/type-system.md",
    "tests/conformance/first-unsigned-arithmetic-slice.md",
    "tests/differential/first-unsigned-arithmetic-slice.md",
];

const UINT64_BASIC_INPUT_SQL: &str = concat!(
    "SELECT CAST(0 AS UNSIGNED) AS u ",
    "UNION ALL ",
    "SELECT CAST(7 AS UNSIGNED) AS u ",
    "UNION ALL ",
    "SELECT CAST(42 AS UNSIGNED) AS u"
);

const UINT64_NULLABLE_INPUT_SQL: &str = concat!(
    "SELECT CAST(NULL AS UNSIGNED) AS u ",
    "UNION ALL ",
    "SELECT CAST(5 AS UNSIGNED) AS u ",
    "UNION ALL ",
    "SELECT CAST(NULL AS UNSIGNED) AS u ",
    "UNION ALL ",
    "SELECT CAST(9 AS UNSIGNED) AS u"
);

const UINT64_ADD_BASIC_INPUT_SQL: &str = concat!(
    "SELECT CAST(1 AS UNSIGNED) AS lhs, CAST(2 AS UNSIGNED) AS rhs ",
    "UNION ALL ",
    "SELECT CAST(3 AS UNSIGNED) AS lhs, CAST(4 AS UNSIGNED) AS rhs ",
    "UNION ALL ",
    "SELECT CAST(10 AS UNSIGNED) AS lhs, CAST(20 AS UNSIGNED) AS rhs"
);

const UINT64_ADD_NULLABLE_INPUT_SQL: &str = concat!(
    "SELECT CAST(NULL AS UNSIGNED) AS lhs, CAST(1 AS UNSIGNED) AS rhs ",
    "UNION ALL ",
    "SELECT CAST(2 AS UNSIGNED) AS lhs, CAST(NULL AS UNSIGNED) AS rhs ",
    "UNION ALL ",
    "SELECT CAST(3 AS UNSIGNED) AS lhs, CAST(4 AS UNSIGNED) AS rhs"
);

const UINT64_ADD_OVERFLOW_INPUT_SQL: &str = concat!(
    "SELECT CAST(18446744073709551615 AS UNSIGNED) AS lhs, ",
    "CAST(1 AS UNSIGNED) AS rhs"
);

const MIXED_INT64_UINT64_INPUT_SQL: &str =
    concat!("SELECT CAST(1 AS SIGNED) AS s, CAST(1 AS UNSIGNED) AS u");

const UINT32_BASIC_INPUT_SQL: &str = concat!(
    "SELECT CAST(1 AS UNSIGNED) AS u32 ",
    "UNION ALL ",
    "SELECT CAST(2 AS UNSIGNED) AS u32 ",
    "UNION ALL ",
    "SELECT CAST(3 AS UNSIGNED) AS u32"
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
    UnsignedOverflow,
    MixedSignedUnsigned,
    UnsupportedUnsignedFamily,
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
pub struct TiflashFirstUnsignedArithmeticSliceAdapter;

impl TiflashFirstUnsignedArithmeticSliceAdapter {
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
            slice_id: FIRST_UNSIGNED_ARITHMETIC_SLICE_ID.to_string(),
            case_id: self.case_id.to_string(),
            spec_refs: FIRST_UNSIGNED_ARITHMETIC_SLICE_SPEC_REFS
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
            (Some("column-2"), None) => format!(
                "SELECT input_rows.__missing_column_2 AS __missing_column_2 FROM ({input_sql}) AS input_rows"
            ),
            (Some("literal-uint64-7"), None) => format!(
                "SELECT CAST(7 AS UNSIGNED) AS seven FROM ({input_sql}) AS input_rows"
            ),
            (Some("add-uint64-column-0-column-1"), None) => {
                let add_expression = add_expression(self.input_ref);
                format!(
                    "SELECT CAST({add_expression} AS UNSIGNED) AS sum FROM ({input_sql}) AS input_rows"
                )
            }
            (None, Some("is-not-null-column-0")) => format!(
                "SELECT * FROM ({input_sql}) AS input_rows WHERE {}",
                filter_condition(self.input_ref)
            ),
            _ => unreachable!("validated case definitions always set exactly one operation ref"),
        }
    }
}

const CASE_DEFINITIONS: [CaseDefinition; 9] = [
    CaseDefinition {
        case_id: "uint64-column-passthrough",
        input_ref: "first-uint64-basic",
        comparison_mode: COMPARISON_MODE_ROW_ORDER_PRESERVED,
        projection_ref: Some("column-0"),
        filter_ref: None,
    },
    CaseDefinition {
        case_id: "uint64-literal-projection",
        input_ref: "first-uint64-basic",
        comparison_mode: COMPARISON_MODE_ROW_ORDER_PRESERVED,
        projection_ref: Some("literal-uint64-7"),
        filter_ref: None,
    },
    CaseDefinition {
        case_id: "uint64-add-basic",
        input_ref: "first-uint64-add-basic",
        comparison_mode: COMPARISON_MODE_ROW_ORDER_PRESERVED,
        projection_ref: Some("add-uint64-column-0-column-1"),
        filter_ref: None,
    },
    CaseDefinition {
        case_id: "uint64-add-null-propagation",
        input_ref: "first-uint64-add-nullable",
        comparison_mode: COMPARISON_MODE_ROW_ORDER_PRESERVED,
        projection_ref: Some("add-uint64-column-0-column-1"),
        filter_ref: None,
    },
    CaseDefinition {
        case_id: "uint64-add-overflow-error",
        input_ref: "first-uint64-add-overflow",
        comparison_mode: COMPARISON_MODE_ROW_ORDER_PRESERVED,
        projection_ref: Some("add-uint64-column-0-column-1"),
        filter_ref: None,
    },
    CaseDefinition {
        case_id: "uint64-is-not-null-mixed-keep-drop",
        input_ref: "first-uint64-nullable",
        comparison_mode: COMPARISON_MODE_ROW_ORDER_PRESERVED,
        projection_ref: None,
        filter_ref: Some("is-not-null-column-0"),
    },
    CaseDefinition {
        case_id: "uint64-missing-column-error",
        input_ref: "first-uint64-basic",
        comparison_mode: COMPARISON_MODE_ROW_ORDER_PRESERVED,
        projection_ref: Some("column-2"),
        filter_ref: None,
    },
    CaseDefinition {
        case_id: "mixed-signed-unsigned-arithmetic-error",
        input_ref: "first-mixed-int64-uint64",
        comparison_mode: COMPARISON_MODE_ROW_ORDER_PRESERVED,
        projection_ref: Some("add-uint64-column-0-column-1"),
        filter_ref: None,
    },
    CaseDefinition {
        case_id: "unsupported-unsigned-family-error",
        input_ref: "first-uint32-basic",
        comparison_mode: COMPARISON_MODE_ROW_ORDER_PRESERVED,
        projection_ref: Some("column-0"),
        filter_ref: None,
    },
];

fn validate_request(
    request: &AdapterRequest,
) -> Result<CaseDefinition, AdapterRequestValidationError> {
    if request.slice_id != FIRST_UNSIGNED_ARITHMETIC_SLICE_ID {
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

    let expected_spec_refs: Vec<String> = FIRST_UNSIGNED_ARITHMETIC_SLICE_SPEC_REFS
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
        "first-uint64-basic" => UINT64_BASIC_INPUT_SQL,
        "first-uint64-nullable" => UINT64_NULLABLE_INPUT_SQL,
        "first-uint64-add-basic" => UINT64_ADD_BASIC_INPUT_SQL,
        "first-uint64-add-nullable" => UINT64_ADD_NULLABLE_INPUT_SQL,
        "first-uint64-add-overflow" => UINT64_ADD_OVERFLOW_INPUT_SQL,
        "first-mixed-int64-uint64" => MIXED_INT64_UINT64_INPUT_SQL,
        "first-uint32-basic" => UINT32_BASIC_INPUT_SQL,
        _ => unreachable!("validated input refs should always be known"),
    }
}

fn projection_column_name(input_ref: &str) -> &'static str {
    match input_ref {
        "first-uint64-basic" | "first-uint64-nullable" => "u",
        "first-uint32-basic" => "u32",
        _ => unreachable!("validated input refs should always be known"),
    }
}

fn add_expression(input_ref: &str) -> &'static str {
    match input_ref {
        "first-uint64-add-basic" | "first-uint64-add-nullable" | "first-uint64-add-overflow" => {
            "input_rows.lhs + input_rows.rhs"
        }
        "first-mixed-int64-uint64" => "input_rows.s + input_rows.u",
        _ => unreachable!("validated input refs should always be known"),
    }
}

fn filter_condition(input_ref: &str) -> &'static str {
    match input_ref {
        "first-uint64-basic" | "first-uint64-nullable" => "input_rows.u IS NOT NULL",
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
            } else if is_unsigned_overflow(code.as_deref(), &message) {
                ErrorClass::UnsignedOverflow
            } else if is_mixed_signed_unsigned(&message) {
                ErrorClass::MixedSignedUnsigned
            } else if is_unsupported_unsigned_family(&message) {
                ErrorClass::UnsupportedUnsignedFamily
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
        || normalized_message.contains("missing input column")
}

fn is_unsigned_overflow(engine_code: Option<&str>, engine_message: &str) -> bool {
    let normalized_message = engine_message.to_ascii_lowercase();

    engine_code == Some("1690")
        || ((normalized_message.contains("unsigned") || normalized_message.contains("uint64"))
            && (normalized_message.contains("out of range")
                || normalized_message.contains("overflow")))
}

fn is_mixed_signed_unsigned(engine_message: &str) -> bool {
    let normalized_message = engine_message.to_ascii_lowercase();

    normalized_message.contains("mixed signed and unsigned")
        || (normalized_message.contains("signed and unsigned")
            && normalized_message.contains("unsupported"))
}

fn is_unsupported_unsigned_family(engine_message: &str) -> bool {
    let normalized_message = engine_message.to_ascii_lowercase();

    normalized_message.contains("unsupported unsigned")
        || (normalized_message.contains("uint32") && normalized_message.contains("out of scope"))
        || normalized_message.contains("unsupported unsigned family")
}

fn normalize_logical_type(engine_type: &str) -> String {
    let normalized = engine_type.trim().to_ascii_lowercase();
    let normalized = normalized.split('(').next().unwrap_or(&normalized).trim();

    match normalized {
        "bigint unsigned" | "unsigned bigint" | "unsigned" | "uint64" => "uint64".to_string(),
        "int" | "integer" | "mediumint" | "smallint" | "tinyint" => "int32".to_string(),
        "bigint" | "signed" | "int64" => "int64".to_string(),
        "int unsigned" | "integer unsigned" | "uint32" => "uint32".to_string(),
        other => other.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn canonical_requests_cover_all_documented_cases() {
        let requests = TiflashFirstUnsignedArithmeticSliceAdapter::canonical_requests();
        let case_ids: Vec<&str> = requests
            .iter()
            .map(|request| request.case_id.as_str())
            .collect();

        assert_eq!(requests.len(), 9);
        assert_eq!(
            case_ids,
            vec![
                "uint64-column-passthrough",
                "uint64-literal-projection",
                "uint64-add-basic",
                "uint64-add-null-propagation",
                "uint64-add-overflow-error",
                "uint64-is-not-null-mixed-keep-drop",
                "uint64-missing-column-error",
                "mixed-signed-unsigned-arithmetic-error",
                "unsupported-unsigned-family-error",
            ]
        );

        for request in requests {
            assert_eq!(request.slice_id, FIRST_UNSIGNED_ARITHMETIC_SLICE_ID);
            assert_eq!(request.spec_refs.len(), 4);
            let op_ref_count =
                request.projection_ref.iter().count() + request.filter_ref.iter().count();
            assert_eq!(op_ref_count, 1);
            assert_eq!(
                request.comparison_mode,
                COMPARISON_MODE_ROW_ORDER_PRESERVED.to_string()
            );
        }
    }

    #[test]
    fn execute_rows_normalizes_schema_and_row_count() {
        let request = TiflashFirstUnsignedArithmeticSliceAdapter::canonical_requests()
            .into_iter()
            .find(|request| request.case_id == "uint64-column-passthrough")
            .unwrap();
        let runner = StubRunner::rows(
            vec![EngineColumn {
                name: "u".to_string(),
                engine_type: "bigint unsigned".to_string(),
                nullable: false,
            }],
            vec![vec![json!("0")], vec![json!("7")], vec![json!("42")]],
        );

        let result =
            TiflashFirstUnsignedArithmeticSliceAdapter::execute(&request, &runner).unwrap();

        assert_eq!(
            result.outcome,
            CaseOutcome::Rows {
                schema: vec![SchemaField {
                    name: "u".to_string(),
                    logical_type: "uint64".to_string(),
                    nullable: false,
                }],
                rows: vec![vec![json!("0")], vec![json!("7")], vec![json!("42")]],
                row_count: 3,
            }
        );
    }

    #[test]
    fn execute_normalizes_missing_column_errors() {
        let request = TiflashFirstUnsignedArithmeticSliceAdapter::canonical_requests()
            .into_iter()
            .find(|request| request.case_id == "uint64-missing-column-error")
            .unwrap();
        let runner = StubRunner::error(EngineExecutionError::EngineFailure {
            code: Some("1054".to_string()),
            message: "Unknown column '__missing_column_2' in 'field list'".to_string(),
        });

        let result =
            TiflashFirstUnsignedArithmeticSliceAdapter::execute(&request, &runner).unwrap();

        assert_eq!(
            result.outcome,
            CaseOutcome::Error {
                error_class: ErrorClass::MissingColumn,
                engine_code: Some("1054".to_string()),
                engine_message: Some(
                    "Unknown column '__missing_column_2' in 'field list'".to_string()
                ),
            }
        );
    }

    #[test]
    fn execute_normalizes_adapter_unavailable_without_extra_code() {
        let request = TiflashFirstUnsignedArithmeticSliceAdapter::canonical_requests()
            .into_iter()
            .find(|request| request.case_id == "uint64-add-basic")
            .unwrap();
        let runner = StubRunner::error(EngineExecutionError::AdapterUnavailable {
            message: Some("TiFlash DSN is not configured".to_string()),
        });

        let result =
            TiflashFirstUnsignedArithmeticSliceAdapter::execute(&request, &runner).unwrap();
        let serialized = serde_json::to_value(&result).unwrap();

        assert_eq!(
            result.outcome,
            CaseOutcome::Error {
                error_class: ErrorClass::AdapterUnavailable,
                engine_code: None,
                engine_message: Some("TiFlash DSN is not configured".to_string()),
            }
        );
        assert_eq!(serialized["outcome"]["kind"], "error");
        assert_eq!(serialized["outcome"]["error_class"], "adapter_unavailable");
        assert!(serialized["outcome"].get("engine_code").is_none());
    }

    #[test]
    fn lowering_rejects_requests_with_mismatched_operation_refs() {
        let mut request = TiflashFirstUnsignedArithmeticSliceAdapter::canonical_requests()
            .into_iter()
            .find(|request| request.case_id == "uint64-column-passthrough")
            .unwrap();
        request.projection_ref = None;
        request.filter_ref = Some("is-not-null-column-0".to_string());

        let error =
            TiflashFirstUnsignedArithmeticSliceAdapter::lower_request(&request).unwrap_err();

        assert_eq!(
            error,
            AdapterRequestValidationError::MismatchedCaseDefinition {
                case_id: "uint64-column-passthrough".to_string(),
                expected_input_ref: "first-uint64-basic",
                actual_input_ref: "first-uint64-basic".to_string(),
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
