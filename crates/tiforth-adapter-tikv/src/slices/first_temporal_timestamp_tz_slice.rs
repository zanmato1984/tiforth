use crate::engine;
use serde::{Deserialize, Serialize};
use serde_json::Value;

pub const FIRST_TEMPORAL_TIMESTAMP_TZ_SLICE_ID: &str = "first-temporal-timestamp-tz-slice";
pub const TIKV_ENGINE: &str = engine::ENGINE;
pub const TIKV_ADAPTER: &str = engine::ADAPTER;

const FIRST_TEMPORAL_TIMESTAMP_TZ_SLICE_SPEC_REFS: [&str; 4] = [
    "docs/design/first-temporal-timestamp-tz-slice.md",
    "docs/spec/type-system.md",
    "tests/conformance/first-temporal-timestamp-tz-slice.md",
    "tests/differential/first-temporal-timestamp-tz-slice.md",
];

const TIMESTAMP_TZ_BASIC_INPUT_SQL: &str = concat!(
    "SELECT CAST('1970-01-01 00:00:00+00:00' AS TIMESTAMP(6)) AS ts ",
    "UNION ALL ",
    "SELECT CAST('1970-01-01 00:00:01+00:00' AS TIMESTAMP(6)) AS ts ",
    "UNION ALL ",
    "SELECT CAST('1970-01-01 00:00:02+00:00' AS TIMESTAMP(6)) AS ts"
);

const TIMESTAMP_TZ_EQUIVALENT_INSTANTS_INPUT_SQL: &str = concat!(
    "SELECT CAST('2024-01-01 00:00:00+00:00' AS TIMESTAMP(6)) AS ts ",
    "UNION ALL ",
    "SELECT CAST('2023-12-31 19:00:00-05:00' AS TIMESTAMP(6)) AS ts ",
    "UNION ALL ",
    "SELECT CAST('2024-01-01 09:00:00+09:00' AS TIMESTAMP(6)) AS ts"
);

const TIMESTAMP_TZ_NULLABLE_INPUT_SQL: &str = concat!(
    "SELECT CAST('1970-01-01 00:00:00+00:00' AS TIMESTAMP(6)) AS ts ",
    "UNION ALL ",
    "SELECT CAST(NULL AS TIMESTAMP(6)) AS ts ",
    "UNION ALL ",
    "SELECT CAST('1970-01-01 00:00:02+00:00' AS TIMESTAMP(6)) AS ts ",
    "UNION ALL ",
    "SELECT CAST(NULL AS TIMESTAMP(6)) AS ts"
);

const TIMESTAMP_TZ_ALL_NULL_INPUT_SQL: &str = concat!(
    "SELECT CAST(NULL AS TIMESTAMP(6)) AS ts ",
    "UNION ALL ",
    "SELECT CAST(NULL AS TIMESTAMP(6)) AS ts ",
    "UNION ALL ",
    "SELECT CAST(NULL AS TIMESTAMP(6)) AS ts"
);

const TIMESTAMP_NAIVE_BASIC_INPUT_SQL: &str = concat!(
    "SELECT CAST('1970-01-01 00:00:00' AS DATETIME(6)) AS ts ",
    "UNION ALL ",
    "SELECT CAST('1970-01-01 00:00:01' AS DATETIME(6)) AS ts ",
    "UNION ALL ",
    "SELECT CAST('1970-01-01 00:00:02' AS DATETIME(6)) AS ts"
);

const TIMESTAMP_TZ_MS_BASIC_INPUT_SQL: &str = concat!(
    "SELECT CAST('1970-01-01 00:00:00.000+00:00' AS TIMESTAMP(3)) AS ts ",
    "UNION ALL ",
    "SELECT CAST('1970-01-01 00:00:01.000+00:00' AS TIMESTAMP(3)) AS ts ",
    "UNION ALL ",
    "SELECT CAST('1970-01-01 00:00:02.000+00:00' AS TIMESTAMP(3)) AS ts"
);

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
    pub ordering_ref: Option<String>,
}

pub type TikvExecutionPlan = crate::engine::TikvExecutionPlan<AdapterRequest>;
pub use crate::engine::{EngineColumn, EngineExecutionError, EngineExecutionResult};

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
    pub ordering_ref: Option<String>,
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
    UnsupportedTemporalType,
    UnsupportedTemporalUnit,
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
        expected_ordering_ref: Option<&'static str>,
        actual_ordering_ref: Option<String>,
    },
    MismatchedSpecRefs {
        case_id: String,
        expected_spec_refs: Vec<String>,
        actual_spec_refs: Vec<String>,
    },
}

pub trait TikvRunner {
    fn run(&self, plan: &TikvExecutionPlan) -> Result<EngineExecutionResult, EngineExecutionError>;
}

#[derive(Debug, Default, Clone, Copy)]
pub struct TikvFirstTemporalTimestampTzSliceAdapter;

impl TikvFirstTemporalTimestampTzSliceAdapter {
    pub fn canonical_requests() -> Vec<AdapterRequest> {
        CASE_DEFINITIONS
            .iter()
            .copied()
            .map(CaseDefinition::canonical_request)
            .collect()
    }

    pub fn lower_request(
        request: &AdapterRequest,
    ) -> Result<TikvExecutionPlan, AdapterRequestValidationError> {
        let case = validate_request(request)?;

        Ok(TikvExecutionPlan {
            request: request.clone(),
            sql: case.render_sql(),
        })
    }

    pub fn execute<R: TikvRunner>(
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
            engine: TIKV_ENGINE.to_string(),
            adapter: TIKV_ADAPTER.to_string(),
            case_id: request.case_id.clone(),
            spec_refs: request.spec_refs.clone(),
            input_ref: request.input_ref.clone(),
            projection_ref: request.projection_ref.clone(),
            filter_ref: request.filter_ref.clone(),
            ordering_ref: request.ordering_ref.clone(),
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
    ordering_ref: Option<&'static str>,
}

impl CaseDefinition {
    fn canonical_request(self) -> AdapterRequest {
        AdapterRequest {
            slice_id: FIRST_TEMPORAL_TIMESTAMP_TZ_SLICE_ID.to_string(),
            case_id: self.case_id.to_string(),
            spec_refs: FIRST_TEMPORAL_TIMESTAMP_TZ_SLICE_SPEC_REFS
                .iter()
                .map(|spec_ref| (*spec_ref).to_string())
                .collect(),
            input_ref: self.input_ref.to_string(),
            projection_ref: self.projection_ref.map(str::to_string),
            filter_ref: self.filter_ref.map(str::to_string),
            ordering_ref: self.ordering_ref.map(str::to_string),
        }
    }

    fn render_sql(self) -> String {
        let input_sql = input_sql(self.input_ref);

        match (self.projection_ref, self.filter_ref, self.ordering_ref) {
            (Some("column-0"), None, None) => {
                let column_name = projection_column_name(self.input_ref);
                format!(
                    "SELECT input_rows.{column_name} AS {column_name} FROM ({input_sql}) AS input_rows"
                )
            }
            (None, Some(filter_ref), None) => {
                let filter_condition = filter_condition(self.input_ref, filter_ref);
                format!("SELECT * FROM ({input_sql}) AS input_rows WHERE {filter_condition}")
            }
            (None, None, Some("order-by-column-0-asc-nulls-last")) => {
                let column_name = projection_column_name(self.input_ref);
                format!(
                    "SELECT input_rows.{column_name} AS {column_name} FROM ({input_sql}) AS input_rows ORDER BY (input_rows.{column_name} IS NULL) ASC, input_rows.{column_name} ASC"
                )
            }
            _ => unreachable!("validated case definitions always set exactly one operation ref"),
        }
    }
}

const CASE_DEFINITIONS: [CaseDefinition; 10] = [
    CaseDefinition {
        case_id: "timestamp-tz-column-passthrough",
        input_ref: "first-temporal-timestamp-tz-basic",
        projection_ref: Some("column-0"),
        filter_ref: None,
        ordering_ref: None,
    },
    CaseDefinition {
        case_id: "timestamp-tz-equivalent-instant-normalization",
        input_ref: "first-temporal-timestamp-tz-equivalent-instants",
        projection_ref: Some("column-0"),
        filter_ref: None,
        ordering_ref: None,
    },
    CaseDefinition {
        case_id: "timestamp-tz-column-null-preserve",
        input_ref: "first-temporal-timestamp-tz-nullable",
        projection_ref: Some("column-0"),
        filter_ref: None,
        ordering_ref: None,
    },
    CaseDefinition {
        case_id: "timestamp-tz-is-not-null-all-kept",
        input_ref: "first-temporal-timestamp-tz-basic",
        projection_ref: None,
        filter_ref: Some("is-not-null-column-0"),
        ordering_ref: None,
    },
    CaseDefinition {
        case_id: "timestamp-tz-is-not-null-all-dropped",
        input_ref: "first-temporal-timestamp-tz-all-null",
        projection_ref: None,
        filter_ref: Some("is-not-null-column-0"),
        ordering_ref: None,
    },
    CaseDefinition {
        case_id: "timestamp-tz-is-not-null-mixed-keep-drop",
        input_ref: "first-temporal-timestamp-tz-nullable",
        projection_ref: None,
        filter_ref: Some("is-not-null-column-0"),
        ordering_ref: None,
    },
    CaseDefinition {
        case_id: "timestamp-tz-order-asc-nulls-last",
        input_ref: "first-temporal-timestamp-tz-nullable",
        projection_ref: None,
        filter_ref: None,
        ordering_ref: Some("order-by-column-0-asc-nulls-last"),
    },
    CaseDefinition {
        case_id: "timestamp-tz-missing-column-error",
        input_ref: "first-temporal-timestamp-tz-basic",
        projection_ref: None,
        filter_ref: Some("is-not-null-column-1"),
        ordering_ref: None,
    },
    CaseDefinition {
        case_id: "unsupported-temporal-timestamp-without-timezone-error",
        input_ref: "first-temporal-timestamp-naive-basic",
        projection_ref: Some("column-0"),
        filter_ref: None,
        ordering_ref: None,
    },
    CaseDefinition {
        case_id: "unsupported-temporal-timestamp-unit-error",
        input_ref: "first-temporal-timestamp-tz-ms-basic",
        projection_ref: Some("column-0"),
        filter_ref: None,
        ordering_ref: None,
    },
];

fn validate_request(
    request: &AdapterRequest,
) -> Result<CaseDefinition, AdapterRequestValidationError> {
    if request.slice_id != FIRST_TEMPORAL_TIMESTAMP_TZ_SLICE_ID {
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
        || request.ordering_ref.as_deref() != case.ordering_ref
    {
        return Err(AdapterRequestValidationError::MismatchedCaseDefinition {
            case_id: request.case_id.clone(),
            expected_input_ref: case.input_ref,
            actual_input_ref: request.input_ref.clone(),
            expected_projection_ref: case.projection_ref,
            actual_projection_ref: request.projection_ref.clone(),
            expected_filter_ref: case.filter_ref,
            actual_filter_ref: request.filter_ref.clone(),
            expected_ordering_ref: case.ordering_ref,
            actual_ordering_ref: request.ordering_ref.clone(),
        });
    }

    let expected_spec_refs: Vec<String> = FIRST_TEMPORAL_TIMESTAMP_TZ_SLICE_SPEC_REFS
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
        "first-temporal-timestamp-tz-basic" => TIMESTAMP_TZ_BASIC_INPUT_SQL,
        "first-temporal-timestamp-tz-equivalent-instants" => {
            TIMESTAMP_TZ_EQUIVALENT_INSTANTS_INPUT_SQL
        }
        "first-temporal-timestamp-tz-nullable" => TIMESTAMP_TZ_NULLABLE_INPUT_SQL,
        "first-temporal-timestamp-tz-all-null" => TIMESTAMP_TZ_ALL_NULL_INPUT_SQL,
        "first-temporal-timestamp-naive-basic" => TIMESTAMP_NAIVE_BASIC_INPUT_SQL,
        "first-temporal-timestamp-tz-ms-basic" => TIMESTAMP_TZ_MS_BASIC_INPUT_SQL,
        _ => unreachable!("validated input refs should always be known"),
    }
}

fn projection_column_name(input_ref: &str) -> &'static str {
    match input_ref {
        "first-temporal-timestamp-tz-basic"
        | "first-temporal-timestamp-tz-equivalent-instants"
        | "first-temporal-timestamp-tz-nullable"
        | "first-temporal-timestamp-tz-all-null"
        | "first-temporal-timestamp-naive-basic"
        | "first-temporal-timestamp-tz-ms-basic" => "ts",
        _ => unreachable!("validated input refs should always be known"),
    }
}

fn filter_condition(input_ref: &str, filter_ref: &str) -> &'static str {
    match filter_ref {
        "is-not-null-column-0" => match input_ref {
            "first-temporal-timestamp-tz-basic"
            | "first-temporal-timestamp-tz-equivalent-instants"
            | "first-temporal-timestamp-tz-nullable"
            | "first-temporal-timestamp-tz-all-null"
            | "first-temporal-timestamp-naive-basic"
            | "first-temporal-timestamp-tz-ms-basic" => "input_rows.ts IS NOT NULL",
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
            let error_class = if engine::is_missing_column(code.as_deref(), &message) {
                ErrorClass::MissingColumn
            } else if is_unsupported_temporal_type(code.as_deref(), &message) {
                ErrorClass::UnsupportedTemporalType
            } else if is_unsupported_temporal_unit(code.as_deref(), &message) {
                ErrorClass::UnsupportedTemporalUnit
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

fn is_unsupported_temporal_type(_engine_code: Option<&str>, engine_message: &str) -> bool {
    let normalized_message = engine_message.to_ascii_lowercase();

    normalized_message.contains("unsupported temporal type")
        || normalized_message.contains("timestamp without timezone")
        || (normalized_message.contains("without timezone")
            && normalized_message.contains("out of scope"))
}

fn is_unsupported_temporal_unit(_engine_code: Option<&str>, engine_message: &str) -> bool {
    let normalized_message = engine_message.to_ascii_lowercase();

    normalized_message.contains("unsupported temporal unit")
        || normalized_message.contains("timestamp_tz(ms)")
        || (normalized_message.contains("timestamp")
            && normalized_message.contains("millisecond")
            && normalized_message.contains("out of scope"))
}

fn normalize_logical_type(engine_type: &str) -> String {
    let normalized = engine_type.trim().to_ascii_lowercase();

    if normalized.contains("timestamp_tz(ms)") || normalized.contains("timestamp(3)") {
        return "timestamp_tz(ms)".to_string();
    }

    if normalized.contains("timestamp_tz(us)")
        || normalized.contains("timestamp(6)")
        || normalized.contains("datetime(6)")
        || normalized == "timestamp"
        || normalized == "datetime"
    {
        return "timestamp_tz(us)".to_string();
    }

    match normalized.as_str() {
        "int" | "integer" | "mediumint" | "smallint" | "tinyint" => "int32".to_string(),
        other => other.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn canonical_requests_cover_all_documented_cases() {
        let requests = TikvFirstTemporalTimestampTzSliceAdapter::canonical_requests();
        let case_ids: Vec<&str> = requests
            .iter()
            .map(|request| request.case_id.as_str())
            .collect();

        assert_eq!(requests.len(), 10);
        assert_eq!(
            case_ids,
            vec![
                "timestamp-tz-column-passthrough",
                "timestamp-tz-equivalent-instant-normalization",
                "timestamp-tz-column-null-preserve",
                "timestamp-tz-is-not-null-all-kept",
                "timestamp-tz-is-not-null-all-dropped",
                "timestamp-tz-is-not-null-mixed-keep-drop",
                "timestamp-tz-order-asc-nulls-last",
                "timestamp-tz-missing-column-error",
                "unsupported-temporal-timestamp-without-timezone-error",
                "unsupported-temporal-timestamp-unit-error",
            ]
        );

        for request in requests {
            let op_ref_count = request.projection_ref.iter().count()
                + request.filter_ref.iter().count()
                + request.ordering_ref.iter().count();
            assert_eq!(op_ref_count, 1);
        }
    }

    #[test]
    fn lowering_renders_ordering_sql_for_ordering_ref_case() {
        let request = TikvFirstTemporalTimestampTzSliceAdapter::canonical_requests()
            .into_iter()
            .find(|request| request.case_id == "timestamp-tz-order-asc-nulls-last")
            .unwrap();

        let plan = TikvFirstTemporalTimestampTzSliceAdapter::lower_request(&request).unwrap();

        assert_eq!(
            plan.sql,
            "SELECT input_rows.ts AS ts FROM (SELECT CAST('1970-01-01 00:00:00+00:00' AS TIMESTAMP(6)) AS ts UNION ALL SELECT CAST(NULL AS TIMESTAMP(6)) AS ts UNION ALL SELECT CAST('1970-01-01 00:00:02+00:00' AS TIMESTAMP(6)) AS ts UNION ALL SELECT CAST(NULL AS TIMESTAMP(6)) AS ts) AS input_rows ORDER BY (input_rows.ts IS NULL) ASC, input_rows.ts ASC"
                .to_string()
        );
    }

    #[test]
    fn execute_rows_normalizes_timestamp_tz_schema() {
        let request = TikvFirstTemporalTimestampTzSliceAdapter::canonical_requests()
            .into_iter()
            .find(|request| request.case_id == "timestamp-tz-column-passthrough")
            .unwrap();
        let runner = StubRunner::rows(
            vec![EngineColumn {
                name: "ts".to_string(),
                engine_type: "timestamp_tz(us)".to_string(),
                nullable: false,
            }],
            vec![
                vec![json!(0)],
                vec![json!(1_000_000)],
                vec![json!(2_000_000)],
            ],
        );

        let result = TikvFirstTemporalTimestampTzSliceAdapter::execute(&request, &runner).unwrap();

        assert_eq!(result.engine, TIKV_ENGINE);
        assert_eq!(result.adapter, TIKV_ADAPTER);
        assert_eq!(
            result.outcome,
            CaseOutcome::Rows {
                schema: vec![SchemaField {
                    name: "ts".to_string(),
                    logical_type: "timestamp_tz(us)".to_string(),
                    nullable: false,
                }],
                rows: vec![
                    vec![json!(0)],
                    vec![json!(1_000_000)],
                    vec![json!(2_000_000)]
                ],
                row_count: 3,
            }
        );
    }

    #[test]
    fn execute_normalizes_temporal_error_classes() {
        let type_request = TikvFirstTemporalTimestampTzSliceAdapter::canonical_requests()
            .into_iter()
            .find(|request| {
                request.case_id == "unsupported-temporal-timestamp-without-timezone-error"
            })
            .unwrap();
        let type_runner = StubRunner::error(EngineExecutionError::EngineFailure {
            code: Some("1105".to_string()),
            message: "unsupported temporal type: timestamp without timezone input is out of scope for first-temporal-timestamp-tz-slice".to_string(),
        });
        let type_result =
            TikvFirstTemporalTimestampTzSliceAdapter::execute(&type_request, &type_runner).unwrap();
        assert_eq!(
            type_result.outcome,
            CaseOutcome::Error {
                error_class: ErrorClass::UnsupportedTemporalType,
                engine_code: Some("1105".to_string()),
                engine_message: Some("unsupported temporal type: timestamp without timezone input is out of scope for first-temporal-timestamp-tz-slice".to_string()),
            }
        );

        let unit_request = TikvFirstTemporalTimestampTzSliceAdapter::canonical_requests()
            .into_iter()
            .find(|request| request.case_id == "unsupported-temporal-timestamp-unit-error")
            .unwrap();
        let unit_runner = StubRunner::error(EngineExecutionError::EngineFailure {
            code: Some("1105".to_string()),
            message: "unsupported temporal unit: timestamp_tz(ms) input is out of scope for first-temporal-timestamp-tz-slice".to_string(),
        });
        let unit_result =
            TikvFirstTemporalTimestampTzSliceAdapter::execute(&unit_request, &unit_runner).unwrap();
        assert_eq!(
            unit_result.outcome,
            CaseOutcome::Error {
                error_class: ErrorClass::UnsupportedTemporalUnit,
                engine_code: Some("1105".to_string()),
                engine_message: Some("unsupported temporal unit: timestamp_tz(ms) input is out of scope for first-temporal-timestamp-tz-slice".to_string()),
            }
        );
    }

    #[test]
    fn lowering_rejects_requests_with_mismatched_ordering_ref() {
        let mut request = TikvFirstTemporalTimestampTzSliceAdapter::canonical_requests()
            .into_iter()
            .find(|request| request.case_id == "timestamp-tz-order-asc-nulls-last")
            .unwrap();
        request.ordering_ref = None;
        request.projection_ref = Some("column-0".to_string());

        let error = TikvFirstTemporalTimestampTzSliceAdapter::lower_request(&request).unwrap_err();

        assert_eq!(
            error,
            AdapterRequestValidationError::MismatchedCaseDefinition {
                case_id: "timestamp-tz-order-asc-nulls-last".to_string(),
                expected_input_ref: "first-temporal-timestamp-tz-nullable",
                actual_input_ref: "first-temporal-timestamp-tz-nullable".to_string(),
                expected_projection_ref: None,
                actual_projection_ref: Some("column-0".to_string()),
                expected_filter_ref: None,
                actual_filter_ref: None,
                expected_ordering_ref: Some("order-by-column-0-asc-nulls-last"),
                actual_ordering_ref: None,
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

    impl TikvRunner for StubRunner {
        fn run(
            &self,
            _plan: &TikvExecutionPlan,
        ) -> Result<EngineExecutionResult, EngineExecutionError> {
            self.result.clone()
        }
    }
}
