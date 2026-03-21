use serde::{Deserialize, Serialize};
use serde_json::Value;

pub const FIRST_COLLATION_STRING_SLICE_ID: &str = "first-collation-string-slice";
pub const TIDB_ENGINE: &str = "tidb";
pub const TIDB_ADAPTER: &str = "tidb-sql";

const FIRST_COLLATION_STRING_SLICE_SPEC_REFS: [&str; 4] = [
    "docs/design/first-collation-string-slice.md",
    "docs/spec/type-system.md",
    "tests/conformance/first-collation-string-slice.md",
    "tests/differential/first-collation-string-slice.md",
];

const UTF8_BASIC_INPUT_SQL: &str = concat!(
    "SELECT CAST('Alpha' AS CHAR) AS s ",
    "UNION ALL ",
    "SELECT CAST('alpha' AS CHAR) AS s ",
    "UNION ALL ",
    "SELECT CAST('beta' AS CHAR) AS s"
);

const UTF8_NULLABLE_INPUT_SQL: &str = concat!(
    "SELECT CAST('Alpha' AS CHAR) AS s ",
    "UNION ALL ",
    "SELECT CAST(NULL AS CHAR) AS s ",
    "UNION ALL ",
    "SELECT CAST('beta' AS CHAR) AS s"
);

const UTF8_ORDERING_INPUT_SQL: &str = concat!(
    "SELECT CAST('b' AS CHAR) AS s ",
    "UNION ALL ",
    "SELECT CAST('A' AS CHAR) AS s ",
    "UNION ALL ",
    "SELECT CAST('a' AS CHAR) AS s ",
    "UNION ALL ",
    "SELECT CAST('B' AS CHAR) AS s"
);

const INT32_BASIC_INPUT_SQL: &str = concat!(
    "SELECT CAST(1 AS SIGNED) AS x ",
    "UNION ALL ",
    "SELECT CAST(2 AS SIGNED) AS x ",
    "UNION ALL ",
    "SELECT CAST(3 AS SIGNED) AS x"
);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AdapterRequest {
    pub slice_id: String,
    pub case_id: String,
    pub spec_refs: Vec<String>,
    pub input_ref: String,
    pub collation_ref: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub projection_ref: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub filter_ref: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub comparison_ref: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ordering_ref: Option<String>,
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
    pub collation_ref: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub projection_ref: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub filter_ref: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub comparison_ref: Option<String>,
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
    UnknownCollation,
    UnsupportedCollationType,
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
        expected_collation_ref: &'static str,
        actual_collation_ref: String,
        expected_projection_ref: Option<&'static str>,
        actual_projection_ref: Option<String>,
        expected_filter_ref: Option<&'static str>,
        actual_filter_ref: Option<String>,
        expected_comparison_ref: Option<&'static str>,
        actual_comparison_ref: Option<String>,
        expected_ordering_ref: Option<&'static str>,
        actual_ordering_ref: Option<String>,
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
pub struct TidbFirstCollationStringSliceAdapter;

impl TidbFirstCollationStringSliceAdapter {
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

                CaseOutcome::Rows {
                    row_count: result.rows.len(),
                    rows: result.rows,
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
            collation_ref: request.collation_ref.clone(),
            projection_ref: request.projection_ref.clone(),
            filter_ref: request.filter_ref.clone(),
            comparison_ref: request.comparison_ref.clone(),
            ordering_ref: request.ordering_ref.clone(),
            outcome,
        })
    }
}

#[derive(Debug, Clone, Copy)]
struct CaseDefinition {
    case_id: &'static str,
    input_ref: &'static str,
    collation_ref: &'static str,
    projection_ref: Option<&'static str>,
    filter_ref: Option<&'static str>,
    comparison_ref: Option<&'static str>,
    ordering_ref: Option<&'static str>,
}

impl CaseDefinition {
    fn canonical_request(self) -> AdapterRequest {
        AdapterRequest {
            slice_id: FIRST_COLLATION_STRING_SLICE_ID.to_string(),
            case_id: self.case_id.to_string(),
            spec_refs: FIRST_COLLATION_STRING_SLICE_SPEC_REFS
                .iter()
                .map(|spec_ref| (*spec_ref).to_string())
                .collect(),
            input_ref: self.input_ref.to_string(),
            collation_ref: self.collation_ref.to_string(),
            projection_ref: self.projection_ref.map(str::to_string),
            filter_ref: self.filter_ref.map(str::to_string),
            comparison_ref: self.comparison_ref.map(str::to_string),
            ordering_ref: self.ordering_ref.map(str::to_string),
        }
    }

    fn render_sql(self) -> String {
        let input_sql = input_sql(self.input_ref);

        match (
            self.projection_ref,
            self.filter_ref,
            self.comparison_ref,
            self.ordering_ref,
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
            (None, None, Some(comparison_ref), None) => {
                let expression =
                    comparison_expression(self.input_ref, self.collation_ref, comparison_ref);
                format!("SELECT {expression} AS cmp FROM ({input_sql}) AS input_rows")
            }
            (None, None, None, Some("order-by-column-0-asc")) => {
                let column_name = projection_column_name(self.input_ref);
                let ordering_clause = ordering_clause(self.input_ref, self.collation_ref);
                format!(
                    "SELECT input_rows.{column_name} AS {column_name} FROM ({input_sql}) AS input_rows {ordering_clause}"
                )
            }
            _ => unreachable!("validated case definitions always set exactly one operation ref"),
        }
    }
}

const CASE_DEFINITIONS: [CaseDefinition; 10] = [
    CaseDefinition {
        case_id: "utf8-column-passthrough-binary",
        input_ref: "first-utf8-basic",
        collation_ref: "binary",
        projection_ref: Some("column-0"),
        filter_ref: None,
        comparison_ref: None,
        ordering_ref: None,
    },
    CaseDefinition {
        case_id: "utf8-column-passthrough-unicode-ci",
        input_ref: "first-utf8-basic",
        collation_ref: "unicode_ci",
        projection_ref: Some("column-0"),
        filter_ref: None,
        comparison_ref: None,
        ordering_ref: None,
    },
    CaseDefinition {
        case_id: "utf8-is-not-null-mixed-keep-drop",
        input_ref: "first-utf8-nullable",
        collation_ref: "binary",
        projection_ref: None,
        filter_ref: Some("is-not-null-column-0"),
        comparison_ref: None,
        ordering_ref: None,
    },
    CaseDefinition {
        case_id: "utf8-binary-eq-literal-alpha",
        input_ref: "first-utf8-basic",
        collation_ref: "binary",
        projection_ref: None,
        filter_ref: None,
        comparison_ref: Some("eq-column-0-literal-alpha"),
        ordering_ref: None,
    },
    CaseDefinition {
        case_id: "utf8-unicode-ci-eq-literal-alpha",
        input_ref: "first-utf8-basic",
        collation_ref: "unicode_ci",
        projection_ref: None,
        filter_ref: None,
        comparison_ref: Some("eq-column-0-literal-alpha"),
        ordering_ref: None,
    },
    CaseDefinition {
        case_id: "utf8-binary-lt-literal-beta",
        input_ref: "first-utf8-basic",
        collation_ref: "binary",
        projection_ref: None,
        filter_ref: None,
        comparison_ref: Some("lt-column-0-literal-beta"),
        ordering_ref: None,
    },
    CaseDefinition {
        case_id: "utf8-unicode-ci-ordering-normalization",
        input_ref: "first-utf8-ordering",
        collation_ref: "unicode_ci",
        projection_ref: None,
        filter_ref: None,
        comparison_ref: None,
        ordering_ref: Some("order-by-column-0-asc"),
    },
    CaseDefinition {
        case_id: "utf8-missing-column-error",
        input_ref: "first-utf8-basic",
        collation_ref: "binary",
        projection_ref: None,
        filter_ref: Some("is-not-null-column-1"),
        comparison_ref: None,
        ordering_ref: None,
    },
    CaseDefinition {
        case_id: "unknown-collation-error",
        input_ref: "first-utf8-basic",
        collation_ref: "unknown-collation",
        projection_ref: None,
        filter_ref: None,
        comparison_ref: Some("eq-column-0-literal-alpha"),
        ordering_ref: None,
    },
    CaseDefinition {
        case_id: "unsupported-collation-type-error",
        input_ref: "first-int32-basic",
        collation_ref: "binary",
        projection_ref: None,
        filter_ref: None,
        comparison_ref: Some("eq-column-0-literal-alpha"),
        ordering_ref: None,
    },
];

fn validate_request(
    request: &AdapterRequest,
) -> Result<CaseDefinition, AdapterRequestValidationError> {
    if request.slice_id != FIRST_COLLATION_STRING_SLICE_ID {
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
        || request.collation_ref != case.collation_ref
        || request.projection_ref.as_deref() != case.projection_ref
        || request.filter_ref.as_deref() != case.filter_ref
        || request.comparison_ref.as_deref() != case.comparison_ref
        || request.ordering_ref.as_deref() != case.ordering_ref
    {
        return Err(AdapterRequestValidationError::MismatchedCaseDefinition {
            case_id: request.case_id.clone(),
            expected_input_ref: case.input_ref,
            actual_input_ref: request.input_ref.clone(),
            expected_collation_ref: case.collation_ref,
            actual_collation_ref: request.collation_ref.clone(),
            expected_projection_ref: case.projection_ref,
            actual_projection_ref: request.projection_ref.clone(),
            expected_filter_ref: case.filter_ref,
            actual_filter_ref: request.filter_ref.clone(),
            expected_comparison_ref: case.comparison_ref,
            actual_comparison_ref: request.comparison_ref.clone(),
            expected_ordering_ref: case.ordering_ref,
            actual_ordering_ref: request.ordering_ref.clone(),
        });
    }

    let expected_spec_refs: Vec<String> = FIRST_COLLATION_STRING_SLICE_SPEC_REFS
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
        "first-utf8-basic" => UTF8_BASIC_INPUT_SQL,
        "first-utf8-nullable" => UTF8_NULLABLE_INPUT_SQL,
        "first-utf8-ordering" => UTF8_ORDERING_INPUT_SQL,
        "first-int32-basic" => INT32_BASIC_INPUT_SQL,
        _ => unreachable!("validated input refs should always be known"),
    }
}

fn projection_column_name(input_ref: &str) -> &'static str {
    match input_ref {
        "first-utf8-basic" | "first-utf8-nullable" | "first-utf8-ordering" => "s",
        "first-int32-basic" => "x",
        _ => unreachable!("validated input refs should always be known"),
    }
}

fn filter_condition(input_ref: &str, filter_ref: &str) -> &'static str {
    match filter_ref {
        "is-not-null-column-0" => match input_ref {
            "first-utf8-basic" | "first-utf8-nullable" | "first-utf8-ordering" => {
                "input_rows.s IS NOT NULL"
            }
            "first-int32-basic" => "input_rows.x IS NOT NULL",
            _ => unreachable!("validated input refs should always be known"),
        },
        "is-not-null-column-1" => "input_rows.__missing_column_1 IS NOT NULL",
        _ => unreachable!("validated filter refs should always be known"),
    }
}

fn comparison_expression(
    input_ref: &str,
    collation_ref: &str,
    comparison_ref: &str,
) -> &'static str {
    match (input_ref, collation_ref, comparison_ref) {
        ("first-utf8-basic", "binary", "eq-column-0-literal-alpha") => "input_rows.s = 'alpha'",
        ("first-utf8-basic", "unicode_ci", "eq-column-0-literal-alpha") => {
            "LOWER(input_rows.s) = LOWER('alpha')"
        }
        ("first-utf8-basic", "unknown-collation", "eq-column-0-literal-alpha") => {
            "input_rows.s COLLATE unknown_collation = 'alpha'"
        }
        ("first-utf8-basic", "binary", "lt-column-0-literal-beta") => "input_rows.s < 'beta'",
        ("first-utf8-basic", "unicode_ci", "lt-column-0-literal-beta") => {
            "LOWER(input_rows.s) < LOWER('beta')"
        }
        ("first-int32-basic", "binary", "eq-column-0-literal-alpha") => "input_rows.x = 'alpha'",
        _ => unreachable!("validated comparison refs should always be known"),
    }
}

fn ordering_clause(input_ref: &str, collation_ref: &str) -> &'static str {
    match (input_ref, collation_ref) {
        ("first-utf8-ordering", "binary") => "ORDER BY input_rows.s ASC",
        ("first-utf8-ordering", "unicode_ci") => {
            "ORDER BY LOWER(input_rows.s) ASC, input_rows.s ASC"
        }
        _ => unreachable!("validated ordering refs should always be known"),
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
            } else if is_unknown_collation(code.as_deref(), &message) {
                ErrorClass::UnknownCollation
            } else if is_unsupported_collation_type(code.as_deref(), &message) {
                ErrorClass::UnsupportedCollationType
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

fn is_unknown_collation(engine_code: Option<&str>, engine_message: &str) -> bool {
    let normalized_message = engine_message.to_ascii_lowercase();

    engine_code == Some("1273")
        || normalized_message.contains("unknown collation")
        || normalized_message.contains("unsupported collation_ref")
}

fn is_unsupported_collation_type(engine_code: Option<&str>, engine_message: &str) -> bool {
    let normalized_message = engine_message.to_ascii_lowercase();

    engine_code == Some("TFCS002")
        || normalized_message.contains("unsupported collation type")
        || normalized_message.contains("unsupported collation comparison input")
}

fn normalize_logical_type(engine_type: &str) -> String {
    let normalized = engine_type.trim().to_ascii_lowercase();
    let normalized_base = normalized.split('(').next().unwrap_or(&normalized).trim();

    match normalized_base {
        "char" | "varchar" | "text" => "utf8".to_string(),
        "bool" | "boolean" => "boolean".to_string(),
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
        let requests = TidbFirstCollationStringSliceAdapter::canonical_requests();
        let case_ids: Vec<&str> = requests
            .iter()
            .map(|request| request.case_id.as_str())
            .collect();

        assert_eq!(requests.len(), 10);
        assert_eq!(
            case_ids,
            vec![
                "utf8-column-passthrough-binary",
                "utf8-column-passthrough-unicode-ci",
                "utf8-is-not-null-mixed-keep-drop",
                "utf8-binary-eq-literal-alpha",
                "utf8-unicode-ci-eq-literal-alpha",
                "utf8-binary-lt-literal-beta",
                "utf8-unicode-ci-ordering-normalization",
                "utf8-missing-column-error",
                "unknown-collation-error",
                "unsupported-collation-type-error",
            ]
        );

        for request in requests {
            let op_ref_count = request.projection_ref.iter().count()
                + request.filter_ref.iter().count()
                + request.comparison_ref.iter().count()
                + request.ordering_ref.iter().count();
            assert_eq!(op_ref_count, 1);
        }
    }

    #[test]
    fn lowering_renders_tidb_sql_for_documented_case_shapes() {
        let requests = TidbFirstCollationStringSliceAdapter::canonical_requests();

        for request in &requests {
            let plan = TidbFirstCollationStringSliceAdapter::lower_request(request).unwrap();
            assert!(!plan.sql.is_empty());
        }

        let unknown_collation_request = requests
            .iter()
            .find(|request| request.case_id == "unknown-collation-error")
            .unwrap();
        let unknown_collation_plan =
            TidbFirstCollationStringSliceAdapter::lower_request(unknown_collation_request).unwrap();
        assert!(unknown_collation_plan.sql.contains("unknown_collation"));

        let ordering_request = requests
            .iter()
            .find(|request| request.case_id == "utf8-unicode-ci-ordering-normalization")
            .unwrap();
        let ordering_plan =
            TidbFirstCollationStringSliceAdapter::lower_request(ordering_request).unwrap();
        assert!(ordering_plan.sql.contains("ORDER BY LOWER(input_rows.s)"));
    }

    #[test]
    fn execute_rows_normalizes_utf8_and_boolean_schema() {
        let request = TidbFirstCollationStringSliceAdapter::canonical_requests()
            .into_iter()
            .find(|request| request.case_id == "utf8-binary-eq-literal-alpha")
            .unwrap();
        let runner = StubRunner::rows(
            vec![EngineColumn {
                name: "cmp".to_string(),
                engine_type: "bool".to_string(),
                nullable: false,
            }],
            vec![vec![json!(false)], vec![json!(true)], vec![json!(false)]],
        );

        let result = TidbFirstCollationStringSliceAdapter::execute(&request, &runner).unwrap();

        assert_eq!(result.engine, TIDB_ENGINE);
        assert_eq!(result.adapter, TIDB_ADAPTER);
        assert_eq!(result.collation_ref, "binary");
        assert_eq!(
            result.outcome,
            CaseOutcome::Rows {
                schema: vec![SchemaField {
                    name: "cmp".to_string(),
                    logical_type: "boolean".to_string(),
                    nullable: false,
                }],
                rows: vec![vec![json!(false)], vec![json!(true)], vec![json!(false)],],
                row_count: 3,
            }
        );
    }

    #[test]
    fn execute_normalizes_collation_error_classes() {
        let missing_request = TidbFirstCollationStringSliceAdapter::canonical_requests()
            .into_iter()
            .find(|request| request.case_id == "utf8-missing-column-error")
            .unwrap();
        let missing_runner = StubRunner::error(EngineExecutionError::EngineFailure {
            code: Some("1054".to_string()),
            message: "Unknown column '__missing_column_1' in 'where clause'".to_string(),
        });
        let missing_result =
            TidbFirstCollationStringSliceAdapter::execute(&missing_request, &missing_runner)
                .unwrap();
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

        let unknown_request = TidbFirstCollationStringSliceAdapter::canonical_requests()
            .into_iter()
            .find(|request| request.case_id == "unknown-collation-error")
            .unwrap();
        let unknown_runner = StubRunner::error(EngineExecutionError::EngineFailure {
            code: Some("1273".to_string()),
            message: "Unknown collation: 'unknown-collation'".to_string(),
        });
        let unknown_result =
            TidbFirstCollationStringSliceAdapter::execute(&unknown_request, &unknown_runner)
                .unwrap();
        assert_eq!(
            unknown_result.outcome,
            CaseOutcome::Error {
                error_class: ErrorClass::UnknownCollation,
                engine_code: Some("1273".to_string()),
                engine_message: Some("Unknown collation: 'unknown-collation'".to_string()),
            }
        );

        let unsupported_type_request = TidbFirstCollationStringSliceAdapter::canonical_requests()
            .into_iter()
            .find(|request| request.case_id == "unsupported-collation-type-error")
            .unwrap();
        let unsupported_type_runner = StubRunner::error(EngineExecutionError::EngineFailure {
            code: Some("1105".to_string()),
            message: "unsupported collation comparison input at column 0, got Int32".to_string(),
        });
        let unsupported_type_result = TidbFirstCollationStringSliceAdapter::execute(
            &unsupported_type_request,
            &unsupported_type_runner,
        )
        .unwrap();
        assert_eq!(
            unsupported_type_result.outcome,
            CaseOutcome::Error {
                error_class: ErrorClass::UnsupportedCollationType,
                engine_code: Some("1105".to_string()),
                engine_message: Some(
                    "unsupported collation comparison input at column 0, got Int32".to_string()
                ),
            }
        );
    }

    #[test]
    fn lowering_rejects_requests_with_mismatched_collation_ref() {
        let mut request = TidbFirstCollationStringSliceAdapter::canonical_requests()
            .into_iter()
            .find(|request| request.case_id == "utf8-binary-eq-literal-alpha")
            .unwrap();
        request.collation_ref = "unicode_ci".to_string();

        let error = TidbFirstCollationStringSliceAdapter::lower_request(&request).unwrap_err();

        assert_eq!(
            error,
            AdapterRequestValidationError::MismatchedCaseDefinition {
                case_id: "utf8-binary-eq-literal-alpha".to_string(),
                expected_input_ref: "first-utf8-basic",
                actual_input_ref: "first-utf8-basic".to_string(),
                expected_collation_ref: "binary",
                actual_collation_ref: "unicode_ci".to_string(),
                expected_projection_ref: None,
                actual_projection_ref: None,
                expected_filter_ref: None,
                actual_filter_ref: None,
                expected_comparison_ref: Some("eq-column-0-literal-alpha"),
                actual_comparison_ref: Some("eq-column-0-literal-alpha".to_string()),
                expected_ordering_ref: None,
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

    impl TidbRunner for StubRunner {
        fn run(
            &self,
            _plan: &TidbExecutionPlan,
        ) -> Result<EngineExecutionResult, EngineExecutionError> {
            self.result.clone()
        }
    }
}
