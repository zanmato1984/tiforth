use serde::{Deserialize, Serialize};
use serde_json::Value;
use tiforth_adapter_tidb::first_unsigned_arithmetic_slice as tidb;
use tiforth_adapter_tiflash::first_unsigned_arithmetic_slice as tiflash;

pub const TIDB_CASE_RESULTS_REF: &str =
    "inventory/first-unsigned-arithmetic-slice-tidb-case-results.json";
pub const TIFLASH_CASE_RESULTS_REF: &str =
    "inventory/first-unsigned-arithmetic-slice-tiflash-case-results.json";
pub const DRIFT_REPORT_REF: &str =
    "inventory/first-unsigned-arithmetic-slice-tidb-vs-tiflash-drift-report.md";
pub const DRIFT_REPORT_SIDECAR_REF: &str =
    "inventory/first-unsigned-arithmetic-slice-tidb-vs-tiflash-drift-report.json";

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HarnessError {
    CanonicalRequestMismatch {
        tidb_requests: Vec<CanonicalRequest>,
        tiflash_requests: Vec<CanonicalRequest>,
    },
    TidbAdapterValidation {
        case_id: String,
        error: String,
    },
    TiflashAdapterValidation {
        case_id: String,
        error: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CanonicalRequest {
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
pub struct CaseResultsArtifact {
    pub slice_id: String,
    pub engine: String,
    pub adapter: String,
    pub cases: Vec<CaseResult>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DriftStatus {
    Match,
    Drift,
    Unsupported,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ComparisonDimension {
    FieldName,
    FieldNullability,
    LogicalType,
    RowCount,
    RowValues,
    ErrorClass,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DriftCase {
    pub case_id: String,
    pub status: DriftStatus,
    pub comparison_dimensions: Vec<ComparisonDimension>,
    pub summary: String,
    pub evidence_refs: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub follow_up: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DriftReport {
    pub slice_id: String,
    pub engines: Vec<String>,
    pub spec_refs: Vec<String>,
    pub cases: Vec<DriftCase>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArtifactBundle {
    pub tidb_case_results: CaseResultsArtifact,
    pub tiflash_case_results: CaseResultsArtifact,
    pub drift_report: DriftReport,
}

pub fn canonical_requests() -> Result<Vec<CanonicalRequest>, HarnessError> {
    let tidb_requests: Vec<CanonicalRequest> =
        tidb::TidbFirstUnsignedArithmeticSliceAdapter::canonical_requests()
            .into_iter()
            .map(CanonicalRequest::from)
            .collect();
    let tiflash_requests: Vec<CanonicalRequest> =
        tiflash::TiflashFirstUnsignedArithmeticSliceAdapter::canonical_requests()
            .into_iter()
            .map(CanonicalRequest::from)
            .collect();

    if tidb_requests != tiflash_requests {
        return Err(HarnessError::CanonicalRequestMismatch {
            tidb_requests,
            tiflash_requests,
        });
    }

    Ok(tidb_requests)
}

pub fn execute_first_unsigned_arithmetic_slice<T, F>(
    tidb_runner: &T,
    tiflash_runner: &F,
) -> Result<ArtifactBundle, HarnessError>
where
    T: tidb::TidbRunner,
    F: tiflash::TiflashRunner,
{
    let requests = canonical_requests()?;
    let mut tidb_cases = Vec::with_capacity(requests.len());
    let mut tiflash_cases = Vec::with_capacity(requests.len());

    for request in &requests {
        let tidb_request = tidb::AdapterRequest::from(request);
        let tiflash_request = tiflash::AdapterRequest::from(request);

        let tidb_result =
            tidb::TidbFirstUnsignedArithmeticSliceAdapter::execute(&tidb_request, tidb_runner)
                .map_err(|error| HarnessError::TidbAdapterValidation {
                    case_id: request.case_id.clone(),
                    error: format!("{error:?}"),
                })?;
        let tiflash_result = tiflash::TiflashFirstUnsignedArithmeticSliceAdapter::execute(
            &tiflash_request,
            tiflash_runner,
        )
        .map_err(|error| HarnessError::TiflashAdapterValidation {
            case_id: request.case_id.clone(),
            error: format!("{error:?}"),
        })?;

        tidb_cases.push(CaseResult::from(tidb_result));
        tiflash_cases.push(CaseResult::from(tiflash_result));
    }

    let tidb_case_results = CaseResultsArtifact {
        slice_id: tidb::FIRST_UNSIGNED_ARITHMETIC_SLICE_ID.to_string(),
        engine: tidb::TIDB_ENGINE.to_string(),
        adapter: tidb::TIDB_ADAPTER.to_string(),
        cases: tidb_cases,
    };
    let tiflash_case_results = CaseResultsArtifact {
        slice_id: tiflash::FIRST_UNSIGNED_ARITHMETIC_SLICE_ID.to_string(),
        engine: tiflash::TIFLASH_ENGINE.to_string(),
        adapter: tiflash::TIFLASH_ADAPTER.to_string(),
        cases: tiflash_cases,
    };
    let drift_report = build_drift_report(&tidb_case_results, &tiflash_case_results);

    Ok(ArtifactBundle {
        tidb_case_results,
        tiflash_case_results,
        drift_report,
    })
}

pub fn render_case_results_artifact_json(
    artifact: &CaseResultsArtifact,
) -> Result<String, serde_json::Error> {
    let mut rendered = serde_json::to_string_pretty(artifact)?;
    rendered.push('\n');
    Ok(rendered)
}

pub fn render_drift_report_artifact_json(
    report: &DriftReport,
) -> Result<String, serde_json::Error> {
    let mut rendered = serde_json::to_string_pretty(report)?;
    rendered.push('\n');
    Ok(rendered)
}

pub fn render_drift_report_markdown(report: &DriftReport) -> String {
    let match_count = report
        .cases
        .iter()
        .filter(|case| case.status == DriftStatus::Match)
        .count();
    let drift_count = report
        .cases
        .iter()
        .filter(|case| case.status == DriftStatus::Drift)
        .count();
    let unsupported_count = report
        .cases
        .iter()
        .filter(|case| case.status == DriftStatus::Unsupported)
        .count();

    let mut rendered = String::new();
    rendered.push_str("# First Unsigned Arithmetic Slice TiDB-vs-TiFlash Drift Report\n\n");
    rendered.push_str("Status: issue #310 harness checkpoint\n\n");
    rendered.push_str("Verified: 2026-03-20\n\n");
    rendered.push_str("## Evidence Source\n\n");
    rendered.push_str(
        "- this checkpoint runs the current TiDB and TiFlash unsigned adapter cores through deterministic harness fixture runners\n",
    );
    rendered.push_str(
        "- live engine connection and orchestration remain out of scope for this artifact set\n",
    );
    rendered.push_str("- the stable artifact-carrier boundary lives in `tests/differential/first-unsigned-arithmetic-slice-artifacts.md`\n\n");
    rendered.push_str("## Engines\n\n");
    for engine in &report.engines {
        rendered.push_str("- `");
        rendered.push_str(engine);
        rendered.push_str("`\n");
    }
    rendered.push_str("\n## Spec Refs\n\n");
    for spec_ref in &report.spec_refs {
        rendered.push_str("- `");
        rendered.push_str(spec_ref);
        rendered.push_str("`\n");
    }
    rendered.push_str("\n## Summary\n\n");
    rendered.push_str("- `match`: ");
    rendered.push_str(&match_count.to_string());
    rendered.push('\n');
    rendered.push_str("- `drift`: ");
    rendered.push_str(&drift_count.to_string());
    rendered.push('\n');
    rendered.push_str("- `unsupported`: ");
    rendered.push_str(&unsupported_count.to_string());
    rendered.push_str("\n\n## Cases\n\n");

    for (case_index, case) in report.cases.iter().enumerate() {
        rendered.push_str("### `");
        rendered.push_str(&case.case_id);
        rendered.push_str("`\n\n");
        rendered.push_str("- status: `");
        rendered.push_str(drift_status_name(case.status));
        rendered.push_str("`\n");
        rendered.push_str("- comparison_dimensions: ");
        rendered.push_str(&join_inline_codes(
            case.comparison_dimensions
                .iter()
                .copied()
                .map(comparison_dimension_name),
        ));
        rendered.push('\n');
        rendered.push_str("- summary: ");
        rendered.push_str(&case.summary);
        rendered.push('\n');
        rendered.push_str("- evidence_refs: ");
        rendered.push_str(&join_inline_codes(
            case.evidence_refs.iter().map(String::as_str),
        ));
        rendered.push('\n');
        if let Some(follow_up) = &case.follow_up {
            rendered.push_str("- follow_up: ");
            rendered.push_str(follow_up);
            rendered.push('\n');
        }
        if case_index + 1 < report.cases.len() {
            rendered.push('\n');
        }
    }

    rendered
}

fn build_drift_report(
    tidb_case_results: &CaseResultsArtifact,
    tiflash_case_results: &CaseResultsArtifact,
) -> DriftReport {
    let spec_refs = tidb_case_results
        .cases
        .first()
        .map(|case| case.spec_refs.clone())
        .unwrap_or_default();
    let cases = tidb_case_results
        .cases
        .iter()
        .zip(&tiflash_case_results.cases)
        .map(|(tidb_case, tiflash_case)| compare_case_results(tidb_case, tiflash_case))
        .collect();

    DriftReport {
        slice_id: tidb::FIRST_UNSIGNED_ARITHMETIC_SLICE_ID.to_string(),
        engines: vec![
            tidb::TIDB_ENGINE.to_string(),
            tiflash::TIFLASH_ENGINE.to_string(),
        ],
        spec_refs,
        cases,
    }
}

fn compare_case_results(tidb_case: &CaseResult, tiflash_case: &CaseResult) -> DriftCase {
    let evidence_refs = vec![
        format!("{TIDB_CASE_RESULTS_REF}#{}", tidb_case.case_id),
        format!("{TIFLASH_CASE_RESULTS_REF}#{}", tiflash_case.case_id),
    ];

    match (&tidb_case.outcome, &tiflash_case.outcome) {
        (
            CaseOutcome::Error {
                error_class: ErrorClass::AdapterUnavailable,
                ..
            },
            other,
        ) => DriftCase {
            case_id: tidb_case.case_id.clone(),
            status: DriftStatus::Unsupported,
            comparison_dimensions: vec![ComparisonDimension::ErrorClass],
            summary: unsupported_summary(
                tidb::TIDB_ENGINE,
                ErrorClass::AdapterUnavailable,
                tiflash::TIFLASH_ENGINE,
                outcome_error_class(other),
                &tidb_case.case_id,
            ),
            evidence_refs,
            follow_up: unsupported_follow_up(tidb::TIDB_ENGINE, &tidb_case.case_id),
        },
        (
            other,
            CaseOutcome::Error {
                error_class: ErrorClass::AdapterUnavailable,
                ..
            },
        ) => DriftCase {
            case_id: tidb_case.case_id.clone(),
            status: DriftStatus::Unsupported,
            comparison_dimensions: vec![ComparisonDimension::ErrorClass],
            summary: unsupported_summary(
                tidb::TIDB_ENGINE,
                outcome_error_class(other),
                tiflash::TIFLASH_ENGINE,
                ErrorClass::AdapterUnavailable,
                &tidb_case.case_id,
            ),
            evidence_refs,
            follow_up: unsupported_follow_up(tiflash::TIFLASH_ENGINE, &tidb_case.case_id),
        },
        (
            CaseOutcome::Rows {
                schema: tidb_schema,
                rows: tidb_rows,
                row_count: tidb_row_count,
            },
            CaseOutcome::Rows {
                schema: tiflash_schema,
                rows: tiflash_rows,
                row_count: tiflash_row_count,
            },
        ) => {
            let drift_dimensions = row_drift_dimensions(
                tidb_case,
                tiflash_case,
                tidb_schema,
                tiflash_schema,
                tidb_rows,
                tiflash_rows,
                *tidb_row_count,
                *tiflash_row_count,
            );
            let status = if drift_dimensions.is_empty() {
                DriftStatus::Match
            } else {
                DriftStatus::Drift
            };
            let comparison_dimensions = if drift_dimensions.is_empty() {
                compared_row_dimensions()
            } else {
                drift_dimensions
            };

            DriftCase {
                case_id: tidb_case.case_id.clone(),
                status,
                comparison_dimensions: comparison_dimensions.clone(),
                summary: if status == DriftStatus::Match {
                    matching_rows_summary(
                        &tidb_case.case_id,
                        tidb_schema,
                        *tidb_row_count,
                        &tidb_case.comparison_mode,
                    )
                } else {
                    drifting_rows_summary(
                        &tidb_case.case_id,
                        &comparison_dimensions,
                        &tidb_case.comparison_mode,
                    )
                },
                evidence_refs,
                follow_up: None,
            }
        }
        (
            CaseOutcome::Error {
                error_class: tidb_error,
                ..
            },
            CaseOutcome::Error {
                error_class: tiflash_error,
                ..
            },
        ) => {
            let status = if tidb_error == tiflash_error {
                DriftStatus::Match
            } else {
                DriftStatus::Drift
            };

            DriftCase {
                case_id: tidb_case.case_id.clone(),
                status,
                comparison_dimensions: vec![ComparisonDimension::ErrorClass],
                summary: if status == DriftStatus::Match {
                    matching_error_summary(&tidb_case.case_id, *tidb_error)
                } else {
                    drifting_error_summary(&tidb_case.case_id, *tidb_error, *tiflash_error)
                },
                evidence_refs,
                follow_up: drifting_error_follow_up(&tidb_case.case_id, *tidb_error, *tiflash_error),
            }
        }
        (tidb_outcome, tiflash_outcome) => DriftCase {
            case_id: tidb_case.case_id.clone(),
            status: DriftStatus::Drift,
            comparison_dimensions: vec![ComparisonDimension::ErrorClass],
            summary: mixed_outcome_summary(&tidb_case.case_id, tidb_outcome, tiflash_outcome),
            evidence_refs,
            follow_up: Some(
                "Decide whether the erroring side should align to the shared case or remain explicitly unsupported.".to_string(),
            ),
        },
    }
}

fn row_drift_dimensions(
    tidb_case: &CaseResult,
    tiflash_case: &CaseResult,
    tidb_schema: &[SchemaField],
    tiflash_schema: &[SchemaField],
    tidb_rows: &[Vec<Value>],
    tiflash_rows: &[Vec<Value>],
    tidb_row_count: usize,
    tiflash_row_count: usize,
) -> Vec<ComparisonDimension> {
    let mut dimensions = Vec::new();

    if schema_names(tidb_schema) != schema_names(tiflash_schema) {
        dimensions.push(ComparisonDimension::FieldName);
    }
    if schema_nullability(tidb_schema) != schema_nullability(tiflash_schema) {
        dimensions.push(ComparisonDimension::FieldNullability);
    }
    if schema_logical_types(tidb_schema) != schema_logical_types(tiflash_schema) {
        dimensions.push(ComparisonDimension::LogicalType);
    }
    if tidb_row_count != tiflash_row_count {
        dimensions.push(ComparisonDimension::RowCount);
    }
    if !rows_match_for_case(tidb_case, tiflash_case, tidb_rows, tiflash_rows) {
        dimensions.push(ComparisonDimension::RowValues);
    }

    dimensions
}

fn rows_match_for_case(
    tidb_case: &CaseResult,
    tiflash_case: &CaseResult,
    tidb_rows: &[Vec<Value>],
    tiflash_rows: &[Vec<Value>],
) -> bool {
    tidb_case.comparison_mode == tiflash_case.comparison_mode && tidb_rows == tiflash_rows
}

fn compared_row_dimensions() -> Vec<ComparisonDimension> {
    vec![
        ComparisonDimension::FieldName,
        ComparisonDimension::FieldNullability,
        ComparisonDimension::LogicalType,
        ComparisonDimension::RowCount,
        ComparisonDimension::RowValues,
    ]
}

fn schema_names(schema: &[SchemaField]) -> Vec<&str> {
    schema.iter().map(|field| field.name.as_str()).collect()
}

fn schema_nullability(schema: &[SchemaField]) -> Vec<bool> {
    schema.iter().map(|field| field.nullable).collect()
}

fn schema_logical_types(schema: &[SchemaField]) -> Vec<&str> {
    schema
        .iter()
        .map(|field| field.logical_type.as_str())
        .collect()
}

fn matching_rows_summary(
    case_id: &str,
    schema: &[SchemaField],
    row_count: usize,
    comparison_mode: &str,
) -> String {
    match schema {
        [field] => format!(
            "TiDB and TiFlash both returned {row_count} row(s) for `{case_id}` with field `{}` normalized as `{}` under `{comparison_mode}`.",
            field.name, field.logical_type
        ),
        _ => format!(
            "TiDB and TiFlash both returned matching row output for `{case_id}` under `{comparison_mode}`."
        ),
    }
}

fn drifting_rows_summary(
    case_id: &str,
    dimensions: &[ComparisonDimension],
    comparison_mode: &str,
) -> String {
    format!(
        "TiDB and TiFlash disagree on {} for `{case_id}` under `{comparison_mode}`.",
        join_inline_codes(dimensions.iter().copied().map(comparison_dimension_name))
    )
}

fn matching_error_summary(case_id: &str, error_class: ErrorClass) -> String {
    format!(
        "TiDB and TiFlash both normalized `{case_id}` as `{}`.",
        error_class_name(error_class)
    )
}

fn drifting_error_summary(
    case_id: &str,
    tidb_error: ErrorClass,
    tiflash_error: ErrorClass,
) -> String {
    format!(
        "TiDB normalized `{case_id}` as `{}` while TiFlash normalized it as `{}`.",
        error_class_name(tidb_error),
        error_class_name(tiflash_error)
    )
}

fn drifting_error_follow_up(
    case_id: &str,
    tidb_error: ErrorClass,
    tiflash_error: ErrorClass,
) -> Option<String> {
    if tidb_error != tiflash_error {
        Some(format!(
            "Review whether `{case_id}` should keep diverging as `{}` versus `{}` or whether one adapter should realign its normalization.",
            error_class_name(tidb_error),
            error_class_name(tiflash_error)
        ))
    } else {
        None
    }
}

fn mixed_outcome_summary(
    case_id: &str,
    tidb_outcome: &CaseOutcome,
    tiflash_outcome: &CaseOutcome,
) -> String {
    format!(
        "TiDB returned {} for `{case_id}` while TiFlash returned {}.",
        outcome_kind_name(tidb_outcome),
        outcome_kind_name(tiflash_outcome)
    )
}

fn unsupported_summary(
    tidb_engine: &str,
    tidb_error: ErrorClass,
    tiflash_engine: &str,
    tiflash_error: ErrorClass,
    case_id: &str,
) -> String {
    format!(
        "{} normalized `{case_id}` as `{}` while {} normalized it as `{}`; the pair remains explicitly unsupported.",
        tidb_engine,
        error_class_name(tidb_error),
        tiflash_engine,
        error_class_name(tiflash_error)
    )
}

fn unsupported_follow_up(engine: &str, case_id: &str) -> Option<String> {
    Some(format!(
        "Implement the missing {engine} adapter-side path before treating `{case_id}` as fully comparable."
    ))
}

fn outcome_error_class(outcome: &CaseOutcome) -> ErrorClass {
    match outcome {
        CaseOutcome::Error { error_class, .. } => *error_class,
        CaseOutcome::Rows { .. } => ErrorClass::EngineError,
    }
}

fn outcome_kind_name(outcome: &CaseOutcome) -> &'static str {
    match outcome {
        CaseOutcome::Rows { .. } => "`rows`",
        CaseOutcome::Error { error_class, .. } => match error_class {
            ErrorClass::MissingColumn => "`error/missing_column`",
            ErrorClass::UnsignedOverflow => "`error/unsigned_overflow`",
            ErrorClass::MixedSignedUnsigned => "`error/mixed_signed_unsigned`",
            ErrorClass::UnsupportedUnsignedFamily => "`error/unsupported_unsigned_family`",
            ErrorClass::AdapterUnavailable => "`error/adapter_unavailable`",
            ErrorClass::EngineError => "`error/engine_error`",
        },
    }
}

fn error_class_name(error_class: ErrorClass) -> &'static str {
    match error_class {
        ErrorClass::MissingColumn => "missing_column",
        ErrorClass::UnsignedOverflow => "unsigned_overflow",
        ErrorClass::MixedSignedUnsigned => "mixed_signed_unsigned",
        ErrorClass::UnsupportedUnsignedFamily => "unsupported_unsigned_family",
        ErrorClass::AdapterUnavailable => "adapter_unavailable",
        ErrorClass::EngineError => "engine_error",
    }
}

fn drift_status_name(status: DriftStatus) -> &'static str {
    match status {
        DriftStatus::Match => "match",
        DriftStatus::Drift => "drift",
        DriftStatus::Unsupported => "unsupported",
    }
}

fn comparison_dimension_name(dimension: ComparisonDimension) -> &'static str {
    match dimension {
        ComparisonDimension::FieldName => "field_name",
        ComparisonDimension::FieldNullability => "field_nullability",
        ComparisonDimension::LogicalType => "logical_type",
        ComparisonDimension::RowCount => "row_count",
        ComparisonDimension::RowValues => "row_values",
        ComparisonDimension::ErrorClass => "error_class",
    }
}

fn join_inline_codes<'a, I>(items: I) -> String
where
    I: IntoIterator<Item = &'a str>,
{
    items
        .into_iter()
        .map(|item| format!("`{item}`"))
        .collect::<Vec<_>>()
        .join(", ")
}

impl From<tidb::AdapterRequest> for CanonicalRequest {
    fn from(value: tidb::AdapterRequest) -> Self {
        Self {
            slice_id: value.slice_id,
            case_id: value.case_id,
            spec_refs: value.spec_refs,
            input_ref: value.input_ref,
            comparison_mode: value.comparison_mode,
            projection_ref: value.projection_ref,
            filter_ref: value.filter_ref,
        }
    }
}

impl From<tiflash::AdapterRequest> for CanonicalRequest {
    fn from(value: tiflash::AdapterRequest) -> Self {
        Self {
            slice_id: value.slice_id,
            case_id: value.case_id,
            spec_refs: value.spec_refs,
            input_ref: value.input_ref,
            comparison_mode: value.comparison_mode,
            projection_ref: value.projection_ref,
            filter_ref: value.filter_ref,
        }
    }
}

impl From<&CanonicalRequest> for tidb::AdapterRequest {
    fn from(value: &CanonicalRequest) -> Self {
        Self {
            slice_id: value.slice_id.clone(),
            case_id: value.case_id.clone(),
            spec_refs: value.spec_refs.clone(),
            input_ref: value.input_ref.clone(),
            comparison_mode: value.comparison_mode.clone(),
            projection_ref: value.projection_ref.clone(),
            filter_ref: value.filter_ref.clone(),
        }
    }
}

impl From<&CanonicalRequest> for tiflash::AdapterRequest {
    fn from(value: &CanonicalRequest) -> Self {
        Self {
            slice_id: value.slice_id.clone(),
            case_id: value.case_id.clone(),
            spec_refs: value.spec_refs.clone(),
            input_ref: value.input_ref.clone(),
            comparison_mode: value.comparison_mode.clone(),
            projection_ref: value.projection_ref.clone(),
            filter_ref: value.filter_ref.clone(),
        }
    }
}

impl From<tidb::CaseResult> for CaseResult {
    fn from(value: tidb::CaseResult) -> Self {
        Self {
            slice_id: value.slice_id,
            engine: value.engine,
            adapter: value.adapter,
            case_id: value.case_id,
            spec_refs: value.spec_refs,
            input_ref: value.input_ref,
            comparison_mode: value.comparison_mode,
            projection_ref: value.projection_ref,
            filter_ref: value.filter_ref,
            outcome: convert_tidb_outcome(value.outcome),
        }
    }
}

impl From<tiflash::CaseResult> for CaseResult {
    fn from(value: tiflash::CaseResult) -> Self {
        Self {
            slice_id: value.slice_id,
            engine: value.engine,
            adapter: value.adapter,
            case_id: value.case_id,
            spec_refs: value.spec_refs,
            input_ref: value.input_ref,
            comparison_mode: value.comparison_mode,
            projection_ref: value.projection_ref,
            filter_ref: value.filter_ref,
            outcome: convert_tiflash_outcome(value.outcome),
        }
    }
}

fn convert_tidb_outcome(outcome: tidb::CaseOutcome) -> CaseOutcome {
    match outcome {
        tidb::CaseOutcome::Rows {
            schema,
            rows,
            row_count,
        } => CaseOutcome::Rows {
            schema: schema.into_iter().map(convert_tidb_schema_field).collect(),
            rows,
            row_count,
        },
        tidb::CaseOutcome::Error {
            error_class,
            engine_code,
            engine_message,
        } => CaseOutcome::Error {
            error_class: convert_tidb_error_class(error_class),
            engine_code,
            engine_message,
        },
    }
}

fn convert_tiflash_outcome(outcome: tiflash::CaseOutcome) -> CaseOutcome {
    match outcome {
        tiflash::CaseOutcome::Rows {
            schema,
            rows,
            row_count,
        } => CaseOutcome::Rows {
            schema: schema
                .into_iter()
                .map(convert_tiflash_schema_field)
                .collect(),
            rows,
            row_count,
        },
        tiflash::CaseOutcome::Error {
            error_class,
            engine_code,
            engine_message,
        } => CaseOutcome::Error {
            error_class: convert_tiflash_error_class(error_class),
            engine_code,
            engine_message,
        },
    }
}

fn convert_tidb_schema_field(field: tidb::SchemaField) -> SchemaField {
    SchemaField {
        name: field.name,
        logical_type: field.logical_type,
        nullable: field.nullable,
    }
}

fn convert_tiflash_schema_field(field: tiflash::SchemaField) -> SchemaField {
    SchemaField {
        name: field.name,
        logical_type: field.logical_type,
        nullable: field.nullable,
    }
}

fn convert_tidb_error_class(error_class: tidb::ErrorClass) -> ErrorClass {
    match error_class {
        tidb::ErrorClass::MissingColumn => ErrorClass::MissingColumn,
        tidb::ErrorClass::UnsignedOverflow => ErrorClass::UnsignedOverflow,
        tidb::ErrorClass::MixedSignedUnsigned => ErrorClass::MixedSignedUnsigned,
        tidb::ErrorClass::UnsupportedUnsignedFamily => ErrorClass::UnsupportedUnsignedFamily,
        tidb::ErrorClass::AdapterUnavailable => ErrorClass::AdapterUnavailable,
        tidb::ErrorClass::EngineError => ErrorClass::EngineError,
    }
}

fn convert_tiflash_error_class(error_class: tiflash::ErrorClass) -> ErrorClass {
    match error_class {
        tiflash::ErrorClass::MissingColumn => ErrorClass::MissingColumn,
        tiflash::ErrorClass::UnsignedOverflow => ErrorClass::UnsignedOverflow,
        tiflash::ErrorClass::MixedSignedUnsigned => ErrorClass::MixedSignedUnsigned,
        tiflash::ErrorClass::UnsupportedUnsignedFamily => ErrorClass::UnsupportedUnsignedFamily,
        tiflash::ErrorClass::AdapterUnavailable => ErrorClass::AdapterUnavailable,
        tiflash::ErrorClass::EngineError => ErrorClass::EngineError,
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    const TIDB_CASE_RESULTS_PATH: &str = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../inventory/first-unsigned-arithmetic-slice-tidb-case-results.json"
    );
    const TIFLASH_CASE_RESULTS_PATH: &str = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../inventory/first-unsigned-arithmetic-slice-tiflash-case-results.json"
    );
    const DRIFT_REPORT_PATH: &str = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../inventory/first-unsigned-arithmetic-slice-tidb-vs-tiflash-drift-report.md"
    );
    const DRIFT_REPORT_SIDECAR_PATH: &str = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../inventory/first-unsigned-arithmetic-slice-tidb-vs-tiflash-drift-report.json"
    );

    #[test]
    fn canonical_requests_match_across_adapter_cores() {
        let requests = canonical_requests().unwrap();
        let case_ids: Vec<&str> = requests
            .iter()
            .map(|request| request.case_id.as_str())
            .collect();

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
    }

    #[test]
    fn checked_in_artifacts_match_the_fixture_harness_output() {
        let bundle =
            execute_first_unsigned_arithmetic_slice(&FixtureTidbRunner, &FixtureTiflashRunner)
                .unwrap();

        assert_eq!(
            render_case_results_artifact_json(&bundle.tidb_case_results).unwrap(),
            std::fs::read_to_string(TIDB_CASE_RESULTS_PATH).unwrap()
        );
        assert_eq!(
            render_case_results_artifact_json(&bundle.tiflash_case_results).unwrap(),
            std::fs::read_to_string(TIFLASH_CASE_RESULTS_PATH).unwrap()
        );
        assert_eq!(
            render_drift_report_markdown(&bundle.drift_report),
            std::fs::read_to_string(DRIFT_REPORT_PATH).unwrap()
        );
        assert_eq!(
            render_drift_report_artifact_json(&bundle.drift_report).unwrap(),
            std::fs::read_to_string(DRIFT_REPORT_SIDECAR_PATH).unwrap()
        );
    }

    #[test]
    fn compare_case_results_marks_row_differences_as_drift() {
        let tidb_case = CaseResult {
            slice_id: tidb::FIRST_UNSIGNED_ARITHMETIC_SLICE_ID.to_string(),
            engine: tidb::TIDB_ENGINE.to_string(),
            adapter: tidb::TIDB_ADAPTER.to_string(),
            case_id: "uint64-add-basic".to_string(),
            spec_refs: vec!["tests/differential/first-unsigned-arithmetic-slice.md".to_string()],
            input_ref: "first-uint64-add-basic".to_string(),
            comparison_mode: tidb::COMPARISON_MODE_ROW_ORDER_PRESERVED.to_string(),
            projection_ref: Some("add-uint64-column-0-column-1".to_string()),
            filter_ref: None,
            outcome: CaseOutcome::Rows {
                schema: vec![SchemaField {
                    name: "sum".to_string(),
                    logical_type: "uint64".to_string(),
                    nullable: false,
                }],
                rows: vec![vec![json!("3")], vec![json!("7")], vec![json!("30")]],
                row_count: 3,
            },
        };
        let tiflash_case = CaseResult {
            outcome: CaseOutcome::Rows {
                schema: vec![SchemaField {
                    name: "sum".to_string(),
                    logical_type: "uint64".to_string(),
                    nullable: false,
                }],
                rows: vec![vec![json!("3")], vec![json!("99")], vec![json!("30")]],
                row_count: 3,
            },
            ..tidb_case
                .clone()
                .with_engine(tiflash::TIFLASH_ENGINE, tiflash::TIFLASH_ADAPTER)
        };

        let comparison = compare_case_results(&tidb_case, &tiflash_case);

        assert_eq!(comparison.status, DriftStatus::Drift);
        assert_eq!(
            comparison.comparison_dimensions,
            vec![ComparisonDimension::RowValues]
        );
    }

    #[test]
    fn compare_case_results_marks_adapter_unavailable_as_unsupported() {
        let tidb_case = CaseResult {
            slice_id: tidb::FIRST_UNSIGNED_ARITHMETIC_SLICE_ID.to_string(),
            engine: tidb::TIDB_ENGINE.to_string(),
            adapter: tidb::TIDB_ADAPTER.to_string(),
            case_id: "unsupported-unsigned-family-error".to_string(),
            spec_refs: vec!["tests/differential/first-unsigned-arithmetic-slice.md".to_string()],
            input_ref: "first-uint32-basic".to_string(),
            comparison_mode: tidb::COMPARISON_MODE_ROW_ORDER_PRESERVED.to_string(),
            projection_ref: Some("column-0".to_string()),
            filter_ref: None,
            outcome: CaseOutcome::Error {
                error_class: ErrorClass::AdapterUnavailable,
                engine_code: None,
                engine_message: Some("not implemented".to_string()),
            },
        };
        let tiflash_case = CaseResult {
            slice_id: tiflash::FIRST_UNSIGNED_ARITHMETIC_SLICE_ID.to_string(),
            engine: tiflash::TIFLASH_ENGINE.to_string(),
            adapter: tiflash::TIFLASH_ADAPTER.to_string(),
            case_id: "unsupported-unsigned-family-error".to_string(),
            spec_refs: vec!["tests/differential/first-unsigned-arithmetic-slice.md".to_string()],
            input_ref: "first-uint32-basic".to_string(),
            comparison_mode: tiflash::COMPARISON_MODE_ROW_ORDER_PRESERVED.to_string(),
            projection_ref: Some("column-0".to_string()),
            filter_ref: None,
            outcome: CaseOutcome::Error {
                error_class: ErrorClass::UnsupportedUnsignedFamily,
                engine_code: Some("1105".to_string()),
                engine_message: Some("unsupported unsigned type".to_string()),
            },
        };

        let comparison = compare_case_results(&tidb_case, &tiflash_case);

        assert_eq!(comparison.status, DriftStatus::Unsupported);
        assert_eq!(
            comparison.comparison_dimensions,
            vec![ComparisonDimension::ErrorClass]
        );
    }

    trait CaseResultExt {
        fn with_engine(self, engine: &str, adapter: &str) -> Self;
    }

    impl CaseResultExt for CaseResult {
        fn with_engine(mut self, engine: &str, adapter: &str) -> Self {
            self.engine = engine.to_string();
            self.adapter = adapter.to_string();
            self
        }
    }

    struct FixtureTidbRunner;

    impl tidb::TidbRunner for FixtureTidbRunner {
        fn run(
            &self,
            plan: &tidb::TidbExecutionPlan,
        ) -> Result<tidb::EngineExecutionResult, tidb::EngineExecutionError> {
            match plan.request.case_id.as_str() {
                "uint64-column-passthrough" => Ok(rows_result_tidb(
                    vec![tidb::EngineColumn {
                        name: "u".to_string(),
                        engine_type: "bigint unsigned".to_string(),
                        nullable: false,
                    }],
                    vec![vec![json!("0")], vec![json!("7")], vec![json!("42")]],
                )),
                "uint64-literal-projection" => Ok(rows_result_tidb(
                    vec![tidb::EngineColumn {
                        name: "seven".to_string(),
                        engine_type: "bigint unsigned".to_string(),
                        nullable: false,
                    }],
                    vec![vec![json!("7")], vec![json!("7")], vec![json!("7")]],
                )),
                "uint64-add-basic" => Ok(rows_result_tidb(
                    vec![tidb::EngineColumn {
                        name: "sum".to_string(),
                        engine_type: "bigint unsigned".to_string(),
                        nullable: false,
                    }],
                    vec![vec![json!("3")], vec![json!("7")], vec![json!("30")]],
                )),
                "uint64-add-null-propagation" => Ok(rows_result_tidb(
                    vec![tidb::EngineColumn {
                        name: "sum".to_string(),
                        engine_type: "bigint unsigned".to_string(),
                        nullable: true,
                    }],
                    vec![vec![Value::Null], vec![Value::Null], vec![json!("7")]],
                )),
                "uint64-is-not-null-mixed-keep-drop" => Ok(rows_result_tidb(
                    vec![tidb::EngineColumn {
                        name: "u".to_string(),
                        engine_type: "bigint unsigned".to_string(),
                        nullable: true,
                    }],
                    vec![vec![json!("5")], vec![json!("9")]],
                )),
                "uint64-add-overflow-error" => Err(tidb::EngineExecutionError::EngineFailure {
                    code: Some("1690".to_string()),
                    message:
                        "BIGINT UNSIGNED value is out of range in '(input_rows.lhs + input_rows.rhs)'"
                            .to_string(),
                }),
                "uint64-missing-column-error" => Err(tidb::EngineExecutionError::EngineFailure {
                    code: Some("1054".to_string()),
                    message: "Unknown column '__missing_column_2' in 'field list'".to_string(),
                }),
                "mixed-signed-unsigned-arithmetic-error" => {
                    Err(tidb::EngineExecutionError::EngineFailure {
                        code: Some("1105".to_string()),
                        message: "mixed signed and unsigned arithmetic is unsupported for first-unsigned-arithmetic-slice; got Int64".to_string(),
                    })
                }
                "unsupported-unsigned-family-error" => {
                    Err(tidb::EngineExecutionError::EngineFailure {
                        code: Some("1105".to_string()),
                        message: "unsupported unsigned type: uint32 input is out of scope for first-unsigned-arithmetic-slice".to_string(),
                    })
                }
                other => panic!("unexpected case_id: {other}"),
            }
        }
    }

    struct FixtureTiflashRunner;

    impl tiflash::TiflashRunner for FixtureTiflashRunner {
        fn run(
            &self,
            plan: &tiflash::TiflashExecutionPlan,
        ) -> Result<tiflash::EngineExecutionResult, tiflash::EngineExecutionError> {
            match plan.request.case_id.as_str() {
                "uint64-column-passthrough" => Ok(rows_result_tiflash(
                    vec![tiflash::EngineColumn {
                        name: "u".to_string(),
                        engine_type: "bigint unsigned".to_string(),
                        nullable: false,
                    }],
                    vec![vec![json!("0")], vec![json!("7")], vec![json!("42")]],
                )),
                "uint64-literal-projection" => Ok(rows_result_tiflash(
                    vec![tiflash::EngineColumn {
                        name: "seven".to_string(),
                        engine_type: "bigint unsigned".to_string(),
                        nullable: false,
                    }],
                    vec![vec![json!("7")], vec![json!("7")], vec![json!("7")]],
                )),
                "uint64-add-basic" => Ok(rows_result_tiflash(
                    vec![tiflash::EngineColumn {
                        name: "sum".to_string(),
                        engine_type: "bigint unsigned".to_string(),
                        nullable: false,
                    }],
                    vec![vec![json!("3")], vec![json!("7")], vec![json!("30")]],
                )),
                "uint64-add-null-propagation" => Ok(rows_result_tiflash(
                    vec![tiflash::EngineColumn {
                        name: "sum".to_string(),
                        engine_type: "bigint unsigned".to_string(),
                        nullable: true,
                    }],
                    vec![vec![Value::Null], vec![Value::Null], vec![json!("7")]],
                )),
                "uint64-is-not-null-mixed-keep-drop" => Ok(rows_result_tiflash(
                    vec![tiflash::EngineColumn {
                        name: "u".to_string(),
                        engine_type: "bigint unsigned".to_string(),
                        nullable: true,
                    }],
                    vec![vec![json!("5")], vec![json!("9")]],
                )),
                "uint64-add-overflow-error" => Err(tiflash::EngineExecutionError::EngineFailure {
                    code: Some("1690".to_string()),
                    message:
                        "BIGINT UNSIGNED value is out of range in '(input_rows.lhs + input_rows.rhs)'"
                            .to_string(),
                }),
                "uint64-missing-column-error" => Err(tiflash::EngineExecutionError::EngineFailure {
                    code: Some("1054".to_string()),
                    message: "Unknown column '__missing_column_2' in 'field list'".to_string(),
                }),
                "mixed-signed-unsigned-arithmetic-error" => {
                    Err(tiflash::EngineExecutionError::EngineFailure {
                        code: Some("1105".to_string()),
                        message: "mixed signed and unsigned arithmetic is unsupported for first-unsigned-arithmetic-slice; got Int64".to_string(),
                    })
                }
                "unsupported-unsigned-family-error" => {
                    Err(tiflash::EngineExecutionError::EngineFailure {
                        code: Some("1105".to_string()),
                        message: "unsupported unsigned type: uint32 input is out of scope for first-unsigned-arithmetic-slice".to_string(),
                    })
                }
                other => panic!("unexpected case_id: {other}"),
            }
        }
    }

    fn rows_result_tidb(
        columns: Vec<tidb::EngineColumn>,
        rows: Vec<Vec<Value>>,
    ) -> tidb::EngineExecutionResult {
        tidb::EngineExecutionResult { columns, rows }
    }

    fn rows_result_tiflash(
        columns: Vec<tiflash::EngineColumn>,
        rows: Vec<Vec<Value>>,
    ) -> tiflash::EngineExecutionResult {
        tiflash::EngineExecutionResult { columns, rows }
    }
}
