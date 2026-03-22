use serde::{Deserialize, Serialize};
use serde_json::Value;
use tiforth_adapter_tidb::first_signed_widening_add_int64_slice as tidb;
use tiforth_adapter_tiflash::first_signed_widening_add_int64_slice as tiflash;

pub const TIDB_CASE_RESULTS_REF: &str =
    "inventory/first-signed-widening-add-int64-slice-tidb-case-results.json";
pub const TIFLASH_CASE_RESULTS_REF: &str =
    "inventory/first-signed-widening-add-int64-slice-tiflash-case-results.json";
pub const DRIFT_REPORT_REF: &str =
    "inventory/first-signed-widening-add-int64-slice-tidb-vs-tiflash-drift-report.md";
pub const DRIFT_REPORT_SIDECAR_REF: &str =
    "inventory/first-signed-widening-add-int64-slice-tidb-vs-tiflash-drift-report.json";

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
    ArithmeticOverflow,
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
        tidb::TidbFirstSignedWideningAddInt64SliceAdapter::canonical_requests()
            .into_iter()
            .map(CanonicalRequest::from)
            .collect();
    let tiflash_requests: Vec<CanonicalRequest> =
        tiflash::TiflashFirstSignedWideningAddInt64SliceAdapter::canonical_requests()
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

pub fn execute_first_signed_widening_add_int64_slice<T, F>(
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
            tidb::TidbFirstSignedWideningAddInt64SliceAdapter::execute(&tidb_request, tidb_runner)
                .map_err(|error| HarnessError::TidbAdapterValidation {
                    case_id: request.case_id.clone(),
                    error: format!("{error:?}"),
                })?;
        let tiflash_result = tiflash::TiflashFirstSignedWideningAddInt64SliceAdapter::execute(
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
        slice_id: tidb::FIRST_SIGNED_WIDENING_ADD_INT64_SLICE_ID.to_string(),
        engine: tidb::TIDB_ENGINE.to_string(),
        adapter: tidb::TIDB_ADAPTER.to_string(),
        cases: tidb_cases,
    };
    let tiflash_case_results = CaseResultsArtifact {
        slice_id: tiflash::FIRST_SIGNED_WIDENING_ADD_INT64_SLICE_ID.to_string(),
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
    rendered.push_str("# First Signed-Widening Add Int64 Slice TiDB-vs-TiFlash Drift Report\n\n");
    rendered.push_str("Status: issue #434 harness checkpoint\n\n");
    rendered.push_str("Verified: 2026-03-22\n\n");
    rendered.push_str("## Evidence Source\n\n");
    rendered.push_str(
        "- this checkpoint runs the current TiDB and TiFlash signed-widening adapter cores through deterministic harness fixture runners\n",
    );
    rendered.push_str(
        "- live engine connection and orchestration remain out of scope for this artifact set\n",
    );
    rendered.push_str(
        "- the stable artifact-carrier boundary lives in `tests/differential/first-signed-widening-add-int64-slice-artifacts.md`\n\n",
    );
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

    for (index, case) in report.cases.iter().enumerate() {
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
        if index + 1 < report.cases.len() {
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
        slice_id: tidb::FIRST_SIGNED_WIDENING_ADD_INT64_SLICE_ID.to_string(),
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
            follow_up: Some(format!(
                "Implement the missing {} adapter-side path before treating `{}` as fully comparable.",
                tidb::TIDB_ENGINE,
                tidb_case.case_id
            )),
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
            follow_up: Some(format!(
                "Implement the missing {} adapter-side path before treating `{}` as fully comparable.",
                tiflash::TIFLASH_ENGINE,
                tidb_case.case_id
            )),
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
            if tidb_case.comparison_mode != tiflash_case.comparison_mode || tidb_rows != tiflash_rows {
                dimensions.push(ComparisonDimension::RowValues);
            }

            let status = if dimensions.is_empty() {
                DriftStatus::Match
            } else {
                DriftStatus::Drift
            };
            let comparison_dimensions = if dimensions.is_empty() {
                vec![
                    ComparisonDimension::FieldName,
                    ComparisonDimension::FieldNullability,
                    ComparisonDimension::LogicalType,
                    ComparisonDimension::RowCount,
                    ComparisonDimension::RowValues,
                ]
            } else {
                dimensions.clone()
            };
            let summary = if status == DriftStatus::Match {
                matching_rows_summary(
                    &tidb_case.case_id,
                    tidb_schema,
                    *tidb_row_count,
                    &tidb_case.comparison_mode,
                )
            } else {
                format!(
                    "TiDB and TiFlash disagree on {} for `{}` under `{}`.",
                    join_inline_codes(dimensions.iter().copied().map(comparison_dimension_name)),
                    tidb_case.case_id,
                    tidb_case.comparison_mode
                )
            };

            DriftCase {
                case_id: tidb_case.case_id.clone(),
                status,
                comparison_dimensions,
                summary,
                evidence_refs,
                follow_up: if status == DriftStatus::Drift {
                    Some(
                        "Reconcile the TiDB and TiFlash signed-widening row normalization so the shared first-slice carrier stays aligned."
                            .to_string(),
                    )
                } else {
                    None
                },
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
            let summary = if status == DriftStatus::Match {
                format!(
                    "TiDB and TiFlash both normalized `{}` as `{}`.",
                    tidb_case.case_id,
                    error_class_name(*tidb_error)
                )
            } else {
                format!(
                    "TiDB normalized `{}` as `{}` while TiFlash normalized it as `{}`.",
                    tidb_case.case_id,
                    error_class_name(*tidb_error),
                    error_class_name(*tiflash_error)
                )
            };

            DriftCase {
                case_id: tidb_case.case_id.clone(),
                status,
                comparison_dimensions: vec![ComparisonDimension::ErrorClass],
                summary,
                evidence_refs,
                follow_up: if status == DriftStatus::Drift {
                    Some(format!(
                        "Review whether `{}` should keep diverging as `{}` versus `{}` or whether one adapter should realign its normalization.",
                        tidb_case.case_id,
                        error_class_name(*tidb_error),
                        error_class_name(*tiflash_error)
                    ))
                } else {
                    None
                },
            }
        }
        (tidb_outcome, tiflash_outcome) => DriftCase {
            case_id: tidb_case.case_id.clone(),
            status: DriftStatus::Drift,
            comparison_dimensions: vec![ComparisonDimension::ErrorClass],
            summary: format!(
                "TiDB returned {} for `{}` while TiFlash returned {}.",
                outcome_kind_name(tidb_outcome),
                tidb_case.case_id,
                outcome_kind_name(tiflash_outcome)
            ),
            evidence_refs,
            follow_up: Some(
                "Decide whether the erroring side should align to the shared signed-widening case or remain explicitly unsupported.".to_string(),
            ),
        },
    }
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

fn unsupported_summary(
    left_engine: &str,
    left_error: ErrorClass,
    right_engine: &str,
    right_error: ErrorClass,
    case_id: &str,
) -> String {
    format!(
        "{} normalized `{case_id}` as `{}` while {} normalized it as `{}`; the pair remains explicitly unsupported.",
        left_engine,
        error_class_name(left_error),
        right_engine,
        error_class_name(right_error)
    )
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
            ErrorClass::ArithmeticOverflow => "`error/arithmetic_overflow`",
            ErrorClass::AdapterUnavailable => "`error/adapter_unavailable`",
            ErrorClass::EngineError => "`error/engine_error`",
        },
    }
}

fn error_class_name(error_class: ErrorClass) -> &'static str {
    match error_class {
        ErrorClass::MissingColumn => "missing_column",
        ErrorClass::ArithmeticOverflow => "arithmetic_overflow",
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
        tidb::ErrorClass::ArithmeticOverflow => ErrorClass::ArithmeticOverflow,
        tidb::ErrorClass::AdapterUnavailable => ErrorClass::AdapterUnavailable,
        tidb::ErrorClass::EngineError => ErrorClass::EngineError,
    }
}

fn convert_tiflash_error_class(error_class: tiflash::ErrorClass) -> ErrorClass {
    match error_class {
        tiflash::ErrorClass::MissingColumn => ErrorClass::MissingColumn,
        tiflash::ErrorClass::ArithmeticOverflow => ErrorClass::ArithmeticOverflow,
        tiflash::ErrorClass::AdapterUnavailable => ErrorClass::AdapterUnavailable,
        tiflash::ErrorClass::EngineError => ErrorClass::EngineError,
    }
}
