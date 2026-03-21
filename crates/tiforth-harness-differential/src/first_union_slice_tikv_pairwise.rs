use serde_json::Value;
use tiforth_adapter_tidb::first_union_slice as tidb;
use tiforth_adapter_tiflash::first_union_slice as tiflash;
use tiforth_adapter_tikv::first_union_slice as tikv;

use crate::first_union_slice::{
    self, execute_first_union_slice,
    render_drift_report_artifact_json as render_shared_drift_report_artifact_json, CaseOutcome,
    CaseResult, CaseResultsArtifact, ComparisonDimension, DriftCase, DriftReport, DriftStatus,
    ErrorClass, HarnessError as TidbTiflashHarnessError, SchemaField, TIDB_CASE_RESULTS_REF,
    TIFLASH_CASE_RESULTS_REF,
};
use crate::first_union_slice_tikv::{
    self, execute_first_union_slice_tikv, CaseResultsArtifact as TikvCaseResultsArtifact,
    HarnessError as TikvHarnessError, TIKV_CASE_RESULTS_REF,
};

pub const TIDB_VS_TIKV_DRIFT_REPORT_REF: &str =
    "inventory/first-union-slice-tidb-vs-tikv-drift-report.md";
pub const TIDB_VS_TIKV_DRIFT_REPORT_SIDECAR_REF: &str =
    "inventory/first-union-slice-tidb-vs-tikv-drift-report.json";
pub const TIFLASH_VS_TIKV_DRIFT_REPORT_REF: &str =
    "inventory/first-union-slice-tiflash-vs-tikv-drift-report.md";
pub const TIFLASH_VS_TIKV_DRIFT_REPORT_SIDECAR_REF: &str =
    "inventory/first-union-slice-tiflash-vs-tikv-drift-report.json";

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HarnessError {
    CanonicalRequestMismatch {
        shared_requests: Vec<CanonicalRequest>,
        tikv_requests: Vec<CanonicalRequest>,
    },
    TidbTiflashAdapterValidation {
        error: TidbTiflashHarnessError,
    },
    TikvAdapterValidation {
        error: TikvHarnessError,
    },
    CaseCountMismatch {
        left_engine: String,
        left_count: usize,
        right_engine: String,
        right_count: usize,
    },
    CaseIdentityMismatch {
        left_engine: String,
        right_engine: String,
        left_case_id: String,
        right_case_id: String,
        left_input_ref: String,
        right_input_ref: String,
        left_projection_ref: Option<String>,
        right_projection_ref: Option<String>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArtifactBundle {
    pub tikv_case_results: TikvCaseResultsArtifact,
    pub tidb_vs_tikv_drift_report: DriftReport,
    pub tiflash_vs_tikv_drift_report: DriftReport,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CanonicalRequest {
    pub slice_id: String,
    pub case_id: String,
    pub spec_refs: Vec<String>,
    pub input_ref: String,
    pub projection_ref: Option<String>,
}

pub fn canonical_requests() -> Result<Vec<CanonicalRequest>, HarnessError> {
    let shared_requests: Vec<CanonicalRequest> = first_union_slice::canonical_requests()
        .map_err(|error| HarnessError::TidbTiflashAdapterValidation { error })?
        .into_iter()
        .map(CanonicalRequest::from)
        .collect();
    let tikv_requests: Vec<CanonicalRequest> = first_union_slice_tikv::canonical_requests()
        .into_iter()
        .map(CanonicalRequest::from)
        .collect();

    if shared_requests != tikv_requests {
        return Err(HarnessError::CanonicalRequestMismatch {
            shared_requests,
            tikv_requests,
        });
    }

    Ok(shared_requests)
}

pub fn execute_first_union_slice_tikv_pairwise<T, F, K>(
    tidb_runner: &T,
    tiflash_runner: &F,
    tikv_runner: &K,
) -> Result<ArtifactBundle, HarnessError>
where
    T: tidb::TidbRunner,
    F: tiflash::TiflashRunner,
    K: tikv::TikvRunner,
{
    let _ = canonical_requests()?;

    let tidb_tiflash_bundle = execute_first_union_slice(tidb_runner, tiflash_runner)
        .map_err(|error| HarnessError::TidbTiflashAdapterValidation { error })?;
    let tikv_case_results = execute_first_union_slice_tikv(tikv_runner)
        .map_err(|error| HarnessError::TikvAdapterValidation { error })?;
    let comparable_tikv_case_results = convert_tikv_case_results(tikv_case_results.clone());

    let tidb_vs_tikv_drift_report = build_pairwise_drift_report(
        &tidb_tiflash_bundle.tidb_case_results,
        TIDB_CASE_RESULTS_REF,
        &comparable_tikv_case_results,
        TIKV_CASE_RESULTS_REF,
    )?;
    let tiflash_vs_tikv_drift_report = build_pairwise_drift_report(
        &tidb_tiflash_bundle.tiflash_case_results,
        TIFLASH_CASE_RESULTS_REF,
        &comparable_tikv_case_results,
        TIKV_CASE_RESULTS_REF,
    )?;

    Ok(ArtifactBundle {
        tikv_case_results,
        tidb_vs_tikv_drift_report,
        tiflash_vs_tikv_drift_report,
    })
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

    let (left_engine, right_engine) = engine_pair(report);
    let left_display = display_engine_name(left_engine);
    let right_display = display_engine_name(right_engine);

    let mut rendered = String::new();
    rendered.push_str("# First Union Slice ");
    rendered.push_str(&left_display);
    rendered.push_str("-vs-");
    rendered.push_str(&right_display);
    rendered.push_str(" Drift Report\n\n");
    rendered.push_str("Status: issue #368 harness checkpoint\n\n");
    rendered.push_str("Verified: 2026-03-21\n\n");
    rendered.push_str("## Evidence Source\n\n");
    rendered.push_str("- this checkpoint runs the current ");
    rendered.push_str(&left_display);
    rendered.push_str(" and ");
    rendered.push_str(&right_display);
    rendered.push_str(" adapter cores through deterministic harness fixture runners\n");
    rendered.push_str(
        "- live engine connection and orchestration remain out of scope for this artifact set\n",
    );
    rendered.push_str("- the stable artifact-carrier boundary lives in `tests/differential/first-union-slice-artifacts.md`\n\n");
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

pub fn render_drift_report_artifact_json(
    report: &DriftReport,
) -> Result<String, serde_json::Error> {
    render_shared_drift_report_artifact_json(report)
}

fn build_pairwise_drift_report(
    left_case_results: &CaseResultsArtifact,
    left_case_results_ref: &str,
    right_case_results: &CaseResultsArtifact,
    right_case_results_ref: &str,
) -> Result<DriftReport, HarnessError> {
    if left_case_results.cases.len() != right_case_results.cases.len() {
        return Err(HarnessError::CaseCountMismatch {
            left_engine: left_case_results.engine.clone(),
            left_count: left_case_results.cases.len(),
            right_engine: right_case_results.engine.clone(),
            right_count: right_case_results.cases.len(),
        });
    }

    let mut cases = Vec::with_capacity(left_case_results.cases.len());
    for (left_case, right_case) in left_case_results
        .cases
        .iter()
        .zip(&right_case_results.cases)
    {
        if left_case.case_id != right_case.case_id
            || left_case.input_ref != right_case.input_ref
            || left_case.projection_ref != right_case.projection_ref
        {
            return Err(HarnessError::CaseIdentityMismatch {
                left_engine: left_case_results.engine.clone(),
                right_engine: right_case_results.engine.clone(),
                left_case_id: left_case.case_id.clone(),
                right_case_id: right_case.case_id.clone(),
                left_input_ref: left_case.input_ref.clone(),
                right_input_ref: right_case.input_ref.clone(),
                left_projection_ref: left_case.projection_ref.clone(),
                right_projection_ref: right_case.projection_ref.clone(),
            });
        }

        cases.push(compare_case_results(
            left_case,
            right_case,
            left_case_results_ref,
            right_case_results_ref,
        ));
    }

    let spec_refs = left_case_results
        .cases
        .first()
        .map(|case| case.spec_refs.clone())
        .unwrap_or_default();

    Ok(DriftReport {
        slice_id: tidb::FIRST_UNION_SLICE_ID.to_string(),
        engines: vec![
            left_case_results.engine.clone(),
            right_case_results.engine.clone(),
        ],
        spec_refs,
        cases,
    })
}

fn compare_case_results(
    left_case: &CaseResult,
    right_case: &CaseResult,
    left_case_results_ref: &str,
    right_case_results_ref: &str,
) -> DriftCase {
    let evidence_refs = vec![
        format!("{left_case_results_ref}#{}", left_case.case_id),
        format!("{right_case_results_ref}#{}", right_case.case_id),
    ];

    let left_engine = left_case.engine.as_str();
    let right_engine = right_case.engine.as_str();

    match (&left_case.outcome, &right_case.outcome) {
        (
            CaseOutcome::Error {
                error_class: ErrorClass::AdapterUnavailable,
                ..
            },
            other,
        ) => DriftCase {
            case_id: left_case.case_id.clone(),
            status: DriftStatus::Unsupported,
            comparison_dimensions: vec![ComparisonDimension::ErrorClass],
            summary: unsupported_summary(
                left_engine,
                ErrorClass::AdapterUnavailable,
                right_engine,
                outcome_error_class(other),
                &left_case.case_id,
            ),
            evidence_refs,
            follow_up: unsupported_follow_up(left_engine, &left_case.case_id),
        },
        (
            other,
            CaseOutcome::Error {
                error_class: ErrorClass::AdapterUnavailable,
                ..
            },
        ) => DriftCase {
            case_id: left_case.case_id.clone(),
            status: DriftStatus::Unsupported,
            comparison_dimensions: vec![ComparisonDimension::ErrorClass],
            summary: unsupported_summary(
                left_engine,
                outcome_error_class(other),
                right_engine,
                ErrorClass::AdapterUnavailable,
                &left_case.case_id,
            ),
            evidence_refs,
            follow_up: unsupported_follow_up(right_engine, &left_case.case_id),
        },
        (
            CaseOutcome::Rows {
                schema: left_schema,
                rows: left_rows,
                row_count: left_row_count,
            },
            CaseOutcome::Rows {
                schema: right_schema,
                rows: right_rows,
                row_count: right_row_count,
            },
        ) => {
            let drift_dimensions = row_drift_dimensions(
                left_schema,
                right_schema,
                left_rows,
                right_rows,
                *left_row_count,
                *right_row_count,
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
                case_id: left_case.case_id.clone(),
                status,
                comparison_dimensions: comparison_dimensions.clone(),
                summary: if status == DriftStatus::Match {
                    matching_rows_summary(
                        left_engine,
                        right_engine,
                        &left_case.case_id,
                        left_schema,
                        *left_row_count,
                    )
                } else {
                    drifting_rows_summary(
                        left_engine,
                        right_engine,
                        &left_case.case_id,
                        &comparison_dimensions,
                    )
                },
                evidence_refs,
                follow_up: if status == DriftStatus::Drift {
                    Some(
                        "Align shared row-level normalization policy across both engines for this case."
                            .to_string(),
                    )
                } else {
                    None
                },
            }
        }
        (
            CaseOutcome::Error {
                error_class: left_error,
                ..
            },
            CaseOutcome::Error {
                error_class: right_error,
                ..
            },
        ) => {
            let status = if left_error == right_error {
                DriftStatus::Match
            } else {
                DriftStatus::Drift
            };

            DriftCase {
                case_id: left_case.case_id.clone(),
                status,
                comparison_dimensions: vec![ComparisonDimension::ErrorClass],
                summary: if status == DriftStatus::Match {
                    matching_error_summary(left_engine, right_engine, &left_case.case_id, *left_error)
                } else {
                    drifting_error_summary(
                        left_engine,
                        right_engine,
                        &left_case.case_id,
                        *left_error,
                        *right_error,
                    )
                },
                evidence_refs,
                follow_up: if status == DriftStatus::Drift {
                    Some(
                        "Align shared error-class normalization policy across both engines for this case."
                            .to_string(),
                    )
                } else {
                    None
                },
            }
        }
        (left_outcome, right_outcome) => DriftCase {
            case_id: left_case.case_id.clone(),
            status: DriftStatus::Drift,
            comparison_dimensions: vec![ComparisonDimension::ErrorClass],
            summary: mixed_outcome_summary(
                left_engine,
                right_engine,
                &left_case.case_id,
                left_outcome,
                right_outcome,
            ),
            evidence_refs,
            follow_up: Some(
                "Decide whether the erroring side should align to the shared case or remain explicitly unsupported."
                    .to_string(),
            ),
        },
    }
}

fn row_drift_dimensions(
    left_schema: &[SchemaField],
    right_schema: &[SchemaField],
    left_rows: &[Vec<Value>],
    right_rows: &[Vec<Value>],
    left_row_count: usize,
    right_row_count: usize,
) -> Vec<ComparisonDimension> {
    let mut dimensions = Vec::new();

    if schema_names(left_schema) != schema_names(right_schema) {
        dimensions.push(ComparisonDimension::FieldName);
    }
    if schema_nullability(left_schema) != schema_nullability(right_schema) {
        dimensions.push(ComparisonDimension::FieldNullability);
    }
    if schema_logical_types(left_schema) != schema_logical_types(right_schema) {
        dimensions.push(ComparisonDimension::LogicalType);
    }
    if left_row_count != right_row_count {
        dimensions.push(ComparisonDimension::RowCount);
    }
    if left_rows != right_rows {
        dimensions.push(ComparisonDimension::RowValues);
    }

    dimensions
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
    left_engine: &str,
    right_engine: &str,
    case_id: &str,
    schema: &[SchemaField],
    row_count: usize,
) -> String {
    format!(
        "{left_engine} and {right_engine} both returned matching rows for `{case_id}` ({row_count} row(s), schema [{}]).",
        join_schema_fields(schema)
    )
}

fn drifting_rows_summary(
    left_engine: &str,
    right_engine: &str,
    case_id: &str,
    dimensions: &[ComparisonDimension],
) -> String {
    format!(
        "{left_engine} and {right_engine} drifted on `{case_id}` across {}.",
        join_inline_codes(dimensions.iter().copied().map(comparison_dimension_name))
    )
}

fn matching_error_summary(
    left_engine: &str,
    right_engine: &str,
    case_id: &str,
    error_class: ErrorClass,
) -> String {
    format!(
        "{left_engine} and {right_engine} both normalized `{case_id}` to `{}`.",
        error_class_name(error_class)
    )
}

fn drifting_error_summary(
    left_engine: &str,
    right_engine: &str,
    case_id: &str,
    left_error: ErrorClass,
    right_error: ErrorClass,
) -> String {
    format!(
        "{left_engine} normalized `{case_id}` to `{}` while {right_engine} normalized it to `{}`.",
        error_class_name(left_error),
        error_class_name(right_error)
    )
}

fn mixed_outcome_summary(
    left_engine: &str,
    right_engine: &str,
    case_id: &str,
    left_outcome: &CaseOutcome,
    right_outcome: &CaseOutcome,
) -> String {
    format!(
        "{left_engine} produced `{}` while {right_engine} produced `{}` for `{case_id}`.",
        outcome_kind_name(left_outcome),
        outcome_kind_name(right_outcome)
    )
}

fn unsupported_summary(
    left_engine: &str,
    left_error: ErrorClass,
    right_engine: &str,
    right_error: ErrorClass,
    case_id: &str,
) -> String {
    format!(
        "{left_engine} reports `{}` while {right_engine} reports `{}` for `{case_id}`; treat as unsupported until adapter coverage is complete.",
        error_class_name(left_error),
        error_class_name(right_error)
    )
}

fn unsupported_follow_up(engine: &str, case_id: &str) -> Option<String> {
    Some(format!(
        "Add or align `{engine}` adapter coverage for `{case_id}` to eliminate `adapter_unavailable` status."
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
        CaseOutcome::Rows { .. } => "rows",
        CaseOutcome::Error { .. } => "error",
    }
}

fn join_schema_fields(schema: &[SchemaField]) -> String {
    schema
        .iter()
        .map(|field| {
            format!(
                "{}:{}{}",
                field.name,
                field.logical_type,
                if field.nullable { "?" } else { "" }
            )
        })
        .collect::<Vec<_>>()
        .join(", ")
}

fn engine_pair(report: &DriftReport) -> (&str, &str) {
    let left = report
        .engines
        .first()
        .map(String::as_str)
        .unwrap_or("unknown");
    let right = report
        .engines
        .get(1)
        .map(String::as_str)
        .unwrap_or("unknown");
    (left, right)
}

fn display_engine_name(engine: &str) -> String {
    match engine {
        "tidb" => "TiDB".to_string(),
        "tiflash" => "TiFlash".to_string(),
        "tikv" => "TiKV".to_string(),
        other => other.to_string(),
    }
}

fn error_class_name(error_class: ErrorClass) -> &'static str {
    match error_class {
        ErrorClass::MissingColumn => "missing_column",
        ErrorClass::UnsupportedNestedFamily => "unsupported_nested_family",
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

impl From<first_union_slice::CanonicalRequest> for CanonicalRequest {
    fn from(value: first_union_slice::CanonicalRequest) -> Self {
        Self {
            slice_id: value.slice_id,
            case_id: value.case_id,
            spec_refs: value.spec_refs,
            input_ref: value.input_ref,
            projection_ref: value.projection_ref,
        }
    }
}

impl From<tikv::AdapterRequest> for CanonicalRequest {
    fn from(value: tikv::AdapterRequest) -> Self {
        Self {
            slice_id: value.slice_id,
            case_id: value.case_id,
            spec_refs: value.spec_refs,
            input_ref: value.input_ref,
            projection_ref: value.projection_ref,
        }
    }
}

fn convert_tikv_case_results(value: TikvCaseResultsArtifact) -> CaseResultsArtifact {
    CaseResultsArtifact {
        slice_id: value.slice_id,
        engine: value.engine,
        adapter: value.adapter,
        cases: value
            .cases
            .into_iter()
            .map(convert_tikv_case_result)
            .collect(),
    }
}

fn convert_tikv_case_result(value: tikv::CaseResult) -> CaseResult {
    CaseResult {
        slice_id: value.slice_id,
        engine: value.engine,
        adapter: value.adapter,
        case_id: value.case_id,
        spec_refs: value.spec_refs,
        input_ref: value.input_ref,
        projection_ref: value.projection_ref,
        outcome: convert_tikv_outcome(value.outcome),
    }
}

fn convert_tikv_outcome(outcome: tikv::CaseOutcome) -> CaseOutcome {
    match outcome {
        tikv::CaseOutcome::Rows {
            schema,
            rows,
            row_count,
        } => CaseOutcome::Rows {
            schema: schema.into_iter().map(convert_tikv_schema_field).collect(),
            rows,
            row_count,
        },
        tikv::CaseOutcome::Error {
            error_class,
            engine_code,
            engine_message,
        } => CaseOutcome::Error {
            error_class: convert_tikv_error_class(error_class),
            engine_code,
            engine_message,
        },
    }
}

fn convert_tikv_schema_field(field: tikv::SchemaField) -> SchemaField {
    SchemaField {
        name: field.name,
        logical_type: field.logical_type,
        nullable: field.nullable,
    }
}

fn convert_tikv_error_class(error_class: tikv::ErrorClass) -> ErrorClass {
    match error_class {
        tikv::ErrorClass::MissingColumn => ErrorClass::MissingColumn,
        tikv::ErrorClass::UnsupportedNestedFamily => ErrorClass::UnsupportedNestedFamily,
        tikv::ErrorClass::AdapterUnavailable => ErrorClass::AdapterUnavailable,
        tikv::ErrorClass::EngineError => ErrorClass::EngineError,
    }
}

#[cfg(test)]
mod tests {
    use serde_json::{json, Value};

    use super::*;

    const TIDB_VS_TIKV_DRIFT_REPORT_PATH: &str = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../inventory/first-union-slice-tidb-vs-tikv-drift-report.md"
    );
    const TIDB_VS_TIKV_DRIFT_REPORT_SIDECAR_PATH: &str = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../inventory/first-union-slice-tidb-vs-tikv-drift-report.json"
    );
    const TIFLASH_VS_TIKV_DRIFT_REPORT_PATH: &str = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../inventory/first-union-slice-tiflash-vs-tikv-drift-report.md"
    );
    const TIFLASH_VS_TIKV_DRIFT_REPORT_SIDECAR_PATH: &str = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../inventory/first-union-slice-tiflash-vs-tikv-drift-report.json"
    );

    #[test]
    fn canonical_requests_match_across_shared_and_tikv_cores() {
        let requests = canonical_requests().unwrap();
        let case_ids: Vec<&str> = requests
            .iter()
            .map(|request| request.case_id.as_str())
            .collect();

        assert_eq!(requests.len(), 5);
        assert_eq!(
            case_ids,
            vec![
                "union-column-passthrough",
                "union-variant-switch-preserve",
                "union-variant-null-preserve",
                "union-missing-column-error",
                "unsupported-nested-family-error",
            ]
        );

        for request in requests {
            assert!(request.projection_ref.is_some());
        }
    }

    #[test]
    fn checked_in_artifacts_match_fixture_harness_output() {
        let bundle = execute_first_union_slice_tikv_pairwise(
            &FixtureTidbRunner,
            &FixtureTiflashRunner,
            &FixtureTikvRunner,
        )
        .unwrap();

        assert_eq!(
            render_drift_report_markdown(&bundle.tidb_vs_tikv_drift_report),
            std::fs::read_to_string(TIDB_VS_TIKV_DRIFT_REPORT_PATH).unwrap()
        );
        assert_eq!(
            render_drift_report_artifact_json(&bundle.tidb_vs_tikv_drift_report).unwrap(),
            std::fs::read_to_string(TIDB_VS_TIKV_DRIFT_REPORT_SIDECAR_PATH).unwrap()
        );
        assert_eq!(
            render_drift_report_markdown(&bundle.tiflash_vs_tikv_drift_report),
            std::fs::read_to_string(TIFLASH_VS_TIKV_DRIFT_REPORT_PATH).unwrap()
        );
        assert_eq!(
            render_drift_report_artifact_json(&bundle.tiflash_vs_tikv_drift_report).unwrap(),
            std::fs::read_to_string(TIFLASH_VS_TIKV_DRIFT_REPORT_SIDECAR_PATH).unwrap()
        );
    }

    #[test]
    fn compare_case_results_marks_row_value_drift() {
        let left = CaseResult {
            slice_id: tidb::FIRST_UNION_SLICE_ID.to_string(),
            engine: tidb::TIDB_ENGINE.to_string(),
            adapter: tidb::TIDB_ADAPTER.to_string(),
            case_id: "union-column-passthrough".to_string(),
            spec_refs: vec!["tests/differential/first-union-slice.md".to_string()],
            input_ref: "first-union-basic".to_string(),
            projection_ref: Some("column-0".to_string()),
            outcome: CaseOutcome::Rows {
                schema: vec![SchemaField {
                    name: "u".to_string(),
                    logical_type: "dense_union<i:int32,n:int32?>".to_string(),
                    nullable: false,
                }],
                rows: vec![
                    vec![json!({"tag": "i", "value": 1})],
                    vec![json!({"tag": "n", "value": 2})],
                ],
                row_count: 2,
            },
        };
        let right = CaseResult {
            engine: tikv::TIKV_ENGINE.to_string(),
            adapter: tikv::TIKV_ADAPTER.to_string(),
            outcome: CaseOutcome::Rows {
                schema: vec![SchemaField {
                    name: "u".to_string(),
                    logical_type: "dense_union<i:int32,n:int32?>".to_string(),
                    nullable: false,
                }],
                rows: vec![
                    vec![json!({"tag": "i", "value": 1})],
                    vec![json!({"tag": "n", "value": 999})],
                ],
                row_count: 2,
            },
            ..left.clone()
        };

        let drift = compare_case_results(&left, &right, "left", "right");

        assert_eq!(drift.status, DriftStatus::Drift);
        assert_eq!(
            drift.comparison_dimensions,
            vec![ComparisonDimension::RowValues]
        );
    }

    struct FixtureTidbRunner;

    impl tidb::TidbRunner for FixtureTidbRunner {
        fn run(
            &self,
            plan: &tidb::TidbExecutionPlan,
        ) -> Result<tidb::EngineExecutionResult, tidb::EngineExecutionError> {
            match plan.request.case_id.as_str() {
                "union-column-passthrough" => Ok(rows_result_tidb(
                    vec![tidb::EngineColumn {
                        name: "u".to_string(),
                        engine_type: "dense_union<i:int32,n:int32?>".to_string(),
                        nullable: false,
                    }],
                    vec![
                        vec![json!({"tag": "i", "value": 1})],
                        vec![json!({"tag": "n", "value": 2})],
                        vec![json!({"tag": "i", "value": 3})],
                    ],
                )),
                "union-variant-switch-preserve" => Ok(rows_result_tidb(
                    vec![tidb::EngineColumn {
                        name: "u".to_string(),
                        engine_type: "dense_union<i:int32,n:int32?>".to_string(),
                        nullable: false,
                    }],
                    vec![
                        vec![json!({"tag": "i", "value": 1})],
                        vec![json!({"tag": "n", "value": 2})],
                        vec![json!({"tag": "i", "value": 3})],
                    ],
                )),
                "union-variant-null-preserve" => Ok(rows_result_tidb(
                    vec![tidb::EngineColumn {
                        name: "u".to_string(),
                        engine_type: "dense_union<i:int32,n:int32?>".to_string(),
                        nullable: false,
                    }],
                    vec![
                        vec![json!({"tag": "n", "value": Value::Null})],
                        vec![json!({"tag": "i", "value": 4})],
                        vec![json!({"tag": "n", "value": 5})],
                    ],
                )),
                "union-missing-column-error" => Err(tidb::EngineExecutionError::EngineFailure {
                    code: Some("1054".to_string()),
                    message: "Unknown column '__missing_column_1' in 'field list'".to_string(),
                }),
                "unsupported-nested-family-error" => {
                    Err(tidb::EngineExecutionError::EngineFailure {
                        code: Some("1105".to_string()),
                        message: "unsupported nested expression input at column 0".to_string(),
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
                "union-column-passthrough" => Ok(rows_result_tiflash(
                    vec![tiflash::EngineColumn {
                        name: "u".to_string(),
                        engine_type: "dense_union<i:int32,n:int32?>".to_string(),
                        nullable: false,
                    }],
                    vec![
                        vec![json!({"tag": "i", "value": 1})],
                        vec![json!({"tag": "n", "value": 2})],
                        vec![json!({"tag": "i", "value": 3})],
                    ],
                )),
                "union-variant-switch-preserve" => Ok(rows_result_tiflash(
                    vec![tiflash::EngineColumn {
                        name: "u".to_string(),
                        engine_type: "dense_union<i:int32,n:int32?>".to_string(),
                        nullable: false,
                    }],
                    vec![
                        vec![json!({"tag": "i", "value": 1})],
                        vec![json!({"tag": "n", "value": 2})],
                        vec![json!({"tag": "i", "value": 3})],
                    ],
                )),
                "union-variant-null-preserve" => Ok(rows_result_tiflash(
                    vec![tiflash::EngineColumn {
                        name: "u".to_string(),
                        engine_type: "dense_union<i:int32,n:int32?>".to_string(),
                        nullable: false,
                    }],
                    vec![
                        vec![json!({"tag": "n", "value": Value::Null})],
                        vec![json!({"tag": "i", "value": 4})],
                        vec![json!({"tag": "n", "value": 5})],
                    ],
                )),
                "union-missing-column-error" => Err(tiflash::EngineExecutionError::EngineFailure {
                    code: Some("1054".to_string()),
                    message: "Unknown column '__missing_column_1' in 'field list'".to_string(),
                }),
                "unsupported-nested-family-error" => {
                    Err(tiflash::EngineExecutionError::EngineFailure {
                        code: Some("1105".to_string()),
                        message: "unsupported nested expression input at column 0".to_string(),
                    })
                }
                other => panic!("unexpected case_id: {other}"),
            }
        }
    }

    struct FixtureTikvRunner;

    impl tikv::TikvRunner for FixtureTikvRunner {
        fn run(
            &self,
            plan: &tikv::TikvExecutionPlan,
        ) -> Result<tikv::EngineExecutionResult, tikv::EngineExecutionError> {
            match plan.request.case_id.as_str() {
                "union-column-passthrough" => Ok(rows_result_tikv(
                    vec![tikv::EngineColumn {
                        name: "u".to_string(),
                        engine_type: "dense_union<i:int32,n:int32?>".to_string(),
                        nullable: false,
                    }],
                    vec![
                        vec![json!({"tag": "i", "value": 1})],
                        vec![json!({"tag": "n", "value": 2})],
                        vec![json!({"tag": "i", "value": 3})],
                    ],
                )),
                "union-variant-switch-preserve" => Ok(rows_result_tikv(
                    vec![tikv::EngineColumn {
                        name: "u".to_string(),
                        engine_type: "dense_union<i:int32,n:int32?>".to_string(),
                        nullable: false,
                    }],
                    vec![
                        vec![json!({"tag": "i", "value": 1})],
                        vec![json!({"tag": "n", "value": 2})],
                        vec![json!({"tag": "i", "value": 3})],
                    ],
                )),
                "union-variant-null-preserve" => Ok(rows_result_tikv(
                    vec![tikv::EngineColumn {
                        name: "u".to_string(),
                        engine_type: "dense_union<i:int32,n:int32?>".to_string(),
                        nullable: false,
                    }],
                    vec![
                        vec![json!({"tag": "n", "value": Value::Null})],
                        vec![json!({"tag": "i", "value": 4})],
                        vec![json!({"tag": "n", "value": 5})],
                    ],
                )),
                "union-missing-column-error" => Err(tikv::EngineExecutionError::EngineFailure {
                    code: Some("1054".to_string()),
                    message: "Unknown column '__missing_column_1' in 'field list'".to_string(),
                }),
                "unsupported-nested-family-error" => {
                    Err(tikv::EngineExecutionError::EngineFailure {
                        code: Some("1105".to_string()),
                        message: "unsupported nested expression input at column 0".to_string(),
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

    fn rows_result_tikv(
        columns: Vec<tikv::EngineColumn>,
        rows: Vec<Vec<Value>>,
    ) -> tikv::EngineExecutionResult {
        tikv::EngineExecutionResult { columns, rows }
    }
}
