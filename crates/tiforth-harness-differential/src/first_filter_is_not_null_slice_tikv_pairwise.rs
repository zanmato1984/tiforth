use serde_json::Value;
use tiforth_adapter_tidb::first_filter_is_not_null_slice as tidb;
use tiforth_adapter_tiflash::first_filter_is_not_null_slice as tiflash;
use tiforth_adapter_tikv::first_filter_is_not_null_slice as tikv;

use crate::slices::first_filter_is_not_null_slice::{
    self, execute_first_filter_is_not_null_slice,
    render_drift_report_artifact_json as render_shared_drift_report_artifact_json, CaseOutcome,
    CaseResult, CaseResultsArtifact, ComparisonDimension, DriftCase, DriftReport, DriftStatus,
    ErrorClass, HarnessError as TidbTiflashHarnessError, SchemaField, TIDB_CASE_RESULTS_REF,
    TIFLASH_CASE_RESULTS_REF,
};
use crate::tikv::first_filter_is_not_null_slice_tikv::{
    self, execute_first_filter_is_not_null_slice_tikv,
    CaseResultsArtifact as TikvCaseResultsArtifact, HarnessError as TikvHarnessError,
    TIKV_CASE_RESULTS_REF,
};

pub const TIDB_VS_TIKV_DRIFT_REPORT_REF: &str =
    "inventory/first-filter-is-not-null-slice-tidb-vs-tikv-drift-report.md";
pub const TIDB_VS_TIKV_DRIFT_REPORT_SIDECAR_REF: &str =
    "inventory/first-filter-is-not-null-slice-tidb-vs-tikv-drift-report.json";
pub const TIFLASH_VS_TIKV_DRIFT_REPORT_REF: &str =
    "inventory/first-filter-is-not-null-slice-tiflash-vs-tikv-drift-report.md";
pub const TIFLASH_VS_TIKV_DRIFT_REPORT_SIDECAR_REF: &str =
    "inventory/first-filter-is-not-null-slice-tiflash-vs-tikv-drift-report.json";

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
        left_filter_ref: String,
        right_filter_ref: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArtifactBundle {
    pub tidb_vs_tikv_drift_report: DriftReport,
    pub tiflash_vs_tikv_drift_report: DriftReport,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CanonicalRequest {
    pub slice_id: String,
    pub case_id: String,
    pub spec_refs: Vec<String>,
    pub input_ref: String,
    pub filter_ref: String,
}

pub fn canonical_requests() -> Result<Vec<CanonicalRequest>, HarnessError> {
    let shared_requests: Vec<CanonicalRequest> =
        first_filter_is_not_null_slice::canonical_requests()
            .map_err(|error| HarnessError::TidbTiflashAdapterValidation { error })?
            .into_iter()
            .map(CanonicalRequest::from)
            .collect();
    let tikv_requests: Vec<CanonicalRequest> =
        first_filter_is_not_null_slice_tikv::canonical_requests()
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

pub fn execute_first_filter_is_not_null_slice_tikv_pairwise<T, F, K>(
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

    let tidb_tiflash_bundle =
        execute_first_filter_is_not_null_slice(tidb_runner, tiflash_runner)
            .map_err(|error| HarnessError::TidbTiflashAdapterValidation { error })?;
    let tikv_case_results = execute_first_filter_is_not_null_slice_tikv(tikv_runner)
        .map_err(|error| HarnessError::TikvAdapterValidation { error })?;
    let tikv_case_results = convert_tikv_case_results(tikv_case_results);

    let tidb_vs_tikv_drift_report = build_pairwise_drift_report(
        &tidb_tiflash_bundle.tidb_case_results,
        TIDB_CASE_RESULTS_REF,
        &tikv_case_results,
        TIKV_CASE_RESULTS_REF,
    )?;
    let tiflash_vs_tikv_drift_report = build_pairwise_drift_report(
        &tidb_tiflash_bundle.tiflash_case_results,
        TIFLASH_CASE_RESULTS_REF,
        &tikv_case_results,
        TIKV_CASE_RESULTS_REF,
    )?;

    Ok(ArtifactBundle {
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
    rendered.push_str("# First Filter Slice ");
    rendered.push_str(&left_display);
    rendered.push_str("-vs-");
    rendered.push_str(&right_display);
    rendered.push_str(" Drift Report\n\n");
    rendered.push_str("Status: issue #249 follow-on harness checkpoint\n\n");
    rendered.push_str("Verified: 2026-03-20\n\n");
    rendered.push_str("## Evidence Source\n\n");
    rendered.push_str("- this checkpoint runs the current ");
    rendered.push_str(&left_display);
    rendered.push_str(" and ");
    rendered.push_str(&right_display);
    rendered.push_str(" adapter cores through deterministic harness fixture runners\n");
    rendered.push_str(
        "- live engine connection and orchestration remain out of scope for this artifact set\n",
    );
    rendered.push_str("- the stable artifact-carrier boundary lives in `tests/differential/first-filter-is-not-null-slice-artifacts.md`\n\n");
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
            || left_case.filter_ref != right_case.filter_ref
        {
            return Err(HarnessError::CaseIdentityMismatch {
                left_engine: left_case_results.engine.clone(),
                right_engine: right_case_results.engine.clone(),
                left_case_id: left_case.case_id.clone(),
                right_case_id: right_case.case_id.clone(),
                left_input_ref: left_case.input_ref.clone(),
                right_input_ref: right_case.input_ref.clone(),
                left_filter_ref: left_case.filter_ref.clone(),
                right_filter_ref: right_case.filter_ref.clone(),
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
        slice_id: tidb::FIRST_FILTER_IS_NOT_NULL_SLICE_ID.to_string(),
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
                follow_up: None,
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
                follow_up: drifting_error_follow_up(&left_case.case_id, *left_error, *right_error),
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
    let left_display = display_engine_name(left_engine);
    let right_display = display_engine_name(right_engine);
    match schema {
        [field] => format!(
            "{left_display} and {right_display} both returned {row_count} row(s) for `{case_id}` with field `{}` normalized as `{}`.",
            field.name, field.logical_type
        ),
        _ => format!(
            "{left_display} and {right_display} both returned matching row output for `{case_id}`."
        ),
    }
}

fn drifting_rows_summary(
    left_engine: &str,
    right_engine: &str,
    case_id: &str,
    dimensions: &[ComparisonDimension],
) -> String {
    let left_display = display_engine_name(left_engine);
    let right_display = display_engine_name(right_engine);
    format!(
        "{left_display} and {right_display} disagree on {} for `{case_id}`.",
        join_inline_codes(dimensions.iter().copied().map(comparison_dimension_name))
    )
}

fn matching_error_summary(
    left_engine: &str,
    right_engine: &str,
    case_id: &str,
    error_class: ErrorClass,
) -> String {
    let left_display = display_engine_name(left_engine);
    let right_display = display_engine_name(right_engine);
    format!(
        "{left_display} and {right_display} both normalized `{case_id}` as `{}`.",
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
    let left_display = display_engine_name(left_engine);
    let right_display = display_engine_name(right_engine);
    format!(
        "{left_display} normalized `{case_id}` as `{}` while {right_display} normalized it as `{}`.",
        error_class_name(left_error),
        error_class_name(right_error)
    )
}

fn drifting_error_follow_up(
    case_id: &str,
    left_error: ErrorClass,
    right_error: ErrorClass,
) -> Option<String> {
    if left_error != right_error {
        Some(format!(
            "Review whether `{case_id}` should keep diverging as `{}` versus `{}` or whether one adapter should realign its normalization.",
            error_class_name(left_error),
            error_class_name(right_error)
        ))
    } else {
        None
    }
}

fn mixed_outcome_summary(
    left_engine: &str,
    right_engine: &str,
    case_id: &str,
    left_outcome: &CaseOutcome,
    right_outcome: &CaseOutcome,
) -> String {
    let left_display = display_engine_name(left_engine);
    let right_display = display_engine_name(right_engine);
    format!(
        "{left_display} returned {} for `{case_id}` while {right_display} returned {}.",
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
        "{left_engine} normalized `{case_id}` as `{}` while {right_engine} normalized it as `{}`; the pair remains explicitly unsupported.",
        error_class_name(left_error),
        error_class_name(right_error)
    )
}

fn unsupported_follow_up(engine: &str, case_id: &str) -> Option<String> {
    if case_id == "unsupported-predicate-type-error" {
        Some(format!(
            "Decide whether {engine} should normalize `{case_id}` as `unsupported_predicate_type` or remain explicitly unsupported."
        ))
    } else {
        Some(format!(
            "Implement the missing {engine} adapter-side path before treating `{case_id}` as fully comparable."
        ))
    }
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
            ErrorClass::UnsupportedPredicateType => "`error/unsupported_predicate_type`",
            ErrorClass::AdapterUnavailable => "`error/adapter_unavailable`",
            ErrorClass::EngineError => "`error/engine_error`",
        },
    }
}

fn error_class_name(error_class: ErrorClass) -> &'static str {
    match error_class {
        ErrorClass::MissingColumn => "missing_column",
        ErrorClass::UnsupportedPredicateType => "unsupported_predicate_type",
        ErrorClass::AdapterUnavailable => "adapter_unavailable",
        ErrorClass::EngineError => "engine_error",
    }
}

fn display_engine_name(engine: &str) -> String {
    match engine {
        tidb::TIDB_ENGINE => "TiDB".to_string(),
        tiflash::TIFLASH_ENGINE => "TiFlash".to_string(),
        tikv::TIKV_ENGINE => "TiKV".to_string(),
        _ => engine.to_string(),
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

fn engine_pair(report: &DriftReport) -> (&str, &str) {
    let left = report.engines.first().map(String::as_str).unwrap_or("");
    let right = report.engines.get(1).map(String::as_str).unwrap_or("");
    (left, right)
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
        filter_ref: value.filter_ref,
        outcome: convert_tikv_case_outcome(value.outcome),
    }
}

fn convert_tikv_case_outcome(outcome: tikv::CaseOutcome) -> CaseOutcome {
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
        tikv::ErrorClass::UnsupportedPredicateType => ErrorClass::UnsupportedPredicateType,
        tikv::ErrorClass::AdapterUnavailable => ErrorClass::AdapterUnavailable,
        tikv::ErrorClass::EngineError => ErrorClass::EngineError,
    }
}

impl From<first_filter_is_not_null_slice::CanonicalRequest> for CanonicalRequest {
    fn from(value: first_filter_is_not_null_slice::CanonicalRequest) -> Self {
        Self {
            slice_id: value.slice_id,
            case_id: value.case_id,
            spec_refs: value.spec_refs,
            input_ref: value.input_ref,
            filter_ref: value.filter_ref,
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
            filter_ref: value.filter_ref,
        }
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    const TIDB_VS_TIKV_DRIFT_REPORT_PATH: &str = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../inventory/first-filter-is-not-null-slice-tidb-vs-tikv-drift-report.md"
    );
    const TIDB_VS_TIKV_DRIFT_REPORT_SIDECAR_PATH: &str = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../inventory/first-filter-is-not-null-slice-tidb-vs-tikv-drift-report.json"
    );
    const TIFLASH_VS_TIKV_DRIFT_REPORT_PATH: &str = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../inventory/first-filter-is-not-null-slice-tiflash-vs-tikv-drift-report.md"
    );
    const TIFLASH_VS_TIKV_DRIFT_REPORT_SIDECAR_PATH: &str = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../inventory/first-filter-is-not-null-slice-tiflash-vs-tikv-drift-report.json"
    );

    #[test]
    fn canonical_requests_match_across_shared_and_tikv_cores() {
        let requests = canonical_requests().unwrap();
        let case_ids: Vec<&str> = requests
            .iter()
            .map(|request| request.case_id.as_str())
            .collect();

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
    }

    #[test]
    fn checked_in_artifacts_match_fixture_harness_output() {
        let bundle = execute_first_filter_is_not_null_slice_tikv_pairwise(
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
    fn compare_case_results_marks_row_differences_as_drift() {
        let left_case = CaseResult {
            slice_id: tidb::FIRST_FILTER_IS_NOT_NULL_SLICE_ID.to_string(),
            engine: tidb::TIDB_ENGINE.to_string(),
            adapter: tidb::TIDB_ADAPTER.to_string(),
            case_id: "all-rows-kept".to_string(),
            spec_refs: vec!["tests/differential/first-filter-is-not-null-slice.md".to_string()],
            input_ref: "first-filter-is-not-null-int32-basic".to_string(),
            filter_ref: "is-not-null-column-0".to_string(),
            outcome: CaseOutcome::Rows {
                schema: vec![SchemaField {
                    name: "a".to_string(),
                    logical_type: "int32".to_string(),
                    nullable: false,
                }],
                rows: vec![vec![json!(1)], vec![json!(2)], vec![json!(3)]],
                row_count: 3,
            },
        };
        let right_case = CaseResult {
            outcome: CaseOutcome::Rows {
                schema: vec![SchemaField {
                    name: "a".to_string(),
                    logical_type: "int32".to_string(),
                    nullable: false,
                }],
                rows: vec![vec![json!(1)], vec![json!(99)], vec![json!(3)]],
                row_count: 3,
            },
            ..left_case
                .clone()
                .with_engine(tikv::TIKV_ENGINE, tikv::TIKV_ADAPTER)
        };

        let comparison = compare_case_results(
            &left_case,
            &right_case,
            TIDB_CASE_RESULTS_REF,
            TIKV_CASE_RESULTS_REF,
        );

        assert_eq!(comparison.status, DriftStatus::Drift);
        assert_eq!(
            comparison.comparison_dimensions,
            vec![ComparisonDimension::RowValues]
        );
    }

    #[test]
    fn compare_case_results_marks_adapter_unavailable_as_unsupported() {
        let left_case = CaseResult {
            slice_id: tidb::FIRST_FILTER_IS_NOT_NULL_SLICE_ID.to_string(),
            engine: tidb::TIDB_ENGINE.to_string(),
            adapter: tidb::TIDB_ADAPTER.to_string(),
            case_id: "unsupported-predicate-type-error".to_string(),
            spec_refs: vec!["tests/differential/first-filter-is-not-null-slice.md".to_string()],
            input_ref: "first-filter-is-not-null-utf8-basic".to_string(),
            filter_ref: "is-not-null-column-0".to_string(),
            outcome: CaseOutcome::Error {
                error_class: ErrorClass::AdapterUnavailable,
                engine_code: None,
                engine_message: Some("not implemented".to_string()),
            },
        };
        let right_case = CaseResult {
            slice_id: tikv::FIRST_FILTER_IS_NOT_NULL_SLICE_ID.to_string(),
            engine: tikv::TIKV_ENGINE.to_string(),
            adapter: tikv::TIKV_ADAPTER.to_string(),
            case_id: "unsupported-predicate-type-error".to_string(),
            spec_refs: vec!["tests/differential/first-filter-is-not-null-slice.md".to_string()],
            input_ref: "first-filter-is-not-null-utf8-basic".to_string(),
            filter_ref: "is-not-null-column-0".to_string(),
            outcome: CaseOutcome::Error {
                error_class: ErrorClass::UnsupportedPredicateType,
                engine_code: Some("1105".to_string()),
                engine_message: Some("unsupported predicate type".to_string()),
            },
        };

        let comparison = compare_case_results(
            &left_case,
            &right_case,
            TIDB_CASE_RESULTS_REF,
            TIKV_CASE_RESULTS_REF,
        );

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
                "all-rows-kept" => Ok(rows_result_tidb(
                    vec![tidb::EngineColumn {
                        name: "a".to_string(),
                        engine_type: "int".to_string(),
                        nullable: false,
                    }],
                    vec![vec![json!(1)], vec![json!(2)], vec![json!(3)]],
                )),
                "all-rows-dropped" => Ok(rows_result_tidb(
                    vec![tidb::EngineColumn {
                        name: "a".to_string(),
                        engine_type: "int".to_string(),
                        nullable: true,
                    }],
                    vec![],
                )),
                "mixed-keep-drop" => Ok(rows_result_tidb(
                    vec![
                        tidb::EngineColumn {
                            name: "a".to_string(),
                            engine_type: "int".to_string(),
                            nullable: true,
                        },
                        tidb::EngineColumn {
                            name: "b".to_string(),
                            engine_type: "int".to_string(),
                            nullable: false,
                        },
                    ],
                    vec![vec![json!(1), json!(10)], vec![json!(3), json!(30)]],
                )),
                "missing-column-error" => Err(tidb::EngineExecutionError::EngineFailure {
                    code: Some("1054".to_string()),
                    message: "Unknown column '__missing_column_1' in 'where clause'".to_string(),
                }),
                "unsupported-predicate-type-error" => {
                    Err(tidb::EngineExecutionError::EngineFailure {
                        code: Some("1105".to_string()),
                        message:
                            "unsupported predicate type: utf8 input is out of scope for first-filter-is-not-null-slice"
                                .to_string(),
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
                "all-rows-kept" => Ok(rows_result_tiflash(
                    vec![tiflash::EngineColumn {
                        name: "a".to_string(),
                        engine_type: "int".to_string(),
                        nullable: false,
                    }],
                    vec![vec![json!(1)], vec![json!(2)], vec![json!(3)]],
                )),
                "all-rows-dropped" => Ok(rows_result_tiflash(
                    vec![tiflash::EngineColumn {
                        name: "a".to_string(),
                        engine_type: "int".to_string(),
                        nullable: true,
                    }],
                    vec![],
                )),
                "mixed-keep-drop" => Ok(rows_result_tiflash(
                    vec![
                        tiflash::EngineColumn {
                            name: "a".to_string(),
                            engine_type: "int".to_string(),
                            nullable: true,
                        },
                        tiflash::EngineColumn {
                            name: "b".to_string(),
                            engine_type: "int".to_string(),
                            nullable: false,
                        },
                    ],
                    vec![vec![json!(1), json!(10)], vec![json!(3), json!(30)]],
                )),
                "missing-column-error" => Err(tiflash::EngineExecutionError::EngineFailure {
                    code: Some("1054".to_string()),
                    message: "Unknown column '__missing_column_1' in 'where clause'".to_string(),
                }),
                "unsupported-predicate-type-error" => {
                    Err(tiflash::EngineExecutionError::EngineFailure {
                        code: Some("1105".to_string()),
                        message:
                            "unsupported predicate type: utf8 input is out of scope for first-filter-is-not-null-slice"
                                .to_string(),
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
                "all-rows-kept" => Ok(rows_result_tikv(
                    vec![tikv::EngineColumn {
                        name: "a".to_string(),
                        engine_type: "int".to_string(),
                        nullable: false,
                    }],
                    vec![vec![json!(1)], vec![json!(2)], vec![json!(3)]],
                )),
                "all-rows-dropped" => Ok(rows_result_tikv(
                    vec![tikv::EngineColumn {
                        name: "a".to_string(),
                        engine_type: "int".to_string(),
                        nullable: true,
                    }],
                    vec![],
                )),
                "mixed-keep-drop" => Ok(rows_result_tikv(
                    vec![
                        tikv::EngineColumn {
                            name: "a".to_string(),
                            engine_type: "int".to_string(),
                            nullable: true,
                        },
                        tikv::EngineColumn {
                            name: "b".to_string(),
                            engine_type: "int".to_string(),
                            nullable: false,
                        },
                    ],
                    vec![vec![json!(1), json!(10)], vec![json!(3), json!(30)]],
                )),
                "missing-column-error" => Err(tikv::EngineExecutionError::EngineFailure {
                    code: Some("1054".to_string()),
                    message: "Unknown column '__missing_column_1' in 'where clause'".to_string(),
                }),
                "unsupported-predicate-type-error" => {
                    Err(tikv::EngineExecutionError::EngineFailure {
                        code: Some("1105".to_string()),
                        message:
                            "unsupported predicate type: utf8 input is out of scope for first-filter-is-not-null-slice"
                                .to_string(),
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
