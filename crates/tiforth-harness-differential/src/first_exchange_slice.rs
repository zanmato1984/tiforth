use serde::{Deserialize, Serialize};
use tiforth_adapter_tidb::{
    first_expression_slice as tidb_expression, first_filter_is_not_null_slice as tidb_filter,
};
use tiforth_adapter_tiflash::{
    first_expression_slice as tiflash_expression, first_filter_is_not_null_slice as tiflash_filter,
};

use crate::{first_expression_slice, first_filter_is_not_null_slice};

pub const FIRST_EXCHANGE_SLICE_ID: &str = "first-exchange-slice";
pub const DRIFT_REPORT_REF: &str =
    "inventory/first-exchange-slice-baseline-vs-exchange-drift-report.md";
pub const DRIFT_REPORT_SIDECAR_REF: &str =
    "inventory/first-exchange-slice-baseline-vs-exchange-drift-report.json";

const FIRST_EXCHANGE_SLICE_SPEC_REFS: [&str; 5] = [
    "docs/design/first-in-contract-exchange-slice.md",
    "tests/differential/first-expression-slice.md",
    "tests/differential/first-filter-is-not-null-slice.md",
    "tests/differential/first-exchange-slice.md",
    "tests/differential/first-exchange-slice-artifacts.md",
];

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HarnessError {
    BaselineExpression {
        error: String,
    },
    ExchangeExpression {
        error: String,
    },
    BaselineFilter {
        error: String,
    },
    ExchangeFilter {
        error: String,
    },
    CaseSetMismatch {
        slice_id: String,
        subject: ComparisonSubject,
        baseline_case_ids: Vec<String>,
        exchange_case_ids: Vec<String>,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ParityStatus {
    Match,
    Drift,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ComparisonSubject {
    TidbCaseResults,
    TiflashCaseResults,
    TidbVsTiflashDrift,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ComparisonDimension {
    OutcomeKind,
    ErrorClass,
    FieldName,
    FieldNullability,
    LogicalType,
    RowCount,
    RowValues,
    DriftStatus,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ParityCase {
    pub subject: ComparisonSubject,
    pub slice_id: String,
    pub case_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub engine: Option<String>,
    pub status: ParityStatus,
    pub comparison_dimensions: Vec<ComparisonDimension>,
    pub summary: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExchangeParityReport {
    pub slice_id: String,
    pub spec_refs: Vec<String>,
    pub cases: Vec<ParityCase>,
}

pub fn execute_first_exchange_slice<BT, BF, ET, EF>(
    baseline_tidb_runner: &BT,
    baseline_tiflash_runner: &BF,
    exchange_tidb_runner: &ET,
    exchange_tiflash_runner: &EF,
) -> Result<ExchangeParityReport, HarnessError>
where
    BT: tidb_expression::TidbRunner + tidb_filter::TidbRunner,
    BF: tiflash_expression::TiflashRunner + tiflash_filter::TiflashRunner,
    ET: tidb_expression::TidbRunner + tidb_filter::TidbRunner,
    EF: tiflash_expression::TiflashRunner + tiflash_filter::TiflashRunner,
{
    let baseline_expression = first_expression_slice::execute_first_expression_slice(
        baseline_tidb_runner,
        baseline_tiflash_runner,
    )
    .map_err(|error| HarnessError::BaselineExpression {
        error: format!("{error:?}"),
    })?;
    let exchange_expression = first_expression_slice::execute_first_expression_slice(
        exchange_tidb_runner,
        exchange_tiflash_runner,
    )
    .map_err(|error| HarnessError::ExchangeExpression {
        error: format!("{error:?}"),
    })?;

    let baseline_filter = first_filter_is_not_null_slice::execute_first_filter_is_not_null_slice(
        baseline_tidb_runner,
        baseline_tiflash_runner,
    )
    .map_err(|error| HarnessError::BaselineFilter {
        error: format!("{error:?}"),
    })?;
    let exchange_filter = first_filter_is_not_null_slice::execute_first_filter_is_not_null_slice(
        exchange_tidb_runner,
        exchange_tiflash_runner,
    )
    .map_err(|error| HarnessError::ExchangeFilter {
        error: format!("{error:?}"),
    })?;

    let mut cases = Vec::new();

    compare_expression_case_results(
        ComparisonSubject::TidbCaseResults,
        &baseline_expression.tidb_case_results.cases,
        &exchange_expression.tidb_case_results.cases,
        &mut cases,
    )?;
    compare_expression_case_results(
        ComparisonSubject::TiflashCaseResults,
        &baseline_expression.tiflash_case_results.cases,
        &exchange_expression.tiflash_case_results.cases,
        &mut cases,
    )?;

    compare_filter_case_results(
        ComparisonSubject::TidbCaseResults,
        &baseline_filter.tidb_case_results.cases,
        &exchange_filter.tidb_case_results.cases,
        &mut cases,
    )?;
    compare_filter_case_results(
        ComparisonSubject::TiflashCaseResults,
        &baseline_filter.tiflash_case_results.cases,
        &exchange_filter.tiflash_case_results.cases,
        &mut cases,
    )?;

    compare_expression_drift(
        &baseline_expression.drift_report.cases,
        &exchange_expression.drift_report.cases,
        &mut cases,
    )?;
    compare_filter_drift(
        &baseline_filter.drift_report.cases,
        &exchange_filter.drift_report.cases,
        &mut cases,
    )?;

    Ok(ExchangeParityReport {
        slice_id: FIRST_EXCHANGE_SLICE_ID.to_string(),
        spec_refs: FIRST_EXCHANGE_SLICE_SPEC_REFS
            .iter()
            .map(|spec_ref| (*spec_ref).to_string())
            .collect(),
        cases,
    })
}

pub fn render_exchange_parity_artifact_json(
    report: &ExchangeParityReport,
) -> Result<String, serde_json::Error> {
    let mut rendered = serde_json::to_string_pretty(report)?;
    rendered.push('\n');
    Ok(rendered)
}

pub fn render_exchange_parity_markdown(report: &ExchangeParityReport) -> String {
    let match_count = report
        .cases
        .iter()
        .filter(|case| case.status == ParityStatus::Match)
        .count();
    let drift_count = report
        .cases
        .iter()
        .filter(|case| case.status == ParityStatus::Drift)
        .count();

    let mut rendered = String::new();
    rendered.push_str("# First Exchange Slice Differential Parity Report\n\n");
    rendered.push_str(
        "Status: issue #183 harness checkpoint, issue #221 artifact-carrier checkpoint\n\n",
    );
    rendered.push_str("Verified: 2026-03-19\n\n");
    rendered.push_str("## Spec Refs\n\n");
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
    rendered.push_str("\n\n## Cases\n\n");

    for (index, case) in report.cases.iter().enumerate() {
        rendered.push_str("### `");
        rendered.push_str(&case.slice_id);
        rendered.push('/');
        rendered.push_str(&case.case_id);
        rendered.push_str("`\n\n");
        rendered.push_str("- subject: `");
        rendered.push_str(comparison_subject_name(case.subject));
        rendered.push_str("`\n");
        if let Some(engine) = &case.engine {
            rendered.push_str("- engine: `");
            rendered.push_str(engine);
            rendered.push_str("`\n");
        }
        rendered.push_str("- status: `");
        rendered.push_str(parity_status_name(case.status));
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
        if index + 1 < report.cases.len() {
            rendered.push('\n');
        }
    }

    rendered
}

fn compare_expression_case_results(
    subject: ComparisonSubject,
    baseline_cases: &[first_expression_slice::CaseResult],
    exchange_cases: &[first_expression_slice::CaseResult],
    parity_cases: &mut Vec<ParityCase>,
) -> Result<(), HarnessError> {
    ensure_case_sets_match(
        first_expression_slice::TIDB_CASE_RESULTS_REF,
        subject,
        baseline_cases,
        exchange_cases,
        |case| &case.case_id,
    )?;

    for (baseline_case, exchange_case) in baseline_cases.iter().zip(exchange_cases) {
        let (status, comparison_dimensions) =
            compare_expression_outcomes(&baseline_case.outcome, &exchange_case.outcome);

        let summary = if status == ParityStatus::Match {
            format!(
                "Baseline and exchange paths matched for `{}` `{}` on `{}`.",
                baseline_case.engine,
                baseline_case.case_id,
                comparison_subject_name(subject)
            )
        } else {
            format!(
                "Baseline and exchange paths diverged for `{}` `{}` on {}.",
                baseline_case.engine,
                baseline_case.case_id,
                join_inline_codes(
                    comparison_dimensions
                        .iter()
                        .copied()
                        .map(comparison_dimension_name),
                )
            )
        };

        parity_cases.push(ParityCase {
            subject,
            slice_id: baseline_case.slice_id.clone(),
            case_id: baseline_case.case_id.clone(),
            engine: Some(baseline_case.engine.clone()),
            status,
            comparison_dimensions,
            summary,
        });
    }

    Ok(())
}

fn compare_filter_case_results(
    subject: ComparisonSubject,
    baseline_cases: &[first_filter_is_not_null_slice::CaseResult],
    exchange_cases: &[first_filter_is_not_null_slice::CaseResult],
    parity_cases: &mut Vec<ParityCase>,
) -> Result<(), HarnessError> {
    ensure_case_sets_match(
        first_filter_is_not_null_slice::TIDB_CASE_RESULTS_REF,
        subject,
        baseline_cases,
        exchange_cases,
        |case| &case.case_id,
    )?;

    for (baseline_case, exchange_case) in baseline_cases.iter().zip(exchange_cases) {
        let (status, comparison_dimensions) =
            compare_filter_outcomes(&baseline_case.outcome, &exchange_case.outcome);

        let summary = if status == ParityStatus::Match {
            format!(
                "Baseline and exchange paths matched for `{}` `{}` on `{}`.",
                baseline_case.engine,
                baseline_case.case_id,
                comparison_subject_name(subject)
            )
        } else {
            format!(
                "Baseline and exchange paths diverged for `{}` `{}` on {}.",
                baseline_case.engine,
                baseline_case.case_id,
                join_inline_codes(
                    comparison_dimensions
                        .iter()
                        .copied()
                        .map(comparison_dimension_name),
                )
            )
        };

        parity_cases.push(ParityCase {
            subject,
            slice_id: baseline_case.slice_id.clone(),
            case_id: baseline_case.case_id.clone(),
            engine: Some(baseline_case.engine.clone()),
            status,
            comparison_dimensions,
            summary,
        });
    }

    Ok(())
}

fn compare_expression_drift(
    baseline_cases: &[first_expression_slice::DriftCase],
    exchange_cases: &[first_expression_slice::DriftCase],
    parity_cases: &mut Vec<ParityCase>,
) -> Result<(), HarnessError> {
    ensure_case_sets_match(
        first_expression_slice::DRIFT_REPORT_REF,
        ComparisonSubject::TidbVsTiflashDrift,
        baseline_cases,
        exchange_cases,
        |case| &case.case_id,
    )?;

    for (baseline_case, exchange_case) in baseline_cases.iter().zip(exchange_cases) {
        let status = if baseline_case.status == exchange_case.status {
            ParityStatus::Match
        } else {
            ParityStatus::Drift
        };

        let summary = if status == ParityStatus::Match {
            format!(
                "Baseline and exchange paths kept drift status `{}` for `{}`.",
                expression_drift_status_name(baseline_case.status),
                baseline_case.case_id
            )
        } else {
            format!(
                "Baseline drift status `{}` changed to `{}` for `{}` through the exchange path.",
                expression_drift_status_name(baseline_case.status),
                expression_drift_status_name(exchange_case.status),
                baseline_case.case_id
            )
        };

        parity_cases.push(ParityCase {
            subject: ComparisonSubject::TidbVsTiflashDrift,
            slice_id: "first-expression-slice".to_string(),
            case_id: baseline_case.case_id.clone(),
            engine: None,
            status,
            comparison_dimensions: vec![ComparisonDimension::DriftStatus],
            summary,
        });
    }

    Ok(())
}

fn compare_filter_drift(
    baseline_cases: &[first_filter_is_not_null_slice::DriftCase],
    exchange_cases: &[first_filter_is_not_null_slice::DriftCase],
    parity_cases: &mut Vec<ParityCase>,
) -> Result<(), HarnessError> {
    ensure_case_sets_match(
        first_filter_is_not_null_slice::DRIFT_REPORT_REF,
        ComparisonSubject::TidbVsTiflashDrift,
        baseline_cases,
        exchange_cases,
        |case| &case.case_id,
    )?;

    for (baseline_case, exchange_case) in baseline_cases.iter().zip(exchange_cases) {
        let status = if baseline_case.status == exchange_case.status {
            ParityStatus::Match
        } else {
            ParityStatus::Drift
        };

        let summary = if status == ParityStatus::Match {
            format!(
                "Baseline and exchange paths kept drift status `{}` for `{}`.",
                filter_drift_status_name(baseline_case.status),
                baseline_case.case_id
            )
        } else {
            format!(
                "Baseline drift status `{}` changed to `{}` for `{}` through the exchange path.",
                filter_drift_status_name(baseline_case.status),
                filter_drift_status_name(exchange_case.status),
                baseline_case.case_id
            )
        };

        parity_cases.push(ParityCase {
            subject: ComparisonSubject::TidbVsTiflashDrift,
            slice_id: "first-filter-is-not-null-slice".to_string(),
            case_id: baseline_case.case_id.clone(),
            engine: None,
            status,
            comparison_dimensions: vec![ComparisonDimension::DriftStatus],
            summary,
        });
    }

    Ok(())
}

fn compare_expression_outcomes(
    baseline: &first_expression_slice::CaseOutcome,
    exchange: &first_expression_slice::CaseOutcome,
) -> (ParityStatus, Vec<ComparisonDimension>) {
    match (baseline, exchange) {
        (
            first_expression_slice::CaseOutcome::Rows {
                schema: baseline_schema,
                rows: baseline_rows,
                row_count: baseline_row_count,
            },
            first_expression_slice::CaseOutcome::Rows {
                schema: exchange_schema,
                rows: exchange_rows,
                row_count: exchange_row_count,
            },
        ) => {
            let mut dimensions = Vec::new();
            if expression_schema_names(baseline_schema) != expression_schema_names(exchange_schema)
            {
                dimensions.push(ComparisonDimension::FieldName);
            }
            if expression_schema_nullability(baseline_schema)
                != expression_schema_nullability(exchange_schema)
            {
                dimensions.push(ComparisonDimension::FieldNullability);
            }
            if expression_schema_logical_types(baseline_schema)
                != expression_schema_logical_types(exchange_schema)
            {
                dimensions.push(ComparisonDimension::LogicalType);
            }
            if baseline_row_count != exchange_row_count {
                dimensions.push(ComparisonDimension::RowCount);
            }
            if baseline_rows != exchange_rows {
                dimensions.push(ComparisonDimension::RowValues);
            }

            if dimensions.is_empty() {
                (
                    ParityStatus::Match,
                    vec![
                        ComparisonDimension::FieldName,
                        ComparisonDimension::FieldNullability,
                        ComparisonDimension::LogicalType,
                        ComparisonDimension::RowCount,
                        ComparisonDimension::RowValues,
                    ],
                )
            } else {
                (ParityStatus::Drift, dimensions)
            }
        }
        (
            first_expression_slice::CaseOutcome::Error {
                error_class: baseline_error,
                ..
            },
            first_expression_slice::CaseOutcome::Error {
                error_class: exchange_error,
                ..
            },
        ) => {
            let status = if baseline_error == exchange_error {
                ParityStatus::Match
            } else {
                ParityStatus::Drift
            };
            (status, vec![ComparisonDimension::ErrorClass])
        }
        _ => (ParityStatus::Drift, vec![ComparisonDimension::OutcomeKind]),
    }
}

fn compare_filter_outcomes(
    baseline: &first_filter_is_not_null_slice::CaseOutcome,
    exchange: &first_filter_is_not_null_slice::CaseOutcome,
) -> (ParityStatus, Vec<ComparisonDimension>) {
    match (baseline, exchange) {
        (
            first_filter_is_not_null_slice::CaseOutcome::Rows {
                schema: baseline_schema,
                rows: baseline_rows,
                row_count: baseline_row_count,
            },
            first_filter_is_not_null_slice::CaseOutcome::Rows {
                schema: exchange_schema,
                rows: exchange_rows,
                row_count: exchange_row_count,
            },
        ) => {
            let mut dimensions = Vec::new();
            if filter_schema_names(baseline_schema) != filter_schema_names(exchange_schema) {
                dimensions.push(ComparisonDimension::FieldName);
            }
            if filter_schema_nullability(baseline_schema)
                != filter_schema_nullability(exchange_schema)
            {
                dimensions.push(ComparisonDimension::FieldNullability);
            }
            if filter_schema_logical_types(baseline_schema)
                != filter_schema_logical_types(exchange_schema)
            {
                dimensions.push(ComparisonDimension::LogicalType);
            }
            if baseline_row_count != exchange_row_count {
                dimensions.push(ComparisonDimension::RowCount);
            }
            if baseline_rows != exchange_rows {
                dimensions.push(ComparisonDimension::RowValues);
            }

            if dimensions.is_empty() {
                (
                    ParityStatus::Match,
                    vec![
                        ComparisonDimension::FieldName,
                        ComparisonDimension::FieldNullability,
                        ComparisonDimension::LogicalType,
                        ComparisonDimension::RowCount,
                        ComparisonDimension::RowValues,
                    ],
                )
            } else {
                (ParityStatus::Drift, dimensions)
            }
        }
        (
            first_filter_is_not_null_slice::CaseOutcome::Error {
                error_class: baseline_error,
                ..
            },
            first_filter_is_not_null_slice::CaseOutcome::Error {
                error_class: exchange_error,
                ..
            },
        ) => {
            let status = if baseline_error == exchange_error {
                ParityStatus::Match
            } else {
                ParityStatus::Drift
            };
            (status, vec![ComparisonDimension::ErrorClass])
        }
        _ => (ParityStatus::Drift, vec![ComparisonDimension::OutcomeKind]),
    }
}

fn expression_schema_names(schema: &[first_expression_slice::SchemaField]) -> Vec<&str> {
    schema.iter().map(|field| field.name.as_str()).collect()
}

fn expression_schema_nullability(schema: &[first_expression_slice::SchemaField]) -> Vec<bool> {
    schema.iter().map(|field| field.nullable).collect()
}

fn expression_schema_logical_types(schema: &[first_expression_slice::SchemaField]) -> Vec<&str> {
    schema
        .iter()
        .map(|field| field.logical_type.as_str())
        .collect()
}

fn filter_schema_names(schema: &[first_filter_is_not_null_slice::SchemaField]) -> Vec<&str> {
    schema.iter().map(|field| field.name.as_str()).collect()
}

fn filter_schema_nullability(schema: &[first_filter_is_not_null_slice::SchemaField]) -> Vec<bool> {
    schema.iter().map(|field| field.nullable).collect()
}

fn filter_schema_logical_types(
    schema: &[first_filter_is_not_null_slice::SchemaField],
) -> Vec<&str> {
    schema
        .iter()
        .map(|field| field.logical_type.as_str())
        .collect()
}

fn ensure_case_sets_match<T, F>(
    slice_id: &str,
    subject: ComparisonSubject,
    baseline_cases: &[T],
    exchange_cases: &[T],
    case_id: F,
) -> Result<(), HarnessError>
where
    F: Fn(&T) -> &str,
{
    let baseline_case_ids: Vec<String> = baseline_cases
        .iter()
        .map(|case| case_id(case).to_string())
        .collect();
    let exchange_case_ids: Vec<String> = exchange_cases
        .iter()
        .map(|case| case_id(case).to_string())
        .collect();

    if baseline_case_ids != exchange_case_ids {
        return Err(HarnessError::CaseSetMismatch {
            slice_id: slice_id.to_string(),
            subject,
            baseline_case_ids,
            exchange_case_ids,
        });
    }

    Ok(())
}

fn expression_drift_status_name(status: first_expression_slice::DriftStatus) -> &'static str {
    match status {
        first_expression_slice::DriftStatus::Match => "match",
        first_expression_slice::DriftStatus::Drift => "drift",
        first_expression_slice::DriftStatus::Unsupported => "unsupported",
    }
}

fn filter_drift_status_name(status: first_filter_is_not_null_slice::DriftStatus) -> &'static str {
    match status {
        first_filter_is_not_null_slice::DriftStatus::Match => "match",
        first_filter_is_not_null_slice::DriftStatus::Drift => "drift",
        first_filter_is_not_null_slice::DriftStatus::Unsupported => "unsupported",
    }
}

fn parity_status_name(status: ParityStatus) -> &'static str {
    match status {
        ParityStatus::Match => "match",
        ParityStatus::Drift => "drift",
    }
}

fn comparison_subject_name(subject: ComparisonSubject) -> &'static str {
    match subject {
        ComparisonSubject::TidbCaseResults => "tidb_case_results",
        ComparisonSubject::TiflashCaseResults => "tiflash_case_results",
        ComparisonSubject::TidbVsTiflashDrift => "tidb_vs_tiflash_drift",
    }
}

fn comparison_dimension_name(dimension: ComparisonDimension) -> &'static str {
    match dimension {
        ComparisonDimension::OutcomeKind => "outcome_kind",
        ComparisonDimension::ErrorClass => "error_class",
        ComparisonDimension::FieldName => "field_name",
        ComparisonDimension::FieldNullability => "field_nullability",
        ComparisonDimension::LogicalType => "logical_type",
        ComparisonDimension::RowCount => "row_count",
        ComparisonDimension::RowValues => "row_values",
        ComparisonDimension::DriftStatus => "drift_status",
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

#[cfg(test)]
mod tests {
    use serde_json::{json, Value};

    use super::*;

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum FixtureMode {
        Baseline,
        ExchangeMatch,
        ExchangeTidbRowDrift,
    }

    #[derive(Debug, Clone, Copy)]
    struct FixtureTidbRunner {
        mode: FixtureMode,
    }

    #[derive(Debug, Clone, Copy)]
    struct FixtureTiflashRunner {
        mode: FixtureMode,
    }

    #[test]
    fn exchange_parity_is_match_when_paths_are_equivalent() {
        let baseline_tidb = FixtureTidbRunner {
            mode: FixtureMode::Baseline,
        };
        let baseline_tiflash = FixtureTiflashRunner {
            mode: FixtureMode::Baseline,
        };
        let exchange_tidb = FixtureTidbRunner {
            mode: FixtureMode::ExchangeMatch,
        };
        let exchange_tiflash = FixtureTiflashRunner {
            mode: FixtureMode::ExchangeMatch,
        };

        let report = execute_first_exchange_slice(
            &baseline_tidb,
            &baseline_tiflash,
            &exchange_tidb,
            &exchange_tiflash,
        )
        .unwrap();

        assert_eq!(report.slice_id, FIRST_EXCHANGE_SLICE_ID);
        assert_eq!(report.cases.len(), 33);
        assert!(report
            .cases
            .iter()
            .all(|case| case.status == ParityStatus::Match));
    }

    #[test]
    fn exchange_parity_reports_row_and_drift_status_changes() {
        let baseline_tidb = FixtureTidbRunner {
            mode: FixtureMode::Baseline,
        };
        let baseline_tiflash = FixtureTiflashRunner {
            mode: FixtureMode::Baseline,
        };
        let exchange_tidb = FixtureTidbRunner {
            mode: FixtureMode::ExchangeTidbRowDrift,
        };
        let exchange_tiflash = FixtureTiflashRunner {
            mode: FixtureMode::ExchangeMatch,
        };

        let report = execute_first_exchange_slice(
            &baseline_tidb,
            &baseline_tiflash,
            &exchange_tidb,
            &exchange_tiflash,
        )
        .unwrap();

        let row_case = report
            .cases
            .iter()
            .find(|case| {
                case.subject == ComparisonSubject::TidbCaseResults
                    && case.slice_id == "first-expression-slice"
                    && case.case_id == "column-passthrough"
            })
            .expect("missing expression tidb parity case");
        assert_eq!(row_case.status, ParityStatus::Drift);
        assert_eq!(
            row_case.comparison_dimensions,
            vec![ComparisonDimension::RowValues]
        );

        let drift_case = report
            .cases
            .iter()
            .find(|case| {
                case.subject == ComparisonSubject::TidbVsTiflashDrift
                    && case.slice_id == "first-expression-slice"
                    && case.case_id == "column-passthrough"
            })
            .expect("missing expression drift parity case");
        assert_eq!(drift_case.status, ParityStatus::Drift);
        assert_eq!(
            drift_case.comparison_dimensions,
            vec![ComparisonDimension::DriftStatus]
        );
    }

    #[test]
    fn exchange_markdown_renderer_includes_summary_counts() {
        let report = ExchangeParityReport {
            slice_id: FIRST_EXCHANGE_SLICE_ID.to_string(),
            spec_refs: FIRST_EXCHANGE_SLICE_SPEC_REFS
                .iter()
                .map(|spec_ref| (*spec_ref).to_string())
                .collect(),
            cases: vec![
                ParityCase {
                    subject: ComparisonSubject::TidbCaseResults,
                    slice_id: "first-expression-slice".to_string(),
                    case_id: "column-passthrough".to_string(),
                    engine: Some("tidb".to_string()),
                    status: ParityStatus::Match,
                    comparison_dimensions: vec![ComparisonDimension::RowValues],
                    summary: "ok".to_string(),
                },
                ParityCase {
                    subject: ComparisonSubject::TidbVsTiflashDrift,
                    slice_id: "first-expression-slice".to_string(),
                    case_id: "column-passthrough".to_string(),
                    engine: None,
                    status: ParityStatus::Drift,
                    comparison_dimensions: vec![ComparisonDimension::DriftStatus],
                    summary: "drift".to_string(),
                },
            ],
        };

        let rendered = render_exchange_parity_markdown(&report);

        assert!(rendered.contains("First Exchange Slice Differential Parity Report"));
        assert!(rendered.contains("- `match`: 1"));
        assert!(rendered.contains("- `drift`: 1"));
    }

    #[test]
    fn exchange_json_renderer_round_trips_report_shape() {
        let report = ExchangeParityReport {
            slice_id: FIRST_EXCHANGE_SLICE_ID.to_string(),
            spec_refs: FIRST_EXCHANGE_SLICE_SPEC_REFS
                .iter()
                .map(|spec_ref| (*spec_ref).to_string())
                .collect(),
            cases: vec![ParityCase {
                subject: ComparisonSubject::TidbCaseResults,
                slice_id: "first-expression-slice".to_string(),
                case_id: "column-passthrough".to_string(),
                engine: Some("tidb".to_string()),
                status: ParityStatus::Match,
                comparison_dimensions: vec![ComparisonDimension::RowValues],
                summary: "ok".to_string(),
            }],
        };

        let rendered = render_exchange_parity_artifact_json(&report).unwrap();
        let parsed: ExchangeParityReport = serde_json::from_str(&rendered).unwrap();

        assert_eq!(parsed, report);
        assert!(rendered.ends_with('\n'));
    }

    impl tidb_expression::TidbRunner for FixtureTidbRunner {
        fn run(
            &self,
            plan: &tidb_expression::TidbExecutionPlan,
        ) -> Result<tidb_expression::EngineExecutionResult, tidb_expression::EngineExecutionError>
        {
            let mut result = baseline_tidb_expression(plan.request.case_id.as_str());
            if self.mode == FixtureMode::ExchangeTidbRowDrift
                && plan.request.case_id == "column-passthrough"
            {
                if let Ok(ref mut rows) = result {
                    rows.rows = vec![vec![json!(1)], vec![json!(99)], vec![json!(3)]];
                }
            }
            result
        }
    }

    impl tidb_filter::TidbRunner for FixtureTidbRunner {
        fn run(
            &self,
            plan: &tidb_filter::TidbExecutionPlan,
        ) -> Result<tidb_filter::EngineExecutionResult, tidb_filter::EngineExecutionError> {
            baseline_tidb_filter(plan.request.case_id.as_str())
        }
    }

    impl tiflash_expression::TiflashRunner for FixtureTiflashRunner {
        fn run(
            &self,
            plan: &tiflash_expression::TiflashExecutionPlan,
        ) -> Result<
            tiflash_expression::EngineExecutionResult,
            tiflash_expression::EngineExecutionError,
        > {
            let _mode = self.mode;
            baseline_tiflash_expression(plan.request.case_id.as_str())
        }
    }

    impl tiflash_filter::TiflashRunner for FixtureTiflashRunner {
        fn run(
            &self,
            plan: &tiflash_filter::TiflashExecutionPlan,
        ) -> Result<tiflash_filter::EngineExecutionResult, tiflash_filter::EngineExecutionError>
        {
            let _mode = self.mode;
            baseline_tiflash_filter(plan.request.case_id.as_str())
        }
    }

    fn baseline_tidb_expression(
        case_id: &str,
    ) -> Result<tidb_expression::EngineExecutionResult, tidb_expression::EngineExecutionError> {
        match case_id {
            "column-passthrough" => Ok(rows_result_tidb_expression(
                "a",
                "int",
                false,
                vec![Some(1), Some(2), Some(3)],
            )),
            "literal-int32-seven" => Ok(rows_result_tidb_expression(
                "lit",
                "bigint",
                false,
                vec![Some(7), Some(7), Some(7)],
            )),
            "literal-int32-null" => Ok(rows_result_tidb_expression(
                "lit",
                "bigint",
                true,
                vec![None, None, None],
            )),
            "add-int32-literal" => Ok(rows_result_tidb_expression(
                "a_plus_one",
                "int",
                false,
                vec![Some(2), Some(3), Some(4)],
            )),
            "add-int32-null-propagation" => Ok(rows_result_tidb_expression(
                "a_plus_one",
                "int",
                true,
                vec![Some(2), None, Some(4)],
            )),
            "add-int32-overflow-error" => {
                Err(tidb_expression::EngineExecutionError::AdapterUnavailable {
                    message: Some(
                        "TiDB adapter core does not yet narrow the shared int32 overflow boundary."
                            .to_string(),
                    ),
                })
            }
            other => panic!("unexpected case_id: {other}"),
        }
    }

    fn baseline_tiflash_expression(
        case_id: &str,
    ) -> Result<tiflash_expression::EngineExecutionResult, tiflash_expression::EngineExecutionError>
    {
        match case_id {
            "column-passthrough" => Ok(rows_result_tiflash_expression(
                "a",
                "int",
                false,
                vec![Some(1), Some(2), Some(3)],
            )),
            "literal-int32-seven" => Ok(rows_result_tiflash_expression(
                "lit",
                "bigint",
                false,
                vec![Some(7), Some(7), Some(7)],
            )),
            "literal-int32-null" => Ok(rows_result_tiflash_expression(
                "lit",
                "bigint",
                true,
                vec![None, None, None],
            )),
            "add-int32-literal" => Ok(rows_result_tiflash_expression(
                "a_plus_one",
                "int",
                false,
                vec![Some(2), Some(3), Some(4)],
            )),
            "add-int32-null-propagation" => Ok(rows_result_tiflash_expression(
                "a_plus_one",
                "int",
                true,
                vec![Some(2), None, Some(4)],
            )),
            "add-int32-overflow-error" => {
                Err(tiflash_expression::EngineExecutionError::EngineFailure {
                    code: Some("1690".to_string()),
                    message: "BIGINT value is out of range in 'input_rows.a + cast(1 as signed)'"
                        .to_string(),
                })
            }
            other => panic!("unexpected case_id: {other}"),
        }
    }

    fn baseline_tidb_filter(
        case_id: &str,
    ) -> Result<tidb_filter::EngineExecutionResult, tidb_filter::EngineExecutionError> {
        match case_id {
            "all-rows-kept" => Ok(rows_result_tidb_filter(
                vec![tidb_filter::EngineColumn {
                    name: "a".to_string(),
                    engine_type: "int".to_string(),
                    nullable: false,
                }],
                vec![vec![json!(1)], vec![json!(2)], vec![json!(3)]],
            )),
            "all-rows-dropped" => Ok(rows_result_tidb_filter(
                vec![tidb_filter::EngineColumn {
                    name: "a".to_string(),
                    engine_type: "int".to_string(),
                    nullable: true,
                }],
                vec![],
            )),
            "mixed-keep-drop" => Ok(rows_result_tidb_filter(
                vec![
                    tidb_filter::EngineColumn {
                        name: "a".to_string(),
                        engine_type: "int".to_string(),
                        nullable: true,
                    },
                    tidb_filter::EngineColumn {
                        name: "b".to_string(),
                        engine_type: "int".to_string(),
                        nullable: false,
                    },
                ],
                vec![vec![json!(1), json!(10)], vec![json!(3), json!(30)]],
            )),
            "missing-column-error" => Err(tidb_filter::EngineExecutionError::EngineFailure {
                code: Some("1054".to_string()),
                message: "Unknown column '__missing_column_1' in 'where clause'".to_string(),
            }),
            "unsupported-predicate-type-error" => {
                Err(tidb_filter::EngineExecutionError::EngineFailure {
                    code: Some("1105".to_string()),
                    message: "unsupported predicate type: utf8 input is out of scope for first-filter-is-not-null-slice".to_string(),
                })
            }
            other => panic!("unexpected case_id: {other}"),
        }
    }

    fn baseline_tiflash_filter(
        case_id: &str,
    ) -> Result<tiflash_filter::EngineExecutionResult, tiflash_filter::EngineExecutionError> {
        match case_id {
            "all-rows-kept" => Ok(rows_result_tiflash_filter(
                vec![tiflash_filter::EngineColumn {
                    name: "a".to_string(),
                    engine_type: "int".to_string(),
                    nullable: false,
                }],
                vec![vec![json!(1)], vec![json!(2)], vec![json!(3)]],
            )),
            "all-rows-dropped" => Ok(rows_result_tiflash_filter(
                vec![tiflash_filter::EngineColumn {
                    name: "a".to_string(),
                    engine_type: "int".to_string(),
                    nullable: true,
                }],
                vec![],
            )),
            "mixed-keep-drop" => Ok(rows_result_tiflash_filter(
                vec![
                    tiflash_filter::EngineColumn {
                        name: "a".to_string(),
                        engine_type: "int".to_string(),
                        nullable: true,
                    },
                    tiflash_filter::EngineColumn {
                        name: "b".to_string(),
                        engine_type: "int".to_string(),
                        nullable: false,
                    },
                ],
                vec![vec![json!(1), json!(10)], vec![json!(3), json!(30)]],
            )),
            "missing-column-error" => Err(tiflash_filter::EngineExecutionError::EngineFailure {
                code: Some("1054".to_string()),
                message: "Unknown column '__missing_column_1' in 'where clause'".to_string(),
            }),
            "unsupported-predicate-type-error" => {
                Err(tiflash_filter::EngineExecutionError::EngineFailure {
                    code: Some("1105".to_string()),
                    message: "unsupported predicate type: utf8 input is out of scope for first-filter-is-not-null-slice".to_string(),
                })
            }
            other => panic!("unexpected case_id: {other}"),
        }
    }

    fn rows_result_tidb_expression(
        name: &str,
        engine_type: &str,
        nullable: bool,
        values: Vec<Option<i64>>,
    ) -> tidb_expression::EngineExecutionResult {
        tidb_expression::EngineExecutionResult {
            columns: vec![tidb_expression::EngineColumn {
                name: name.to_string(),
                engine_type: engine_type.to_string(),
                nullable,
            }],
            rows: values
                .into_iter()
                .map(|value| vec![option_i64_to_value(value)])
                .collect(),
        }
    }

    fn rows_result_tiflash_expression(
        name: &str,
        engine_type: &str,
        nullable: bool,
        values: Vec<Option<i64>>,
    ) -> tiflash_expression::EngineExecutionResult {
        tiflash_expression::EngineExecutionResult {
            columns: vec![tiflash_expression::EngineColumn {
                name: name.to_string(),
                engine_type: engine_type.to_string(),
                nullable,
            }],
            rows: values
                .into_iter()
                .map(|value| vec![option_i64_to_value(value)])
                .collect(),
        }
    }

    fn rows_result_tidb_filter(
        columns: Vec<tidb_filter::EngineColumn>,
        rows: Vec<Vec<Value>>,
    ) -> tidb_filter::EngineExecutionResult {
        tidb_filter::EngineExecutionResult { columns, rows }
    }

    fn rows_result_tiflash_filter(
        columns: Vec<tiflash_filter::EngineColumn>,
        rows: Vec<Vec<Value>>,
    ) -> tiflash_filter::EngineExecutionResult {
        tiflash_filter::EngineExecutionResult { columns, rows }
    }

    fn option_i64_to_value(value: Option<i64>) -> Value {
        match value {
            Some(value) => Value::from(value),
            None => Value::Null,
        }
    }
}
