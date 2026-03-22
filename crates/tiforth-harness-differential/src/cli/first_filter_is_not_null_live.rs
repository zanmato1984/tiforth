use std::env;

use crate::cli::artifact_output::{emit_artifacts, ArtifactOutput};
use crate::live::first_filter_is_not_null_live::{
    render_live_drift_report_markdown, LiveTidbRunner, LiveTiflashRunner, TIDB_MYSQL_ENV_PREFIX,
    TIFLASH_MYSQL_ENV_PREFIX,
};
use crate::slices::first_filter_is_not_null_slice::{
    execute_first_filter_is_not_null_slice, render_case_results_artifact_json,
    render_drift_report_artifact_json, DRIFT_REPORT_REF, DRIFT_REPORT_SIDECAR_REF,
    TIDB_CASE_RESULTS_REF, TIFLASH_CASE_RESULTS_REF,
};

const WRITE_FLAG: &str = "--write-artifacts";

pub fn main() {
    if let Err(error) = run() {
        eprintln!("{error}");
        std::process::exit(1);
    }
}

fn run() -> Result<(), String> {
    let write_artifacts = env::args().any(|arg| arg == WRITE_FLAG);

    let bundle = execute_first_filter_is_not_null_slice(
        &LiveTidbRunner::from_env(),
        &LiveTiflashRunner::from_env(),
    )
    .map_err(|error| format!("live runner execution failed: {error:?}"))?;

    let tidb_case_results = render_case_results_artifact_json(&bundle.tidb_case_results)
        .map_err(|error| format!("failed to render TiDB case-results artifact as JSON: {error}"))?;
    let tiflash_case_results = render_case_results_artifact_json(&bundle.tiflash_case_results)
        .map_err(|error| {
            format!("failed to render TiFlash case-results artifact as JSON: {error}")
        })?;
    let drift_report = render_live_drift_report_markdown(&bundle.drift_report);
    let drift_report_sidecar = render_drift_report_artifact_json(&bundle.drift_report)
        .map_err(|error| format!("failed to render drift-report sidecar as JSON: {error}"))?;

    let connection_note = format!(
        "Connection env prefixes: `{TIDB_MYSQL_ENV_PREFIX}_*`, `{TIFLASH_MYSQL_ENV_PREFIX}_*`."
    );
    let rendered_artifacts = [
        ArtifactOutput {
            path: TIDB_CASE_RESULTS_REF,
            contents: &tidb_case_results,
        },
        ArtifactOutput {
            path: TIFLASH_CASE_RESULTS_REF,
            contents: &tiflash_case_results,
        },
        ArtifactOutput {
            path: DRIFT_REPORT_REF,
            contents: &drift_report,
        },
        ArtifactOutput {
            path: DRIFT_REPORT_SIDECAR_REF,
            contents: &drift_report_sidecar,
        },
    ];

    emit_artifacts(
        write_artifacts,
        WRITE_FLAG,
        Some(&connection_note),
        &rendered_artifacts,
    )
}
