use std::env;

use crate::cli::live_output::{emit_live_drift_artifacts, LiveDriftArtifacts};
use crate::live::first_temporal_timestamp_tz_slice_tikv_live::{
    render_live_drift_report_markdown, LiveTidbRunner, LiveTiflashRunner, LiveTikvRunner,
    TIDB_MYSQL_ENV_PREFIX, TIFLASH_MYSQL_ENV_PREFIX, TIKV_MYSQL_ENV_PREFIX,
};
use crate::pairwise::first_temporal_timestamp_tz_slice_tikv_pairwise::{
    execute_first_temporal_timestamp_tz_slice_tikv_pairwise, render_drift_report_artifact_json,
    TIDB_VS_TIKV_DRIFT_REPORT_REF, TIDB_VS_TIKV_DRIFT_REPORT_SIDECAR_REF,
    TIFLASH_VS_TIKV_DRIFT_REPORT_REF, TIFLASH_VS_TIKV_DRIFT_REPORT_SIDECAR_REF,
};
use crate::tikv::first_temporal_timestamp_tz_slice_tikv::{
    render_case_results_artifact_json, TIKV_CASE_RESULTS_REF,
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

    let bundle = execute_first_temporal_timestamp_tz_slice_tikv_pairwise(
        &LiveTidbRunner::from_env(),
        &LiveTiflashRunner::from_env(),
        &LiveTikvRunner::from_env(),
    )
    .map_err(|error| format!("live runner execution failed: {error:?}"))?;

    let tikv_case_results = render_case_results_artifact_json(&bundle.tikv_case_results)
        .map_err(|error| format!("failed to render TiKV case-results artifact as JSON: {error}"))?;
    let tidb_vs_tikv_drift_report =
        render_live_drift_report_markdown(&bundle.tidb_vs_tikv_drift_report);
    let tidb_vs_tikv_drift_report_sidecar =
        render_drift_report_artifact_json(&bundle.tidb_vs_tikv_drift_report)
            .map_err(|error| format!("failed to render TiDB-vs-TiKV sidecar as JSON: {error}"))?;
    let tiflash_vs_tikv_drift_report =
        render_live_drift_report_markdown(&bundle.tiflash_vs_tikv_drift_report);
    let tiflash_vs_tikv_drift_report_sidecar = render_drift_report_artifact_json(
        &bundle.tiflash_vs_tikv_drift_report,
    )
    .map_err(|error| format!("failed to render TiFlash-vs-TiKV sidecar as JSON: {error}"))?;

    emit_live_drift_artifacts(
        write_artifacts,
        WRITE_FLAG,
        LiveDriftArtifacts {
            tikv_case_results_ref: TIKV_CASE_RESULTS_REF,
            tikv_case_results: &tikv_case_results,
            tidb_vs_tikv_drift_report_ref: TIDB_VS_TIKV_DRIFT_REPORT_REF,
            tidb_vs_tikv_drift_report: &tidb_vs_tikv_drift_report,
            tidb_vs_tikv_drift_report_sidecar_ref: TIDB_VS_TIKV_DRIFT_REPORT_SIDECAR_REF,
            tidb_vs_tikv_drift_report_sidecar: &tidb_vs_tikv_drift_report_sidecar,
            tiflash_vs_tikv_drift_report_ref: TIFLASH_VS_TIKV_DRIFT_REPORT_REF,
            tiflash_vs_tikv_drift_report: &tiflash_vs_tikv_drift_report,
            tiflash_vs_tikv_drift_report_sidecar_ref: TIFLASH_VS_TIKV_DRIFT_REPORT_SIDECAR_REF,
            tiflash_vs_tikv_drift_report_sidecar: &tiflash_vs_tikv_drift_report_sidecar,
            tidb_env_prefix: TIDB_MYSQL_ENV_PREFIX,
            tiflash_env_prefix: TIFLASH_MYSQL_ENV_PREFIX,
            tikv_env_prefix: TIKV_MYSQL_ENV_PREFIX,
        },
    )
}
