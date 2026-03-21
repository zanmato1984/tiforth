use std::env;
use std::fs;

use tiforth_harness_differential::first_temporal_date32_slice_tikv::{
    render_case_results_artifact_json, TIKV_CASE_RESULTS_REF,
};
use tiforth_harness_differential::first_temporal_date32_slice_tikv_live::{
    render_live_drift_report_markdown, LiveTidbRunner, LiveTiflashRunner, LiveTikvRunner,
    TIDB_MYSQL_ENV_PREFIX, TIFLASH_MYSQL_ENV_PREFIX, TIKV_MYSQL_ENV_PREFIX,
};
use tiforth_harness_differential::first_temporal_date32_slice_tikv_pairwise::{
    execute_first_temporal_date32_slice_tikv_pairwise, render_drift_report_artifact_json,
    TIDB_VS_TIKV_DRIFT_REPORT_REF, TIDB_VS_TIKV_DRIFT_REPORT_SIDECAR_REF,
    TIFLASH_VS_TIKV_DRIFT_REPORT_REF, TIFLASH_VS_TIKV_DRIFT_REPORT_SIDECAR_REF,
};

const WRITE_FLAG: &str = "--write-artifacts";

fn main() {
    if let Err(error) = run() {
        eprintln!("{error}");
        std::process::exit(1);
    }
}

fn run() -> Result<(), String> {
    let write_artifacts = env::args().any(|arg| arg == WRITE_FLAG);

    let bundle = execute_first_temporal_date32_slice_tikv_pairwise(
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

    if write_artifacts {
        fs::write(TIKV_CASE_RESULTS_REF, &tikv_case_results)
            .map_err(|error| format!("failed to write `{TIKV_CASE_RESULTS_REF}`: {error}"))?;
        fs::write(TIDB_VS_TIKV_DRIFT_REPORT_REF, &tidb_vs_tikv_drift_report).map_err(|error| {
            format!("failed to write `{TIDB_VS_TIKV_DRIFT_REPORT_REF}`: {error}")
        })?;
        fs::write(
            TIDB_VS_TIKV_DRIFT_REPORT_SIDECAR_REF,
            &tidb_vs_tikv_drift_report_sidecar,
        )
        .map_err(|error| {
            format!("failed to write `{TIDB_VS_TIKV_DRIFT_REPORT_SIDECAR_REF}`: {error}")
        })?;
        fs::write(
            TIFLASH_VS_TIKV_DRIFT_REPORT_REF,
            &tiflash_vs_tikv_drift_report,
        )
        .map_err(|error| {
            format!("failed to write `{TIFLASH_VS_TIKV_DRIFT_REPORT_REF}`: {error}")
        })?;
        fs::write(
            TIFLASH_VS_TIKV_DRIFT_REPORT_SIDECAR_REF,
            &tiflash_vs_tikv_drift_report_sidecar,
        )
        .map_err(|error| {
            format!("failed to write `{TIFLASH_VS_TIKV_DRIFT_REPORT_SIDECAR_REF}`: {error}")
        })?;

        println!("Updated:");
        println!("- {TIKV_CASE_RESULTS_REF}");
        println!("- {TIDB_VS_TIKV_DRIFT_REPORT_REF}");
        println!("- {TIDB_VS_TIKV_DRIFT_REPORT_SIDECAR_REF}");
        println!("- {TIFLASH_VS_TIKV_DRIFT_REPORT_REF}");
        println!("- {TIFLASH_VS_TIKV_DRIFT_REPORT_SIDECAR_REF}");
        return Ok(());
    }

    println!("Dry run complete. Use `{WRITE_FLAG}` to overwrite inventory artifacts.");
    println!(
        "Connection env prefixes: `{TIDB_MYSQL_ENV_PREFIX}_*`, `{TIFLASH_MYSQL_ENV_PREFIX}_*`, `{TIKV_MYSQL_ENV_PREFIX}_*`."
    );
    println!();
    println!("=== {TIKV_CASE_RESULTS_REF} ===");
    print!("{tikv_case_results}");
    println!("=== {TIDB_VS_TIKV_DRIFT_REPORT_REF} ===");
    print!("{tidb_vs_tikv_drift_report}");
    println!("=== {TIDB_VS_TIKV_DRIFT_REPORT_SIDECAR_REF} ===");
    print!("{tidb_vs_tikv_drift_report_sidecar}");
    println!("=== {TIFLASH_VS_TIKV_DRIFT_REPORT_REF} ===");
    print!("{tiflash_vs_tikv_drift_report}");
    println!("=== {TIFLASH_VS_TIKV_DRIFT_REPORT_SIDECAR_REF} ===");
    print!("{tiflash_vs_tikv_drift_report_sidecar}");

    Ok(())
}
