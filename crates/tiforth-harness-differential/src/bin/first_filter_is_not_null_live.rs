use std::env;
use std::fs;

use tiforth_harness_differential::first_filter_is_not_null_live::{
    render_live_drift_report_markdown, LiveTidbRunner, LiveTiflashRunner, TIDB_MYSQL_ENV_PREFIX,
    TIFLASH_MYSQL_ENV_PREFIX,
};
use tiforth_harness_differential::first_filter_is_not_null_slice::{
    execute_first_filter_is_not_null_slice, render_case_results_artifact_json, DRIFT_REPORT_REF,
    TIDB_CASE_RESULTS_REF, TIFLASH_CASE_RESULTS_REF,
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

    if write_artifacts {
        fs::write(TIDB_CASE_RESULTS_REF, &tidb_case_results)
            .map_err(|error| format!("failed to write `{TIDB_CASE_RESULTS_REF}`: {error}"))?;
        fs::write(TIFLASH_CASE_RESULTS_REF, &tiflash_case_results)
            .map_err(|error| format!("failed to write `{TIFLASH_CASE_RESULTS_REF}`: {error}"))?;
        fs::write(DRIFT_REPORT_REF, &drift_report)
            .map_err(|error| format!("failed to write `{DRIFT_REPORT_REF}`: {error}"))?;

        println!("Updated:");
        println!("- {TIDB_CASE_RESULTS_REF}");
        println!("- {TIFLASH_CASE_RESULTS_REF}");
        println!("- {DRIFT_REPORT_REF}");
        return Ok(());
    }

    println!("Dry run complete. Use `{WRITE_FLAG}` to overwrite inventory artifacts.");
    println!(
        "Connection env prefixes: `{TIDB_MYSQL_ENV_PREFIX}_*`, `{TIFLASH_MYSQL_ENV_PREFIX}_*`."
    );
    println!();
    println!("=== {TIDB_CASE_RESULTS_REF} ===");
    print!("{tidb_case_results}");
    println!("=== {TIFLASH_CASE_RESULTS_REF} ===");
    print!("{tiflash_case_results}");
    println!("=== {DRIFT_REPORT_REF} ===");
    print!("{drift_report}");

    Ok(())
}
