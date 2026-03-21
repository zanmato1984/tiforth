use std::env;
use std::fs;

use serde_json::{json, Value};
use tiforth_adapter_tidb::first_json_slice as tidb;
use tiforth_adapter_tiflash::first_json_slice as tiflash;
use tiforth_harness_differential::first_json_slice::{
    execute_first_json_slice, render_case_results_artifact_json, render_drift_report_artifact_json,
    render_drift_report_markdown, DRIFT_REPORT_REF, DRIFT_REPORT_SIDECAR_REF,
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

    let bundle = execute_first_json_slice(&FixtureTidbRunner, &FixtureTiflashRunner)
        .map_err(|error| format!("first-json-slice harness execution failed: {error:?}"))?;

    let tidb_case_results = render_case_results_artifact_json(&bundle.tidb_case_results)
        .map_err(|error| format!("failed to render TiDB case-results artifact as JSON: {error}"))?;
    let tiflash_case_results = render_case_results_artifact_json(&bundle.tiflash_case_results)
        .map_err(|error| {
            format!("failed to render TiFlash case-results artifact as JSON: {error}")
        })?;
    let drift_report = render_drift_report_markdown(&bundle.drift_report);
    let drift_report_sidecar = render_drift_report_artifact_json(&bundle.drift_report)
        .map_err(|error| format!("failed to render drift-report sidecar as JSON: {error}"))?;

    if write_artifacts {
        fs::write(TIDB_CASE_RESULTS_REF, &tidb_case_results)
            .map_err(|error| format!("failed to write `{TIDB_CASE_RESULTS_REF}`: {error}"))?;
        fs::write(TIFLASH_CASE_RESULTS_REF, &tiflash_case_results)
            .map_err(|error| format!("failed to write `{TIFLASH_CASE_RESULTS_REF}`: {error}"))?;
        fs::write(DRIFT_REPORT_REF, &drift_report)
            .map_err(|error| format!("failed to write `{DRIFT_REPORT_REF}`: {error}"))?;
        fs::write(DRIFT_REPORT_SIDECAR_REF, &drift_report_sidecar)
            .map_err(|error| format!("failed to write `{DRIFT_REPORT_SIDECAR_REF}`: {error}"))?;
    }

    Ok(())
}

struct FixtureTidbRunner;

impl tidb::TidbRunner for FixtureTidbRunner {
    fn run(
        &self,
        plan: &tidb::TidbExecutionPlan,
    ) -> Result<tidb::EngineExecutionResult, tidb::EngineExecutionError> {
        match plan.request.case_id.as_str() {
            "json-column-passthrough" => Ok(rows_result_tidb(
                vec![tidb::EngineColumn {
                    name: "j".to_string(),
                    engine_type: "json".to_string(),
                    nullable: false,
                }],
                vec![
                    vec![json!("{\"a\":1}")],
                    vec![json!("[1,2]")],
                    vec![json!("true")],
                ],
            )),
            "json-column-null-preserve" => Ok(rows_result_tidb(
                vec![tidb::EngineColumn {
                    name: "j".to_string(),
                    engine_type: "json".to_string(),
                    nullable: true,
                }],
                vec![
                    vec![json!("{\"a\":1}")],
                    vec![json!(null)],
                    vec![json!("null")],
                    vec![json!("{\"b\":2,\"a\":1}")],
                ],
            )),
            "json-object-canonicalization" => Ok(rows_result_tidb(
                vec![tidb::EngineColumn {
                    name: "j".to_string(),
                    engine_type: "json".to_string(),
                    nullable: true,
                }],
                vec![
                    vec![json!("{\"a\":1}")],
                    vec![json!(null)],
                    vec![json!("null")],
                    vec![json!("{\"b\":2,\"a\":1}")],
                ],
            )),
            "json-array-order-preserved" => Ok(rows_result_tidb(
                vec![tidb::EngineColumn {
                    name: "j".to_string(),
                    engine_type: "json".to_string(),
                    nullable: false,
                }],
                vec![vec![json!("[1,2]")], vec![json!("[2,1]")]],
            )),
            "json-is-not-null-all-kept" => Ok(rows_result_tidb(
                vec![tidb::EngineColumn {
                    name: "j".to_string(),
                    engine_type: "json".to_string(),
                    nullable: false,
                }],
                vec![
                    vec![json!("{\"a\":1}")],
                    vec![json!("[1,2]")],
                    vec![json!("true")],
                ],
            )),
            "json-is-not-null-all-dropped" => Ok(rows_result_tidb(
                vec![tidb::EngineColumn {
                    name: "j".to_string(),
                    engine_type: "json".to_string(),
                    nullable: true,
                }],
                vec![],
            )),
            "json-is-not-null-mixed-keep-drop" => Ok(rows_result_tidb(
                vec![tidb::EngineColumn {
                    name: "j".to_string(),
                    engine_type: "json".to_string(),
                    nullable: true,
                }],
                vec![
                    vec![json!("{\"a\":1}")],
                    vec![json!("null")],
                    vec![json!("{\"b\":2,\"a\":1}")],
                ],
            )),
            "json-missing-column-error" => Err(tidb::EngineExecutionError::EngineFailure {
                code: Some("1054".to_string()),
                message: "Unknown column '__missing_column_1' in 'where clause'".to_string(),
            }),
            "unsupported-json-ordering-comparison-error" => {
                Err(tidb::EngineExecutionError::EngineFailure {
                    code: Some("1105".to_string()),
                    message: "unsupported json comparison: ordering probe is out of scope for first-json-slice".to_string(),
                })
            }
            "unsupported-json-cast-error" | "unsupported-cast-to-json-error" => {
                Err(tidb::EngineExecutionError::EngineFailure {
                    code: Some("1105".to_string()),
                    message: "unsupported json cast: explicit cast is out of scope for first-json-slice".to_string(),
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
            "json-column-passthrough" => Ok(rows_result_tiflash(
                vec![tiflash::EngineColumn {
                    name: "j".to_string(),
                    engine_type: "json".to_string(),
                    nullable: false,
                }],
                vec![
                    vec![json!("{\"a\":1}")],
                    vec![json!("[1,2]")],
                    vec![json!("true")],
                ],
            )),
            "json-column-null-preserve" => Ok(rows_result_tiflash(
                vec![tiflash::EngineColumn {
                    name: "j".to_string(),
                    engine_type: "json".to_string(),
                    nullable: true,
                }],
                vec![
                    vec![json!("{\"a\":1}")],
                    vec![json!(null)],
                    vec![json!("null")],
                    vec![json!("{\"a\":1,\"b\":2}")],
                ],
            )),
            "json-object-canonicalization" => Ok(rows_result_tiflash(
                vec![tiflash::EngineColumn {
                    name: "j".to_string(),
                    engine_type: "json".to_string(),
                    nullable: true,
                }],
                vec![
                    vec![json!("{\"a\":1}")],
                    vec![json!(null)],
                    vec![json!("null")],
                    vec![json!("{\"a\":1,\"b\":2}")],
                ],
            )),
            "json-array-order-preserved" => Ok(rows_result_tiflash(
                vec![tiflash::EngineColumn {
                    name: "j".to_string(),
                    engine_type: "json".to_string(),
                    nullable: false,
                }],
                vec![vec![json!("[1,2]")], vec![json!("[2,1]")]],
            )),
            "json-is-not-null-all-kept" => Ok(rows_result_tiflash(
                vec![tiflash::EngineColumn {
                    name: "j".to_string(),
                    engine_type: "json".to_string(),
                    nullable: false,
                }],
                vec![
                    vec![json!("{\"a\":1}")],
                    vec![json!("[1,2]")],
                    vec![json!("true")],
                ],
            )),
            "json-is-not-null-all-dropped" => Ok(rows_result_tiflash(
                vec![tiflash::EngineColumn {
                    name: "j".to_string(),
                    engine_type: "json".to_string(),
                    nullable: true,
                }],
                vec![],
            )),
            "json-is-not-null-mixed-keep-drop" => Ok(rows_result_tiflash(
                vec![tiflash::EngineColumn {
                    name: "j".to_string(),
                    engine_type: "json".to_string(),
                    nullable: true,
                }],
                vec![
                    vec![json!("{\"a\":1}")],
                    vec![json!("null")],
                    vec![json!("{\"a\":1,\"b\":2}")],
                ],
            )),
            "json-missing-column-error" => Err(tiflash::EngineExecutionError::EngineFailure {
                code: Some("1054".to_string()),
                message: "Unknown column '__missing_column_1' in 'where clause'".to_string(),
            }),
            "unsupported-json-ordering-comparison-error" => {
                Err(tiflash::EngineExecutionError::EngineFailure {
                    code: Some("1105".to_string()),
                    message: "unsupported json comparison: ordering probe is out of scope for first-json-slice".to_string(),
                })
            }
            "unsupported-json-cast-error" | "unsupported-cast-to-json-error" => {
                Err(tiflash::EngineExecutionError::EngineFailure {
                    code: Some("1105".to_string()),
                    message: "unsupported json cast: explicit cast is out of scope for first-json-slice".to_string(),
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
