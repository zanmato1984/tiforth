use std::env;
use std::fs;

use crate::slices::first_signed_widening_add_int64_slice::{
    execute_first_signed_widening_add_int64_slice, render_case_results_artifact_json,
    render_drift_report_artifact_json, render_drift_report_markdown, DRIFT_REPORT_REF,
    DRIFT_REPORT_SIDECAR_REF, TIDB_CASE_RESULTS_REF, TIFLASH_CASE_RESULTS_REF,
};
use serde_json::{json, Value};
use tiforth_adapter_tidb::first_signed_widening_add_int64_slice as tidb;
use tiforth_adapter_tiflash::first_signed_widening_add_int64_slice as tiflash;

const WRITE_FLAG: &str = "--write-artifacts";

pub fn main() {
    if let Err(error) = run() {
        eprintln!("{error}");
        std::process::exit(1);
    }
}

fn run() -> Result<(), String> {
    let write_artifacts = env::args().any(|arg| arg == WRITE_FLAG);

    let bundle =
        execute_first_signed_widening_add_int64_slice(&FixtureTidbRunner, &FixtureTiflashRunner)
            .map_err(|error| {
                format!("first-signed-widening-add-int64-slice harness execution failed: {error:?}")
            })?;

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
            "int64-column-passthrough" => Ok(rows_result_tidb(
                vec![tidb::EngineColumn {
                    name: "s64".to_string(),
                    engine_type: "bigint".to_string(),
                    nullable: false,
                }],
                vec![vec![json!(-7)], vec![json!(0)], vec![json!(42)]],
            )),
            "int64-add-basic" => Ok(rows_result_tidb(
                vec![tidb::EngineColumn {
                    name: "sum".to_string(),
                    engine_type: "bigint".to_string(),
                    nullable: false,
                }],
                vec![vec![json!(3)], vec![json!(1)], vec![json!(30)]],
            )),
            "int64-add-null-propagation" => Ok(rows_result_tidb(
                vec![tidb::EngineColumn {
                    name: "sum".to_string(),
                    engine_type: "bigint".to_string(),
                    nullable: true,
                }],
                vec![vec![Value::Null], vec![Value::Null], vec![json!(1)]],
            )),
            "signed-widening-int32-plus-int64" => Ok(rows_result_tidb(
                vec![tidb::EngineColumn {
                    name: "sum".to_string(),
                    engine_type: "bigint".to_string(),
                    nullable: false,
                }],
                vec![vec![json!(3)], vec![json!(1)], vec![json!(300)]],
            )),
            "signed-widening-int64-plus-int32" => Ok(rows_result_tidb(
                vec![tidb::EngineColumn {
                    name: "sum".to_string(),
                    engine_type: "bigint".to_string(),
                    nullable: false,
                }],
                vec![vec![json!(3)], vec![json!(1)], vec![json!(300)]],
            )),
            "int64-add-overflow-error" => Err(tidb::EngineExecutionError::EngineFailure {
                code: Some("1690".to_string()),
                message: "BIGINT value is out of range in '(input_rows.lhs + input_rows.rhs)'"
                    .to_string(),
            }),
            "signed-widening-int32-plus-int64-overflow-error" => {
                Err(tidb::EngineExecutionError::EngineFailure {
                    code: Some("1690".to_string()),
                    message: "BIGINT value is out of range in '(CAST(input_rows.lhs32 AS SIGNED) + input_rows.rhs64)'"
                        .to_string(),
                })
            }
            "int64-missing-column-error" => Err(tidb::EngineExecutionError::EngineFailure {
                code: Some("1054".to_string()),
                message: "Unknown column '__missing_column_1' in 'field list'".to_string(),
            }),
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
            "int64-column-passthrough" => Ok(rows_result_tiflash(
                vec![tiflash::EngineColumn {
                    name: "s64".to_string(),
                    engine_type: "bigint".to_string(),
                    nullable: false,
                }],
                vec![vec![json!(-7)], vec![json!(0)], vec![json!(42)]],
            )),
            "int64-add-basic" => Ok(rows_result_tiflash(
                vec![tiflash::EngineColumn {
                    name: "sum".to_string(),
                    engine_type: "bigint".to_string(),
                    nullable: false,
                }],
                vec![vec![json!(3)], vec![json!(1)], vec![json!(30)]],
            )),
            "int64-add-null-propagation" => Ok(rows_result_tiflash(
                vec![tiflash::EngineColumn {
                    name: "sum".to_string(),
                    engine_type: "bigint".to_string(),
                    nullable: true,
                }],
                vec![vec![Value::Null], vec![Value::Null], vec![json!(1)]],
            )),
            "signed-widening-int32-plus-int64" => Ok(rows_result_tiflash(
                vec![tiflash::EngineColumn {
                    name: "sum".to_string(),
                    engine_type: "bigint".to_string(),
                    nullable: false,
                }],
                vec![vec![json!(3)], vec![json!(1)], vec![json!(300)]],
            )),
            "signed-widening-int64-plus-int32" => Ok(rows_result_tiflash(
                vec![tiflash::EngineColumn {
                    name: "sum".to_string(),
                    engine_type: "bigint".to_string(),
                    nullable: false,
                }],
                vec![vec![json!(3)], vec![json!(1)], vec![json!(300)]],
            )),
            "int64-add-overflow-error" => Err(tiflash::EngineExecutionError::EngineFailure {
                code: Some("1690".to_string()),
                message: "BIGINT value is out of range in '(input_rows.lhs + input_rows.rhs)'"
                    .to_string(),
            }),
            "signed-widening-int32-plus-int64-overflow-error" => {
                Err(tiflash::EngineExecutionError::EngineFailure {
                    code: Some("1690".to_string()),
                    message: "BIGINT value is out of range in '(CAST(input_rows.lhs32 AS SIGNED) + input_rows.rhs64)'"
                        .to_string(),
                })
            }
            "int64-missing-column-error" => {
                Err(tiflash::EngineExecutionError::EngineFailure {
                    code: Some("1054".to_string()),
                    message: "Unknown column '__missing_column_1' in 'field list'".to_string(),
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
