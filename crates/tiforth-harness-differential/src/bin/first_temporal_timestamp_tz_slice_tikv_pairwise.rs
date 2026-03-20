use std::env;
use std::fs;

use serde_json::{json, Value};
use tiforth_adapter_tidb::first_temporal_timestamp_tz_slice as tidb;
use tiforth_adapter_tiflash::first_temporal_timestamp_tz_slice as tiflash;
use tiforth_adapter_tikv::first_temporal_timestamp_tz_slice as tikv;
use tiforth_harness_differential::first_temporal_timestamp_tz_slice_tikv_pairwise::{
    execute_first_temporal_timestamp_tz_slice_tikv_pairwise, render_drift_report_artifact_json,
    render_drift_report_markdown, TIDB_VS_TIKV_DRIFT_REPORT_REF,
    TIDB_VS_TIKV_DRIFT_REPORT_SIDECAR_REF, TIFLASH_VS_TIKV_DRIFT_REPORT_REF,
    TIFLASH_VS_TIKV_DRIFT_REPORT_SIDECAR_REF,
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

    let bundle = execute_first_temporal_timestamp_tz_slice_tikv_pairwise(
        &FixtureTidbRunner,
        &FixtureTiflashRunner,
        &FixtureTikvRunner,
    )
    .map_err(|error| format!("pairwise harness execution failed: {error:?}"))?;

    let tidb_vs_tikv_drift_report = render_drift_report_markdown(&bundle.tidb_vs_tikv_drift_report);
    let tidb_vs_tikv_drift_report_sidecar =
        render_drift_report_artifact_json(&bundle.tidb_vs_tikv_drift_report)
            .map_err(|error| format!("failed to render TiDB-vs-TiKV sidecar as JSON: {error}"))?;
    let tiflash_vs_tikv_drift_report =
        render_drift_report_markdown(&bundle.tiflash_vs_tikv_drift_report);
    let tiflash_vs_tikv_drift_report_sidecar = render_drift_report_artifact_json(
        &bundle.tiflash_vs_tikv_drift_report,
    )
    .map_err(|error| format!("failed to render TiFlash-vs-TiKV sidecar as JSON: {error}"))?;

    if write_artifacts {
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
        println!("- {TIDB_VS_TIKV_DRIFT_REPORT_REF}");
        println!("- {TIDB_VS_TIKV_DRIFT_REPORT_SIDECAR_REF}");
        println!("- {TIFLASH_VS_TIKV_DRIFT_REPORT_REF}");
        println!("- {TIFLASH_VS_TIKV_DRIFT_REPORT_SIDECAR_REF}");
        return Ok(());
    }

    println!("Dry run complete. Use `{WRITE_FLAG}` to overwrite inventory artifacts.");
    println!();
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

struct FixtureTidbRunner;

impl tidb::TidbRunner for FixtureTidbRunner {
    fn run(
        &self,
        plan: &tidb::TidbExecutionPlan,
    ) -> Result<tidb::EngineExecutionResult, tidb::EngineExecutionError> {
        match plan.request.case_id.as_str() {
            "timestamp-tz-column-passthrough" => Ok(rows_result_tidb(
                vec![tidb::EngineColumn {
                    name: "ts".to_string(),
                    engine_type: "timestamp_tz(us)".to_string(),
                    nullable: false,
                }],
                vec![vec![json!(0)], vec![json!(1_000_000)], vec![json!(2_000_000)]],
            )),
            "timestamp-tz-equivalent-instant-normalization" => Ok(rows_result_tidb(
                vec![tidb::EngineColumn {
                    name: "ts".to_string(),
                    engine_type: "timestamp_tz(us)".to_string(),
                    nullable: false,
                }],
                vec![
                    vec![json!(1_704_067_200_000_000_i64)],
                    vec![json!(1_704_067_200_000_000_i64)],
                    vec![json!(1_704_067_200_000_000_i64)],
                ],
            )),
            "timestamp-tz-column-null-preserve" => Ok(rows_result_tidb(
                vec![tidb::EngineColumn {
                    name: "ts".to_string(),
                    engine_type: "timestamp_tz(us)".to_string(),
                    nullable: true,
                }],
                vec![
                    vec![json!(0)],
                    vec![Value::Null],
                    vec![json!(2_000_000)],
                    vec![Value::Null],
                ],
            )),
            "timestamp-tz-is-not-null-all-kept" => Ok(rows_result_tidb(
                vec![tidb::EngineColumn {
                    name: "ts".to_string(),
                    engine_type: "timestamp_tz(us)".to_string(),
                    nullable: false,
                }],
                vec![vec![json!(0)], vec![json!(1_000_000)], vec![json!(2_000_000)]],
            )),
            "timestamp-tz-is-not-null-all-dropped" => Ok(rows_result_tidb(
                vec![tidb::EngineColumn {
                    name: "ts".to_string(),
                    engine_type: "timestamp_tz(us)".to_string(),
                    nullable: true,
                }],
                vec![],
            )),
            "timestamp-tz-is-not-null-mixed-keep-drop" => Ok(rows_result_tidb(
                vec![tidb::EngineColumn {
                    name: "ts".to_string(),
                    engine_type: "timestamp_tz(us)".to_string(),
                    nullable: true,
                }],
                vec![vec![json!(0)], vec![json!(2_000_000)]],
            )),
            "timestamp-tz-order-asc-nulls-last" => Ok(rows_result_tidb(
                vec![tidb::EngineColumn {
                    name: "ts".to_string(),
                    engine_type: "timestamp_tz(us)".to_string(),
                    nullable: true,
                }],
                vec![
                    vec![json!(0)],
                    vec![json!(2_000_000)],
                    vec![Value::Null],
                    vec![Value::Null],
                ],
            )),
            "timestamp-tz-missing-column-error" => Err(tidb::EngineExecutionError::EngineFailure {
                code: Some("1054".to_string()),
                message: "Unknown column '__missing_column_1' in 'where clause'".to_string(),
            }),
            "unsupported-temporal-timestamp-without-timezone-error" => {
                Err(tidb::EngineExecutionError::EngineFailure {
                    code: Some("1105".to_string()),
                    message: "unsupported temporal type: timestamp without timezone input is out of scope for first-temporal-timestamp-tz-slice".to_string(),
                })
            }
            "unsupported-temporal-timestamp-unit-error" => {
                Err(tidb::EngineExecutionError::EngineFailure {
                    code: Some("1105".to_string()),
                    message: "unsupported temporal unit: timestamp_tz(ms) input is out of scope for first-temporal-timestamp-tz-slice".to_string(),
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
            "timestamp-tz-column-passthrough" => Ok(rows_result_tiflash(
                vec![tiflash::EngineColumn {
                    name: "ts".to_string(),
                    engine_type: "timestamp_tz(us)".to_string(),
                    nullable: false,
                }],
                vec![vec![json!(0)], vec![json!(1_000_000)], vec![json!(2_000_000)]],
            )),
            "timestamp-tz-equivalent-instant-normalization" => Ok(rows_result_tiflash(
                vec![tiflash::EngineColumn {
                    name: "ts".to_string(),
                    engine_type: "timestamp_tz(us)".to_string(),
                    nullable: false,
                }],
                vec![
                    vec![json!(1_704_067_200_000_000_i64)],
                    vec![json!(1_704_067_200_000_000_i64)],
                    vec![json!(1_704_067_200_000_000_i64)],
                ],
            )),
            "timestamp-tz-column-null-preserve" => Ok(rows_result_tiflash(
                vec![tiflash::EngineColumn {
                    name: "ts".to_string(),
                    engine_type: "timestamp_tz(us)".to_string(),
                    nullable: true,
                }],
                vec![
                    vec![json!(0)],
                    vec![Value::Null],
                    vec![json!(2_000_000)],
                    vec![Value::Null],
                ],
            )),
            "timestamp-tz-is-not-null-all-kept" => Ok(rows_result_tiflash(
                vec![tiflash::EngineColumn {
                    name: "ts".to_string(),
                    engine_type: "timestamp_tz(us)".to_string(),
                    nullable: false,
                }],
                vec![vec![json!(0)], vec![json!(1_000_000)], vec![json!(2_000_000)]],
            )),
            "timestamp-tz-is-not-null-all-dropped" => Ok(rows_result_tiflash(
                vec![tiflash::EngineColumn {
                    name: "ts".to_string(),
                    engine_type: "timestamp_tz(us)".to_string(),
                    nullable: true,
                }],
                vec![],
            )),
            "timestamp-tz-is-not-null-mixed-keep-drop" => Ok(rows_result_tiflash(
                vec![tiflash::EngineColumn {
                    name: "ts".to_string(),
                    engine_type: "timestamp_tz(us)".to_string(),
                    nullable: true,
                }],
                vec![vec![json!(0)], vec![json!(2_000_000)]],
            )),
            "timestamp-tz-order-asc-nulls-last" => Ok(rows_result_tiflash(
                vec![tiflash::EngineColumn {
                    name: "ts".to_string(),
                    engine_type: "timestamp_tz(us)".to_string(),
                    nullable: true,
                }],
                vec![
                    vec![json!(0)],
                    vec![json!(2_000_000)],
                    vec![Value::Null],
                    vec![Value::Null],
                ],
            )),
            "timestamp-tz-missing-column-error" => {
                Err(tiflash::EngineExecutionError::EngineFailure {
                    code: Some("1054".to_string()),
                    message: "Unknown column '__missing_column_1' in 'where clause'"
                        .to_string(),
                })
            }
            "unsupported-temporal-timestamp-without-timezone-error" => {
                Err(tiflash::EngineExecutionError::EngineFailure {
                    code: Some("1105".to_string()),
                    message: "unsupported temporal type: timestamp without timezone input is out of scope for first-temporal-timestamp-tz-slice".to_string(),
                })
            }
            "unsupported-temporal-timestamp-unit-error" => {
                Err(tiflash::EngineExecutionError::EngineFailure {
                    code: Some("1105".to_string()),
                    message: "unsupported temporal unit: timestamp_tz(ms) input is out of scope for first-temporal-timestamp-tz-slice".to_string(),
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
            "timestamp-tz-column-passthrough" => Ok(rows_result_tikv(
                vec![tikv::EngineColumn {
                    name: "ts".to_string(),
                    engine_type: "timestamp_tz(us)".to_string(),
                    nullable: false,
                }],
                vec![vec![json!(0)], vec![json!(1_000_000)], vec![json!(2_000_000)]],
            )),
            "timestamp-tz-equivalent-instant-normalization" => Ok(rows_result_tikv(
                vec![tikv::EngineColumn {
                    name: "ts".to_string(),
                    engine_type: "timestamp_tz(us)".to_string(),
                    nullable: false,
                }],
                vec![
                    vec![json!(1_704_067_200_000_000_i64)],
                    vec![json!(1_704_067_200_000_000_i64)],
                    vec![json!(1_704_067_200_000_000_i64)],
                ],
            )),
            "timestamp-tz-column-null-preserve" => Ok(rows_result_tikv(
                vec![tikv::EngineColumn {
                    name: "ts".to_string(),
                    engine_type: "timestamp_tz(us)".to_string(),
                    nullable: true,
                }],
                vec![
                    vec![json!(0)],
                    vec![Value::Null],
                    vec![json!(2_000_000)],
                    vec![Value::Null],
                ],
            )),
            "timestamp-tz-is-not-null-all-kept" => Ok(rows_result_tikv(
                vec![tikv::EngineColumn {
                    name: "ts".to_string(),
                    engine_type: "timestamp_tz(us)".to_string(),
                    nullable: false,
                }],
                vec![vec![json!(0)], vec![json!(1_000_000)], vec![json!(2_000_000)]],
            )),
            "timestamp-tz-is-not-null-all-dropped" => Ok(rows_result_tikv(
                vec![tikv::EngineColumn {
                    name: "ts".to_string(),
                    engine_type: "timestamp_tz(us)".to_string(),
                    nullable: true,
                }],
                vec![],
            )),
            "timestamp-tz-is-not-null-mixed-keep-drop" => Ok(rows_result_tikv(
                vec![tikv::EngineColumn {
                    name: "ts".to_string(),
                    engine_type: "timestamp_tz(us)".to_string(),
                    nullable: true,
                }],
                vec![vec![json!(0)], vec![json!(2_000_000)]],
            )),
            "timestamp-tz-order-asc-nulls-last" => Ok(rows_result_tikv(
                vec![tikv::EngineColumn {
                    name: "ts".to_string(),
                    engine_type: "timestamp_tz(us)".to_string(),
                    nullable: true,
                }],
                vec![
                    vec![json!(0)],
                    vec![json!(2_000_000)],
                    vec![Value::Null],
                    vec![Value::Null],
                ],
            )),
            "timestamp-tz-missing-column-error" => Err(tikv::EngineExecutionError::EngineFailure {
                code: Some("1054".to_string()),
                message: "Unknown column '__missing_column_1' in 'where clause'".to_string(),
            }),
            "unsupported-temporal-timestamp-without-timezone-error" => {
                Err(tikv::EngineExecutionError::EngineFailure {
                    code: Some("1105".to_string()),
                    message: "unsupported temporal type: timestamp without timezone input is out of scope for first-temporal-timestamp-tz-slice".to_string(),
                })
            }
            "unsupported-temporal-timestamp-unit-error" => {
                Err(tikv::EngineExecutionError::EngineFailure {
                    code: Some("1105".to_string()),
                    message: "unsupported temporal unit: timestamp_tz(ms) input is out of scope for first-temporal-timestamp-tz-slice".to_string(),
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
