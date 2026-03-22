use std::env;

use crate::cli::pairwise_output::{emit_pairwise_drift_artifacts, PairwiseDriftArtifacts};
use crate::pairwise::first_temporal_date32_slice_tikv_pairwise::{
    execute_first_temporal_date32_slice_tikv_pairwise, render_drift_report_artifact_json,
    render_drift_report_markdown, TIDB_VS_TIKV_DRIFT_REPORT_REF,
    TIDB_VS_TIKV_DRIFT_REPORT_SIDECAR_REF, TIFLASH_VS_TIKV_DRIFT_REPORT_REF,
    TIFLASH_VS_TIKV_DRIFT_REPORT_SIDECAR_REF,
};
use serde_json::{json, Value};
use tiforth_adapter_tidb::first_temporal_date32_slice as tidb;
use tiforth_adapter_tiflash::first_temporal_date32_slice as tiflash;
use tiforth_adapter_tikv::first_temporal_date32_slice as tikv;

const WRITE_FLAG: &str = "--write-artifacts";

pub fn main() {
    if let Err(error) = run() {
        eprintln!("{error}");
        std::process::exit(1);
    }
}

fn run() -> Result<(), String> {
    let write_artifacts = env::args().any(|arg| arg == WRITE_FLAG);

    let bundle = execute_first_temporal_date32_slice_tikv_pairwise(
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

    emit_pairwise_drift_artifacts(
        write_artifacts,
        WRITE_FLAG,
        PairwiseDriftArtifacts {
            tidb_vs_tikv_drift_report_ref: TIDB_VS_TIKV_DRIFT_REPORT_REF,
            tidb_vs_tikv_drift_report: &tidb_vs_tikv_drift_report,
            tidb_vs_tikv_drift_report_sidecar_ref: TIDB_VS_TIKV_DRIFT_REPORT_SIDECAR_REF,
            tidb_vs_tikv_drift_report_sidecar: &tidb_vs_tikv_drift_report_sidecar,
            tiflash_vs_tikv_drift_report_ref: TIFLASH_VS_TIKV_DRIFT_REPORT_REF,
            tiflash_vs_tikv_drift_report: &tiflash_vs_tikv_drift_report,
            tiflash_vs_tikv_drift_report_sidecar_ref: TIFLASH_VS_TIKV_DRIFT_REPORT_SIDECAR_REF,
            tiflash_vs_tikv_drift_report_sidecar: &tiflash_vs_tikv_drift_report_sidecar,
        },
    )
}

struct FixtureTidbRunner;

impl tidb::TidbRunner for FixtureTidbRunner {
    fn run(
        &self,
        plan: &tidb::TidbExecutionPlan,
    ) -> Result<tidb::EngineExecutionResult, tidb::EngineExecutionError> {
        match plan.request.case_id.as_str() {
            "date32-column-passthrough" => Ok(rows_result_tidb(
                vec![tidb::EngineColumn {
                    name: "d".to_string(),
                    engine_type: "date".to_string(),
                    nullable: false,
                }],
                vec![vec![json!(0)], vec![json!(1)], vec![json!(2)]],
            )),
            "date32-column-null-preserve" => Ok(rows_result_tidb(
                vec![tidb::EngineColumn {
                    name: "d".to_string(),
                    engine_type: "date".to_string(),
                    nullable: true,
                }],
                vec![
                    vec![json!(0)],
                    vec![Value::Null],
                    vec![json!(2)],
                    vec![Value::Null],
                ],
            )),
            "date32-is-not-null-all-kept" => Ok(rows_result_tidb(
                vec![tidb::EngineColumn {
                    name: "d".to_string(),
                    engine_type: "date".to_string(),
                    nullable: false,
                }],
                vec![vec![json!(0)], vec![json!(1)], vec![json!(2)]],
            )),
            "date32-is-not-null-all-dropped" => Ok(rows_result_tidb(
                vec![tidb::EngineColumn {
                    name: "d".to_string(),
                    engine_type: "date".to_string(),
                    nullable: true,
                }],
                vec![],
            )),
            "date32-is-not-null-mixed-keep-drop" => Ok(rows_result_tidb(
                vec![tidb::EngineColumn {
                    name: "d".to_string(),
                    engine_type: "date".to_string(),
                    nullable: true,
                }],
                vec![vec![json!(0)], vec![json!(2)]],
            )),
            "date32-missing-column-error" => Err(tidb::EngineExecutionError::EngineFailure {
                code: Some("1054".to_string()),
                message: "Unknown column '__missing_column_1' in 'where clause'".to_string(),
            }),
            "unsupported-temporal-type-error" => Err(tidb::EngineExecutionError::EngineFailure {
                code: Some("1105".to_string()),
                message:
                    "unsupported temporal type: timestamp input is out of scope for first-temporal-date32-slice"
                        .to_string(),
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
            "date32-column-passthrough" => Ok(rows_result_tiflash(
                vec![tiflash::EngineColumn {
                    name: "d".to_string(),
                    engine_type: "date".to_string(),
                    nullable: false,
                }],
                vec![vec![json!(0)], vec![json!(1)], vec![json!(2)]],
            )),
            "date32-column-null-preserve" => Ok(rows_result_tiflash(
                vec![tiflash::EngineColumn {
                    name: "d".to_string(),
                    engine_type: "date".to_string(),
                    nullable: true,
                }],
                vec![
                    vec![json!(0)],
                    vec![Value::Null],
                    vec![json!(2)],
                    vec![Value::Null],
                ],
            )),
            "date32-is-not-null-all-kept" => Ok(rows_result_tiflash(
                vec![tiflash::EngineColumn {
                    name: "d".to_string(),
                    engine_type: "date".to_string(),
                    nullable: false,
                }],
                vec![vec![json!(0)], vec![json!(1)], vec![json!(2)]],
            )),
            "date32-is-not-null-all-dropped" => Ok(rows_result_tiflash(
                vec![tiflash::EngineColumn {
                    name: "d".to_string(),
                    engine_type: "date".to_string(),
                    nullable: true,
                }],
                vec![],
            )),
            "date32-is-not-null-mixed-keep-drop" => Ok(rows_result_tiflash(
                vec![tiflash::EngineColumn {
                    name: "d".to_string(),
                    engine_type: "date".to_string(),
                    nullable: true,
                }],
                vec![vec![json!(0)], vec![json!(2)]],
            )),
            "date32-missing-column-error" => {
                Err(tiflash::EngineExecutionError::EngineFailure {
                    code: Some("1054".to_string()),
                    message: "Unknown column '__missing_column_1' in 'where clause'"
                        .to_string(),
                })
            }
            "unsupported-temporal-type-error" => {
                Err(tiflash::EngineExecutionError::EngineFailure {
                    code: Some("1105".to_string()),
                    message:
                        "unsupported temporal type: timestamp input is out of scope for first-temporal-date32-slice"
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
            "date32-column-passthrough" => Ok(rows_result_tikv(
                vec![tikv::EngineColumn {
                    name: "d".to_string(),
                    engine_type: "date".to_string(),
                    nullable: false,
                }],
                vec![vec![json!(0)], vec![json!(1)], vec![json!(2)]],
            )),
            "date32-column-null-preserve" => Ok(rows_result_tikv(
                vec![tikv::EngineColumn {
                    name: "d".to_string(),
                    engine_type: "date".to_string(),
                    nullable: true,
                }],
                vec![
                    vec![json!(0)],
                    vec![Value::Null],
                    vec![json!(2)],
                    vec![Value::Null],
                ],
            )),
            "date32-is-not-null-all-kept" => Ok(rows_result_tikv(
                vec![tikv::EngineColumn {
                    name: "d".to_string(),
                    engine_type: "date".to_string(),
                    nullable: false,
                }],
                vec![vec![json!(0)], vec![json!(1)], vec![json!(2)]],
            )),
            "date32-is-not-null-all-dropped" => Ok(rows_result_tikv(
                vec![tikv::EngineColumn {
                    name: "d".to_string(),
                    engine_type: "date".to_string(),
                    nullable: true,
                }],
                vec![],
            )),
            "date32-is-not-null-mixed-keep-drop" => Ok(rows_result_tikv(
                vec![tikv::EngineColumn {
                    name: "d".to_string(),
                    engine_type: "date".to_string(),
                    nullable: true,
                }],
                vec![vec![json!(0)], vec![json!(2)]],
            )),
            "date32-missing-column-error" => Err(tikv::EngineExecutionError::EngineFailure {
                code: Some("1054".to_string()),
                message: "Unknown column '__missing_column_1' in 'where clause'".to_string(),
            }),
            "unsupported-temporal-type-error" => Err(tikv::EngineExecutionError::EngineFailure {
                code: Some("1105".to_string()),
                message:
                    "unsupported temporal type: timestamp input is out of scope for first-temporal-date32-slice"
                        .to_string(),
            }),
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
