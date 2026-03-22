use std::env;
use std::fs;

use crate::pairwise::first_filter_is_not_null_slice_tikv_pairwise::{
    execute_first_filter_is_not_null_slice_tikv_pairwise, render_drift_report_artifact_json,
    render_drift_report_markdown, TIDB_VS_TIKV_DRIFT_REPORT_REF,
    TIDB_VS_TIKV_DRIFT_REPORT_SIDECAR_REF, TIFLASH_VS_TIKV_DRIFT_REPORT_REF,
    TIFLASH_VS_TIKV_DRIFT_REPORT_SIDECAR_REF,
};
use serde_json::{json, Value};
use tiforth_adapter_tidb::first_filter_is_not_null_slice as tidb;
use tiforth_adapter_tiflash::first_filter_is_not_null_slice as tiflash;
use tiforth_adapter_tikv::first_filter_is_not_null_slice as tikv;

const WRITE_FLAG: &str = "--write-artifacts";

pub fn main() {
    if let Err(error) = run() {
        eprintln!("{error}");
        std::process::exit(1);
    }
}

fn run() -> Result<(), String> {
    let write_artifacts = env::args().any(|arg| arg == WRITE_FLAG);

    let bundle = execute_first_filter_is_not_null_slice_tikv_pairwise(
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
