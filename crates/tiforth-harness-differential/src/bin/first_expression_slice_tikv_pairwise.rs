use std::env;
use std::fs;

use serde_json::{json, Value};
use tiforth_adapter_tidb::first_expression_slice as tidb;
use tiforth_adapter_tiflash::first_expression_slice as tiflash;
use tiforth_adapter_tikv::first_expression_slice as tikv;
use tiforth_harness_differential::first_expression_slice_tikv_pairwise::{
    execute_first_expression_slice_tikv_pairwise, render_drift_report_artifact_json,
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

    let bundle = execute_first_expression_slice_tikv_pairwise(
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
            "column-passthrough" => Ok(rows_result_tidb(
                "a",
                "int",
                false,
                vec![Some(1), Some(2), Some(3)],
            )),
            "literal-int32-seven" => Ok(rows_result_tidb(
                "lit",
                "bigint",
                false,
                vec![Some(7), Some(7), Some(7)],
            )),
            "literal-int32-null" => Ok(rows_result_tidb(
                "lit",
                "bigint",
                true,
                vec![None, None, None],
            )),
            "add-int32-literal" => Ok(rows_result_tidb(
                "a_plus_one",
                "int",
                false,
                vec![Some(2), Some(3), Some(4)],
            )),
            "add-int32-null-propagation" => Ok(rows_result_tidb(
                "a_plus_one",
                "int",
                true,
                vec![Some(2), None, Some(4)],
            )),
            "add-int32-overflow-error" => Err(tidb::EngineExecutionError::AdapterUnavailable {
                message: Some(
                    "TiDB adapter core does not yet narrow the shared int32 overflow boundary."
                        .to_string(),
                ),
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
            "column-passthrough" => Ok(rows_result_tiflash(
                "a",
                "int",
                false,
                vec![Some(1), Some(2), Some(3)],
            )),
            "literal-int32-seven" => Ok(rows_result_tiflash(
                "lit",
                "bigint",
                false,
                vec![Some(7), Some(7), Some(7)],
            )),
            "literal-int32-null" => Ok(rows_result_tiflash(
                "lit",
                "bigint",
                true,
                vec![None, None, None],
            )),
            "add-int32-literal" => Ok(rows_result_tiflash(
                "a_plus_one",
                "int",
                false,
                vec![Some(2), Some(3), Some(4)],
            )),
            "add-int32-null-propagation" => Ok(rows_result_tiflash(
                "a_plus_one",
                "int",
                true,
                vec![Some(2), None, Some(4)],
            )),
            "add-int32-overflow-error" => Err(tiflash::EngineExecutionError::EngineFailure {
                code: Some("1690".to_string()),
                message: "Error 1690 (22003): value is out of range".to_string(),
            }),
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
            "column-passthrough" => Ok(rows_result_tikv(
                "a",
                "int",
                false,
                vec![Some(1), Some(2), Some(3)],
            )),
            "literal-int32-seven" => Ok(rows_result_tikv(
                "lit",
                "bigint",
                false,
                vec![Some(7), Some(7), Some(7)],
            )),
            "literal-int32-null" => Ok(rows_result_tikv(
                "lit",
                "bigint",
                true,
                vec![None, None, None],
            )),
            "add-int32-literal" => Ok(rows_result_tikv(
                "a_plus_one",
                "bigint",
                false,
                vec![Some(2), Some(3), Some(4)],
            )),
            "add-int32-null-propagation" => Ok(rows_result_tikv(
                "a_plus_one",
                "bigint",
                true,
                vec![Some(2), None, Some(4)],
            )),
            "add-int32-overflow-error" => Err(tikv::EngineExecutionError::EngineFailure {
                code: Some("1690".to_string()),
                message: "Error 1690 (22003): value is out of range".to_string(),
            }),
            other => panic!("unexpected case_id: {other}"),
        }
    }
}

fn rows_result_tidb(
    name: &str,
    engine_type: &str,
    nullable: bool,
    values: Vec<Option<i64>>,
) -> tidb::EngineExecutionResult {
    tidb::EngineExecutionResult {
        columns: vec![tidb::EngineColumn {
            name: name.to_string(),
            engine_type: engine_type.to_string(),
            nullable,
        }],
        rows: values
            .into_iter()
            .map(|value| vec![option_i64_to_json(value)])
            .collect(),
    }
}

fn rows_result_tiflash(
    name: &str,
    engine_type: &str,
    nullable: bool,
    values: Vec<Option<i64>>,
) -> tiflash::EngineExecutionResult {
    tiflash::EngineExecutionResult {
        columns: vec![tiflash::EngineColumn {
            name: name.to_string(),
            engine_type: engine_type.to_string(),
            nullable,
        }],
        rows: values
            .into_iter()
            .map(|value| vec![option_i64_to_json(value)])
            .collect(),
    }
}

fn rows_result_tikv(
    name: &str,
    engine_type: &str,
    nullable: bool,
    values: Vec<Option<i64>>,
) -> tikv::EngineExecutionResult {
    tikv::EngineExecutionResult {
        columns: vec![tikv::EngineColumn {
            name: name.to_string(),
            engine_type: engine_type.to_string(),
            nullable,
        }],
        rows: values
            .into_iter()
            .map(|value| vec![option_i64_to_json(value)])
            .collect(),
    }
}

fn option_i64_to_json(value: Option<i64>) -> Value {
    match value {
        Some(value) => json!(value),
        None => Value::Null,
    }
}
