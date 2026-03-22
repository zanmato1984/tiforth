use std::env;
use std::fs;

use crate::slices::first_exchange_slice::{
    execute_first_exchange_slice, render_exchange_parity_artifact_json,
    render_exchange_parity_markdown, DRIFT_REPORT_REF, DRIFT_REPORT_SIDECAR_REF,
};
use serde_json::{json, Value};
use tiforth_adapter_tidb::{
    first_expression_slice as tidb_expression, first_filter_is_not_null_slice as tidb_filter,
};
use tiforth_adapter_tiflash::{
    first_expression_slice as tiflash_expression, first_filter_is_not_null_slice as tiflash_filter,
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

    let report = execute_first_exchange_slice(
        &FixtureTidbRunner,
        &FixtureTiflashRunner,
        &FixtureTidbRunner,
        &FixtureTiflashRunner,
    )
    .map_err(|error| format!("exchange parity harness execution failed: {error:?}"))?;

    let drift_report = render_exchange_parity_markdown(&report);
    let drift_report_sidecar = render_exchange_parity_artifact_json(&report)
        .map_err(|error| format!("failed to render exchange parity sidecar as JSON: {error}"))?;

    if write_artifacts {
        fs::write(DRIFT_REPORT_REF, &drift_report)
            .map_err(|error| format!("failed to write `{DRIFT_REPORT_REF}`: {error}"))?;
        fs::write(DRIFT_REPORT_SIDECAR_REF, &drift_report_sidecar)
            .map_err(|error| format!("failed to write `{DRIFT_REPORT_SIDECAR_REF}`: {error}"))?;
    }

    Ok(())
}

struct FixtureTidbRunner;

impl tidb_expression::TidbRunner for FixtureTidbRunner {
    fn run(
        &self,
        plan: &tidb_expression::TidbExecutionPlan,
    ) -> Result<tidb_expression::EngineExecutionResult, tidb_expression::EngineExecutionError> {
        match plan.request.case_id.as_str() {
            "column-passthrough" => Ok(rows_result_tidb_expression(
                "a",
                "int",
                false,
                vec![Some(1), Some(2), Some(3)],
            )),
            "literal-int32-seven" => Ok(rows_result_tidb_expression(
                "lit",
                "bigint",
                false,
                vec![Some(7), Some(7), Some(7)],
            )),
            "literal-int32-null" => Ok(rows_result_tidb_expression(
                "lit",
                "bigint",
                true,
                vec![None, None, None],
            )),
            "add-int32-literal" => Ok(rows_result_tidb_expression(
                "a_plus_one",
                "int",
                false,
                vec![Some(2), Some(3), Some(4)],
            )),
            "add-int32-null-propagation" => Ok(rows_result_tidb_expression(
                "a_plus_one",
                "int",
                true,
                vec![Some(2), None, Some(4)],
            )),
            "add-int32-overflow-error" => {
                Err(tidb_expression::EngineExecutionError::AdapterUnavailable {
                    message: Some(
                        "TiDB adapter core does not yet narrow the shared int32 overflow boundary."
                            .to_string(),
                    ),
                })
            }
            other => panic!("unexpected case_id: {other}"),
        }
    }
}

impl tidb_filter::TidbRunner for FixtureTidbRunner {
    fn run(
        &self,
        plan: &tidb_filter::TidbExecutionPlan,
    ) -> Result<tidb_filter::EngineExecutionResult, tidb_filter::EngineExecutionError> {
        match plan.request.case_id.as_str() {
            "all-rows-kept" => Ok(rows_result_tidb_filter(
                vec![tidb_filter::EngineColumn {
                    name: "a".to_string(),
                    engine_type: "int".to_string(),
                    nullable: false,
                }],
                vec![vec![json!(1)], vec![json!(2)], vec![json!(3)]],
            )),
            "all-rows-dropped" => Ok(rows_result_tidb_filter(
                vec![tidb_filter::EngineColumn {
                    name: "a".to_string(),
                    engine_type: "int".to_string(),
                    nullable: true,
                }],
                vec![],
            )),
            "mixed-keep-drop" => Ok(rows_result_tidb_filter(
                vec![
                    tidb_filter::EngineColumn {
                        name: "a".to_string(),
                        engine_type: "int".to_string(),
                        nullable: true,
                    },
                    tidb_filter::EngineColumn {
                        name: "b".to_string(),
                        engine_type: "int".to_string(),
                        nullable: false,
                    },
                ],
                vec![vec![json!(1), json!(10)], vec![json!(3), json!(30)]],
            )),
            "missing-column-error" => Err(tidb_filter::EngineExecutionError::EngineFailure {
                code: Some("1054".to_string()),
                message: "Unknown column '__missing_column_1' in 'where clause'".to_string(),
            }),
            "unsupported-predicate-type-error" => {
                Err(tidb_filter::EngineExecutionError::EngineFailure {
                    code: Some("1105".to_string()),
                    message: "unsupported predicate type: utf8 input is out of scope for first-filter-is-not-null-slice".to_string(),
                })
            }
            other => panic!("unexpected case_id: {other}"),
        }
    }
}

struct FixtureTiflashRunner;

impl tiflash_expression::TiflashRunner for FixtureTiflashRunner {
    fn run(
        &self,
        plan: &tiflash_expression::TiflashExecutionPlan,
    ) -> Result<tiflash_expression::EngineExecutionResult, tiflash_expression::EngineExecutionError>
    {
        match plan.request.case_id.as_str() {
            "column-passthrough" => Ok(rows_result_tiflash_expression(
                "a",
                "int",
                false,
                vec![Some(1), Some(2), Some(3)],
            )),
            "literal-int32-seven" => Ok(rows_result_tiflash_expression(
                "lit",
                "bigint",
                false,
                vec![Some(7), Some(7), Some(7)],
            )),
            "literal-int32-null" => Ok(rows_result_tiflash_expression(
                "lit",
                "bigint",
                true,
                vec![None, None, None],
            )),
            "add-int32-literal" => Ok(rows_result_tiflash_expression(
                "a_plus_one",
                "int",
                false,
                vec![Some(2), Some(3), Some(4)],
            )),
            "add-int32-null-propagation" => Ok(rows_result_tiflash_expression(
                "a_plus_one",
                "int",
                true,
                vec![Some(2), None, Some(4)],
            )),
            "add-int32-overflow-error" => {
                Err(tiflash_expression::EngineExecutionError::EngineFailure {
                    code: Some("1690".to_string()),
                    message: "BIGINT value is out of range in 'input_rows.a + cast(1 as signed)'"
                        .to_string(),
                })
            }
            other => panic!("unexpected case_id: {other}"),
        }
    }
}

impl tiflash_filter::TiflashRunner for FixtureTiflashRunner {
    fn run(
        &self,
        plan: &tiflash_filter::TiflashExecutionPlan,
    ) -> Result<tiflash_filter::EngineExecutionResult, tiflash_filter::EngineExecutionError> {
        match plan.request.case_id.as_str() {
            "all-rows-kept" => Ok(rows_result_tiflash_filter(
                vec![tiflash_filter::EngineColumn {
                    name: "a".to_string(),
                    engine_type: "int".to_string(),
                    nullable: false,
                }],
                vec![vec![json!(1)], vec![json!(2)], vec![json!(3)]],
            )),
            "all-rows-dropped" => Ok(rows_result_tiflash_filter(
                vec![tiflash_filter::EngineColumn {
                    name: "a".to_string(),
                    engine_type: "int".to_string(),
                    nullable: true,
                }],
                vec![],
            )),
            "mixed-keep-drop" => Ok(rows_result_tiflash_filter(
                vec![
                    tiflash_filter::EngineColumn {
                        name: "a".to_string(),
                        engine_type: "int".to_string(),
                        nullable: true,
                    },
                    tiflash_filter::EngineColumn {
                        name: "b".to_string(),
                        engine_type: "int".to_string(),
                        nullable: false,
                    },
                ],
                vec![vec![json!(1), json!(10)], vec![json!(3), json!(30)]],
            )),
            "missing-column-error" => {
                Err(tiflash_filter::EngineExecutionError::EngineFailure {
                    code: Some("1054".to_string()),
                    message: "Unknown column '__missing_column_1' in 'where clause'".to_string(),
                })
            }
            "unsupported-predicate-type-error" => {
                Err(tiflash_filter::EngineExecutionError::EngineFailure {
                    code: Some("1105".to_string()),
                    message: "unsupported predicate type: utf8 input is out of scope for first-filter-is-not-null-slice".to_string(),
                })
            }
            other => panic!("unexpected case_id: {other}"),
        }
    }
}

fn rows_result_tidb_expression(
    name: &str,
    engine_type: &str,
    nullable: bool,
    values: Vec<Option<i64>>,
) -> tidb_expression::EngineExecutionResult {
    tidb_expression::EngineExecutionResult {
        columns: vec![tidb_expression::EngineColumn {
            name: name.to_string(),
            engine_type: engine_type.to_string(),
            nullable,
        }],
        rows: values
            .into_iter()
            .map(|value| vec![option_i64_to_value(value)])
            .collect(),
    }
}

fn rows_result_tiflash_expression(
    name: &str,
    engine_type: &str,
    nullable: bool,
    values: Vec<Option<i64>>,
) -> tiflash_expression::EngineExecutionResult {
    tiflash_expression::EngineExecutionResult {
        columns: vec![tiflash_expression::EngineColumn {
            name: name.to_string(),
            engine_type: engine_type.to_string(),
            nullable,
        }],
        rows: values
            .into_iter()
            .map(|value| vec![option_i64_to_value(value)])
            .collect(),
    }
}

fn rows_result_tidb_filter(
    columns: Vec<tidb_filter::EngineColumn>,
    rows: Vec<Vec<Value>>,
) -> tidb_filter::EngineExecutionResult {
    tidb_filter::EngineExecutionResult { columns, rows }
}

fn rows_result_tiflash_filter(
    columns: Vec<tiflash_filter::EngineColumn>,
    rows: Vec<Vec<Value>>,
) -> tiflash_filter::EngineExecutionResult {
    tiflash_filter::EngineExecutionResult { columns, rows }
}

fn option_i64_to_value(value: Option<i64>) -> Value {
    match value {
        Some(value) => Value::from(value),
        None => Value::Null,
    }
}
