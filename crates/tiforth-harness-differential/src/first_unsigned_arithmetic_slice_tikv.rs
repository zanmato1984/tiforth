use serde::{Deserialize, Serialize};
use tiforth_adapter_tikv::first_unsigned_arithmetic_slice as tikv;

pub const TIKV_CASE_RESULTS_REF: &str =
    "inventory/first-unsigned-arithmetic-slice-tikv-case-results.json";

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HarnessError {
    TikvAdapterValidation { case_id: String, error: String },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CaseResultsArtifact {
    pub slice_id: String,
    pub engine: String,
    pub adapter: String,
    pub cases: Vec<tikv::CaseResult>,
}

pub fn canonical_requests() -> Vec<tikv::AdapterRequest> {
    tikv::TikvFirstUnsignedArithmeticSliceAdapter::canonical_requests()
}

pub fn execute_first_unsigned_arithmetic_slice_tikv<R: tikv::TikvRunner>(
    tikv_runner: &R,
) -> Result<CaseResultsArtifact, HarnessError> {
    let requests = canonical_requests();
    let mut cases = Vec::with_capacity(requests.len());

    for request in requests {
        let case_id = request.case_id.clone();
        let result = tikv::TikvFirstUnsignedArithmeticSliceAdapter::execute(&request, tikv_runner)
            .map_err(|error| HarnessError::TikvAdapterValidation {
                case_id,
                error: format!("{error:?}"),
            })?;
        cases.push(result);
    }

    Ok(CaseResultsArtifact {
        slice_id: tikv::FIRST_UNSIGNED_ARITHMETIC_SLICE_ID.to_string(),
        engine: tikv::TIKV_ENGINE.to_string(),
        adapter: tikv::TIKV_ADAPTER.to_string(),
        cases,
    })
}

pub fn render_case_results_artifact_json(
    artifact: &CaseResultsArtifact,
) -> Result<String, serde_json::Error> {
    let mut rendered = serde_json::to_string_pretty(artifact)?;
    rendered.push('\n');
    Ok(rendered)
}

#[cfg(test)]
mod tests {
    use serde_json::{json, Value};

    use super::*;

    const TIKV_CASE_RESULTS_PATH: &str = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../inventory/first-unsigned-arithmetic-slice-tikv-case-results.json"
    );

    #[test]
    fn canonical_requests_cover_all_documented_cases() {
        let requests = canonical_requests();
        let case_ids: Vec<&str> = requests
            .iter()
            .map(|request| request.case_id.as_str())
            .collect();

        assert_eq!(requests.len(), 9);
        assert_eq!(
            case_ids,
            vec![
                "uint64-column-passthrough",
                "uint64-literal-projection",
                "uint64-add-basic",
                "uint64-add-null-propagation",
                "uint64-add-overflow-error",
                "uint64-is-not-null-mixed-keep-drop",
                "uint64-missing-column-error",
                "mixed-signed-unsigned-arithmetic-error",
                "unsupported-unsigned-family-error",
            ]
        );
    }

    #[test]
    fn checked_in_artifact_matches_fixture_harness_output() {
        let artifact = execute_first_unsigned_arithmetic_slice_tikv(&FixtureTikvRunner).unwrap();

        assert_eq!(
            render_case_results_artifact_json(&artifact).unwrap(),
            std::fs::read_to_string(TIKV_CASE_RESULTS_PATH).unwrap()
        );
    }

    struct FixtureTikvRunner;

    impl tikv::TikvRunner for FixtureTikvRunner {
        fn run(
            &self,
            plan: &tikv::TikvExecutionPlan,
        ) -> Result<tikv::EngineExecutionResult, tikv::EngineExecutionError> {
            match plan.request.case_id.as_str() {
                "uint64-column-passthrough" => Ok(rows_result(
                    vec![tikv::EngineColumn {
                        name: "u".to_string(),
                        engine_type: "bigint unsigned".to_string(),
                        nullable: false,
                    }],
                    vec![vec![json!("0")], vec![json!("7")], vec![json!("42")]],
                )),
                "uint64-literal-projection" => Ok(rows_result(
                    vec![tikv::EngineColumn {
                        name: "seven".to_string(),
                        engine_type: "bigint unsigned".to_string(),
                        nullable: false,
                    }],
                    vec![vec![json!("7")], vec![json!("7")], vec![json!("7")]],
                )),
                "uint64-add-basic" => Ok(rows_result(
                    vec![tikv::EngineColumn {
                        name: "sum".to_string(),
                        engine_type: "bigint unsigned".to_string(),
                        nullable: false,
                    }],
                    vec![vec![json!("3")], vec![json!("7")], vec![json!("30")]],
                )),
                "uint64-add-null-propagation" => Ok(rows_result(
                    vec![tikv::EngineColumn {
                        name: "sum".to_string(),
                        engine_type: "bigint unsigned".to_string(),
                        nullable: true,
                    }],
                    vec![vec![Value::Null], vec![Value::Null], vec![json!("7")]],
                )),
                "uint64-add-overflow-error" => Err(tikv::EngineExecutionError::EngineFailure {
                    code: Some("1690".to_string()),
                    message:
                        "BIGINT UNSIGNED value is out of range in '(input_rows.lhs + input_rows.rhs)'"
                            .to_string(),
                }),
                "uint64-is-not-null-mixed-keep-drop" => Ok(rows_result(
                    vec![tikv::EngineColumn {
                        name: "u".to_string(),
                        engine_type: "bigint unsigned".to_string(),
                        nullable: true,
                    }],
                    vec![vec![json!("5")], vec![json!("9")]],
                )),
                "uint64-missing-column-error" => Err(tikv::EngineExecutionError::EngineFailure {
                    code: Some("1054".to_string()),
                    message: "Unknown column '__missing_column_2' in 'field list'".to_string(),
                }),
                "mixed-signed-unsigned-arithmetic-error" => {
                    Err(tikv::EngineExecutionError::EngineFailure {
                        code: Some("1105".to_string()),
                        message: "mixed signed and unsigned arithmetic is unsupported for first-unsigned-arithmetic-slice; got Int64"
                            .to_string(),
                    })
                }
                "unsupported-unsigned-family-error" => {
                    Err(tikv::EngineExecutionError::EngineFailure {
                        code: Some("1105".to_string()),
                        message: "unsupported unsigned type: uint32 input is out of scope for first-unsigned-arithmetic-slice"
                            .to_string(),
                    })
                }
                other => panic!("unexpected case_id: {other}"),
            }
        }
    }

    fn rows_result(
        columns: Vec<tikv::EngineColumn>,
        rows: Vec<Vec<Value>>,
    ) -> tikv::EngineExecutionResult {
        tikv::EngineExecutionResult { columns, rows }
    }
}
