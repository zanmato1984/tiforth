use serde::{Deserialize, Serialize};
use tiforth_adapter_tikv::first_decimal128_slice as tikv;

pub const TIKV_CASE_RESULTS_REF: &str = "inventory/first-decimal128-slice-tikv-case-results.json";

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
    tikv::TikvFirstDecimal128SliceAdapter::canonical_requests()
}

pub fn execute_first_decimal128_slice_tikv<R: tikv::TikvRunner>(
    tikv_runner: &R,
) -> Result<CaseResultsArtifact, HarnessError> {
    let requests = canonical_requests();
    let mut cases = Vec::with_capacity(requests.len());

    for request in requests {
        let case_id = request.case_id.clone();
        let result = tikv::TikvFirstDecimal128SliceAdapter::execute(&request, tikv_runner)
            .map_err(|error| HarnessError::TikvAdapterValidation {
                case_id,
                error: format!("{error:?}"),
            })?;
        cases.push(result);
    }

    Ok(CaseResultsArtifact {
        slice_id: tikv::FIRST_DECIMAL128_SLICE_ID.to_string(),
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
        "/../../inventory/first-decimal128-slice-tikv-case-results.json"
    );

    #[test]
    fn canonical_requests_cover_all_documented_cases() {
        let requests = canonical_requests();
        let case_ids: Vec<&str> = requests
            .iter()
            .map(|request| request.case_id.as_str())
            .collect();

        assert_eq!(requests.len(), 8);
        assert_eq!(
            case_ids,
            vec![
                "decimal128-column-passthrough",
                "decimal128-column-null-preserve",
                "decimal128-is-not-null-all-kept",
                "decimal128-is-not-null-all-dropped",
                "decimal128-is-not-null-mixed-keep-drop",
                "decimal128-missing-column-error",
                "unsupported-decimal-type-error",
                "invalid-decimal-metadata-error",
            ]
        );
    }

    #[test]
    fn checked_in_artifact_matches_fixture_harness_output() {
        let artifact = execute_first_decimal128_slice_tikv(&FixtureTikvRunner).unwrap();

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
                "decimal128-column-passthrough" => Ok(rows_result(
                    vec![tikv::EngineColumn {
                        name: "d".to_string(),
                        engine_type: "decimal(10,2)".to_string(),
                        nullable: false,
                    }],
                    vec![
                        vec![json!("1.00")],
                        vec![json!("2.50")],
                        vec![json!("-3.75")],
                    ],
                )),
                "decimal128-column-null-preserve" => Ok(rows_result(
                    vec![tikv::EngineColumn {
                        name: "d".to_string(),
                        engine_type: "decimal(10,2)".to_string(),
                        nullable: true,
                    }],
                    vec![
                        vec![json!("1.00")],
                        vec![Value::Null],
                        vec![json!("-3.75")],
                        vec![Value::Null],
                    ],
                )),
                "decimal128-is-not-null-all-kept" => Ok(rows_result(
                    vec![tikv::EngineColumn {
                        name: "d".to_string(),
                        engine_type: "decimal(10,2)".to_string(),
                        nullable: false,
                    }],
                    vec![
                        vec![json!("1.00")],
                        vec![json!("2.50")],
                        vec![json!("-3.75")],
                    ],
                )),
                "decimal128-is-not-null-all-dropped" => Ok(rows_result(
                    vec![tikv::EngineColumn {
                        name: "d".to_string(),
                        engine_type: "decimal(10,2)".to_string(),
                        nullable: true,
                    }],
                    vec![],
                )),
                "decimal128-is-not-null-mixed-keep-drop" => Ok(rows_result(
                    vec![tikv::EngineColumn {
                        name: "d".to_string(),
                        engine_type: "decimal(10,2)".to_string(),
                        nullable: true,
                    }],
                    vec![vec![json!("1.00")], vec![json!("-3.75")]],
                )),
                "decimal128-missing-column-error" => {
                    Err(tikv::EngineExecutionError::EngineFailure {
                        code: Some("1054".to_string()),
                        message: "Unknown column '__missing_column_1' in 'where clause'"
                            .to_string(),
                    })
                }
                "unsupported-decimal-type-error" => {
                    Err(tikv::EngineExecutionError::EngineFailure {
                        code: Some("1105".to_string()),
                        message: "unsupported decimal type: decimal256 input is out of scope for first-decimal128-slice"
                            .to_string(),
                    })
                }
                "invalid-decimal-metadata-error" => {
                    Err(tikv::EngineExecutionError::EngineFailure {
                        code: Some("1105".to_string()),
                        message: "invalid decimal metadata: decimal(10,12) has scale greater than precision"
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
