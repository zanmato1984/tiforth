use serde::{Deserialize, Serialize};
use tiforth_adapter_tikv::first_temporal_date32_slice as tikv;

pub const TIKV_CASE_RESULTS_REF: &str =
    "inventory/first-temporal-date32-slice-tikv-case-results.json";

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
    tikv::TikvFirstTemporalDate32SliceAdapter::canonical_requests()
}

pub fn execute_first_temporal_date32_slice_tikv<R: tikv::TikvRunner>(
    tikv_runner: &R,
) -> Result<CaseResultsArtifact, HarnessError> {
    let requests = canonical_requests();
    let mut cases = Vec::with_capacity(requests.len());

    for request in requests {
        let case_id = request.case_id.clone();
        let result = tikv::TikvFirstTemporalDate32SliceAdapter::execute(&request, tikv_runner)
            .map_err(|error| HarnessError::TikvAdapterValidation {
                case_id,
                error: format!("{error:?}"),
            })?;
        cases.push(result);
    }

    Ok(CaseResultsArtifact {
        slice_id: tikv::FIRST_TEMPORAL_DATE32_SLICE_ID.to_string(),
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
        "/../../inventory/first-temporal-date32-slice-tikv-case-results.json"
    );

    #[test]
    fn canonical_requests_cover_all_documented_cases() {
        let requests = canonical_requests();
        let case_ids: Vec<&str> = requests
            .iter()
            .map(|request| request.case_id.as_str())
            .collect();

        assert_eq!(requests.len(), 7);
        assert_eq!(
            case_ids,
            vec![
                "date32-column-passthrough",
                "date32-column-null-preserve",
                "date32-is-not-null-all-kept",
                "date32-is-not-null-all-dropped",
                "date32-is-not-null-mixed-keep-drop",
                "date32-missing-column-error",
                "unsupported-temporal-type-error",
            ]
        );
    }

    #[test]
    fn checked_in_artifact_matches_fixture_harness_output() {
        let artifact = execute_first_temporal_date32_slice_tikv(&FixtureTikvRunner).unwrap();

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
                "date32-column-passthrough" => Ok(rows_result(
                    vec![tikv::EngineColumn {
                        name: "d".to_string(),
                        engine_type: "date".to_string(),
                        nullable: false,
                    }],
                    vec![vec![json!(0)], vec![json!(1)], vec![json!(2)]],
                )),
                "date32-column-null-preserve" => Ok(rows_result(
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
                "date32-is-not-null-all-kept" => Ok(rows_result(
                    vec![tikv::EngineColumn {
                        name: "d".to_string(),
                        engine_type: "date".to_string(),
                        nullable: false,
                    }],
                    vec![vec![json!(0)], vec![json!(1)], vec![json!(2)]],
                )),
                "date32-is-not-null-all-dropped" => Ok(rows_result(
                    vec![tikv::EngineColumn {
                        name: "d".to_string(),
                        engine_type: "date".to_string(),
                        nullable: true,
                    }],
                    vec![],
                )),
                "date32-is-not-null-mixed-keep-drop" => Ok(rows_result(
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
                "unsupported-temporal-type-error" => {
                    Err(tikv::EngineExecutionError::EngineFailure {
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

    fn rows_result(
        columns: Vec<tikv::EngineColumn>,
        rows: Vec<Vec<Value>>,
    ) -> tikv::EngineExecutionResult {
        tikv::EngineExecutionResult { columns, rows }
    }
}
