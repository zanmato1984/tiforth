use serde::{Deserialize, Serialize};
use tiforth_adapter_tikv::first_temporal_timestamp_tz_slice as tikv;

pub const TIKV_CASE_RESULTS_REF: &str =
    "inventory/first-temporal-timestamp-tz-slice-tikv-case-results.json";

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
    tikv::TikvFirstTemporalTimestampTzSliceAdapter::canonical_requests()
}

pub fn execute_first_temporal_timestamp_tz_slice_tikv<R: tikv::TikvRunner>(
    tikv_runner: &R,
) -> Result<CaseResultsArtifact, HarnessError> {
    let requests = canonical_requests();
    let mut cases = Vec::with_capacity(requests.len());

    for request in requests {
        let case_id = request.case_id.clone();
        let result = tikv::TikvFirstTemporalTimestampTzSliceAdapter::execute(&request, tikv_runner)
            .map_err(|error| HarnessError::TikvAdapterValidation {
                case_id,
                error: format!("{error:?}"),
            })?;
        cases.push(result);
    }

    Ok(CaseResultsArtifact {
        slice_id: tikv::FIRST_TEMPORAL_TIMESTAMP_TZ_SLICE_ID.to_string(),
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
        "/../../inventory/first-temporal-timestamp-tz-slice-tikv-case-results.json"
    );

    #[test]
    fn canonical_requests_cover_all_documented_cases() {
        let requests = canonical_requests();
        let case_ids: Vec<&str> = requests
            .iter()
            .map(|request| request.case_id.as_str())
            .collect();

        assert_eq!(requests.len(), 10);
        assert_eq!(
            case_ids,
            vec![
                "timestamp-tz-column-passthrough",
                "timestamp-tz-equivalent-instant-normalization",
                "timestamp-tz-column-null-preserve",
                "timestamp-tz-is-not-null-all-kept",
                "timestamp-tz-is-not-null-all-dropped",
                "timestamp-tz-is-not-null-mixed-keep-drop",
                "timestamp-tz-order-asc-nulls-last",
                "timestamp-tz-missing-column-error",
                "unsupported-temporal-timestamp-without-timezone-error",
                "unsupported-temporal-timestamp-unit-error",
            ]
        );
    }

    #[test]
    fn checked_in_artifact_matches_fixture_harness_output() {
        let artifact = execute_first_temporal_timestamp_tz_slice_tikv(&FixtureTikvRunner).unwrap();

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
                "timestamp-tz-column-passthrough" => Ok(rows_result(
                    vec![tikv::EngineColumn {
                        name: "ts".to_string(),
                        engine_type: "timestamp_tz(us)".to_string(),
                        nullable: false,
                    }],
                    vec![vec![json!(0)], vec![json!(1_000_000)], vec![json!(2_000_000)]],
                )),
                "timestamp-tz-equivalent-instant-normalization" => Ok(rows_result(
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
                "timestamp-tz-column-null-preserve" => Ok(rows_result(
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
                "timestamp-tz-is-not-null-all-kept" => Ok(rows_result(
                    vec![tikv::EngineColumn {
                        name: "ts".to_string(),
                        engine_type: "timestamp_tz(us)".to_string(),
                        nullable: false,
                    }],
                    vec![vec![json!(0)], vec![json!(1_000_000)], vec![json!(2_000_000)]],
                )),
                "timestamp-tz-is-not-null-all-dropped" => Ok(rows_result(
                    vec![tikv::EngineColumn {
                        name: "ts".to_string(),
                        engine_type: "timestamp_tz(us)".to_string(),
                        nullable: true,
                    }],
                    vec![],
                )),
                "timestamp-tz-is-not-null-mixed-keep-drop" => Ok(rows_result(
                    vec![tikv::EngineColumn {
                        name: "ts".to_string(),
                        engine_type: "timestamp_tz(us)".to_string(),
                        nullable: true,
                    }],
                    vec![vec![json!(0)], vec![json!(2_000_000)]],
                )),
                "timestamp-tz-order-asc-nulls-last" => Ok(rows_result(
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
                "timestamp-tz-missing-column-error" => {
                    Err(tikv::EngineExecutionError::EngineFailure {
                        code: Some("1054".to_string()),
                        message: "Unknown column '__missing_column_1' in 'where clause'"
                            .to_string(),
                    })
                }
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

    fn rows_result(
        columns: Vec<tikv::EngineColumn>,
        rows: Vec<Vec<Value>>,
    ) -> tikv::EngineExecutionResult {
        tikv::EngineExecutionResult { columns, rows }
    }
}
