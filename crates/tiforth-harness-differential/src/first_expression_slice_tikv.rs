use serde::{Deserialize, Serialize};
use tiforth_adapter_tikv::first_expression_slice as tikv;

pub const TIKV_CASE_RESULTS_REF: &str = "inventory/first-expression-slice-tikv-case-results.json";

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
    tikv::TikvFirstExpressionSliceAdapter::canonical_requests()
}

pub fn execute_first_expression_slice_tikv<R: tikv::TikvRunner>(
    tikv_runner: &R,
) -> Result<CaseResultsArtifact, HarnessError> {
    let requests = canonical_requests();
    let mut cases = Vec::with_capacity(requests.len());

    for request in requests {
        let case_id = request.case_id.clone();
        let result = tikv::TikvFirstExpressionSliceAdapter::execute(&request, tikv_runner)
            .map_err(|error| HarnessError::TikvAdapterValidation {
                case_id,
                error: format!("{error:?}"),
            })?;
        cases.push(result);
    }

    Ok(CaseResultsArtifact {
        slice_id: tikv::FIRST_EXPRESSION_SLICE_ID.to_string(),
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
        "/../../inventory/first-expression-slice-tikv-case-results.json"
    );

    #[test]
    fn canonical_requests_cover_all_documented_cases() {
        let requests = canonical_requests();
        let case_ids: Vec<&str> = requests
            .iter()
            .map(|request| request.case_id.as_str())
            .collect();

        assert_eq!(requests.len(), 6);
        assert_eq!(
            case_ids,
            vec![
                "column-passthrough",
                "literal-int32-seven",
                "literal-int32-null",
                "add-int32-literal",
                "add-int32-null-propagation",
                "add-int32-overflow-error",
            ]
        );
    }

    #[test]
    fn checked_in_artifact_matches_fixture_harness_output() {
        let artifact = execute_first_expression_slice_tikv(&FixtureTikvRunner).unwrap();

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
                "column-passthrough" => Ok(rows_result(
                    "a",
                    "int",
                    false,
                    vec![Some(1), Some(2), Some(3)],
                )),
                "literal-int32-seven" => Ok(rows_result(
                    "lit",
                    "bigint",
                    false,
                    vec![Some(7), Some(7), Some(7)],
                )),
                "literal-int32-null" => {
                    Ok(rows_result("lit", "bigint", true, vec![None, None, None]))
                }
                "add-int32-literal" => Ok(rows_result(
                    "a_plus_one",
                    "bigint",
                    false,
                    vec![Some(2), Some(3), Some(4)],
                )),
                "add-int32-null-propagation" => Ok(rows_result(
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

    fn rows_result(
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
}
