use std::env;
use std::process::Command;

use serde_json::Value;
use tiforth_adapter_tidb::first_temporal_date32_slice as tidb;
use tiforth_adapter_tiflash::first_temporal_date32_slice as tiflash;
use tiforth_adapter_tikv::first_temporal_date32_slice as tikv;

use crate::pairwise::first_temporal_date32_slice_tikv_pairwise;
use crate::slices::first_temporal_date32_slice;

pub const TIDB_MYSQL_ENV_PREFIX: &str = "TIFORTH_TIDB_MYSQL";
pub const TIFLASH_MYSQL_ENV_PREFIX: &str = "TIFORTH_TIFLASH_MYSQL";
pub const TIKV_MYSQL_ENV_PREFIX: &str = "TIFORTH_TIKV_MYSQL";

#[derive(Debug, Clone, PartialEq, Eq)]
struct MysqlConnectionConfig {
    bin: String,
    host: String,
    port: String,
    user: String,
    database: String,
    password: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct MysqlQueryResult {
    headers: Vec<String>,
    rows: Vec<Vec<Value>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct MysqlQueryError {
    code: Option<String>,
    message: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ExpectedColumn {
    engine_type: &'static str,
    nullable: bool,
}

#[derive(Debug, Default, Clone, Copy)]
pub struct LiveTidbRunner;

#[derive(Debug, Default, Clone, Copy)]
pub struct LiveTiflashRunner;

#[derive(Debug, Default, Clone, Copy)]
pub struct LiveTikvRunner;

impl LiveTidbRunner {
    pub fn from_env() -> Self {
        Self
    }
}

impl LiveTiflashRunner {
    pub fn from_env() -> Self {
        Self
    }
}

impl LiveTikvRunner {
    pub fn from_env() -> Self {
        Self
    }
}

pub fn render_live_drift_report_markdown(
    report: &first_temporal_date32_slice::DriftReport,
) -> String {
    let rendered = first_temporal_date32_slice_tikv_pairwise::render_drift_report_markdown(report)
        .replacen(
            "Status: issue #270 follow-on harness checkpoint",
            "Status: issue #270 follow-on harness checkpoint, issue #380 live-runner checkpoint",
            1,
        );
    let rendered = rendered.replace(
        "through deterministic harness fixture runners",
        "through env-configured live MySQL runners",
    );

    rendered.replace(
        "- live engine connection and orchestration remain out of scope for this artifact set",
        "- missing runner configuration still surfaces explicit `adapter_unavailable` outcomes in the normalized carrier",
    )
}

impl tidb::TidbRunner for LiveTidbRunner {
    fn run(
        &self,
        plan: &tidb::TidbExecutionPlan,
    ) -> Result<tidb::EngineExecutionResult, tidb::EngineExecutionError> {
        let config = load_connection_config(TIDB_MYSQL_ENV_PREFIX).map_err(|message| {
            tidb::EngineExecutionError::AdapterUnavailable {
                message: Some(format!("TiDB live runner unavailable: {message}")),
            }
        })?;
        let query_result = run_mysql_query(&config, &plan.sql).map_err(|error| {
            tidb::EngineExecutionError::EngineFailure {
                code: error.code,
                message: error.message,
            }
        })?;
        let columns = build_engine_columns(&plan.request.case_id, &query_result.headers).map_err(
            |message| tidb::EngineExecutionError::EngineFailure {
                code: None,
                message,
            },
        )?;

        Ok(tidb::EngineExecutionResult {
            columns: columns
                .into_iter()
                .map(|(name, expected)| tidb::EngineColumn {
                    name,
                    engine_type: expected.engine_type.to_string(),
                    nullable: expected.nullable,
                })
                .collect(),
            rows: query_result.rows,
        })
    }
}

impl tiflash::TiflashRunner for LiveTiflashRunner {
    fn run(
        &self,
        plan: &tiflash::TiflashExecutionPlan,
    ) -> Result<tiflash::EngineExecutionResult, tiflash::EngineExecutionError> {
        let config = load_connection_config(TIFLASH_MYSQL_ENV_PREFIX).map_err(|message| {
            tiflash::EngineExecutionError::AdapterUnavailable {
                message: Some(format!("TiFlash live runner unavailable: {message}")),
            }
        })?;
        let query_result = run_mysql_query(&config, &plan.sql).map_err(|error| {
            tiflash::EngineExecutionError::EngineFailure {
                code: error.code,
                message: error.message,
            }
        })?;
        let columns = build_engine_columns(&plan.request.case_id, &query_result.headers).map_err(
            |message| tiflash::EngineExecutionError::EngineFailure {
                code: None,
                message,
            },
        )?;

        Ok(tiflash::EngineExecutionResult {
            columns: columns
                .into_iter()
                .map(|(name, expected)| tiflash::EngineColumn {
                    name,
                    engine_type: expected.engine_type.to_string(),
                    nullable: expected.nullable,
                })
                .collect(),
            rows: query_result.rows,
        })
    }
}

impl tikv::TikvRunner for LiveTikvRunner {
    fn run(
        &self,
        plan: &tikv::TikvExecutionPlan,
    ) -> Result<tikv::EngineExecutionResult, tikv::EngineExecutionError> {
        let config = load_connection_config(TIKV_MYSQL_ENV_PREFIX).map_err(|message| {
            tikv::EngineExecutionError::AdapterUnavailable {
                message: Some(format!("TiKV live runner unavailable: {message}")),
            }
        })?;
        let query_result = run_mysql_query(&config, &plan.sql).map_err(|error| {
            tikv::EngineExecutionError::EngineFailure {
                code: error.code,
                message: error.message,
            }
        })?;
        let columns = build_engine_columns(&plan.request.case_id, &query_result.headers).map_err(
            |message| tikv::EngineExecutionError::EngineFailure {
                code: None,
                message,
            },
        )?;

        Ok(tikv::EngineExecutionResult {
            columns: columns
                .into_iter()
                .map(|(name, expected)| tikv::EngineColumn {
                    name,
                    engine_type: expected.engine_type.to_string(),
                    nullable: expected.nullable,
                })
                .collect(),
            rows: query_result.rows,
        })
    }
}

fn load_connection_config(env_prefix: &str) -> Result<MysqlConnectionConfig, String> {
    let required = |suffix: &str| -> Result<String, String> {
        let key = format!("{env_prefix}_{suffix}");
        env::var(&key).map_err(|_| format!("missing required env var `{key}`"))
    };

    let bin_key = format!("{env_prefix}_BIN");

    let bin = env::var(&bin_key).unwrap_or_else(|_| "mysql".to_string());
    let host = required("HOST")?;
    let port = required("PORT")?;
    let user = required("USER")?;
    let database = required("DATABASE")?;
    let password = env::var(format!("{env_prefix}_PASSWORD")).ok();

    if port.parse::<u16>().is_err() {
        return Err(format!(
            "invalid `{env_prefix}_PORT` value `{port}` (expected u16)"
        ));
    }

    Ok(MysqlConnectionConfig {
        bin,
        host,
        port,
        user,
        database,
        password,
    })
}

fn run_mysql_query(
    config: &MysqlConnectionConfig,
    sql: &str,
) -> Result<MysqlQueryResult, MysqlQueryError> {
    let mut command = Command::new(&config.bin);
    command
        .arg("--batch")
        .arg("--raw")
        .arg("--host")
        .arg(&config.host)
        .arg("--port")
        .arg(&config.port)
        .arg("--user")
        .arg(&config.user)
        .arg("--database")
        .arg(&config.database)
        .arg("--execute")
        .arg(sql);

    if let Some(password) = &config.password {
        command.env("MYSQL_PWD", password);
    }

    let output = command.output().map_err(|error| MysqlQueryError {
        code: None,
        message: format!("failed to execute `{}`: {error}", config.bin),
    })?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
        return Err(MysqlQueryError {
            code: extract_mysql_error_code(&stderr),
            message: if stderr.is_empty() {
                format!("mysql exited with status {}", output.status)
            } else {
                stderr
            },
        });
    }

    let stdout = String::from_utf8_lossy(&output.stdout).to_string();
    parse_mysql_batch_output(&stdout).map_err(|message| MysqlQueryError {
        code: None,
        message,
    })
}

fn parse_mysql_batch_output(stdout: &str) -> Result<MysqlQueryResult, String> {
    let mut lines = stdout.lines();
    let header_line = lines
        .next()
        .ok_or_else(|| "mysql output did not include a header row".to_string())?;
    let headers: Vec<String> = header_line
        .split('\t')
        .map(|cell| cell.to_string())
        .collect();
    if headers.is_empty() || headers.iter().any(|name| name.is_empty()) {
        return Err("mysql output header row was empty".to_string());
    }

    let mut rows = Vec::new();
    for (index, line) in lines.enumerate() {
        if line.is_empty() {
            continue;
        }

        let fields: Vec<&str> = line.split('\t').collect();
        if fields.len() != headers.len() {
            return Err(format!(
                "row {} had {} field(s) but header has {} column(s)",
                index + 1,
                fields.len(),
                headers.len()
            ));
        }

        rows.push(fields.into_iter().map(parse_mysql_cell).collect());
    }

    Ok(MysqlQueryResult { headers, rows })
}

fn parse_mysql_cell(cell: &str) -> Value {
    if cell == "NULL" || cell == "\\N" {
        return Value::Null;
    }

    if let Some(days) = parse_date32_days(cell) {
        return Value::from(days);
    }

    if let Ok(value) = cell.parse::<i64>() {
        Value::from(value)
    } else {
        Value::String(cell.to_string())
    }
}

fn parse_date32_days(cell: &str) -> Option<i32> {
    let mut parts = cell.split('-');
    let year = parts.next()?.parse::<i32>().ok()?;
    let month = parts.next()?.parse::<u32>().ok()?;
    let day = parts.next()?.parse::<u32>().ok()?;
    if parts.next().is_some() {
        return None;
    }
    if !(1..=12).contains(&month) {
        return None;
    }
    if day == 0 || day > days_in_month(year, month) {
        return None;
    }

    Some(days_from_civil(year, month, day))
}

fn days_in_month(year: i32, month: u32) -> u32 {
    match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 => {
            if is_leap_year(year) {
                29
            } else {
                28
            }
        }
        _ => 0,
    }
}

fn is_leap_year(year: i32) -> bool {
    (year % 4 == 0 && year % 100 != 0) || year % 400 == 0
}

fn days_from_civil(year: i32, month: u32, day: u32) -> i32 {
    let adjusted_year = year - if month <= 2 { 1 } else { 0 };
    let era = if adjusted_year >= 0 {
        adjusted_year
    } else {
        adjusted_year - 399
    } / 400;
    let year_of_era = adjusted_year - era * 400;
    let month_prime = month as i32 + if month > 2 { -3 } else { 9 };
    let day_of_year = (153 * month_prime + 2) / 5 + day as i32 - 1;
    let day_of_era = year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year;

    era * 146_097 + day_of_era - 719_468
}

fn build_engine_columns(
    case_id: &str,
    header_names: &[String],
) -> Result<Vec<(String, ExpectedColumn)>, String> {
    let expected = expected_row_case_schema(case_id).ok_or_else(|| {
        format!("case `{case_id}` returned rows but does not define a row schema in this slice")
    })?;

    if expected.len() != header_names.len() {
        return Err(format!(
            "case `{case_id}` expected {} column(s) but mysql returned {}",
            expected.len(),
            header_names.len()
        ));
    }

    Ok(header_names
        .iter()
        .cloned()
        .zip(expected.into_iter())
        .collect())
}

fn expected_row_case_schema(case_id: &str) -> Option<Vec<ExpectedColumn>> {
    match case_id {
        "date32-column-passthrough" | "date32-is-not-null-all-kept" => Some(vec![ExpectedColumn {
            engine_type: "date",
            nullable: false,
        }]),
        "date32-column-null-preserve"
        | "date32-is-not-null-all-dropped"
        | "date32-is-not-null-mixed-keep-drop" => Some(vec![ExpectedColumn {
            engine_type: "date",
            nullable: true,
        }]),
        _ => None,
    }
}

fn extract_mysql_error_code(stderr: &str) -> Option<String> {
    let marker = "ERROR ";
    let start = stderr.find(marker)? + marker.len();
    let code: String = stderr[start..]
        .chars()
        .take_while(|char| char.is_ascii_digit())
        .collect();
    if code.is_empty() {
        None
    } else {
        Some(code)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Mutex, OnceLock};

    use super::*;
    use tiforth_adapter_tidb::first_temporal_date32_slice::TidbRunner;
    use tiforth_adapter_tiflash::first_temporal_date32_slice::TiflashRunner;
    use tiforth_adapter_tikv::first_temporal_date32_slice::TikvRunner;

    static ENV_LOCK: OnceLock<Mutex<()>> = OnceLock::new();

    #[test]
    fn parse_mysql_batch_output_converts_dates_to_date32_days() {
        let parsed = parse_mysql_batch_output("d\n1970-01-01\nNULL\n1970-01-03\n").unwrap();

        assert_eq!(parsed.headers, vec!["d"]);
        assert_eq!(
            parsed.rows,
            vec![
                vec![Value::from(0)],
                vec![Value::Null],
                vec![Value::from(2)]
            ],
        );
    }

    #[test]
    fn parse_mysql_batch_output_rejects_missing_header() {
        let error = parse_mysql_batch_output("").unwrap_err();

        assert_eq!(error, "mysql output did not include a header row");
    }

    #[test]
    fn parse_date32_days_supports_leap_year_and_epoch_offsets() {
        assert_eq!(parse_date32_days("1970-01-01"), Some(0));
        assert_eq!(parse_date32_days("1972-02-29"), Some(789));
        assert_eq!(parse_date32_days("1969-12-31"), Some(-1));
        assert_eq!(parse_date32_days("1971-02-29"), None);
    }

    #[test]
    fn build_engine_columns_requires_known_row_case() {
        let error =
            build_engine_columns("date32-missing-column-error", &[String::from("d")]).unwrap_err();

        assert_eq!(
            error,
            "case `date32-missing-column-error` returned rows but does not define a row schema in this slice"
        );
    }

    #[test]
    fn extract_mysql_error_code_reads_numeric_code() {
        let code = extract_mysql_error_code("ERROR 1054 (42S22): Unknown column 'x'");

        assert_eq!(code, Some("1054".to_string()));
    }

    #[test]
    fn live_drift_renderer_relabels_the_evidence_source_block() {
        let report = first_temporal_date32_slice::DriftReport {
            slice_id: "first-temporal-date32-slice".to_string(),
            engines: vec!["tidb".to_string(), "tikv".to_string()],
            spec_refs: vec!["tests/differential/first-temporal-date32-slice.md".to_string()],
            cases: Vec::new(),
        };

        let rendered = render_live_drift_report_markdown(&report);

        assert!(rendered.contains("issue #380 live-runner checkpoint"));
        assert!(rendered.contains("env-configured live MySQL runners"));
        assert!(rendered.contains("adapter_unavailable"));
        assert!(!rendered.contains("deterministic harness fixture runners"));
    }

    #[test]
    fn tidb_live_runner_reports_adapter_unavailable_when_config_is_missing() {
        let _guard = ENV_LOCK.get_or_init(|| Mutex::new(())).lock().unwrap();
        let _env = ScopedEnv::clear_prefix(TIDB_MYSQL_ENV_PREFIX);

        let request = tidb::TidbFirstTemporalDate32SliceAdapter::canonical_requests()
            .into_iter()
            .find(|request| request.case_id == "date32-column-passthrough")
            .unwrap();
        let plan = tidb::TidbFirstTemporalDate32SliceAdapter::lower_request(&request).unwrap();

        let error = LiveTidbRunner::from_env().run(&plan).unwrap_err();

        assert_eq!(
            error,
            tidb::EngineExecutionError::AdapterUnavailable {
                message: Some(
                    "TiDB live runner unavailable: missing required env var `TIFORTH_TIDB_MYSQL_HOST`"
                        .to_string(),
                ),
            }
        );
    }

    #[test]
    fn tiflash_live_runner_reports_adapter_unavailable_when_config_is_missing() {
        let _guard = ENV_LOCK.get_or_init(|| Mutex::new(())).lock().unwrap();
        let _env = ScopedEnv::clear_prefix(TIFLASH_MYSQL_ENV_PREFIX);

        let request = tiflash::TiflashFirstTemporalDate32SliceAdapter::canonical_requests()
            .into_iter()
            .find(|request| request.case_id == "date32-column-passthrough")
            .unwrap();
        let plan =
            tiflash::TiflashFirstTemporalDate32SliceAdapter::lower_request(&request).unwrap();

        let error = LiveTiflashRunner::from_env().run(&plan).unwrap_err();

        assert_eq!(
            error,
            tiflash::EngineExecutionError::AdapterUnavailable {
                message: Some(
                    "TiFlash live runner unavailable: missing required env var `TIFORTH_TIFLASH_MYSQL_HOST`"
                        .to_string(),
                ),
            }
        );
    }

    #[test]
    fn tikv_live_runner_reports_adapter_unavailable_when_config_is_missing() {
        let _guard = ENV_LOCK.get_or_init(|| Mutex::new(())).lock().unwrap();
        let _env = ScopedEnv::clear_prefix(TIKV_MYSQL_ENV_PREFIX);

        let request = tikv::TikvFirstTemporalDate32SliceAdapter::canonical_requests()
            .into_iter()
            .find(|request| request.case_id == "date32-column-passthrough")
            .unwrap();
        let plan = tikv::TikvFirstTemporalDate32SliceAdapter::lower_request(&request).unwrap();

        let error = LiveTikvRunner::from_env().run(&plan).unwrap_err();

        assert_eq!(
            error,
            tikv::EngineExecutionError::AdapterUnavailable {
                message: Some(
                    "TiKV live runner unavailable: missing required env var `TIFORTH_TIKV_MYSQL_HOST`"
                        .to_string(),
                ),
            }
        );
    }

    struct ScopedEnv {
        key: String,
        previous: Option<String>,
    }

    impl ScopedEnv {
        fn clear_prefix(prefix: &str) -> Vec<Self> {
            ["HOST", "PORT", "USER", "DATABASE", "PASSWORD", "BIN"]
                .into_iter()
                .map(|suffix| {
                    let key = format!("{prefix}_{suffix}");
                    let previous = env::var(&key).ok();
                    env::remove_var(&key);
                    Self { key, previous }
                })
                .collect()
        }
    }

    impl Drop for ScopedEnv {
        fn drop(&mut self) {
            if let Some(previous) = &self.previous {
                env::set_var(&self.key, previous);
            } else {
                env::remove_var(&self.key);
            }
        }
    }
}
