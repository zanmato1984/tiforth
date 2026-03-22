use crate::cli::artifact_output::{emit_artifacts, ArtifactOutput};

pub struct LiveDriftArtifacts<'a> {
    pub tikv_case_results_ref: &'a str,
    pub tikv_case_results: &'a str,
    pub tidb_vs_tikv_drift_report_ref: &'a str,
    pub tidb_vs_tikv_drift_report: &'a str,
    pub tidb_vs_tikv_drift_report_sidecar_ref: &'a str,
    pub tidb_vs_tikv_drift_report_sidecar: &'a str,
    pub tiflash_vs_tikv_drift_report_ref: &'a str,
    pub tiflash_vs_tikv_drift_report: &'a str,
    pub tiflash_vs_tikv_drift_report_sidecar_ref: &'a str,
    pub tiflash_vs_tikv_drift_report_sidecar: &'a str,
    pub tidb_env_prefix: &'a str,
    pub tiflash_env_prefix: &'a str,
    pub tikv_env_prefix: &'a str,
}

pub fn emit_live_drift_artifacts(
    write_artifacts: bool,
    write_flag: &str,
    artifacts: LiveDriftArtifacts<'_>,
) -> Result<(), String> {
    let connection_note = format!(
        "Connection env prefixes: `{}_*`, `{}_*`, `{}_*`.",
        artifacts.tidb_env_prefix, artifacts.tiflash_env_prefix, artifacts.tikv_env_prefix,
    );
    let rendered_artifacts = [
        ArtifactOutput {
            path: artifacts.tikv_case_results_ref,
            contents: artifacts.tikv_case_results,
        },
        ArtifactOutput {
            path: artifacts.tidb_vs_tikv_drift_report_ref,
            contents: artifacts.tidb_vs_tikv_drift_report,
        },
        ArtifactOutput {
            path: artifacts.tidb_vs_tikv_drift_report_sidecar_ref,
            contents: artifacts.tidb_vs_tikv_drift_report_sidecar,
        },
        ArtifactOutput {
            path: artifacts.tiflash_vs_tikv_drift_report_ref,
            contents: artifacts.tiflash_vs_tikv_drift_report,
        },
        ArtifactOutput {
            path: artifacts.tiflash_vs_tikv_drift_report_sidecar_ref,
            contents: artifacts.tiflash_vs_tikv_drift_report_sidecar,
        },
    ];

    emit_artifacts(
        write_artifacts,
        write_flag,
        Some(&connection_note),
        &rendered_artifacts,
    )
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    use super::{emit_live_drift_artifacts, LiveDriftArtifacts};

    #[test]
    fn write_mode_persists_all_live_artifacts() {
        let temp_dir = temp_artifact_dir("writes-all");
        std::fs::create_dir_all(&temp_dir).expect("temp dir should be created");

        let case_results_ref = temp_dir.join("tikv-case-results.json");
        let tidb_report_ref = temp_dir.join("tidb-vs-tikv.md");
        let tidb_sidecar_ref = temp_dir.join("tidb-vs-tikv.json");
        let tiflash_report_ref = temp_dir.join("tiflash-vs-tikv.md");
        let tiflash_sidecar_ref = temp_dir.join("tiflash-vs-tikv.json");

        let result = emit_live_drift_artifacts(
            true,
            "--write-artifacts",
            LiveDriftArtifacts {
                tikv_case_results_ref: case_results_ref.to_str().unwrap(),
                tikv_case_results: "tikv case results",
                tidb_vs_tikv_drift_report_ref: tidb_report_ref.to_str().unwrap(),
                tidb_vs_tikv_drift_report: "tidb report",
                tidb_vs_tikv_drift_report_sidecar_ref: tidb_sidecar_ref.to_str().unwrap(),
                tidb_vs_tikv_drift_report_sidecar: "tidb sidecar",
                tiflash_vs_tikv_drift_report_ref: tiflash_report_ref.to_str().unwrap(),
                tiflash_vs_tikv_drift_report: "tiflash report",
                tiflash_vs_tikv_drift_report_sidecar_ref: tiflash_sidecar_ref.to_str().unwrap(),
                tiflash_vs_tikv_drift_report_sidecar: "tiflash sidecar",
                tidb_env_prefix: "TIDB_MYSQL",
                tiflash_env_prefix: "TIFLASH_MYSQL",
                tikv_env_prefix: "TIKV_MYSQL",
            },
        );

        assert!(result.is_ok(), "write mode should succeed: {result:?}");
        assert_eq!(
            std::fs::read_to_string(&case_results_ref).unwrap(),
            "tikv case results"
        );
        assert_eq!(
            std::fs::read_to_string(&tidb_report_ref).unwrap(),
            "tidb report"
        );
        assert_eq!(
            std::fs::read_to_string(&tidb_sidecar_ref).unwrap(),
            "tidb sidecar"
        );
        assert_eq!(
            std::fs::read_to_string(&tiflash_report_ref).unwrap(),
            "tiflash report"
        );
        assert_eq!(
            std::fs::read_to_string(&tiflash_sidecar_ref).unwrap(),
            "tiflash sidecar"
        );

        std::fs::remove_dir_all(&temp_dir).expect("temp dir should be removed");
    }

    #[test]
    fn write_mode_surfaces_path_in_write_errors() {
        let missing_parent = temp_artifact_dir("missing-parent").join("missing");
        let failing_ref = missing_parent.join("tikv-case-results.json");

        let error = emit_live_drift_artifacts(
            true,
            "--write-artifacts",
            LiveDriftArtifacts {
                tikv_case_results_ref: failing_ref.to_str().unwrap(),
                tikv_case_results: "tikv case results",
                tidb_vs_tikv_drift_report_ref: failing_ref.to_str().unwrap(),
                tidb_vs_tikv_drift_report: "tidb report",
                tidb_vs_tikv_drift_report_sidecar_ref: failing_ref.to_str().unwrap(),
                tidb_vs_tikv_drift_report_sidecar: "tidb sidecar",
                tiflash_vs_tikv_drift_report_ref: failing_ref.to_str().unwrap(),
                tiflash_vs_tikv_drift_report: "tiflash report",
                tiflash_vs_tikv_drift_report_sidecar_ref: failing_ref.to_str().unwrap(),
                tiflash_vs_tikv_drift_report_sidecar: "tiflash sidecar",
                tidb_env_prefix: "TIDB_MYSQL",
                tiflash_env_prefix: "TIFLASH_MYSQL",
                tikv_env_prefix: "TIKV_MYSQL",
            },
        )
        .expect_err("write mode should report missing-parent write failure");

        assert!(error.contains(failing_ref.to_str().unwrap()));
    }

    fn temp_artifact_dir(label: &str) -> PathBuf {
        let unique_suffix = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock should be after unix epoch")
            .as_nanos();
        std::env::temp_dir().join(format!(
            "tiforth-live-output-{label}-{}-{unique_suffix}",
            std::process::id()
        ))
    }
}
