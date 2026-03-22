use crate::cli::artifact_output::{emit_artifacts, ArtifactOutput};

pub struct PairwiseDriftArtifacts<'a> {
    pub tidb_vs_tikv_drift_report_ref: &'a str,
    pub tidb_vs_tikv_drift_report: &'a str,
    pub tidb_vs_tikv_drift_report_sidecar_ref: &'a str,
    pub tidb_vs_tikv_drift_report_sidecar: &'a str,
    pub tiflash_vs_tikv_drift_report_ref: &'a str,
    pub tiflash_vs_tikv_drift_report: &'a str,
    pub tiflash_vs_tikv_drift_report_sidecar_ref: &'a str,
    pub tiflash_vs_tikv_drift_report_sidecar: &'a str,
}

pub fn emit_pairwise_drift_artifacts(
    write_artifacts: bool,
    write_flag: &str,
    artifacts: PairwiseDriftArtifacts<'_>,
) -> Result<(), String> {
    let rendered_artifacts = [
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

    emit_artifacts(write_artifacts, write_flag, None, &rendered_artifacts)
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    use super::{emit_pairwise_drift_artifacts, PairwiseDriftArtifacts};

    #[test]
    fn write_mode_persists_all_pairwise_artifacts() {
        let temp_dir = temp_artifact_dir("writes-all");
        std::fs::create_dir_all(&temp_dir).expect("temp dir should be created");

        let tidb_report_ref = temp_dir.join("tidb-vs-tikv.md");
        let tidb_sidecar_ref = temp_dir.join("tidb-vs-tikv.json");
        let tiflash_report_ref = temp_dir.join("tiflash-vs-tikv.md");
        let tiflash_sidecar_ref = temp_dir.join("tiflash-vs-tikv.json");

        let result = emit_pairwise_drift_artifacts(
            true,
            "--write-artifacts",
            PairwiseDriftArtifacts {
                tidb_vs_tikv_drift_report_ref: tidb_report_ref.to_str().unwrap(),
                tidb_vs_tikv_drift_report: "tidb report",
                tidb_vs_tikv_drift_report_sidecar_ref: tidb_sidecar_ref.to_str().unwrap(),
                tidb_vs_tikv_drift_report_sidecar: "tidb sidecar",
                tiflash_vs_tikv_drift_report_ref: tiflash_report_ref.to_str().unwrap(),
                tiflash_vs_tikv_drift_report: "tiflash report",
                tiflash_vs_tikv_drift_report_sidecar_ref: tiflash_sidecar_ref.to_str().unwrap(),
                tiflash_vs_tikv_drift_report_sidecar: "tiflash sidecar",
            },
        );

        assert!(result.is_ok(), "write mode should succeed: {result:?}");
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
        let failing_ref = missing_parent.join("tidb-vs-tikv.md");

        let error = emit_pairwise_drift_artifacts(
            true,
            "--write-artifacts",
            PairwiseDriftArtifacts {
                tidb_vs_tikv_drift_report_ref: failing_ref.to_str().unwrap(),
                tidb_vs_tikv_drift_report: "tidb report",
                tidb_vs_tikv_drift_report_sidecar_ref: failing_ref.to_str().unwrap(),
                tidb_vs_tikv_drift_report_sidecar: "tidb sidecar",
                tiflash_vs_tikv_drift_report_ref: failing_ref.to_str().unwrap(),
                tiflash_vs_tikv_drift_report: "tiflash report",
                tiflash_vs_tikv_drift_report_sidecar_ref: failing_ref.to_str().unwrap(),
                tiflash_vs_tikv_drift_report_sidecar: "tiflash sidecar",
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
            "tiforth-pairwise-output-{label}-{}-{unique_suffix}",
            std::process::id()
        ))
    }
}
