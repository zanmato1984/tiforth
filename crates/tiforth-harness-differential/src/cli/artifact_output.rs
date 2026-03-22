use std::fs;

pub struct ArtifactOutput<'a> {
    pub path: &'a str,
    pub contents: &'a str,
}

pub fn emit_artifacts(
    write_artifacts: bool,
    write_flag: &str,
    extra_note: Option<&str>,
    artifacts: &[ArtifactOutput<'_>],
) -> Result<(), String> {
    if write_artifacts {
        for artifact in artifacts {
            write_artifact(artifact.path, artifact.contents)?;
        }

        println!("Updated:");
        for artifact in artifacts {
            println!("- {}", artifact.path);
        }
        return Ok(());
    }

    println!("Dry run complete. Use `{write_flag}` to overwrite inventory artifacts.");
    if let Some(extra_note) = extra_note {
        println!("{extra_note}");
    }
    println!();
    for artifact in artifacts {
        println!("=== {} ===", artifact.path);
        print!("{}", artifact.contents);
    }

    Ok(())
}

pub(crate) fn write_artifact(path: &str, contents: &str) -> Result<(), String> {
    fs::write(path, contents).map_err(|error| format!("failed to write `{path}`: {error}"))
}
