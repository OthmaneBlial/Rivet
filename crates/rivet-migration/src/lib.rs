//! Bounded, evidence-led analysis for migrating Jenkinsfiles to Rivet.
//!
//! This crate intentionally does not execute Groovy or attempt to interpret
//! arbitrary Jenkins plugins. It recognizes common declarative constructs and
//! reports the exact line and migration boundary for each one.

use rivet_core::{Pipeline, Stage, Step};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use thiserror::Error;

pub const ANALYZER_VERSION: u32 = 1;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SupportLevel {
    Supported,
    Partial,
    Unsupported,
}

impl SupportLevel {
    fn rank(self) -> u8 {
        match self {
            Self::Supported => 0,
            Self::Partial => 1,
            Self::Unsupported => 2,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct AnalysisSummary {
    pub supported: usize,
    pub partial: usize,
    pub unsupported: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ConstructFinding {
    pub kind: String,
    pub status: SupportLevel,
    pub line: usize,
    pub evidence: String,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rivet_mapping: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct JenkinsfileAnalysis {
    pub analyzer_version: u32,
    pub status: SupportLevel,
    pub summary: AnalysisSummary,
    pub constructs: Vec<ConstructFinding>,
    pub recommendations: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RivetfileDraft {
    pub status: SupportLevel,
    pub converted_steps: usize,
    pub skipped_stages: Vec<String>,
    pub warnings: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rivetfile_toml: Option<String>,
}

#[derive(Debug, Error)]
pub enum MigrationError {
    #[error("could not read Jenkinsfile {path}: {source}")]
    Read {
        path: PathBuf,
        source: std::io::Error,
    },
    #[error("could not serialize generated Rivetfile: {0}")]
    Serialize(#[from] toml::ser::Error),
}

/// Analyze a Jenkinsfile without executing its Groovy or plugin code.
pub fn analyze_jenkinsfile(source: &str) -> JenkinsfileAnalysis {
    let mut findings = Vec::new();
    let mut recommendations = Vec::new();
    let mut in_block_comment = false;
    let mut saw_pipeline = false;

    for (index, raw_line) in source.lines().enumerate() {
        let line_number = index + 1;
        let code_line = strip_comments(raw_line, &mut in_block_comment);
        let trimmed = code_line.trim();
        if trimmed.is_empty() {
            continue;
        }

        if starts_with_construct(trimmed, "pipeline") {
            saw_pipeline = true;
            add_finding(
                &mut findings,
                &mut recommendations,
                line_number,
                "pipeline",
                SupportLevel::Supported,
                "Declarative pipeline root is recognized.",
                Some("Rivetfile.toml pipeline definition"),
                evidence("pipeline", None),
            );
        } else if starts_with_construct(trimmed, "node") {
            add_finding(
                &mut findings,
                &mut recommendations,
                line_number,
                "scripted_pipeline",
                SupportLevel::Unsupported,
                "Scripted pipeline execution requires Groovy interpretation and is not evaluated.",
                None,
                evidence("node", None),
            );
        } else if starts_with_construct(trimmed, "agent") {
            let kind = if trimmed.contains("docker") {
                "docker_agent"
            } else {
                "agent"
            };
            add_finding(
                &mut findings,
                &mut recommendations,
                line_number,
                kind,
                SupportLevel::Partial,
                "Executor selection is visible, but labels and capacity need an explicit Rivet agent mapping.",
                Some("agent registry requirements and labels"),
                evidence(kind, None),
            );
        } else if starts_with_construct(trimmed, "stages") {
            add_finding(
                &mut findings,
                &mut recommendations,
                line_number,
                "stages",
                SupportLevel::Supported,
                "Sequential stage grouping is recognized.",
                Some("[[stages]]"),
                evidence("stages", None),
            );
        } else if starts_with_construct(trimmed, "stage") {
            let name = quoted_argument(trimmed, "stage");
            let message = match name.as_deref() {
                Some(name) => format!("Declarative stage {name:?} can map to a Rivet stage."),
                None => {
                    "Declarative stage is recognized; confirm its name before conversion.".into()
                }
            };
            add_finding(
                &mut findings,
                &mut recommendations,
                line_number,
                "stage",
                SupportLevel::Supported,
                &message,
                Some("[[stages]]"),
                evidence("stage", name.as_deref()),
            );
        } else if starts_with_construct(trimmed, "steps") {
            add_finding(
                &mut findings,
                &mut recommendations,
                line_number,
                "steps",
                SupportLevel::Supported,
                "A declarative steps block is recognized; individual commands still need mapping.",
                Some("[[stages.steps]]"),
                evidence("steps", None),
            );
        } else if starts_with_construct(trimmed, "sh") {
            add_finding(
                &mut findings,
                &mut recommendations,
                line_number,
                "shell_step",
                SupportLevel::Partial,
                "Shell execution is detected, but Rivet requires an explicit executable and argument array.",
                Some("Step.program and Step.args"),
                evidence("sh", None),
            );
        } else if starts_with_construct(trimmed, "bat") {
            add_finding(
                &mut findings,
                &mut recommendations,
                line_number,
                "batch_step",
                SupportLevel::Partial,
                "Batch execution is detected, but the command must be mapped to an explicit executable and arguments.",
                Some("Step.program and Step.args"),
                evidence("bat", None),
            );
        } else if starts_with_construct(trimmed, "echo") {
            add_finding(
                &mut findings,
                &mut recommendations,
                line_number,
                "echo",
                SupportLevel::Partial,
                "Jenkins console output is detected; preserve it as an intentional Rivet process step or log boundary.",
                Some("step output or an explicit logging command"),
                evidence("echo", None),
            );
        } else if starts_with_construct(trimmed, "parameters") {
            add_finding(
                &mut findings,
                &mut recommendations,
                line_number,
                "parameters",
                SupportLevel::Partial,
                "Parameter declarations are recognized, but types and secret handling require an explicit Rivet review.",
                Some("[[parameters]]"),
                evidence("parameters", None),
            );
        } else if starts_with_construct(trimmed, "environment") {
            add_finding(
                &mut findings,
                &mut recommendations,
                line_number,
                "environment",
                SupportLevel::Partial,
                "Environment declarations need a deliberate split between step variables and secret parameters.",
                Some("Step.env or a secret ParameterSpec"),
                evidence("environment", None),
            );
        } else if starts_with_construct(trimmed, "archiveArtifacts") {
            add_finding(
                &mut findings,
                &mut recommendations,
                line_number,
                "archive_artifacts",
                SupportLevel::Partial,
                "Artifact publication is detected; paths and retention need an explicit Rivet artifact declaration.",
                Some("[[artifacts]]"),
                evidence("archiveArtifacts", None),
            );
        } else if starts_with_construct(trimmed, "junit") {
            add_finding(
                &mut findings,
                &mut recommendations,
                line_number,
                "junit_reports",
                SupportLevel::Partial,
                "JUnit report publication is detected, but report indexing is not part of the current Rivet artifact model.",
                Some("artifact collection plus a future test-report adapter"),
                evidence("junit", None),
            );
        } else if starts_with_construct(trimmed, "post") {
            add_finding(
                &mut findings,
                &mut recommendations,
                line_number,
                "post",
                SupportLevel::Partial,
                "Post-build lifecycle behavior is detected and must be reviewed for an explicit Rivet trigger or follow-up build.",
                None,
                evidence("post", None),
            );
        } else if starts_with_construct(trimmed, "when") {
            add_finding(
                &mut findings,
                &mut recommendations,
                line_number,
                "when",
                SupportLevel::Unsupported,
                "Conditional stage execution is not inferred from Groovy expressions.",
                None,
                evidence("when", None),
            );
        } else if starts_with_construct(trimmed, "parallel") {
            add_finding(
                &mut findings,
                &mut recommendations,
                line_number,
                "parallel",
                SupportLevel::Unsupported,
                "Parallel branch semantics are not silently converted; the current runner executes stages sequentially.",
                None,
                evidence("parallel", None),
            );
        } else if starts_with_construct(trimmed, "script") {
            add_finding(
                &mut findings,
                &mut recommendations,
                line_number,
                "script",
                SupportLevel::Unsupported,
                "Arbitrary Groovy in a script block is not executed or guessed at migration time.",
                None,
                evidence("script", None),
            );
        } else if starts_with_construct(trimmed, "withCredentials") {
            add_finding(
                &mut findings,
                &mut recommendations,
                line_number,
                "with_credentials",
                SupportLevel::Unsupported,
                "Credential binding requires an explicit Rivet credential store and access policy.",
                None,
                evidence("withCredentials", None),
            );
        } else if starts_with_construct(trimmed, "input") {
            add_finding(
                &mut findings,
                &mut recommendations,
                line_number,
                "input",
                SupportLevel::Unsupported,
                "Interactive approval steps are not inferred into an unattended Rivet pipeline.",
                None,
                evidence("input", None),
            );
        } else if starts_with_construct(trimmed, "timeout") {
            add_finding(
                &mut findings,
                &mut recommendations,
                line_number,
                "timeout",
                SupportLevel::Partial,
                "Timeout behavior is detected and can map to a step timeout after scope is confirmed.",
                Some("Step.timeout_seconds"),
                evidence("timeout", None),
            );
        } else if starts_with_construct(trimmed, "retry") {
            add_finding(
                &mut findings,
                &mut recommendations,
                line_number,
                "retry",
                SupportLevel::Partial,
                "Retry behavior is detected; build retry and step retry are different failure semantics in Rivet.",
                Some("explicit retry policy after review"),
                evidence("retry", None),
            );
        } else if starts_with_construct(trimmed, "checkout") {
            add_finding(
                &mut findings,
                &mut recommendations,
                line_number,
                "checkout",
                SupportLevel::Partial,
                "SCM checkout is detected; map it to explicit repository preparation and credential boundaries.",
                Some("rivet scm prepare or build-admission SCM options"),
                evidence("checkout", None),
            );
        } else if starts_with_construct(trimmed, "tools") {
            add_finding(
                &mut findings,
                &mut recommendations,
                line_number,
                "tools",
                SupportLevel::Partial,
                "Tool installation declarations are detected; setup must be represented by an explicit reproducible step or image.",
                Some("explicit setup step or container image"),
                evidence("tools", None),
            );
        } else if starts_with_construct(trimmed, "dir") {
            add_finding(
                &mut findings,
                &mut recommendations,
                line_number,
                "dir",
                SupportLevel::Partial,
                "Directory scoping is detected and should map to a validated step working_dir.",
                Some("Step.working_dir"),
                evidence("dir", None),
            );
        } else if starts_with_construct(trimmed, "stash")
            || starts_with_construct(trimmed, "unstash")
        {
            add_finding(
                &mut findings,
                &mut recommendations,
                line_number,
                "stash",
                SupportLevel::Partial,
                "Jenkins stash transfer is detected; map it to explicit artifacts or a future remote transfer boundary.",
                Some("artifacts or a future agent transfer protocol"),
                evidence("stash", None),
            );
        } else if starts_with_construct(trimmed, "docker") {
            add_finding(
                &mut findings,
                &mut recommendations,
                line_number,
                "docker",
                SupportLevel::Partial,
                "Docker usage is detected; Rivet requires an explicit container image and its runtime gate must be verified.",
                Some("[stages.steps.container] image = ..."),
                evidence("docker", None),
            );
        } else if starts_with_construct(trimmed, "library")
            || starts_with_construct(trimmed, "properties")
            || starts_with_construct(trimmed, "def")
        {
            add_finding(
                &mut findings,
                &mut recommendations,
                line_number,
                "groovy_extension",
                SupportLevel::Unsupported,
                "Custom Groovy/plugin extension behavior is not executed or translated implicitly.",
                None,
                evidence("groovy_extension", None),
            );
        }
    }

    if !saw_pipeline {
        add_finding(
            &mut findings,
            &mut recommendations,
            1,
            "pipeline_root",
            SupportLevel::Unsupported,
            "No declarative pipeline root was recognized; the file needs manual review before migration.",
            None,
            "pipeline root not found".to_owned(),
        );
    }

    let summary = summarize(&findings);
    let status = overall_status(&findings);
    JenkinsfileAnalysis {
        analyzer_version: ANALYZER_VERSION,
        status,
        summary,
        constructs: findings,
        recommendations,
    }
}

pub fn analyze_jenkinsfile_file(path: &Path) -> Result<JenkinsfileAnalysis, MigrationError> {
    let source = fs::read_to_string(path).map_err(|source| MigrationError::Read {
        path: path.to_owned(),
        source,
    })?;
    Ok(analyze_jenkinsfile(&source))
}

/// Generate a valid Rivetfile draft for simple, explicitly quoted shell steps.
///
/// Ambiguous commands, unsupported blocks, and stages without a deterministic
/// command are left as warnings. The generated draft is never presented as a
/// complete migration.
pub fn generate_rivetfile_draft(source: &str) -> Result<RivetfileDraft, MigrationError> {
    let analysis = analyze_jenkinsfile(source);
    let mut stage_drafts: Vec<StageDraft> = Vec::new();
    let mut current_stage = None;
    let mut warnings = Vec::new();
    let mut in_block_comment = false;

    for (index, raw_line) in source.lines().enumerate() {
        let line = index + 1;
        let code_line = strip_comments(raw_line, &mut in_block_comment);
        let trimmed = code_line.trim();
        if trimmed.is_empty() {
            continue;
        }

        if starts_with_construct(trimmed, "stage") {
            let Some(name) = quoted_argument(trimmed, "stage") else {
                current_stage = None;
                warnings.push(format!(
                    "line {line}: stage name is not a simple quoted argument"
                ));
                continue;
            };
            if let Some(existing) = stage_drafts.iter().position(|stage| stage.name == name) {
                current_stage = Some(existing);
                warnings.push(format!(
                    "line {line}: duplicate stage {name:?} was merged into one draft stage"
                ));
            } else {
                stage_drafts.push(StageDraft {
                    name,
                    steps: Vec::new(),
                });
                current_stage = Some(stage_drafts.len() - 1);
            }
            continue;
        }

        let command_kind = if starts_with_construct(trimmed, "sh") {
            Some(("sh", "sh", "-c"))
        } else if starts_with_construct(trimmed, "bat") {
            Some(("bat", "cmd", "/C"))
        } else {
            None
        };
        let Some((jenkins_kind, program, shell_flag)) = command_kind else {
            continue;
        };

        let Some(command) = quoted_command_argument(trimmed, jenkins_kind) else {
            warnings.push(format!(
                "line {line}: {jenkins_kind} command is not a single-line quoted string; multi-line forms need manual review"
            ));
            continue;
        };
        if command.trim().is_empty() {
            warnings.push(format!("line {line}: {jenkins_kind} command is empty"));
            continue;
        }
        let Some(stage_index) = current_stage else {
            warnings.push(format!(
                "line {line}: {jenkins_kind} command is outside a named stage"
            ));
            continue;
        };
        let step_number = stage_drafts[stage_index].steps.len() + 1;
        stage_drafts[stage_index].steps.push(Step {
            name: format!("jenkins-{jenkins_kind}-{line}-{step_number}"),
            program: program.to_owned(),
            args: vec![shell_flag.to_owned(), command],
            env: BTreeMap::new(),
            working_dir: None,
            timeout_seconds: None,
            retries: 0,
            retry_delay_seconds: 0,
            container: None,
            agent: None,
        });
    }

    let mut skipped_stages = Vec::new();
    let stages = stage_drafts
        .into_iter()
        .filter_map(|stage| {
            if stage.steps.is_empty() {
                skipped_stages.push(stage.name.clone());
                warnings.push(format!(
                    "stage {:?}: no deterministic sh/bat step was generated",
                    stage.name
                ));
                return None;
            }
            Some(Stage {
                name: stage.name,
                depends_on: Vec::new(),
                steps: stage.steps,
            })
        })
        .collect::<Vec<_>>();

    let converted_steps = stages.iter().map(|stage| stage.steps.len()).sum();
    let rivetfile_toml = if stages.is_empty() {
        None
    } else {
        let pipeline = Pipeline {
            version: 1,
            name: "migrated-jenkinsfile".to_owned(),
            workspace: None,
            environment: BTreeMap::new(),
            parameters: Vec::new(),
            artifacts: Vec::new(),
            caches: Vec::new(),
            stages,
        };
        Some(toml::to_string_pretty(&pipeline)?)
    };

    let status = match analysis.status {
        SupportLevel::Unsupported => SupportLevel::Unsupported,
        SupportLevel::Partial if rivetfile_toml.is_some() => SupportLevel::Partial,
        SupportLevel::Partial => SupportLevel::Unsupported,
        SupportLevel::Supported if rivetfile_toml.is_some() && warnings.is_empty() => {
            SupportLevel::Supported
        }
        SupportLevel::Supported => SupportLevel::Partial,
    };
    Ok(RivetfileDraft {
        status,
        converted_steps,
        skipped_stages,
        warnings,
        rivetfile_toml,
    })
}

pub fn generate_rivetfile_draft_file(path: &Path) -> Result<RivetfileDraft, MigrationError> {
    let source = fs::read_to_string(path).map_err(|source| MigrationError::Read {
        path: path.to_owned(),
        source,
    })?;
    generate_rivetfile_draft(&source)
}

#[derive(Debug)]
struct StageDraft {
    name: String,
    steps: Vec<Step>,
}

fn add_finding(
    findings: &mut Vec<ConstructFinding>,
    recommendations: &mut Vec<String>,
    line: usize,
    kind: &str,
    status: SupportLevel,
    message: &str,
    rivet_mapping: Option<&str>,
    evidence: String,
) {
    findings.push(ConstructFinding {
        kind: kind.to_owned(),
        status,
        line,
        evidence,
        message: message.to_owned(),
        rivet_mapping: rivet_mapping.map(str::to_owned),
    });
    let recommendation = match status {
        SupportLevel::Supported => None,
        SupportLevel::Partial => Some(format!("Review line {line} ({kind}): {message}")),
        SupportLevel::Unsupported => Some(format!(
            "Manual review required for line {line} ({kind}): {message}"
        )),
    };
    if let Some(recommendation) = recommendation
        && !recommendations.contains(&recommendation)
    {
        recommendations.push(recommendation);
    }
}

fn summarize(findings: &[ConstructFinding]) -> AnalysisSummary {
    let mut summary = AnalysisSummary::default();
    for finding in findings {
        match finding.status {
            SupportLevel::Supported => summary.supported += 1,
            SupportLevel::Partial => summary.partial += 1,
            SupportLevel::Unsupported => summary.unsupported += 1,
        }
    }
    summary
}

fn overall_status(findings: &[ConstructFinding]) -> SupportLevel {
    findings
        .iter()
        .map(|finding| finding.status)
        .max_by_key(|status| status.rank())
        .unwrap_or(SupportLevel::Unsupported)
}

fn starts_with_construct(line: &str, construct: &str) -> bool {
    let Some(rest) = line.strip_prefix(construct) else {
        return false;
    };
    rest.is_empty()
        || rest
            .chars()
            .next()
            .is_some_and(|character| character.is_whitespace() || "({:".contains(character))
}

fn quoted_argument(line: &str, construct: &str) -> Option<String> {
    let rest = line.strip_prefix(construct)?.trim_start();
    let rest = rest.strip_prefix('(').map(str::trim_start).unwrap_or(rest);
    let rest = rest
        .strip_prefix("name:")
        .map(str::trim_start)
        .unwrap_or(rest);
    let mut characters = rest.chars();
    let quote = characters.next()?;
    if quote != '\'' && quote != '"' {
        return None;
    }
    let mut value = String::new();
    let mut escaped = false;
    for character in characters {
        if escaped {
            value.push(character);
            escaped = false;
        } else if character == '\\' {
            escaped = true;
        } else if character == quote {
            return Some(value);
        } else {
            value.push(character);
        }
    }
    None
}

fn quoted_command_argument(line: &str, construct: &str) -> Option<String> {
    let rest = line.strip_prefix(construct)?.trim_start();
    let rest = rest.strip_prefix('(').map(str::trim_start).unwrap_or(rest);
    let rest = rest
        .strip_prefix("script:")
        .map(str::trim_start)
        .unwrap_or(rest);
    if rest.starts_with("'''") || rest.starts_with("\"\"\"") {
        return None;
    }
    let mut characters = rest.chars();
    let quote = characters.next()?;
    if quote != '\'' && quote != '"' {
        return None;
    }
    let mut value = String::new();
    let mut escaped = false;
    let mut closed = false;
    for character in characters.by_ref() {
        if escaped {
            value.push(character);
            escaped = false;
        } else if character == '\\' {
            escaped = true;
        } else if character == quote {
            closed = true;
            break;
        } else {
            value.push(character);
        }
    }
    if !closed {
        return None;
    }
    let trailing = characters.as_str().trim();
    if trailing.is_empty() || trailing == ")" {
        Some(value)
    } else {
        None
    }
}

fn evidence(kind: &str, label: Option<&str>) -> String {
    match label {
        Some(label) => format!("{kind}({label:?})"),
        None => format!("{kind} construct"),
    }
}

fn strip_comments(line: &str, in_block_comment: &mut bool) -> String {
    let mut output = String::new();
    let mut characters = line.chars().peekable();
    let mut quote = None;
    while let Some(character) = characters.next() {
        if *in_block_comment {
            if character == '*' && characters.peek() == Some(&'/') {
                characters.next();
                *in_block_comment = false;
            }
            continue;
        }
        if let Some(active_quote) = quote {
            output.push(character);
            if character == '\\' {
                if let Some(escaped) = characters.next() {
                    output.push(escaped);
                }
            } else if character == active_quote {
                quote = None;
            }
            continue;
        }
        if character == '/' && characters.peek() == Some(&'/') {
            break;
        }
        if character == '/' && characters.peek() == Some(&'*') {
            characters.next();
            *in_block_comment = true;
            continue;
        }
        if character == '\'' || character == '"' {
            quote = Some(character);
        }
        output.push(character);
    }
    output
}

#[cfg(test)]
mod tests {
    use super::{SupportLevel, analyze_jenkinsfile, generate_rivetfile_draft};

    #[test]
    fn declarative_pipeline_reports_mappable_and_partial_constructs() {
        let analysis = analyze_jenkinsfile(
            r#"pipeline {
  agent any
  stages {
    stage('Build') {
      steps {
        sh 'cargo build'
        archiveArtifacts artifacts: 'target/**'
      }
    }
  }
}"#,
        );

        assert_eq!(analysis.analyzer_version, 1);
        assert_eq!(analysis.status, SupportLevel::Partial);
        assert_eq!(analysis.summary.supported, 4);
        assert_eq!(analysis.summary.partial, 3);
        assert_eq!(analysis.summary.unsupported, 0);
        assert!(
            analysis
                .constructs
                .iter()
                .any(|finding| finding.kind == "stage" && finding.line == 4)
        );
        assert!(
            analysis
                .recommendations
                .iter()
                .any(|recommendation| recommendation.contains("shell_step"))
        );
    }

    #[test]
    fn unsupported_constructs_are_never_marked_supported() {
        let analysis = analyze_jenkinsfile(
            r#"pipeline {
  stages {
    stage("Deploy") {
      when { branch 'main' }
      steps {
        script { sh 'deploy.sh' }
        parallel first: { sh 'a' }
        withCredentials([]) { sh 'secret' }
      }
    }
  }
}"#,
        );

        assert_eq!(analysis.status, SupportLevel::Unsupported);
        assert!(analysis.summary.unsupported >= 3);
        for kind in ["when", "script", "parallel", "with_credentials"] {
            assert!(
                analysis
                    .constructs
                    .iter()
                    .any(|finding| finding.kind == kind
                        && finding.status == SupportLevel::Unsupported)
            );
        }
    }

    #[test]
    fn comments_and_strings_do_not_create_false_constructs() {
        let analysis = analyze_jenkinsfile(
            r#"// parallel { fake }
/* stage('fake') */
pipeline {
  stages {
    stage('Build') {
      steps {
        echo "parallel() is only text"
      }
    }
  }
}"#,
        );

        assert!(
            !analysis
                .constructs
                .iter()
                .any(|finding| finding.kind == "parallel")
        );
        assert_eq!(
            analysis
                .constructs
                .iter()
                .filter(|finding| finding.kind == "stage")
                .count(),
            1
        );
        assert_eq!(analysis.summary.unsupported, 0);
    }

    #[test]
    fn missing_declarative_root_is_explicitly_blocked() {
        let analysis = analyze_jenkinsfile("def build() { println 'not declarative' }\n");

        assert_eq!(analysis.status, SupportLevel::Unsupported);
        assert!(
            analysis
                .constructs
                .iter()
                .any(|finding| finding.kind == "pipeline_root")
        );
    }

    #[test]
    fn generates_a_valid_partial_rivetfile_for_simple_shell_steps() {
        let draft = generate_rivetfile_draft(
            r#"pipeline {
  stages {
    stage('Build') {
      steps {
        sh 'cargo build'
        sh(script: 'cargo test')
      }
    }
  }
}"#,
        )
        .expect("draft");

        assert_eq!(draft.status, SupportLevel::Partial);
        assert_eq!(draft.converted_steps, 2);
        let rivetfile = draft.rivetfile_toml.expect("rivetfile");
        let pipeline = rivet_core::Pipeline::from_toml_str(&rivetfile).expect("valid Rivetfile");
        assert_eq!(pipeline.stages[0].name, "Build");
        assert_eq!(pipeline.stages[0].steps[0].program, "sh");
        assert_eq!(pipeline.stages[0].steps[0].args, ["-c", "cargo build"]);
        assert!(draft.warnings.is_empty());
    }

    #[test]
    fn leaves_ambiguous_commands_and_empty_stages_visible() {
        let draft = generate_rivetfile_draft(
            r#"pipeline {
  stages {
    stage('Review') {
      steps {
        sh '''multi
line'''
      }
    }
    stage('Deploy') {
      steps {
        input message: 'Approve?'
      }
    }
  }
}"#,
        )
        .expect("draft");

        assert_eq!(draft.status, SupportLevel::Unsupported);
        assert_eq!(draft.converted_steps, 0);
        assert_eq!(draft.rivetfile_toml, None);
        assert_eq!(draft.skipped_stages, ["Review", "Deploy"]);
        assert!(
            draft
                .warnings
                .iter()
                .any(|warning| warning.contains("multi-line"))
        );
        assert!(
            draft
                .warnings
                .iter()
                .any(|warning| warning.contains("no deterministic"))
        );
    }
}
