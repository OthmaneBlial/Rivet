//! Bounded, evidence-led analysis for migrating Jenkinsfiles to Rivet.
//!
//! This crate intentionally does not execute Groovy or attempt to interpret
//! arbitrary Jenkins plugins. It recognizes common declarative constructs and
//! reports the exact line and migration boundary for each one.

use rivet_core::{ArtifactSpec, ParameterKind, ParameterSpec, Pipeline, Stage, Step};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};
use thiserror::Error;

pub const ANALYZER_VERSION: u32 = 2;

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
        } else if let Some(kind) = typed_parameter_construct(trimmed) {
            add_finding(
                &mut findings,
                &mut recommendations,
                line_number,
                "typed_parameter",
                SupportLevel::Supported,
                &format!(
                    "The deterministic {kind} parameter form can map to a typed Rivet runtime input."
                ),
                Some("[[parameters]] with kind-specific validation"),
                evidence("typed_parameter", Some(kind)),
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
                SupportLevel::Partial,
                "Rivet can run independent stages concurrently, but Groovy branch names and their dependencies require explicit migration review.",
                Some("independent [[stages]] entries with explicit depends_on"),
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
    let metadata = parse_declarative_metadata(source);
    let mut stage_drafts: Vec<StageDraft> = Vec::new();
    let mut current_stage = None;
    let mut warnings = metadata.warnings;
    let mut artifacts = metadata.artifacts;
    let mut in_block_comment = false;
    let mut brace_depth = 0usize;
    let mut next_parallel_group = 0usize;
    let mut active_parallel_group = None;
    let mut parallel_close_depth = None;

    for (index, raw_line) in source.lines().enumerate() {
        let line = index + 1;
        let code_line = strip_comments(raw_line, &mut in_block_comment);
        let trimmed = code_line.trim();
        if trimmed.is_empty() {
            continue;
        }

        let line_brace_depth = brace_depth;
        let inside_parallel =
            parallel_close_depth.is_some_and(|close_depth| line_brace_depth > close_depth);
        let opens = trimmed
            .chars()
            .filter(|character| *character == '{')
            .count();
        let closes = trimmed
            .chars()
            .filter(|character| *character == '}')
            .count();
        if parallel_close_depth.is_some_and(|close_depth| line_brace_depth <= close_depth) {
            active_parallel_group = None;
            parallel_close_depth = None;
        }
        update_brace_depth(&mut brace_depth, opens, closes);

        if starts_with_construct(trimmed, "parallel") {
            if trimmed.contains('{') {
                active_parallel_group = Some(next_parallel_group);
                next_parallel_group += 1;
                parallel_close_depth = Some(line_brace_depth);
            } else {
                warnings.push(format!(
                    "line {line}: parallel syntax is not a simple named branch block"
                ));
            }
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
                    parallel_group: inside_parallel.then_some(active_parallel_group).flatten(),
                });
                current_stage = Some(stage_drafts.len() - 1);
            }
            continue;
        }

        if starts_with_construct(trimmed, "archiveArtifacts") {
            match parse_archive_artifact(line, trimmed) {
                Ok(artifact) => artifacts.push(artifact),
                Err(warning) => warnings.push(warning),
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
            if parallel_close_depth.is_some_and(|close_depth| brace_depth <= close_depth) {
                active_parallel_group = None;
                parallel_close_depth = None;
            }
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

        if parallel_close_depth.is_some_and(|close_depth| brace_depth <= close_depth) {
            active_parallel_group = None;
            parallel_close_depth = None;
        }
    }

    let mut skipped_stages = Vec::new();
    let mut stages = Vec::new();
    let mut active_group = None;
    let mut group_dependencies = Vec::new();
    let mut completed_group = Vec::new();
    for stage in stage_drafts {
        if stage.steps.is_empty() {
            skipped_stages.push(stage.name.clone());
            warnings.push(format!(
                "stage {:?}: no deterministic sh/bat step was generated",
                stage.name
            ));
            continue;
        }
        let depends_on = if let Some(group) = stage.parallel_group {
            if active_group != Some(group) {
                active_group = Some(group);
                completed_group.clear();
                group_dependencies = stages
                    .last()
                    .map(|stage: &Stage| vec![stage.name.clone()])
                    .unwrap_or_default();
            }
            completed_group.push(stage.name.clone());
            group_dependencies.clone()
        } else if active_group.take().is_some() {
            let dependencies = completed_group.clone();
            completed_group.clear();
            dependencies
        } else {
            stages
                .last()
                .map(|stage: &Stage| vec![stage.name.clone()])
                .unwrap_or_default()
        };
        stages.push(Stage {
            name: stage.name,
            depends_on,
            condition: None,
            steps: stage.steps,
        });
    }

    let converted_steps = stages.iter().map(|stage| stage.steps.len()).sum();
    let rivetfile_toml = if stages.is_empty() {
        None
    } else {
        let pipeline = Pipeline {
            version: 1,
            name: "migrated-jenkinsfile".to_owned(),
            workspace: None,
            environment: metadata.environment,
            parameters: metadata.parameters,
            artifacts,
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
    parallel_group: Option<usize>,
}

fn update_brace_depth(depth: &mut usize, opens: usize, closes: usize) {
    *depth = depth.saturating_add(opens).saturating_sub(closes);
}

#[derive(Debug, Default)]
struct DeclarativeMetadata {
    environment: BTreeMap<String, String>,
    parameters: Vec<ParameterSpec>,
    artifacts: Vec<ArtifactSpec>,
    warnings: Vec<String>,
}

fn parse_declarative_metadata(source: &str) -> DeclarativeMetadata {
    let mut metadata = DeclarativeMetadata::default();

    for (line, statement) in declaration_block_lines(source, "environment") {
        let statement = statement.trim().trim_end_matches(';').trim();
        let Some((name, raw_value)) = statement.split_once('=') else {
            metadata.warnings.push(format!(
                "line {line}: environment assignment is not a simple NAME = quoted value"
            ));
            continue;
        };
        let name = name.trim();
        let Some(value) = quoted_literal(raw_value.trim()) else {
            metadata.warnings.push(format!(
                "line {line}: environment value for {name:?} is dynamic or not a single quoted string"
            ));
            continue;
        };
        if value.contains('$') {
            metadata.warnings.push(format!(
                "line {line}: environment value for {name:?} is dynamic and needs manual review"
            ));
            continue;
        }
        if !valid_declarative_name(name) {
            metadata.warnings.push(format!(
                "line {line}: environment variable name {name:?} is invalid for Rivet"
            ));
            continue;
        }
        if metadata
            .environment
            .insert(name.to_owned(), value)
            .is_some()
        {
            metadata.warnings.push(format!(
                "line {line}: duplicate environment variable {name:?} was not converted"
            ));
            metadata.environment.remove(name);
        }
    }

    let mut parameter_names = std::collections::HashSet::new();
    for (line, statement) in declaration_block_lines(source, "parameters") {
        let statement = statement.trim().trim_end_matches(';').trim();
        let (kind, parameter_kind, secret) = if starts_with_construct(statement, "string") {
            ("string", ParameterKind::String, false)
        } else if starts_with_construct(statement, "password") {
            ("password", ParameterKind::Password, true)
        } else if starts_with_construct(statement, "text") {
            ("text", ParameterKind::Text, false)
        } else if starts_with_construct(statement, "booleanParam") {
            ("booleanParam", ParameterKind::Boolean, false)
        } else if starts_with_construct(statement, "choice") {
            ("choice", ParameterKind::Choice, false)
        } else {
            metadata.warnings.push(format!(
                "line {line}: parameter declaration is not a deterministic supported typed mapping"
            ));
            continue;
        };

        let Some(name) = named_quoted_argument(statement, "name") else {
            metadata.warnings.push(format!(
                "line {line}: {kind} parameter has no simple quoted name"
            ));
            continue;
        };
        if !valid_declarative_name(&name) {
            metadata.warnings.push(format!(
                "line {line}: {kind} parameter name {name:?} is invalid for Rivet"
            ));
            continue;
        }
        if !parameter_names.insert(name.clone()) {
            metadata.warnings.push(format!(
                "line {line}: duplicate parameter {name:?} was not converted"
            ));
            continue;
        }

        let mut choices = Vec::new();
        let mut default = match parameter_kind {
            ParameterKind::Boolean => {
                if let Some(value) = named_boolean_argument(statement, "defaultValue") {
                    Some(value.to_string())
                } else if statement.contains("defaultValue:") {
                    metadata.warnings.push(format!(
                        "line {line}: boolean default for parameter {name:?} is dynamic and was omitted"
                    ));
                    None
                } else {
                    Some("false".to_owned())
                }
            }
            _ => named_quoted_argument(statement, "defaultValue"),
        };
        if parameter_kind == ParameterKind::Choice {
            let Some(parsed_choices) = named_quoted_list_argument(statement, "choices") else {
                metadata.warnings.push(format!(
                    "line {line}: choice parameter {name:?} has no deterministic quoted choices"
                ));
                continue;
            };
            let mut seen_choices = HashSet::new();
            if parsed_choices.is_empty()
                || parsed_choices.iter().any(|choice| choice.is_empty())
                || parsed_choices
                    .iter()
                    .any(|choice| !seen_choices.insert(choice.as_str()))
            {
                metadata.warnings.push(format!(
                    "line {line}: choice parameter {name:?} has empty or duplicate choices"
                ));
                continue;
            }
            choices = parsed_choices;
            if default.is_none() {
                default = choices.first().cloned();
            }
            if default
                .as_ref()
                .is_some_and(|value| !choices.iter().any(|choice| choice == value))
            {
                metadata.warnings.push(format!(
                    "line {line}: default for choice parameter {name:?} is not one of its choices and was omitted"
                ));
                default = None;
            }
        }
        if default.as_deref().is_some_and(|value| value.contains('$')) {
            metadata.warnings.push(format!(
                "line {line}: default for parameter {name:?} is dynamic and was omitted"
            ));
            default = None;
        }
        if secret && default.is_some() {
            metadata.warnings.push(format!(
                "line {line}: secret password default for {name:?} was omitted"
            ));
            default = None;
        }
        metadata.parameters.push(ParameterSpec {
            name,
            kind: parameter_kind,
            default,
            secret,
            choices,
        });
    }

    metadata
}

fn parse_archive_artifact(line: usize, statement: &str) -> Result<ArtifactSpec, String> {
    let Some(patterns) = named_quoted_argument(statement, "artifacts") else {
        return Err(format!(
            "line {line}: archiveArtifacts patterns are not a single quoted value"
        ));
    };
    let paths = patterns
        .split(',')
        .map(str::trim)
        .filter(|path| !path.is_empty())
        .map(ToOwned::to_owned)
        .collect::<Vec<_>>();
    if paths.is_empty() || paths.iter().any(|path| !valid_artifact_path(path)) {
        return Err(format!(
            "line {line}: archiveArtifacts contains an empty or unsafe path"
        ));
    }

    let allow_empty = named_boolean_argument(statement, "allowEmpty").unwrap_or(false);
    Ok(ArtifactSpec {
        name: format!("jenkins-archive-{line}"),
        paths,
        allow_empty,
    })
}

fn declaration_block_lines(source: &str, construct: &str) -> Vec<(usize, String)> {
    let mut lines = Vec::new();
    let mut in_block = false;
    let mut depth = 0i32;
    let mut in_block_comment = false;

    for (index, raw_line) in source.lines().enumerate() {
        let code_line = strip_comments(raw_line, &mut in_block_comment);
        let trimmed = code_line.trim();
        if !in_block {
            if starts_with_construct(trimmed, construct) {
                let delta = brace_balance(&code_line);
                if delta > 0 {
                    in_block = true;
                    depth = delta;
                }
            }
            continue;
        }

        if !trimmed.is_empty() && trimmed != "}" {
            lines.push((index + 1, trimmed.to_owned()));
        }
        depth += brace_balance(&code_line);
        if depth <= 0 {
            in_block = false;
            depth = 0;
        }
    }
    lines
}

fn brace_balance(line: &str) -> i32 {
    let mut balance = 0;
    let mut quote = None;
    let mut escaped = false;
    for character in line.chars() {
        if let Some(active_quote) = quote {
            if escaped {
                escaped = false;
            } else if character == '\\' {
                escaped = true;
            } else if character == active_quote {
                quote = None;
            }
            continue;
        }
        if character == '\'' || character == '"' {
            quote = Some(character);
        } else if character == '{' {
            balance += 1;
        } else if character == '}' {
            balance -= 1;
        }
    }
    balance
}

fn named_quoted_argument(line: &str, name: &str) -> Option<String> {
    let needle = format!("{name}:");
    let start = line.find(&needle)?;
    if start > 0
        && line[..start]
            .chars()
            .next_back()
            .is_some_and(|character| character.is_ascii_alphanumeric() || character == '_')
    {
        return None;
    }
    quoted_prefix(line[start + needle.len()..].trim_start()).map(|(value, _)| value)
}

fn typed_parameter_construct(line: &str) -> Option<&'static str> {
    ["string", "password", "text", "booleanParam", "choice"]
        .into_iter()
        .find(|construct| starts_with_construct(line, construct))
}

fn named_quoted_list_argument(line: &str, name: &str) -> Option<Vec<String>> {
    let needle = format!("{name}:");
    let start = line.find(&needle)?;
    if start > 0
        && line[..start]
            .chars()
            .next_back()
            .is_some_and(|character| character.is_ascii_alphanumeric() || character == '_')
    {
        return None;
    }
    let mut rest = line[start + needle.len()..]
        .trim_start()
        .strip_prefix('[')?
        .trim_start();
    let mut values = Vec::new();
    loop {
        if rest.starts_with(']') {
            return Some(values);
        }
        let (value, trailing) = quoted_prefix(rest)?;
        values.push(value);
        rest = trailing.trim_start();
        if let Some(after_comma) = rest.strip_prefix(',') {
            rest = after_comma.trim_start();
            continue;
        }
        if rest.starts_with(']') {
            return Some(values);
        }
        return None;
    }
}

fn named_boolean_argument(line: &str, name: &str) -> Option<bool> {
    let needle = format!("{name}:");
    let start = line.find(&needle)?;
    let value = line[start + needle.len()..].trim_start();
    if value.starts_with("true") {
        Some(true)
    } else if value.starts_with("false") {
        Some(false)
    } else {
        None
    }
}

fn quoted_literal(value: &str) -> Option<String> {
    let (value, trailing) = quoted_prefix(value.trim())?;
    if trailing.trim().is_empty() {
        Some(value)
    } else {
        None
    }
}

fn quoted_prefix(value: &str) -> Option<(String, &str)> {
    let mut characters = value.chars();
    let quote = characters.next()?;
    if quote != '\'' && quote != '"' {
        return None;
    }
    let mut result = String::new();
    let mut escaped = false;
    for character in characters.by_ref() {
        if escaped {
            result.push(character);
            escaped = false;
        } else if character == '\\' {
            escaped = true;
        } else if character == quote {
            return Some((result, characters.as_str()));
        } else {
            result.push(character);
        }
    }
    None
}

fn valid_declarative_name(name: &str) -> bool {
    let mut characters = name.chars();
    match characters.next() {
        Some(character) if character == '_' || character.is_ascii_alphabetic() => {}
        _ => return false,
    }
    characters.all(|character| character == '_' || character.is_ascii_alphanumeric())
}

fn valid_artifact_path(path: &str) -> bool {
    let value = Path::new(path);
    !path.is_empty()
        && !path.contains('\0')
        && !path.chars().any(char::is_control)
        && !value.is_absolute()
        && !value
            .components()
            .any(|component| matches!(component, std::path::Component::ParentDir))
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
    use rivet_core::ParameterKind;

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

        assert_eq!(analysis.analyzer_version, 2);
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
        for kind in ["when", "script", "with_credentials"] {
            assert!(
                analysis
                    .constructs
                    .iter()
                    .any(|finding| finding.kind == kind
                        && finding.status == SupportLevel::Unsupported)
            );
        }
        assert!(analysis.constructs.iter().any(|finding| {
            finding.kind == "parallel" && finding.status == SupportLevel::Partial
        }));
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
    fn parallel_blocks_are_reported_as_reviewable_rivet_dag_work() {
        let analysis = analyze_jenkinsfile(
            r#"pipeline {
  stages {
    stage('Checks') {
      parallel {
        stage('Lint') { steps { sh 'cargo fmt --check' } }
        stage('Test') { steps { sh 'cargo test' } }
      }
    }
  }
}"#,
        );

        let finding = analysis
            .constructs
            .iter()
            .find(|finding| finding.kind == "parallel")
            .expect("parallel finding");
        assert_eq!(finding.status, SupportLevel::Partial);
        assert_eq!(
            finding.rivet_mapping.as_deref(),
            Some("independent [[stages]] entries with explicit depends_on")
        );
        assert!(analysis.summary.partial >= 1);
    }

    #[test]
    fn generates_independent_rivet_stages_for_simple_parallel_branches() {
        let draft = generate_rivetfile_draft(
            r#"pipeline {
  stages {
    stage('Build') {
      steps {
        sh 'cargo build'
      }
    }
    stage('Checks') {
      parallel {
        stage('Lint') {
          steps {
            sh 'cargo fmt --check'
          }
        }
        stage('Test') {
          steps {
            sh 'cargo test'
          }
        }
      }
    }
    stage('Package') {
      steps {
        sh 'cargo package'
      }
    }
  }
}"#,
        )
        .expect("parallel draft");

        let rivetfile = draft.rivetfile_toml.expect("parallel Rivetfile");
        let pipeline = rivet_core::Pipeline::from_toml_str(&rivetfile).expect("valid Rivetfile");
        assert_eq!(
            pipeline
                .stages
                .iter()
                .map(|stage| stage.name.as_str())
                .collect::<Vec<_>>(),
            ["Build", "Lint", "Test", "Package"]
        );
        assert_eq!(pipeline.stages[1].depends_on, ["Build"]);
        assert_eq!(pipeline.stages[2].depends_on, ["Build"]);
        assert_eq!(pipeline.stages[3].depends_on, ["Lint", "Test"]);
        assert_eq!(draft.converted_steps, 4);
        assert_eq!(draft.skipped_stages, ["Checks"]);
        assert!(
            draft
                .warnings
                .iter()
                .any(|warning| warning.contains("Checks") && warning.contains("no deterministic"))
        );
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
    fn fixture_conversion_preserves_jenkins_stage_order_and_unsupported_review() {
        let source = include_str!("../fixtures/sequential-shell-pipeline.Jenkinsfile");
        let draft = generate_rivetfile_draft(source).expect("fixture draft");

        assert_eq!(draft.status, SupportLevel::Unsupported);
        assert_eq!(draft.converted_steps, 2);
        assert_eq!(draft.skipped_stages, ["Review"]);
        assert!(
            draft
                .warnings
                .iter()
                .any(|warning| warning.contains("no deterministic"))
        );

        let rivetfile = draft.rivetfile_toml.expect("fixture Rivetfile");
        let pipeline = rivet_core::Pipeline::from_toml_str(&rivetfile).expect("valid Rivetfile");
        assert_eq!(
            pipeline
                .stages
                .iter()
                .map(|stage| stage.name.as_str())
                .collect::<Vec<_>>(),
            ["Build", "Test"]
        );
        assert!(pipeline.stages[0].depends_on.is_empty());
        assert_eq!(pipeline.stages[1].depends_on, ["Build"]);
        assert_eq!(pipeline.stages[1].steps[0].args, ["-c", "cargo test"]);
    }

    #[test]
    fn fixture_conversion_maps_static_metadata_and_archive_patterns() {
        let source = include_str!("../fixtures/declarative-metadata-pipeline.Jenkinsfile");
        let draft = generate_rivetfile_draft(source).expect("metadata fixture draft");

        assert_eq!(draft.status, SupportLevel::Partial);
        assert_eq!(draft.converted_steps, 1);
        assert!(draft.warnings.is_empty());

        let rivetfile = draft.rivetfile_toml.expect("metadata Rivetfile");
        let pipeline = rivet_core::Pipeline::from_toml_str(&rivetfile).expect("valid Rivetfile");
        assert_eq!(pipeline.environment["BUILD_CHANNEL"], "nightly");
        assert_eq!(pipeline.environment["RELEASE_TARGET"], "staging");
        assert_eq!(pipeline.parameters[0].name, "TARGET");
        assert_eq!(pipeline.parameters[0].default.as_deref(), Some("release"));
        assert!(!pipeline.parameters[0].secret);
        assert_eq!(pipeline.parameters[1].name, "DEPLOY_TOKEN");
        assert!(pipeline.parameters[1].default.is_none());
        assert!(pipeline.parameters[1].secret);
        assert_eq!(pipeline.artifacts[0].name, "jenkins-archive-15");
        assert_eq!(
            pipeline.artifacts[0].paths,
            ["target/release/**", "manifest.json"]
        );
        assert!(pipeline.artifacts[0].allow_empty);
        assert_eq!(
            pipeline.stages[0].steps[0].args,
            ["-c", "cargo build --release"]
        );
    }

    #[test]
    fn typed_jenkins_parameters_become_valid_rivet_inputs() {
        let draft = generate_rivetfile_draft(
            r#"pipeline {
  parameters {
    booleanParam(name: 'PUBLISH', defaultValue: true)
    choice(name: 'TARGET', choices: ['staging', 'production'])
    text(name: 'NOTES', defaultValue: 'ship carefully')
    password(name: 'TOKEN')
  }
  stages {
    stage('Build') {
      steps {
        sh 'cargo build'
      }
    }
  }
}"#,
        )
        .expect("typed parameter draft");

        assert!(draft.warnings.is_empty());
        let rivetfile = draft.rivetfile_toml.expect("typed Rivetfile");
        let pipeline = rivet_core::Pipeline::from_toml_str(&rivetfile).expect("valid Rivetfile");
        assert_eq!(pipeline.parameters[0].kind, ParameterKind::Boolean);
        assert_eq!(pipeline.parameters[0].default.as_deref(), Some("true"));
        assert_eq!(pipeline.parameters[1].kind, ParameterKind::Choice);
        assert_eq!(pipeline.parameters[1].choices, ["staging", "production"]);
        assert_eq!(pipeline.parameters[1].default.as_deref(), Some("staging"));
        assert_eq!(pipeline.parameters[2].kind, ParameterKind::Text);
        assert_eq!(
            pipeline.parameters[2].default.as_deref(),
            Some("ship carefully")
        );
        assert_eq!(pipeline.parameters[3].kind, ParameterKind::Password);
        assert!(pipeline.parameters[3].secret);
        assert!(pipeline.parameters[3].default.is_none());
    }

    #[test]
    fn dynamic_metadata_and_unsafe_archives_remain_outside_the_generated_draft() {
        let draft = generate_rivetfile_draft(
            r#"pipeline {
  environment {
    RELEASE = "${params.RELEASE}"
  }
  parameters {
    booleanParam(name: 'PUBLISH', defaultValue: true)
    password(name: 'TOKEN', defaultValue: 'do-not-copy')
  }
  stages {
    stage('Build') {
      steps {
        sh 'cargo build'
        archiveArtifacts artifacts: '../outside/**'
      }
    }
  }
}"#,
        )
        .expect("bounded metadata draft");

        assert!(
            draft
                .warnings
                .iter()
                .any(|warning| warning.contains("dynamic"))
        );
        assert!(
            draft
                .warnings
                .iter()
                .any(|warning| warning.contains("secret password default"))
        );
        assert!(
            draft
                .warnings
                .iter()
                .any(|warning| warning.contains("unsafe path"))
        );

        let rivetfile = draft.rivetfile_toml.expect("bounded Rivetfile");
        let pipeline = rivet_core::Pipeline::from_toml_str(&rivetfile).expect("valid Rivetfile");
        assert!(pipeline.environment.is_empty());
        assert_eq!(pipeline.parameters.len(), 2);
        assert_eq!(pipeline.parameters[0].name, "PUBLISH");
        assert_eq!(pipeline.parameters[0].kind, ParameterKind::Boolean);
        assert_eq!(pipeline.parameters[0].default.as_deref(), Some("true"));
        assert_eq!(pipeline.parameters[1].name, "TOKEN");
        assert_eq!(pipeline.parameters[1].kind, ParameterKind::Password);
        assert!(pipeline.parameters[1].secret);
        assert!(pipeline.parameters[1].default.is_none());
        assert!(pipeline.artifacts.is_empty());
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
