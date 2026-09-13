//! Versioned wire vocabulary for Rivet agents.
//!
//! This crate intentionally contains only protocol data and validation. It
//! does not open sockets, execute builds, or persist credentials. Keeping the
//! envelope independent lets the server and a future agent binary evolve
//! against the same compatibility contract.

use chrono::{DateTime, Utc};
use rivet_core::{
    AgentRequirement, BuildEvent, BuildId, BuildStatus, ExecutionPlan, LogStream, Pipeline,
    ProjectId,
};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use thiserror::Error;
use uuid::Uuid;

pub const PROTOCOL_NAME: &str = "rivet-agent";
pub const PROTOCOL_VERSION: u16 = 4;
pub type AgentId = Uuid;

const MAX_AGENT_NAME_BYTES: usize = 128;
const MAX_LABELS: usize = 64;
const MAX_LABEL_BYTES: usize = 64;
const MAX_RUNNING_BUILDS: usize = 256;
const MAX_REQUIREMENT_VALUE_BYTES: usize = 64;
const MAX_CPU_CORES: u16 = 4096;
const MAX_MEMORY_MB: u64 = 4 * 1024 * 1024;
pub const MAX_WORKSPACE_CHUNK_BYTES: usize = 128 * 1024;
pub const MAX_WORKSPACE_FILES: u32 = 100_000;
pub const MAX_WORKSPACE_BYTES: u64 = 512 * 1024 * 1024;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AgentCapabilities {
    pub os: String,
    pub arch: String,
    pub docker: bool,
    pub labels: Vec<String>,
    pub executors: u16,
    /// Optional allocatable CPU capacity. `None` means the agent did not
    /// advertise a resource dimension and cannot satisfy an explicit CPU
    /// requirement.
    #[serde(default)]
    pub cpu_cores: Option<u16>,
    /// Optional allocatable memory capacity in MiB. `None` means the agent
    /// did not advertise a resource dimension and cannot satisfy an explicit
    /// memory requirement.
    #[serde(default)]
    pub memory_mb: Option<u64>,
}

impl AgentCapabilities {
    pub fn validate(&self) -> Result<(), ProtocolError> {
        if self.os.trim().is_empty() {
            return Err(ProtocolError::EmptyCapability("os"));
        }
        if self.arch.trim().is_empty() {
            return Err(ProtocolError::EmptyCapability("arch"));
        }
        if self.executors == 0 {
            return Err(ProtocolError::ZeroExecutors);
        }
        validate_capacity(self.cpu_cores, "cpu_cores")?;
        validate_memory(self.memory_mb, "memory_mb")?;
        if self.labels.len() > MAX_LABELS {
            return Err(ProtocolError::TooManyLabels);
        }
        for label in &self.labels {
            if label.trim().is_empty() || label.len() > MAX_LABEL_BYTES {
                return Err(ProtocolError::InvalidLabel);
            }
            if label.chars().any(char::is_control) {
                return Err(ProtocolError::InvalidLabel);
            }
        }
        Ok(())
    }

    pub fn supports(&self, requirements: &AgentRequirements) -> bool {
        requirements.os.as_ref().is_none_or(|os| os == &self.os)
            && requirements
                .arch
                .as_ref()
                .is_none_or(|arch| arch == &self.arch)
            && (!requirements.docker || self.docker)
            && requirements
                .labels
                .iter()
                .all(|label| self.labels.iter().any(|candidate| candidate == label))
            && requirements
                .executors
                .is_none_or(|executors| self.executors >= executors)
            && requirements.cpu_cores.is_none_or(|required| {
                self.cpu_cores
                    .is_some_and(|available| available >= required)
            })
            && requirements.memory_mb.is_none_or(|required| {
                self.memory_mb
                    .is_some_and(|available| available >= required)
            })
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct AgentRequirements {
    #[serde(default)]
    pub os: Option<String>,
    #[serde(default)]
    pub arch: Option<String>,
    #[serde(default)]
    pub docker: bool,
    #[serde(default)]
    pub labels: Vec<String>,
    #[serde(default)]
    pub executors: Option<u16>,
    #[serde(default)]
    pub cpu_cores: Option<u16>,
    #[serde(default)]
    pub memory_mb: Option<u64>,
}

impl AgentRequirements {
    /// Validate scheduler input before it reaches a matcher or a future
    /// remote assignment path. An omitted executor requirement means one
    /// available slot, which keeps an unconstrained build from matching a
    /// saturated agent.
    pub fn validate(&self) -> Result<(), ProtocolError> {
        validate_requirement_value(self.os.as_deref(), "os")?;
        validate_requirement_value(self.arch.as_deref(), "arch")?;
        if self.labels.len() > MAX_LABELS {
            return Err(ProtocolError::TooManyRequirementLabels);
        }
        for label in &self.labels {
            if label.trim().is_empty()
                || label.len() > MAX_LABEL_BYTES
                || label.chars().any(char::is_control)
            {
                return Err(ProtocolError::InvalidRequirementLabel);
            }
        }
        if self.executors == Some(0) {
            return Err(ProtocolError::ZeroRequiredExecutors);
        }
        validate_capacity(self.cpu_cores, "cpu_cores").map_err(|error| match error {
            ProtocolError::ZeroCapability(_) => ProtocolError::ZeroRequiredCpuCores,
            ProtocolError::CapabilityTooLarge(_) => ProtocolError::RequiredCpuCoresTooLarge,
            other => other,
        })?;
        validate_memory(self.memory_mb, "memory_mb").map_err(|error| match error {
            ProtocolError::ZeroMemory(_) => ProtocolError::ZeroRequiredMemory,
            ProtocolError::MemoryTooLarge(_) => ProtocolError::RequiredMemoryTooLarge,
            other => other,
        })?;
        Ok(())
    }

    pub fn requested_executors(&self) -> u16 {
        self.executors.unwrap_or(1)
    }

    /// Return the explicitly requested CPU capacity. An omitted dimension
    /// consumes no CPU reservation because the pipeline did not ask the
    /// scheduler to account for it.
    pub fn requested_cpu_cores(&self) -> u16 {
        self.cpu_cores.unwrap_or(0)
    }

    /// Return the explicitly requested memory capacity in MiB. An omitted
    /// dimension consumes no memory reservation for the same reason.
    pub fn requested_memory_mb(&self) -> u64 {
        self.memory_mb.unwrap_or(0)
    }
}

impl From<&AgentRequirement> for AgentRequirements {
    fn from(requirement: &AgentRequirement) -> Self {
        Self {
            os: requirement.os.clone(),
            arch: requirement.arch.clone(),
            docker: requirement.docker,
            labels: requirement.labels.clone(),
            executors: requirement.executors,
            cpu_cores: requirement.cpu_cores,
            memory_mb: requirement.memory_mb,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct WorkspaceTransfer {
    pub total_bytes: u64,
    pub file_count: u32,
    pub sha256: String,
}

impl WorkspaceTransfer {
    pub fn validate(&self) -> Result<(), ProtocolError> {
        if self.total_bytes > MAX_WORKSPACE_BYTES {
            return Err(ProtocolError::WorkspaceTooLarge);
        }
        if self.file_count > MAX_WORKSPACE_FILES {
            return Err(ProtocolError::TooManyWorkspaceFiles);
        }
        if self.sha256.len() != 64
            || self
                .sha256
                .chars()
                .any(|character| !character.is_ascii_hexdigit())
        {
            return Err(ProtocolError::InvalidWorkspaceChecksum);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AgentRegistration {
    pub protocol_version: u16,
    pub agent_id: AgentId,
    pub name: String,
    pub capabilities: AgentCapabilities,
}

impl AgentRegistration {
    pub fn validate(&self) -> Result<(), ProtocolError> {
        validate_version(self.protocol_version)?;
        validate_name(&self.name)?;
        self.capabilities.validate()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AgentHeartbeat {
    pub protocol_version: u16,
    pub agent_id: AgentId,
    pub session_id: Uuid,
    pub sequence: u64,
    pub running: Vec<BuildId>,
    pub sent_at: DateTime<Utc>,
}

impl AgentHeartbeat {
    pub fn validate(&self) -> Result<(), ProtocolError> {
        validate_version(self.protocol_version)?;
        if self.running.len() > MAX_RUNNING_BUILDS {
            return Err(ProtocolError::TooManyRunningBuilds);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum AgentMessage {
    Register(AgentRegistration),
    Registered {
        protocol_version: u16,
        agent_id: AgentId,
        session_id: Uuid,
    },
    Heartbeat(AgentHeartbeat),
    HeartbeatAck {
        protocol_version: u16,
        sequence: u64,
        server_time: DateTime<Utc>,
    },
    Assign {
        protocol_version: u16,
        build_id: BuildId,
        attempt_id: Uuid,
        project_id: ProjectId,
        plan: ExecutionPlan,
        pipeline: Pipeline,
        parameters: BTreeMap<String, String>,
        workspace: WorkspaceTransfer,
    },
    AssignmentAccepted {
        protocol_version: u16,
        build_id: BuildId,
    },
    WorkspaceChunk {
        protocol_version: u16,
        build_id: BuildId,
        sequence: u32,
        data: Vec<u8>,
    },
    WorkspaceReady {
        protocol_version: u16,
        build_id: BuildId,
    },
    ArtifactsReady {
        protocol_version: u16,
        build_id: BuildId,
        transfer: WorkspaceTransfer,
    },
    ArtifactChunk {
        protocol_version: u16,
        build_id: BuildId,
        sequence: u32,
        data: Vec<u8>,
    },
    Event {
        protocol_version: u16,
        attempt_id: Uuid,
        sequence: u64,
        event: BuildEvent,
    },
    Cancel {
        protocol_version: u16,
        build_id: BuildId,
    },
    Log {
        protocol_version: u16,
        build_id: BuildId,
        sequence: i64,
        stream: LogStream,
        line: String,
        timestamp: DateTime<Utc>,
    },
    Finished {
        protocol_version: u16,
        build_id: BuildId,
        status: BuildStatus,
        timestamp: DateTime<Utc>,
    },
    Error {
        protocol_version: u16,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        build_id: Option<BuildId>,
        code: String,
        message: String,
    },
}

/// Reliable application-message envelope used on an agent WebSocket.
///
/// Application messages are acknowledged only after the receiver accepts the
/// message for processing. Receivers can therefore safely ignore a duplicate
/// `Message` with the same delivery ID while still acknowledging it. The
/// payload remains a separate versioned vocabulary so transport reliability
/// does not leak into build-domain events.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum AgentTransportMessage {
    Message {
        protocol_version: u16,
        delivery_id: Uuid,
        payload: AgentMessage,
    },
    Ack {
        protocol_version: u16,
        delivery_id: Uuid,
    },
}

impl AgentTransportMessage {
    pub fn message(payload: AgentMessage) -> Self {
        Self::Message {
            protocol_version: PROTOCOL_VERSION,
            delivery_id: Uuid::new_v4(),
            payload,
        }
    }

    pub fn ack(delivery_id: Uuid) -> Self {
        Self::Ack {
            protocol_version: PROTOCOL_VERSION,
            delivery_id,
        }
    }

    pub fn delivery_id(&self) -> Uuid {
        match self {
            Self::Message { delivery_id, .. } | Self::Ack { delivery_id, .. } => *delivery_id,
        }
    }

    pub fn validate(&self) -> Result<(), ProtocolError> {
        let protocol_version = match self {
            Self::Message {
                protocol_version, ..
            }
            | Self::Ack {
                protocol_version, ..
            } => *protocol_version,
        };
        validate_version(protocol_version)?;
        if self.delivery_id().is_nil() {
            return Err(ProtocolError::EmptyDeliveryId);
        }
        if let Self::Message { payload, .. } = self {
            payload.validate()?;
        }
        Ok(())
    }
}

impl AgentMessage {
    pub fn validate(&self) -> Result<(), ProtocolError> {
        match self {
            Self::Register(registration) => registration.validate(),
            Self::Registered {
                protocol_version, ..
            }
            | Self::HeartbeatAck {
                protocol_version, ..
            }
            | Self::Assign {
                protocol_version, ..
            }
            | Self::AssignmentAccepted {
                protocol_version, ..
            }
            | Self::WorkspaceChunk {
                protocol_version, ..
            }
            | Self::WorkspaceReady {
                protocol_version, ..
            }
            | Self::ArtifactsReady {
                protocol_version, ..
            }
            | Self::ArtifactChunk {
                protocol_version, ..
            }
            | Self::Event {
                protocol_version, ..
            }
            | Self::Cancel {
                protocol_version, ..
            }
            | Self::Log {
                protocol_version, ..
            }
            | Self::Finished {
                protocol_version, ..
            }
            | Self::Error {
                protocol_version, ..
            } => validate_version(*protocol_version),
            Self::Heartbeat(heartbeat) => heartbeat.validate(),
        }
        .and_then(|()| match self {
            Self::Assign { attempt_id, .. } | Self::Event { attempt_id, .. }
                if attempt_id.is_nil() =>
            {
                Err(ProtocolError::EmptyAttemptId)
            }
            Self::Event { sequence, .. } if *sequence == 0 => Err(ProtocolError::ZeroEventSequence),
            Self::Assign { workspace, .. } => workspace.validate(),
            Self::ArtifactsReady { transfer, .. } => transfer.validate(),
            Self::WorkspaceChunk { data, .. } | Self::ArtifactChunk { data, .. }
                if data.len() > MAX_WORKSPACE_CHUNK_BYTES =>
            {
                Err(ProtocolError::WorkspaceChunkTooLarge)
            }
            _ => Ok(()),
        })
    }

    pub fn protocol_version(&self) -> u16 {
        match self {
            Self::Register(registration) => registration.protocol_version,
            Self::Registered {
                protocol_version, ..
            }
            | Self::HeartbeatAck {
                protocol_version, ..
            }
            | Self::Assign {
                protocol_version, ..
            }
            | Self::AssignmentAccepted {
                protocol_version, ..
            }
            | Self::WorkspaceChunk {
                protocol_version, ..
            }
            | Self::WorkspaceReady {
                protocol_version, ..
            }
            | Self::ArtifactsReady {
                protocol_version, ..
            }
            | Self::ArtifactChunk {
                protocol_version, ..
            }
            | Self::Event {
                protocol_version, ..
            }
            | Self::Cancel {
                protocol_version, ..
            }
            | Self::Log {
                protocol_version, ..
            }
            | Self::Finished {
                protocol_version, ..
            }
            | Self::Error {
                protocol_version, ..
            } => *protocol_version,
            Self::Heartbeat(heartbeat) => heartbeat.protocol_version,
        }
    }
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum ProtocolError {
    #[error("unsupported agent protocol version {found}; expected {expected}")]
    UnsupportedVersion { found: u16, expected: u16 },
    #[error("agent name cannot be empty")]
    EmptyAgentName,
    #[error("agent name is too long")]
    AgentNameTooLong,
    #[error("agent capability {0} cannot be empty")]
    EmptyCapability(&'static str),
    #[error("agent must advertise at least one executor")]
    ZeroExecutors,
    #[error("agent advertises too many labels")]
    TooManyLabels,
    #[error("agent label is empty, too long, or contains control characters")]
    InvalidLabel,
    #[error("agent heartbeat advertises too many running builds")]
    TooManyRunningBuilds,
    #[error("agent requirement {0} cannot be empty")]
    EmptyRequirement(&'static str),
    #[error("agent requirement {0} is too long")]
    RequirementTooLong(&'static str),
    #[error("agent requirement label is empty, too long, or contains control characters")]
    InvalidRequirementLabel,
    #[error("agent requires at least one executor")]
    ZeroRequiredExecutors,
    #[error("agent requires too many labels")]
    TooManyRequirementLabels,
    #[error("agent capability {0} must be positive")]
    ZeroCapability(&'static str),
    #[error("agent capability {0} exceeds the protocol limit")]
    CapabilityTooLarge(&'static str),
    #[error("agent memory capability {0} must be positive")]
    ZeroMemory(&'static str),
    #[error("agent memory capability {0} exceeds the protocol limit")]
    MemoryTooLarge(&'static str),
    #[error("agent requirement cpu_cores must be positive")]
    ZeroRequiredCpuCores,
    #[error("agent requirement cpu_cores exceeds the protocol limit")]
    RequiredCpuCoresTooLarge,
    #[error("agent requirement memory_mb must be positive")]
    ZeroRequiredMemory,
    #[error("agent requirement memory_mb exceeds the protocol limit")]
    RequiredMemoryTooLarge,
    #[error("workspace transfer exceeds the protocol size limit")]
    WorkspaceTooLarge,
    #[error("workspace transfer contains too many files")]
    TooManyWorkspaceFiles,
    #[error("workspace transfer checksum must be 64 hexadecimal characters")]
    InvalidWorkspaceChecksum,
    #[error("workspace transfer chunk exceeds the protocol size limit")]
    WorkspaceChunkTooLarge,
    #[error("agent transport delivery ID cannot be nil")]
    EmptyDeliveryId,
    #[error("remote attempt ID cannot be nil")]
    EmptyAttemptId,
    #[error("remote event sequence must be positive")]
    ZeroEventSequence,
}

fn validate_version(version: u16) -> Result<(), ProtocolError> {
    if version == PROTOCOL_VERSION {
        Ok(())
    } else {
        Err(ProtocolError::UnsupportedVersion {
            found: version,
            expected: PROTOCOL_VERSION,
        })
    }
}

fn validate_name(name: &str) -> Result<(), ProtocolError> {
    if name.trim().is_empty() {
        return Err(ProtocolError::EmptyAgentName);
    }
    if name.len() > MAX_AGENT_NAME_BYTES || name.chars().any(char::is_control) {
        return Err(ProtocolError::AgentNameTooLong);
    }
    Ok(())
}

fn validate_requirement_value(
    value: Option<&str>,
    field: &'static str,
) -> Result<(), ProtocolError> {
    let Some(value) = value else {
        return Ok(());
    };
    if value.trim().is_empty() {
        return Err(ProtocolError::EmptyRequirement(field));
    }
    if value.len() > MAX_REQUIREMENT_VALUE_BYTES || value.chars().any(char::is_control) {
        return Err(ProtocolError::RequirementTooLong(field));
    }
    Ok(())
}

fn validate_capacity(value: Option<u16>, field: &'static str) -> Result<(), ProtocolError> {
    let Some(value) = value else {
        return Ok(());
    };
    if value == 0 {
        return Err(ProtocolError::ZeroCapability(field));
    }
    if value > MAX_CPU_CORES {
        return Err(ProtocolError::CapabilityTooLarge(field));
    }
    Ok(())
}

fn validate_memory(value: Option<u64>, field: &'static str) -> Result<(), ProtocolError> {
    let Some(value) = value else {
        return Ok(());
    };
    if value == 0 {
        return Err(ProtocolError::ZeroMemory(field));
    }
    if value > MAX_MEMORY_MB {
        return Err(ProtocolError::MemoryTooLarge(field));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn capabilities() -> AgentCapabilities {
        AgentCapabilities {
            os: "linux".into(),
            arch: "x86_64".into(),
            docker: true,
            labels: vec!["large-memory".into(), "production".into()],
            executors: 4,
            cpu_cores: Some(8),
            memory_mb: Some(16 * 1024),
        }
    }

    #[test]
    fn registration_round_trips_as_a_versioned_wire_message() {
        let message = AgentMessage::Register(AgentRegistration {
            protocol_version: PROTOCOL_VERSION,
            agent_id: Uuid::new_v4(),
            name: "linux-01".into(),
            capabilities: capabilities(),
        });
        message.validate().expect("valid registration");
        let encoded = serde_json::to_string(&message).expect("encode");
        assert!(encoded.contains(r#""type":"register""#));
        let decoded: AgentMessage = serde_json::from_str(&encoded).expect("decode");
        assert_eq!(decoded, message);
    }

    #[test]
    fn assignment_round_trips_with_a_bounded_workspace_transfer() {
        let pipeline = Pipeline::from_toml_str(
            r#"
version = 1
name = "remote-build"
[[stages]]
name = "Test"
[[stages.steps]]
name = "unit"
program = "true"
"#,
        )
        .expect("pipeline");
        let build_id = Uuid::new_v4();
        let project_id = Uuid::new_v4();
        let message = AgentMessage::Assign {
            protocol_version: PROTOCOL_VERSION,
            build_id,
            attempt_id: Uuid::new_v4(),
            project_id,
            plan: ExecutionPlan::from_pipeline(&pipeline, build_id, project_id),
            pipeline,
            parameters: BTreeMap::new(),
            workspace: WorkspaceTransfer {
                total_bytes: 4096,
                file_count: 2,
                sha256: "a".repeat(64),
            },
        };
        message.validate().expect("valid assignment");
        let encoded = serde_json::to_string(&message).expect("encode");
        let decoded: AgentMessage = serde_json::from_str(&encoded).expect("decode");
        assert_eq!(decoded, message);
    }

    #[test]
    fn remote_event_identity_is_required_and_versioned() {
        let build_id = Uuid::new_v4();
        let event = AgentMessage::Event {
            protocol_version: PROTOCOL_VERSION,
            attempt_id: Uuid::new_v4(),
            sequence: 1,
            event: BuildEvent::BuildStarted {
                build_id,
                timestamp: Utc::now(),
            },
        };
        event.validate().expect("valid remote event");
        let encoded = serde_json::to_string(&event).expect("encode");
        assert!(encoded.contains(r#""sequence":1"#));
        assert_eq!(
            serde_json::from_str::<AgentMessage>(&encoded).unwrap(),
            event
        );

        let mut invalid = event.clone();
        if let AgentMessage::Event { sequence, .. } = &mut invalid {
            *sequence = 0;
        }
        assert_eq!(invalid.validate(), Err(ProtocolError::ZeroEventSequence));
    }

    #[test]
    fn workspace_chunks_are_bounded_before_transport() {
        let message = AgentMessage::WorkspaceChunk {
            protocol_version: PROTOCOL_VERSION,
            build_id: Uuid::new_v4(),
            sequence: 1,
            data: vec![0; MAX_WORKSPACE_CHUNK_BYTES + 1],
        };
        assert_eq!(
            message.validate(),
            Err(ProtocolError::WorkspaceChunkTooLarge)
        );
    }

    #[test]
    fn protocol_rejects_unknown_versions_and_invalid_capabilities() {
        let mut registration = AgentRegistration {
            protocol_version: PROTOCOL_VERSION + 1,
            agent_id: Uuid::new_v4(),
            name: "linux-01".into(),
            capabilities: capabilities(),
        };
        assert!(matches!(
            registration.validate(),
            Err(ProtocolError::UnsupportedVersion { .. })
        ));
        registration.protocol_version = PROTOCOL_VERSION;
        registration.capabilities.executors = 0;
        assert_eq!(registration.validate(), Err(ProtocolError::ZeroExecutors));
        registration.capabilities.executors = 1;
        registration.capabilities.cpu_cores = Some(0);
        assert_eq!(
            registration.validate(),
            Err(ProtocolError::ZeroCapability("cpu_cores"))
        );
        registration.capabilities.cpu_cores = None;
        registration.capabilities.memory_mb = Some(0);
        assert_eq!(
            registration.validate(),
            Err(ProtocolError::ZeroMemory("memory_mb"))
        );
    }

    #[test]
    fn requirements_match_capabilities_without_fuzzy_labels() {
        let capabilities = capabilities();
        let requirements = AgentRequirements {
            os: Some("linux".into()),
            arch: Some("x86_64".into()),
            docker: true,
            labels: vec!["production".into()],
            executors: Some(2),
            cpu_cores: Some(4),
            memory_mb: Some(8 * 1024),
        };
        requirements.validate().expect("valid requirements");
        assert!(capabilities.supports(&requirements));
        assert!(!capabilities.supports(&AgentRequirements {
            labels: vec!["prod".into()],
            ..AgentRequirements::default()
        }));
        assert!(!capabilities.supports(&AgentRequirements {
            os: Some("darwin".into()),
            ..AgentRequirements::default()
        }));
        assert!(!capabilities.supports(&AgentRequirements {
            cpu_cores: Some(16),
            ..AgentRequirements::default()
        }));
        assert!(!capabilities.supports(&AgentRequirements {
            memory_mb: Some(32 * 1024),
            ..AgentRequirements::default()
        }));
    }

    #[test]
    fn requirements_reject_empty_values_and_zero_capacity() {
        assert_eq!(
            AgentRequirements {
                os: Some("   ".into()),
                ..AgentRequirements::default()
            }
            .validate(),
            Err(ProtocolError::EmptyRequirement("os"))
        );
        assert_eq!(
            AgentRequirements {
                executors: Some(0),
                ..AgentRequirements::default()
            }
            .validate(),
            Err(ProtocolError::ZeroRequiredExecutors)
        );
        assert_eq!(AgentRequirements::default().requested_executors(), 1);
        assert_eq!(
            AgentRequirements {
                cpu_cores: Some(0),
                ..AgentRequirements::default()
            }
            .validate(),
            Err(ProtocolError::ZeroRequiredCpuCores)
        );
        assert_eq!(
            AgentRequirements {
                memory_mb: Some(0),
                ..AgentRequirements::default()
            }
            .validate(),
            Err(ProtocolError::ZeroRequiredMemory)
        );
    }

    #[test]
    fn pipeline_agent_requirements_convert_to_wire_requirements() {
        let pipeline_requirement = AgentRequirement {
            os: Some("linux".into()),
            arch: Some("x86_64".into()),
            docker: true,
            labels: vec!["build".into()],
            executors: Some(2),
            cpu_cores: Some(4),
            memory_mb: Some(8192),
        };
        let wire = AgentRequirements::from(&pipeline_requirement);
        assert_eq!(wire.os.as_deref(), Some("linux"));
        assert_eq!(wire.arch.as_deref(), Some("x86_64"));
        assert!(wire.docker);
        assert_eq!(wire.labels, ["build"]);
        assert_eq!(wire.executors, Some(2));
        assert_eq!(wire.cpu_cores, Some(4));
        assert_eq!(wire.memory_mb, Some(8192));
    }

    #[test]
    fn heartbeat_limits_advertised_running_work() {
        let heartbeat = AgentHeartbeat {
            protocol_version: PROTOCOL_VERSION,
            agent_id: Uuid::new_v4(),
            session_id: Uuid::new_v4(),
            sequence: 1,
            running: (0..=MAX_RUNNING_BUILDS).map(|_| Uuid::new_v4()).collect(),
            sent_at: Utc::now(),
        };
        assert_eq!(
            heartbeat.validate(),
            Err(ProtocolError::TooManyRunningBuilds)
        );
    }

    #[test]
    fn transport_envelope_round_trips_messages_and_acknowledgements() {
        let payload = AgentMessage::Heartbeat(AgentHeartbeat {
            protocol_version: PROTOCOL_VERSION,
            agent_id: Uuid::new_v4(),
            session_id: Uuid::new_v4(),
            sequence: 7,
            running: vec![],
            sent_at: Utc::now(),
        });
        let envelope = AgentTransportMessage::message(payload);
        envelope.validate().expect("valid transport message");
        let delivery_id = envelope.delivery_id();
        let encoded = serde_json::to_string(&envelope).expect("encode message");
        let decoded: AgentTransportMessage =
            serde_json::from_str(&encoded).expect("decode message");
        assert_eq!(decoded, envelope);
        assert_eq!(decoded.delivery_id(), delivery_id);

        let ack = AgentTransportMessage::ack(delivery_id);
        ack.validate().expect("valid acknowledgement");
        let encoded = serde_json::to_string(&ack).expect("encode acknowledgement");
        let decoded: AgentTransportMessage =
            serde_json::from_str(&encoded).expect("decode acknowledgement");
        assert_eq!(decoded, ack);
        assert!(matches!(decoded, AgentTransportMessage::Ack { .. }));
    }

    #[test]
    fn transport_envelope_rejects_nil_delivery_ids_and_invalid_payloads() {
        let ack = AgentTransportMessage::Ack {
            protocol_version: PROTOCOL_VERSION,
            delivery_id: Uuid::nil(),
        };
        assert_eq!(ack.validate(), Err(ProtocolError::EmptyDeliveryId));

        let invalid = AgentTransportMessage::Message {
            protocol_version: PROTOCOL_VERSION,
            delivery_id: Uuid::new_v4(),
            payload: AgentMessage::Heartbeat(AgentHeartbeat {
                protocol_version: PROTOCOL_VERSION - 1,
                agent_id: Uuid::new_v4(),
                session_id: Uuid::new_v4(),
                sequence: 0,
                running: vec![],
                sent_at: Utc::now(),
            }),
        };
        assert!(matches!(
            invalid.validate(),
            Err(ProtocolError::UnsupportedVersion { .. })
        ));
    }
}
