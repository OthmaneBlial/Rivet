use chrono::{DateTime, Duration, Utc};
use rivet_agent_protocol::{
    AgentCapabilities, AgentHeartbeat, AgentId, AgentMessage, AgentRegistration, AgentRequirements,
    AgentTransportMessage, ProtocolError,
};
use serde::Serialize;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use thiserror::Error;
use tokio::sync::{RwLock, mpsc};
use uuid::Uuid;

const DEFAULT_STALE_AFTER: Duration = Duration::seconds(30);

#[derive(Clone)]
pub struct AgentRegistry {
    agents: Arc<RwLock<HashMap<AgentId, AgentEntry>>>,
    stale_after: Duration,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum AgentStatus {
    Online,
    Stale,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct AgentSummary {
    pub agent_id: AgentId,
    pub name: String,
    pub protocol_version: u16,
    pub capabilities: AgentCapabilities,
    pub connected_at: DateTime<Utc>,
    pub last_heartbeat: DateTime<Utc>,
    pub last_sequence: u64,
    pub running: Vec<rivet_core::BuildId>,
    pub reserved: Vec<rivet_core::BuildId>,
    pub available_executors: u16,
    pub available_cpu_cores: Option<u16>,
    pub available_memory_mb: Option<u64>,
    pub available_disk_mb: Option<u64>,
    pub status: AgentStatus,
}

#[derive(Debug, Clone)]
pub struct AgentLease {
    pub agent_id: AgentId,
    pub session_id: Uuid,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentReservation {
    pub agent_id: AgentId,
    pub session_id: Uuid,
    pub build_id: rivet_core::BuildId,
}

#[derive(Debug, Error)]
pub enum AgentRegistryError {
    #[error(transparent)]
    Protocol(#[from] ProtocolError),
    #[error("agent {0} is not registered")]
    UnknownAgent(AgentId),
    #[error("agent {0} session is no longer current")]
    StaleSession(AgentId),
    #[error("agent {agent_id} heartbeat sequence {sequence} is older than {last_sequence}")]
    OutOfOrderHeartbeat {
        agent_id: AgentId,
        sequence: u64,
        last_sequence: u64,
    },
    #[error("no online agent matches the requested capabilities and available capacity")]
    NoMatchingAgent,
    #[error("agent {0} is not connected for remote assignment")]
    AgentNotConnected(AgentId),
    #[error("agent {0} assignment channel is closed")]
    AgentChannelClosed(AgentId),
    #[error("build {0} already has an agent reservation")]
    ReservationConflict(rivet_core::BuildId),
    #[error("agent {agent_id} reservation for build {build_id} is no longer current")]
    StaleReservation {
        agent_id: AgentId,
        build_id: rivet_core::BuildId,
    },
}

struct AgentEntry {
    agent_id: AgentId,
    name: String,
    protocol_version: u16,
    capabilities: AgentCapabilities,
    connected_at: DateTime<Utc>,
    last_heartbeat: DateTime<Utc>,
    last_sequence: u64,
    running: Vec<rivet_core::BuildId>,
    reserved: HashMap<rivet_core::BuildId, ReservedResources>,
    session_id: Uuid,
    outbound: Option<mpsc::Sender<AgentTransportMessage>>,
}

#[derive(Debug, Clone, Copy)]
struct ReservedResources {
    executors: u16,
    cpu_cores: u16,
    memory_mb: u64,
    disk_mb: u64,
}

impl Default for AgentRegistry {
    fn default() -> Self {
        Self::new(DEFAULT_STALE_AFTER)
    }
}

impl AgentRegistry {
    pub fn new(stale_after: Duration) -> Self {
        assert!(
            stale_after > Duration::zero(),
            "stale duration must be positive"
        );
        Self {
            agents: Arc::new(RwLock::new(HashMap::new())),
            stale_after,
        }
    }

    pub async fn register(
        &self,
        registration: AgentRegistration,
        now: DateTime<Utc>,
    ) -> Result<AgentLease, AgentRegistryError> {
        self.register_inner(registration, now, None).await
    }

    pub async fn register_with_sender(
        &self,
        registration: AgentRegistration,
        now: DateTime<Utc>,
        outbound: mpsc::Sender<AgentTransportMessage>,
    ) -> Result<AgentLease, AgentRegistryError> {
        self.register_inner(registration, now, Some(outbound)).await
    }

    async fn register_inner(
        &self,
        registration: AgentRegistration,
        now: DateTime<Utc>,
        outbound: Option<mpsc::Sender<AgentTransportMessage>>,
    ) -> Result<AgentLease, AgentRegistryError> {
        registration.validate()?;
        let session_id = Uuid::new_v4();
        let entry = AgentEntry {
            agent_id: registration.agent_id,
            name: registration.name,
            protocol_version: registration.protocol_version,
            capabilities: registration.capabilities,
            connected_at: now,
            last_heartbeat: now,
            last_sequence: 0,
            running: Vec::new(),
            reserved: HashMap::new(),
            session_id,
            outbound,
        };
        self.agents
            .write()
            .await
            .insert(registration.agent_id, entry);
        Ok(AgentLease {
            agent_id: registration.agent_id,
            session_id,
        })
    }

    pub async fn heartbeat(
        &self,
        heartbeat: AgentHeartbeat,
        now: DateTime<Utc>,
    ) -> Result<AgentSummary, AgentRegistryError> {
        heartbeat.validate()?;
        let mut agents = self.agents.write().await;
        let entry = agents
            .get_mut(&heartbeat.agent_id)
            .ok_or(AgentRegistryError::UnknownAgent(heartbeat.agent_id))?;
        if entry.session_id != heartbeat.session_id {
            return Err(AgentRegistryError::StaleSession(heartbeat.agent_id));
        }
        if heartbeat.sequence < entry.last_sequence {
            return Err(AgentRegistryError::OutOfOrderHeartbeat {
                agent_id: heartbeat.agent_id,
                sequence: heartbeat.sequence,
                last_sequence: entry.last_sequence,
            });
        }
        entry.last_heartbeat = now;
        entry.last_sequence = heartbeat.sequence;
        entry.running = heartbeat.running;
        Ok(summary(entry, self.status(entry, now)))
    }

    pub async fn unregister(&self, agent_id: AgentId, session_id: Uuid) {
        let mut agents = self.agents.write().await;
        if agents
            .get(&agent_id)
            .is_some_and(|entry| entry.session_id == session_id)
        {
            agents.remove(&agent_id);
        }
    }

    pub async fn list(&self, now: DateTime<Utc>) -> Vec<AgentSummary> {
        let agents = self.agents.read().await;
        let mut summaries: Vec<_> = agents
            .values()
            .map(|entry| summary(entry, self.status(entry, now)))
            .collect();
        summaries.sort_by(|left, right| left.name.cmp(&right.name));
        summaries
    }

    pub async fn matching(
        &self,
        requirements: &AgentRequirements,
        now: DateTime<Utc>,
    ) -> Result<Vec<AgentSummary>, AgentRegistryError> {
        requirements.validate()?;
        let agents = self.agents.read().await;
        let mut summaries: Vec<_> = agents
            .values()
            .filter(|entry| self.status(entry, now) == AgentStatus::Online)
            .filter(|entry| entry.capabilities.supports(requirements))
            .filter(|entry| resources_available(entry, requirements))
            .map(|entry| summary(entry, AgentStatus::Online))
            .collect();
        summaries.sort_by(|left, right| left.name.cmp(&right.name));
        Ok(summaries)
    }

    pub async fn reserve(
        &self,
        requirements: &AgentRequirements,
        build_id: rivet_core::BuildId,
        now: DateTime<Utc>,
    ) -> Result<AgentReservation, AgentRegistryError> {
        requirements.validate()?;
        let mut agents = self.agents.write().await;
        if agents
            .values()
            .any(|entry| entry.reserved.contains_key(&build_id))
        {
            return Err(AgentRegistryError::ReservationConflict(build_id));
        }
        let requested = requirements.requested_executors();
        let mut candidates: Vec<_> = agents
            .values()
            .filter(|entry| self.status(entry, now) == AgentStatus::Online)
            .filter(|entry| entry.capabilities.supports(requirements))
            .filter(|entry| resources_available(entry, requirements))
            .map(|entry| {
                (
                    available_executors(entry),
                    entry.name.clone(),
                    entry.agent_id,
                )
            })
            .collect();
        candidates.sort_by(|left, right| {
            right
                .0
                .cmp(&left.0)
                .then_with(|| left.1.cmp(&right.1))
                .then_with(|| left.2.cmp(&right.2))
        });
        let Some((_, _, agent_id)) = candidates.into_iter().next() else {
            return Err(AgentRegistryError::NoMatchingAgent);
        };
        let entry = agents
            .get_mut(&agent_id)
            .expect("matching agent remains in registry write lock");
        entry.reserved.insert(
            build_id,
            ReservedResources {
                executors: requested,
                cpu_cores: requirements.requested_cpu_cores(),
                memory_mb: requirements.requested_memory_mb(),
                disk_mb: requirements.requested_disk_mb(),
            },
        );
        Ok(AgentReservation {
            agent_id,
            session_id: entry.session_id,
            build_id,
        })
    }

    pub async fn reserve_for_agent(
        &self,
        requirements: &AgentRequirements,
        build_id: rivet_core::BuildId,
        agent_id: AgentId,
        now: DateTime<Utc>,
    ) -> Result<AgentReservation, AgentRegistryError> {
        requirements.validate()?;
        let mut agents = self.agents.write().await;
        if agents
            .values()
            .any(|entry| entry.reserved.contains_key(&build_id))
        {
            return Err(AgentRegistryError::ReservationConflict(build_id));
        }
        let requested = requirements.requested_executors();
        let entry = agents
            .get_mut(&agent_id)
            .filter(|entry| self.status(entry, now) == AgentStatus::Online)
            .filter(|entry| entry.capabilities.supports(requirements))
            .filter(|entry| resources_available(entry, requirements))
            .ok_or(AgentRegistryError::NoMatchingAgent)?;
        entry.reserved.insert(
            build_id,
            ReservedResources {
                executors: requested,
                cpu_cores: requirements.requested_cpu_cores(),
                memory_mb: requirements.requested_memory_mb(),
                disk_mb: requirements.requested_disk_mb(),
            },
        );
        Ok(AgentReservation {
            agent_id,
            session_id: entry.session_id,
            build_id,
        })
    }

    pub async fn release(&self, reservation: &AgentReservation) -> bool {
        let mut agents = self.agents.write().await;
        let Some(entry) = agents.get_mut(&reservation.agent_id) else {
            return false;
        };
        if entry.session_id != reservation.session_id {
            return false;
        }
        entry.reserved.remove(&reservation.build_id).is_some()
    }

    pub async fn send(
        &self,
        reservation: &AgentReservation,
        message: AgentMessage,
    ) -> Result<(), AgentRegistryError> {
        let sender = {
            let agents = self.agents.read().await;
            let entry = agents
                .get(&reservation.agent_id)
                .ok_or(AgentRegistryError::AgentNotConnected(reservation.agent_id))?;
            if entry.session_id != reservation.session_id
                || !entry.reserved.contains_key(&reservation.build_id)
            {
                return Err(AgentRegistryError::StaleReservation {
                    agent_id: reservation.agent_id,
                    build_id: reservation.build_id,
                });
            }
            entry
                .outbound
                .clone()
                .ok_or(AgentRegistryError::AgentNotConnected(reservation.agent_id))?
        };
        sender
            .send(AgentTransportMessage::message(message))
            .await
            .map_err(|_| AgentRegistryError::AgentChannelClosed(reservation.agent_id))
    }

    pub async fn reservation_online(
        &self,
        reservation: &AgentReservation,
        now: DateTime<Utc>,
    ) -> bool {
        let agents = self.agents.read().await;
        agents.get(&reservation.agent_id).is_some_and(|entry| {
            entry.session_id == reservation.session_id
                && entry.reserved.contains_key(&reservation.build_id)
                && self.status(entry, now) == AgentStatus::Online
        })
    }

    fn status(&self, entry: &AgentEntry, now: DateTime<Utc>) -> AgentStatus {
        if now - entry.last_heartbeat <= self.stale_after {
            AgentStatus::Online
        } else {
            AgentStatus::Stale
        }
    }
}

fn summary(entry: &AgentEntry, status: AgentStatus) -> AgentSummary {
    AgentSummary {
        agent_id: entry.agent_id,
        name: entry.name.clone(),
        protocol_version: entry.protocol_version,
        capabilities: entry.capabilities.clone(),
        connected_at: entry.connected_at,
        last_heartbeat: entry.last_heartbeat,
        last_sequence: entry.last_sequence,
        running: entry.running.clone(),
        reserved: {
            let mut reserved = entry.reserved.keys().copied().collect::<Vec<_>>();
            reserved.sort();
            reserved
        },
        available_executors: available_executors(entry),
        available_cpu_cores: available_cpu_cores(entry),
        available_memory_mb: available_memory_mb(entry),
        available_disk_mb: available_disk_mb(entry),
        status,
    }
}

fn available_executors(entry: &AgentEntry) -> u16 {
    let reserved_executors = entry
        .reserved
        .values()
        .map(|resources| u32::from(resources.executors))
        .sum::<u32>();
    let reserved_builds = entry.reserved.keys().collect::<HashSet<_>>();
    let running_executors = entry
        .running
        .iter()
        .filter(|build_id| !reserved_builds.contains(build_id))
        .collect::<HashSet<_>>()
        .len() as u32;
    entry
        .capabilities
        .executors
        .saturating_sub((reserved_executors + running_executors).min(u32::from(u16::MAX)) as u16)
}

fn resources_available(entry: &AgentEntry, requirements: &AgentRequirements) -> bool {
    if available_executors(entry) < requirements.requested_executors() {
        return false;
    }
    requirements.cpu_cores.is_none_or(|required| {
        available_cpu_cores(entry).is_some_and(|available| available >= required)
    }) && requirements.memory_mb.is_none_or(|required| {
        available_memory_mb(entry).is_some_and(|available| available >= required)
    }) && requirements.disk_mb.is_none_or(|required| {
        available_disk_mb(entry).is_some_and(|available| available >= required)
    })
}

fn has_unreserved_running_build(entry: &AgentEntry) -> bool {
    entry
        .running
        .iter()
        .any(|build_id| !entry.reserved.contains_key(build_id))
}

fn available_cpu_cores(entry: &AgentEntry) -> Option<u16> {
    let capacity = entry.capabilities.cpu_cores?;
    if has_unreserved_running_build(entry) {
        return Some(0);
    }
    let reserved = entry
        .reserved
        .values()
        .map(|resources| u32::from(resources.cpu_cores))
        .sum::<u32>();
    Some(capacity.saturating_sub(reserved.min(u32::from(u16::MAX)) as u16))
}

fn available_memory_mb(entry: &AgentEntry) -> Option<u64> {
    let capacity = entry.capabilities.memory_mb?;
    if has_unreserved_running_build(entry) {
        return Some(0);
    }
    let reserved = entry
        .reserved
        .values()
        .map(|resources| resources.memory_mb)
        .sum::<u64>();
    Some(capacity.saturating_sub(reserved))
}

fn available_disk_mb(entry: &AgentEntry) -> Option<u64> {
    let capacity = entry.capabilities.disk_mb?;
    if has_unreserved_running_build(entry) {
        return Some(0);
    }
    let reserved = entry
        .reserved
        .values()
        .map(|resources| resources.disk_mb)
        .sum::<u64>();
    Some(capacity.saturating_sub(reserved))
}

#[cfg(test)]
mod tests {
    use super::*;
    use rivet_agent_protocol::{AgentCapabilities, PROTOCOL_VERSION};

    fn registration(agent_id: AgentId) -> AgentRegistration {
        AgentRegistration {
            protocol_version: PROTOCOL_VERSION,
            agent_id,
            name: "linux-01".into(),
            capabilities: AgentCapabilities {
                os: "linux".into(),
                arch: "x86_64".into(),
                docker: true,
                labels: vec!["build".into()],
                executors: 2,
                cpu_cores: Some(8),
                memory_mb: Some(16 * 1024),
                disk_mb: Some(128 * 1024),
            },
        }
    }

    #[tokio::test]
    async fn registry_tracks_heartbeat_and_marks_silent_agents_stale() {
        let registry = AgentRegistry::new(Duration::seconds(10));
        let agent_id = Uuid::new_v4();
        let connected_at = Utc::now();
        let lease = registry
            .register(registration(agent_id), connected_at)
            .await
            .expect("register");
        let heartbeat = AgentHeartbeat {
            protocol_version: PROTOCOL_VERSION,
            agent_id,
            session_id: lease.session_id,
            sequence: 4,
            running: vec![Uuid::new_v4()],
            sent_at: connected_at,
        };
        let updated = registry
            .heartbeat(heartbeat, connected_at + Duration::seconds(5))
            .await
            .expect("heartbeat");
        assert_eq!(updated.last_sequence, 4);
        assert_eq!(updated.status, AgentStatus::Online);
        assert_eq!(
            registry.list(connected_at + Duration::seconds(16)).await[0].status,
            AgentStatus::Stale
        );
    }

    #[tokio::test]
    async fn stale_sessions_and_old_sequences_cannot_mutate_a_reconnected_agent() {
        let registry = AgentRegistry::new(Duration::seconds(10));
        let agent_id = Uuid::new_v4();
        let first = registry
            .register(registration(agent_id), Utc::now())
            .await
            .expect("first register");
        let second = registry
            .register(registration(agent_id), Utc::now())
            .await
            .expect("reconnect");
        let stale = AgentHeartbeat {
            protocol_version: PROTOCOL_VERSION,
            agent_id,
            session_id: first.session_id,
            sequence: 1,
            running: vec![],
            sent_at: Utc::now(),
        };
        assert!(matches!(
            registry.heartbeat(stale, Utc::now()).await,
            Err(AgentRegistryError::StaleSession(_))
        ));
        let current = AgentHeartbeat {
            protocol_version: PROTOCOL_VERSION,
            agent_id,
            session_id: second.session_id,
            sequence: 5,
            running: vec![],
            sent_at: Utc::now(),
        };
        registry
            .heartbeat(current, Utc::now())
            .await
            .expect("current heartbeat");
        let old = AgentHeartbeat {
            protocol_version: PROTOCOL_VERSION,
            agent_id,
            session_id: second.session_id,
            sequence: 4,
            running: vec![],
            sent_at: Utc::now(),
        };
        assert!(matches!(
            registry.heartbeat(old, Utc::now()).await,
            Err(AgentRegistryError::OutOfOrderHeartbeat { .. })
        ));
    }

    #[tokio::test]
    async fn old_disconnect_does_not_remove_a_new_session() {
        let registry = AgentRegistry::default();
        let agent_id = Uuid::new_v4();
        let first = registry
            .register(registration(agent_id), Utc::now())
            .await
            .expect("first register");
        let second = registry
            .register(registration(agent_id), Utc::now())
            .await
            .expect("second register");
        registry.unregister(agent_id, first.session_id).await;
        assert_eq!(registry.list(Utc::now()).await.len(), 1);
        registry.unregister(agent_id, second.session_id).await;
        assert!(registry.list(Utc::now()).await.is_empty());
    }

    #[tokio::test]
    async fn matching_is_capacity_aware_and_excludes_stale_or_incompatible_agents() {
        let registry = AgentRegistry::new(Duration::seconds(10));
        let now = Utc::now();
        let available_id = Uuid::new_v4();
        let busy_id = Uuid::new_v4();
        let stale_id = Uuid::new_v4();
        registry
            .register(registration(available_id), now)
            .await
            .expect("available agent");
        let busy = registry
            .register(registration(busy_id), now)
            .await
            .expect("busy agent");
        registry
            .register(registration(stale_id), now - Duration::seconds(20))
            .await
            .expect("stale agent");
        registry
            .heartbeat(
                AgentHeartbeat {
                    protocol_version: PROTOCOL_VERSION,
                    agent_id: busy_id,
                    session_id: busy.session_id,
                    sequence: 1,
                    running: vec![Uuid::new_v4(), Uuid::new_v4()],
                    sent_at: now,
                },
                now,
            )
            .await
            .expect("busy heartbeat");

        let matches = registry
            .matching(
                &AgentRequirements {
                    os: Some("linux".into()),
                    arch: Some("x86_64".into()),
                    docker: true,
                    labels: vec!["build".into()],
                    executors: Some(2),
                    cpu_cores: Some(4),
                    memory_mb: Some(8 * 1024),
                    disk_mb: Some(64 * 1024),
                },
                now,
            )
            .await
            .expect("matches");
        assert_eq!(matches.len(), 1);
        assert_eq!(matches[0].agent_id, available_id);
        assert_eq!(matches[0].available_executors, 2);

        let resource_matches = registry
            .matching(
                &AgentRequirements {
                    cpu_cores: Some(4),
                    memory_mb: Some(8 * 1024),
                    ..AgentRequirements::default()
                },
                now,
            )
            .await
            .expect("resource matches");
        assert_eq!(resource_matches.len(), 1);
        assert_eq!(resource_matches[0].agent_id, available_id);
        assert!(
            registry
                .matching(
                    &AgentRequirements {
                        memory_mb: Some(32 * 1024),
                        ..AgentRequirements::default()
                    },
                    now,
                )
                .await
                .expect("insufficient memory matches")
                .is_empty()
        );

        let resource_reservation = registry
            .reserve(
                &AgentRequirements {
                    cpu_cores: Some(6),
                    memory_mb: Some(12 * 1024),
                    disk_mb: Some(64 * 1024),
                    ..AgentRequirements::default()
                },
                Uuid::new_v4(),
                now,
            )
            .await
            .expect("resource reservation");
        let summary = registry.list(now).await;
        let available_summary = summary
            .iter()
            .find(|summary| summary.agent_id == available_id)
            .expect("available resource summary");
        assert_eq!(available_summary.available_cpu_cores, Some(2));
        assert_eq!(available_summary.available_memory_mb, Some(4 * 1024));
        assert_eq!(available_summary.available_disk_mb, Some(64 * 1024));
        assert!(
            registry
                .matching(
                    &AgentRequirements {
                        cpu_cores: Some(4),
                        ..AgentRequirements::default()
                    },
                    now,
                )
                .await
                .expect("resource capacity is reserved")
                .is_empty()
        );
        assert!(registry.release(&resource_reservation).await);

        let one_slot = registry
            .matching(
                &AgentRequirements {
                    os: Some("linux".into()),
                    executors: Some(1),
                    ..AgentRequirements::default()
                },
                now,
            )
            .await
            .expect("one-slot matches");
        assert_eq!(one_slot.len(), 1);
        assert_eq!(one_slot[0].agent_id, available_id);
        assert!(
            registry
                .matching(&AgentRequirements::default(), now + Duration::seconds(11))
                .await
                .expect("stale matches")
                .is_empty()
        );
    }

    #[tokio::test]
    async fn invalid_requirements_are_rejected_before_matching() {
        let registry = AgentRegistry::default();
        let error = registry
            .matching(
                &AgentRequirements {
                    labels: vec!["".into()],
                    ..AgentRequirements::default()
                },
                Utc::now(),
            )
            .await
            .expect_err("invalid requirement");
        assert!(matches!(
            error,
            AgentRegistryError::Protocol(ProtocolError::InvalidRequirementLabel)
        ));
    }

    #[tokio::test]
    async fn reservations_are_atomic_and_release_capacity_after_completion() {
        let registry = AgentRegistry::default();
        let now = Utc::now();
        let agent_id = Uuid::new_v4();
        let (outbound, mut received) = mpsc::channel(4);
        let lease = registry
            .register_with_sender(registration(agent_id), now, outbound)
            .await
            .expect("register agent");
        let first_build = Uuid::new_v4();
        let first = registry
            .reserve(
                &AgentRequirements {
                    executors: Some(2),
                    ..AgentRequirements::default()
                },
                first_build,
                now,
            )
            .await
            .expect("first reservation");
        assert_eq!(first.agent_id, agent_id);
        assert_eq!(registry.list(now).await[0].available_executors, 0);
        assert!(matches!(
            registry
                .reserve(&AgentRequirements::default(), Uuid::new_v4(), now)
                .await,
            Err(AgentRegistryError::NoMatchingAgent)
        ));

        registry
            .send(
                &first,
                AgentMessage::AssignmentAccepted {
                    protocol_version: PROTOCOL_VERSION,
                    build_id: first_build,
                },
            )
            .await
            .expect("send assignment");
        assert!(matches!(
            received.recv().await,
            Some(AgentTransportMessage::Message {
                payload: AgentMessage::AssignmentAccepted { build_id, .. },
                ..
            }) if build_id == first_build
        ));
        assert!(registry.release(&first).await);
        let second = registry
            .reserve(&AgentRequirements::default(), Uuid::new_v4(), now)
            .await
            .expect("released capacity");
        assert_eq!(second.agent_id, agent_id);
        assert_eq!(lease.session_id, first.session_id);
    }

    #[tokio::test]
    async fn durable_recovery_can_prefer_the_original_agent_session() {
        let registry = AgentRegistry::default();
        let now = Utc::now();
        let preferred_id = Uuid::new_v4();
        let other_id = Uuid::new_v4();
        registry
            .register(registration(preferred_id), now)
            .await
            .expect("preferred agent");
        registry
            .register(registration(other_id), now)
            .await
            .expect("other agent");
        let build_id = Uuid::new_v4();
        let reservation = registry
            .reserve_for_agent(&AgentRequirements::default(), build_id, preferred_id, now)
            .await
            .expect("preferred reservation");
        assert_eq!(reservation.agent_id, preferred_id);
        let preferred = registry
            .list(now)
            .await
            .into_iter()
            .find(|agent| agent.agent_id == preferred_id)
            .expect("preferred summary");
        assert_eq!(preferred.reserved, vec![build_id]);
    }
}
