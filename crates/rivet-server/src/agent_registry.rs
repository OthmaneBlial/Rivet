use chrono::{DateTime, Duration, Utc};
use rivet_agent_protocol::{
    AgentCapabilities, AgentHeartbeat, AgentId, AgentRegistration, ProtocolError,
};
use serde::Serialize;
use std::collections::HashMap;
use std::sync::Arc;
use thiserror::Error;
use tokio::sync::RwLock;
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
    pub status: AgentStatus,
}

#[derive(Debug, Clone)]
pub struct AgentLease {
    pub agent_id: AgentId,
    pub session_id: Uuid,
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
    session_id: Uuid,
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
            session_id,
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
        status,
    }
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
}
