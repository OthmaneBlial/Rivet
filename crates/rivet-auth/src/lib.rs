//! Small, provider-neutral authentication and authorization primitives.
//!
//! Policy files contain only SHA-256 token digests. The raw API token is
//! supplied at runtime, hashed for comparison, and never represented by a
//! persisted or debug-printable value in this crate.

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::BTreeSet;
use std::fmt;
use subtle::ConstantTimeEq;
use thiserror::Error;

const POLICY_VERSION: u8 = 1;
const TOKEN_DIGEST_BYTES: usize = 32;
const MAX_TOKEN_ID_BYTES: usize = 64;
const MAX_PROJECT_NAME_BYTES: usize = 128;

#[derive(Debug, Error, PartialEq, Eq)]
pub enum AuthError {
    #[error("authentication policy version {0} is unsupported; expected {POLICY_VERSION}")]
    UnsupportedVersion(u8),
    #[error("authentication policy must contain at least one token")]
    EmptyPolicy,
    #[error("authentication token ID is invalid: {0}")]
    InvalidTokenId(String),
    #[error("authentication token IDs must be unique: {0}")]
    DuplicateTokenId(String),
    #[error("authentication token digest for {0} is invalid")]
    InvalidTokenDigest(String),
    #[error("authentication project scope is invalid: {0}")]
    InvalidProjectScope(String),
    #[error("authentication policy JSON is invalid: {0}")]
    InvalidJson(String),
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum Role {
    Admin,
    Operator,
    Viewer,
    Agent,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Permission {
    Read,
    Build,
    Administer,
    ConnectAgent,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct AuthPolicyDocument {
    pub version: u8,
    pub tokens: Vec<ApiTokenRecord>,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct ApiTokenRecord {
    pub id: String,
    /// Lowercase hexadecimal SHA-256 digest of the runtime Bearer token.
    pub sha256: String,
    pub role: Role,
    #[serde(default)]
    pub projects: Vec<String>,
}

#[derive(Clone, PartialEq, Eq)]
pub struct Principal {
    id: String,
    role: Role,
    projects: BTreeSet<String>,
}

impl Principal {
    /// Identity used by an unauthenticated loopback/local instance.
    pub fn local_admin() -> Self {
        Self {
            id: "local".into(),
            role: Role::Admin,
            projects: BTreeSet::from(["*".into()]),
        }
    }

    /// Compatibility identity for the legacy single Bearer-token setting.
    pub fn legacy_admin() -> Self {
        Self {
            id: "legacy-token".into(),
            role: Role::Admin,
            projects: BTreeSet::from(["*".into()]),
        }
    }

    pub fn id(&self) -> &str {
        &self.id
    }

    pub fn role(&self) -> Role {
        self.role
    }

    pub fn projects(&self) -> impl Iterator<Item = &str> {
        self.projects.iter().map(String::as_str)
    }

    pub fn can_global(&self, permission: Permission) -> bool {
        match permission {
            Permission::Read => matches!(self.role, Role::Admin | Role::Operator | Role::Viewer),
            Permission::Build => matches!(self.role, Role::Admin | Role::Operator),
            Permission::Administer => self.role == Role::Admin,
            Permission::ConnectAgent => matches!(self.role, Role::Admin | Role::Agent),
        }
    }

    pub fn can_project(&self, permission: Permission, project: &str) -> bool {
        self.can_global(permission)
            && (self.role == Role::Admin
                || self.projects.contains("*")
                || self.projects.contains(project))
    }
}

impl fmt::Debug for Principal {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Principal")
            .field("id", &self.id)
            .field("role", &self.role)
            .field("projects", &self.projects)
            .finish()
    }
}

#[derive(Debug, Clone)]
pub struct AuthPolicy {
    document: AuthPolicyDocument,
    digests: Vec<[u8; TOKEN_DIGEST_BYTES]>,
}

impl AuthPolicy {
    pub fn from_json(bytes: &[u8]) -> Result<Self, AuthError> {
        let document: AuthPolicyDocument = serde_json::from_slice(bytes)
            .map_err(|error| AuthError::InvalidJson(error.to_string()))?;
        Self::from_document(document)
    }

    pub fn from_document(document: AuthPolicyDocument) -> Result<Self, AuthError> {
        if document.version != POLICY_VERSION {
            return Err(AuthError::UnsupportedVersion(document.version));
        }
        if document.tokens.is_empty() {
            return Err(AuthError::EmptyPolicy);
        }

        let mut ids = BTreeSet::new();
        let mut digests = Vec::with_capacity(document.tokens.len());
        for token in &document.tokens {
            validate_token_id(&token.id)?;
            if !ids.insert(token.id.clone()) {
                return Err(AuthError::DuplicateTokenId(token.id.clone()));
            }
            let digest = hex::decode(&token.sha256)
                .ok()
                .and_then(|bytes| bytes.try_into().ok())
                .ok_or_else(|| AuthError::InvalidTokenDigest(token.id.clone()))?;
            if token.sha256.len() != TOKEN_DIGEST_BYTES * 2
                || token.sha256.bytes().any(|byte| !byte.is_ascii_hexdigit())
            {
                return Err(AuthError::InvalidTokenDigest(token.id.clone()));
            }
            for project in &token.projects {
                validate_project_scope(project)?;
            }
            digests.push(digest);
        }
        Ok(Self { document, digests })
    }

    pub fn document(&self) -> &AuthPolicyDocument {
        &self.document
    }

    pub fn authenticate(&self, token: &str) -> Option<Principal> {
        if token.is_empty() {
            return None;
        }
        let candidate = Sha256::digest(token.as_bytes());
        self.document
            .tokens
            .iter()
            .zip(&self.digests)
            .find(|(_, digest)| bool::from(candidate.as_slice().ct_eq(digest.as_slice())))
            .map(|(record, _)| Principal {
                id: record.id.clone(),
                role: record.role,
                projects: record.projects.iter().cloned().collect(),
            })
    }
}

fn validate_token_id(id: &str) -> Result<(), AuthError> {
    if id.is_empty()
        || id.len() > MAX_TOKEN_ID_BYTES
        || id
            .chars()
            .any(|character| character.is_control() || character.is_whitespace())
    {
        return Err(AuthError::InvalidTokenId(id.to_owned()));
    }
    Ok(())
}

fn validate_project_scope(project: &str) -> Result<(), AuthError> {
    if project != "*"
        && (project.is_empty()
            || project.len() > MAX_PROJECT_NAME_BYTES
            || project
                .chars()
                .any(|character| character.is_control() || character.is_whitespace()))
    {
        return Err(AuthError::InvalidProjectScope(project.to_owned()));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    const TOKEN: &str = "rivet-local-test-token";
    const DIGEST: &str = "b7b8d56d1e2b7a588c7794f7d2ebc4f7b9d5948659c13c8c1744aa2d6ef5a0c0";

    fn policy() -> AuthPolicy {
        AuthPolicy::from_document(AuthPolicyDocument {
            version: POLICY_VERSION,
            tokens: vec![ApiTokenRecord {
                id: "operator".into(),
                sha256: hex::encode(Sha256::digest(TOKEN.as_bytes())),
                role: Role::Operator,
                projects: vec!["demo".into()],
            }],
        })
        .expect("policy")
    }

    #[test]
    fn authenticates_digest_without_retaining_raw_token() {
        let policy = policy();
        let principal = policy.authenticate(TOKEN).expect("principal");
        assert_eq!(principal.id(), "operator");
        assert_eq!(principal.role(), Role::Operator);
        assert!(principal.can_project(Permission::Read, "demo"));
        assert!(principal.can_project(Permission::Build, "demo"));
        assert!(!principal.can_project(Permission::Build, "other"));
        assert!(!format!("{principal:?}").contains(TOKEN));
        assert!(policy.authenticate("wrong-token").is_none());
    }

    #[test]
    fn roles_define_global_and_project_permissions() {
        let admin = AuthPolicy::from_document(AuthPolicyDocument {
            version: POLICY_VERSION,
            tokens: vec![ApiTokenRecord {
                id: "admin".into(),
                sha256: hex::encode(Sha256::digest(b"admin-token")),
                role: Role::Admin,
                projects: vec![],
            }],
        })
        .expect("admin policy")
        .authenticate("admin-token")
        .expect("admin");
        assert!(admin.can_project(Permission::Administer, "any-project"));
        assert!(admin.can_global(Permission::ConnectAgent));

        let agent = AuthPolicy::from_document(AuthPolicyDocument {
            version: POLICY_VERSION,
            tokens: vec![ApiTokenRecord {
                id: "agent".into(),
                sha256: hex::encode(Sha256::digest(b"agent-token")),
                role: Role::Agent,
                projects: vec![],
            }],
        })
        .expect("agent policy")
        .authenticate("agent-token")
        .expect("agent");
        assert!(agent.can_global(Permission::ConnectAgent));
        assert!(!agent.can_global(Permission::Read));
    }

    #[test]
    fn malformed_policy_is_rejected() {
        assert!(matches!(
            AuthPolicy::from_json(br#"{"version":2,"tokens":[]}"#),
            Err(AuthError::UnsupportedVersion(2))
        ));
        assert!(matches!(
            AuthPolicy::from_document(AuthPolicyDocument {
                version: POLICY_VERSION,
                tokens: vec![
                    ApiTokenRecord {
                        id: "duplicate".into(),
                        sha256: DIGEST.into(),
                        role: Role::Viewer,
                        projects: vec![],
                    },
                    ApiTokenRecord {
                        id: "duplicate".into(),
                        sha256: DIGEST.into(),
                        role: Role::Viewer,
                        projects: vec![],
                    }
                ],
            }),
            Err(AuthError::DuplicateTokenId(_))
        ));
        assert!(matches!(
            AuthPolicy::from_document(AuthPolicyDocument {
                version: POLICY_VERSION,
                tokens: vec![ApiTokenRecord {
                    id: "token".into(),
                    sha256: "not-a-digest".into(),
                    role: Role::Viewer,
                    projects: vec![],
                }],
            }),
            Err(AuthError::InvalidTokenDigest(_))
        ));
    }
}
