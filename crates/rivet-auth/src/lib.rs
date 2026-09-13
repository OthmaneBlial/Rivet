//! Small, provider-neutral authentication and authorization primitives.
//!
//! Policy files contain only SHA-256 token digests. The raw API token is
//! supplied at runtime, hashed for comparison, and never represented by a
//! persisted or debug-printable value in this crate.

use argon2::Argon2;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::BTreeSet;
use std::fmt;
use subtle::ConstantTimeEq;
use thiserror::Error;
use zeroize::Zeroizing;

pub const AUTH_POLICY_VERSION: u8 = 1;
const TOKEN_DIGEST_BYTES: usize = 32;
const GENERATED_TOKEN_BYTES: usize = 32;
const MAX_TOKEN_ID_BYTES: usize = 64;
const MAX_PROJECT_NAME_BYTES: usize = 128;
const MAX_USERNAME_BYTES: usize = 128;
const PASSWORD_SALT_BYTES: usize = 16;
const PASSWORD_DERIVED_BYTES: usize = 32;
const MIN_PASSWORD_BYTES: usize = 12;
const MAX_PASSWORD_BYTES: usize = 4096;
const PASSWORD_HASH_PREFIX: &str = "rivet-argon2id-v1";

pub const AUTH_USERS_VERSION: u8 = 1;

#[derive(Debug, Error, PartialEq, Eq)]
pub enum AuthError {
    #[error("authentication policy version {0} is unsupported; expected {AUTH_POLICY_VERSION}")]
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
    #[error("authentication token generation failed: {0}")]
    Randomness(String),
    #[error("authentication policy JSON is invalid: {0}")]
    InvalidJson(String),
    #[error("authentication user policy must contain at least one user")]
    EmptyUserPolicy,
    #[error("authentication username is empty, too long, or contains control characters")]
    InvalidUsername,
    #[error("authentication user IDs must be unique: {0}")]
    DuplicateUserId(String),
    #[error("authentication usernames must be unique: {0}")]
    DuplicateUsername(String),
    #[error(
        "authentication password must contain at least {MIN_PASSWORD_BYTES} bytes and at most {MAX_PASSWORD_BYTES} bytes"
    )]
    WeakPassword,
    #[error("authentication password hash is invalid")]
    InvalidPasswordHash,
    #[error("authentication password hashing failed: {0}")]
    PasswordHashing(String),
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

impl AuthPolicyDocument {
    pub fn empty() -> Self {
        Self {
            version: AUTH_POLICY_VERSION,
            tokens: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct ApiTokenRecord {
    pub id: String,
    /// Lowercase hexadecimal SHA-256 digest of the runtime Bearer token.
    pub sha256: String,
    pub role: Role,
    #[serde(default)]
    pub projects: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expires_at: Option<DateTime<Utc>>,
}

/// Private, file-backed local accounts. The password field contains only an
/// Argon2id-derived verifier; this record is never returned by an API route.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct AuthUserRecord {
    pub id: String,
    pub username: String,
    pub password_hash: String,
    pub role: Role,
    #[serde(default)]
    pub projects: Vec<String>,
    #[serde(default)]
    pub disabled: bool,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct AuthUsersDocument {
    pub version: u8,
    pub users: Vec<AuthUserRecord>,
}

impl AuthUsersDocument {
    pub fn empty() -> Self {
        Self {
            version: AUTH_USERS_VERSION,
            users: Vec::new(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct AuthUsers {
    document: AuthUsersDocument,
}

impl AuthUsers {
    pub fn from_json(bytes: &[u8]) -> Result<Self, AuthError> {
        let document: AuthUsersDocument = serde_json::from_slice(bytes)
            .map_err(|error| AuthError::InvalidJson(error.to_string()))?;
        Self::from_document(document)
    }

    pub fn from_document(document: AuthUsersDocument) -> Result<Self, AuthError> {
        if document.version != AUTH_USERS_VERSION {
            return Err(AuthError::UnsupportedVersion(document.version));
        }
        if document.users.is_empty() {
            return Err(AuthError::EmptyUserPolicy);
        }

        let mut ids = BTreeSet::new();
        let mut usernames = BTreeSet::new();
        for user in &document.users {
            validate_token_id(&user.id)?;
            if !ids.insert(user.id.clone()) {
                return Err(AuthError::DuplicateUserId(user.id.clone()));
            }
            validate_username(&user.username)?;
            let username_key = user.username.to_ascii_lowercase();
            if !usernames.insert(username_key) {
                return Err(AuthError::DuplicateUsername(user.username.clone()));
            }
            validate_password_hash(&user.password_hash)?;
            for project in &user.projects {
                validate_project_scope(project)?;
            }
        }
        Ok(Self { document })
    }

    pub fn document(&self) -> &AuthUsersDocument {
        &self.document
    }

    pub fn authenticate(&self, username: &str, password: &str) -> Option<Principal> {
        let user = self
            .document
            .users
            .iter()
            .find(|user| user.username.eq_ignore_ascii_case(username))?;
        if user.disabled || !verify_password(&user.password_hash, password) {
            return None;
        }
        Some(Principal::from_parts(
            user.id.clone(),
            user.role,
            user.projects.clone(),
        ))
    }
}

/// Hash a local account password for private policy-file storage.
pub fn hash_password(password: &str) -> Result<String, AuthError> {
    validate_password(password)?;
    let mut salt = [0_u8; PASSWORD_SALT_BYTES];
    getrandom::fill(&mut salt).map_err(|error| AuthError::PasswordHashing(error.to_string()))?;
    let mut derived = [0_u8; PASSWORD_DERIVED_BYTES];
    Argon2::default()
        .hash_password_into(password.as_bytes(), &salt, &mut derived)
        .map_err(|error| AuthError::PasswordHashing(error.to_string()))?;
    Ok(format!(
        "{PASSWORD_HASH_PREFIX}${}${}",
        hex::encode(salt),
        hex::encode(derived)
    ))
}

fn verify_password(hash: &str, password: &str) -> bool {
    let Ok((salt, expected)) = parse_password_hash(hash) else {
        return false;
    };
    if validate_password(password).is_err() {
        return false;
    }
    let mut derived = [0_u8; PASSWORD_DERIVED_BYTES];
    if Argon2::default()
        .hash_password_into(password.as_bytes(), &salt, &mut derived)
        .is_err()
    {
        return false;
    }
    bool::from(derived.as_slice().ct_eq(expected.as_slice()))
}

fn validate_password_hash(hash: &str) -> Result<(), AuthError> {
    parse_password_hash(hash).map(|_| ())
}

fn parse_password_hash(
    hash: &str,
) -> Result<([u8; PASSWORD_SALT_BYTES], [u8; PASSWORD_DERIVED_BYTES]), AuthError> {
    let fields = hash.split('$').collect::<Vec<_>>();
    if fields.len() != 3 || fields[0] != PASSWORD_HASH_PREFIX {
        return Err(AuthError::InvalidPasswordHash);
    }
    let salt = hex::decode(fields[1]).map_err(|_| AuthError::InvalidPasswordHash)?;
    let derived = hex::decode(fields[2]).map_err(|_| AuthError::InvalidPasswordHash)?;
    let salt = salt
        .try_into()
        .map_err(|_| AuthError::InvalidPasswordHash)?;
    let derived = derived
        .try_into()
        .map_err(|_| AuthError::InvalidPasswordHash)?;
    Ok((salt, derived))
}

fn validate_password(password: &str) -> Result<(), AuthError> {
    if password.len() < MIN_PASSWORD_BYTES
        || password.len() > MAX_PASSWORD_BYTES
        || password.chars().all(char::is_whitespace)
    {
        return Err(AuthError::WeakPassword);
    }
    Ok(())
}

fn validate_username(username: &str) -> Result<(), AuthError> {
    if username.trim().is_empty()
        || username.len() > MAX_USERNAME_BYTES
        || username.chars().any(char::is_control)
    {
        return Err(AuthError::InvalidUsername);
    }
    Ok(())
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

    /// Rebuild a principal from a server-side session snapshot.
    ///
    /// The constructor is intentionally small: session persistence belongs to
    /// the server/storage boundary, while this crate owns the authorization
    /// semantics used by every transport.
    pub fn from_parts(id: impl Into<String>, role: Role, projects: Vec<String>) -> Self {
        Self {
            id: id.into(),
            role,
            projects: projects.into_iter().collect(),
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
        if document.version != AUTH_POLICY_VERSION {
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
        self.authenticate_at(token, Utc::now())
    }

    pub fn authenticate_at(&self, token: &str, now: DateTime<Utc>) -> Option<Principal> {
        if token.is_empty() {
            return None;
        }
        let candidate = Sha256::digest(token.as_bytes());
        self.document
            .tokens
            .iter()
            .zip(&self.digests)
            .find(|(record, digest)| {
                record.expires_at.is_none_or(|expires_at| expires_at > now)
                    && bool::from(candidate.as_slice().ct_eq(digest.as_slice()))
            })
            .map(|(record, _)| Principal {
                id: record.id.clone(),
                role: record.role,
                projects: record.projects.iter().cloned().collect(),
            })
    }
}

/// Return the lowercase hexadecimal digest persisted in an authentication
/// policy. The raw token remains the caller's responsibility and is never
/// stored by this crate.
pub fn token_digest(token: &str) -> String {
    hex::encode(Sha256::digest(token.as_bytes()))
}

/// Generate a high-entropy token for local policy administration.
///
/// The returned string is zeroized when dropped so callers can write it to a
/// protected file without leaving an ordinary owned copy behind.
pub fn generate_token() -> Result<Zeroizing<String>, AuthError> {
    let mut bytes = Zeroizing::new([0_u8; GENERATED_TOKEN_BYTES]);
    getrandom::fill(&mut *bytes).map_err(|error| AuthError::Randomness(error.to_string()))?;
    Ok(Zeroizing::new(hex::encode(&*bytes)))
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
            version: AUTH_POLICY_VERSION,
            tokens: vec![ApiTokenRecord {
                id: "operator".into(),
                sha256: hex::encode(Sha256::digest(TOKEN.as_bytes())),
                role: Role::Operator,
                projects: vec!["demo".into()],
                expires_at: None,
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
            version: AUTH_POLICY_VERSION,
            tokens: vec![ApiTokenRecord {
                id: "admin".into(),
                sha256: hex::encode(Sha256::digest(b"admin-token")),
                role: Role::Admin,
                projects: vec![],
                expires_at: None,
            }],
        })
        .expect("admin policy")
        .authenticate("admin-token")
        .expect("admin");
        assert!(admin.can_project(Permission::Administer, "any-project"));
        assert!(admin.can_global(Permission::ConnectAgent));

        let agent = AuthPolicy::from_document(AuthPolicyDocument {
            version: AUTH_POLICY_VERSION,
            tokens: vec![ApiTokenRecord {
                id: "agent".into(),
                sha256: hex::encode(Sha256::digest(b"agent-token")),
                role: Role::Agent,
                projects: vec![],
                expires_at: None,
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
                version: AUTH_POLICY_VERSION,
                tokens: vec![
                    ApiTokenRecord {
                        id: "duplicate".into(),
                        sha256: DIGEST.into(),
                        role: Role::Viewer,
                        projects: vec![],
                        expires_at: None,
                    },
                    ApiTokenRecord {
                        id: "duplicate".into(),
                        sha256: DIGEST.into(),
                        role: Role::Viewer,
                        projects: vec![],
                        expires_at: None,
                    }
                ],
            }),
            Err(AuthError::DuplicateTokenId(_))
        ));
        assert!(matches!(
            AuthPolicy::from_document(AuthPolicyDocument {
                version: AUTH_POLICY_VERSION,
                tokens: vec![ApiTokenRecord {
                    id: "token".into(),
                    sha256: "not-a-digest".into(),
                    role: Role::Viewer,
                    projects: vec![],
                    expires_at: None,
                }],
            }),
            Err(AuthError::InvalidTokenDigest(_))
        ));
    }

    #[test]
    fn generated_tokens_are_digestable_and_not_empty() {
        let first = generate_token().expect("token");
        let second = generate_token().expect("token");
        assert_eq!(first.len(), GENERATED_TOKEN_BYTES * 2);
        assert_eq!(second.len(), GENERATED_TOKEN_BYTES * 2);
        assert_ne!(&*first, &*second);
        assert_eq!(
            token_digest(&first),
            hex::encode(Sha256::digest(first.as_bytes()))
        );
    }

    #[test]
    fn expired_tokens_are_rejected_without_breaking_legacy_policy_json() {
        let now = Utc::now();
        let policy = AuthPolicy::from_document(AuthPolicyDocument {
            version: AUTH_POLICY_VERSION,
            tokens: vec![
                ApiTokenRecord {
                    id: "expired".into(),
                    sha256: token_digest("expired-token"),
                    role: Role::Viewer,
                    projects: vec![],
                    expires_at: Some(now - chrono::Duration::seconds(1)),
                },
                ApiTokenRecord {
                    id: "future".into(),
                    sha256: token_digest("future-token"),
                    role: Role::Viewer,
                    projects: vec![],
                    expires_at: Some(now + chrono::Duration::seconds(60)),
                },
            ],
        })
        .expect("expiring policy");
        assert!(policy.authenticate_at("expired-token", now).is_none());
        assert_eq!(
            policy
                .authenticate_at("future-token", now)
                .expect("future token")
                .id(),
            "future"
        );

        let legacy: ApiTokenRecord = serde_json::from_str(&format!(
            "{{\"id\":\"legacy\",\"sha256\":\"{}\",\"role\":\"viewer\",\"projects\":[]}}",
            token_digest("legacy-token")
        ))
        .expect("legacy token record");
        assert!(legacy.expires_at.is_none());
    }

    #[test]
    fn local_user_passwords_are_salted_and_verified_without_plaintext() {
        let first = hash_password("a-correct-local-password").expect("hash");
        let second = hash_password("a-correct-local-password").expect("hash");
        assert_ne!(first, second);
        assert!(!first.contains("a-correct-local-password"));

        let users = AuthUsers::from_document(AuthUsersDocument {
            version: AUTH_USERS_VERSION,
            users: vec![AuthUserRecord {
                id: "user-1".into(),
                username: "operator@example.test".into(),
                password_hash: first,
                role: Role::Operator,
                projects: vec!["demo".into()],
                disabled: false,
            }],
        })
        .expect("users");
        let principal = users
            .authenticate("OPERATOR@example.test", "a-correct-local-password")
            .expect("principal");
        assert_eq!(principal.id(), "user-1");
        assert!(principal.can_project(Permission::Build, "demo"));
        assert!(
            users
                .authenticate("operator@example.test", "wrong-password")
                .is_none()
        );
    }

    #[test]
    fn local_user_policy_rejects_duplicates_disabled_users_and_weak_passwords() {
        assert!(matches!(
            hash_password("short"),
            Err(AuthError::WeakPassword)
        ));
        let hash = hash_password("a-correct-local-password").expect("hash");
        let duplicate = AuthUsers::from_document(AuthUsersDocument {
            version: AUTH_USERS_VERSION,
            users: vec![
                AuthUserRecord {
                    id: "user-1".into(),
                    username: "one@example.test".into(),
                    password_hash: hash.clone(),
                    role: Role::Viewer,
                    projects: vec![],
                    disabled: false,
                },
                AuthUserRecord {
                    id: "user-2".into(),
                    username: "ONE@example.test".into(),
                    password_hash: hash,
                    role: Role::Viewer,
                    projects: vec![],
                    disabled: false,
                },
            ],
        });
        assert!(matches!(duplicate, Err(AuthError::DuplicateUsername(_))));

        let disabled_hash = hash_password("a-correct-local-password").expect("hash");
        let disabled = AuthUsers::from_document(AuthUsersDocument {
            version: AUTH_USERS_VERSION,
            users: vec![AuthUserRecord {
                id: "disabled".into(),
                username: "disabled@example.test".into(),
                password_hash: disabled_hash,
                role: Role::Viewer,
                projects: vec![],
                disabled: true,
            }],
        })
        .expect("disabled users");
        assert!(
            disabled
                .authenticate("disabled@example.test", "a-correct-local-password")
                .is_none()
        );
    }
}
