//! Passphrase-encrypted local credentials for provider integrations.
//!
//! The vault stores only authenticated ciphertext on disk. A caller must
//! provide the passphrase at runtime; the decrypted credentials are kept in
//! memory and are intentionally omitted from `Debug` output and metadata.

use aes_gcm::aead::{Aead, KeyInit, Payload};
use aes_gcm::{Aes256Gcm, Nonce};
use argon2::Argon2;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use thiserror::Error;
use uuid::Uuid;
use zeroize::{Zeroize, Zeroizing};

const VAULT_VERSION: u8 = 1;
const KDF_NAME: &str = "argon2id-v1-default";
const AAD: &[u8] = b"rivet-credentials-v1";
const SALT_BYTES: usize = 16;
const NONCE_BYTES: usize = 12;
const KEY_BYTES: usize = 32;
const MIN_PASSPHRASE_BYTES: usize = 12;
const MAX_ID_BYTES: usize = 64;
const MAX_USERNAME_BYTES: usize = 256;
const MAX_PROJECT_BYTES: usize = 256;
const MAX_PROJECTS: usize = 64;
const MAX_SECRET_BYTES: usize = 64 * 1024;
const MAX_KEYCHAIN_LABEL_BYTES: usize = 256;
pub const DEFAULT_KEYCHAIN_SERVICE: &str = "Rivet";

#[derive(Debug, Error)]
pub enum CredentialError {
    #[error("credential vault path is a symbolic link: {0}")]
    SymlinkPath(PathBuf),
    #[error("credential vault file was not found: {0}")]
    VaultNotFound(PathBuf),
    #[error("credential vault is not a regular file: {0}")]
    VaultNotAFile(PathBuf),
    #[error("credential vault passphrase must contain at least {MIN_PASSPHRASE_BYTES} bytes")]
    WeakPassphrase,
    #[error("credential ID is invalid: {0}")]
    InvalidId(String),
    #[error("credential username is empty or too long")]
    InvalidUsername,
    #[error("credential project scope is invalid: {0}")]
    InvalidProject(String),
    #[error("credential project scope contains too many projects")]
    TooManyProjects,
    #[error("keychain {0} is empty or too long")]
    InvalidKeychainLabel(&'static str),
    #[error("credential secret cannot be empty")]
    EmptySecret,
    #[error("credential secret is too large or contains NUL bytes")]
    InvalidSecret,
    #[error("credential was not found: {0}")]
    CredentialNotFound(String),
    #[error("credential vault format is invalid: {0}")]
    InvalidFormat(String),
    #[error("credential vault encryption or passphrase verification failed")]
    Cryptography,
    #[error("randomness source failed: {0}")]
    Randomness(String),
    #[error("credential vault serialization failed: {0}")]
    Serialization(#[from] serde_json::Error),
    #[error("credential vault filesystem operation failed: {0}")]
    Filesystem(#[from] std::io::Error),
    #[error("OS keychain operation failed: {0}")]
    Keychain(String),
}

/// Small OS-backed secret store used for vault passphrases. The passphrase is
/// never placed in command-line arguments or Rivet configuration files.
pub struct CredentialKeychain {
    service: String,
}

impl CredentialKeychain {
    pub fn new(service: impl Into<String>) -> Result<Self, CredentialError> {
        let service = service.into();
        validate_keychain_label(&service, "service")?;
        Ok(Self { service })
    }

    pub fn rivet() -> Self {
        Self {
            service: DEFAULT_KEYCHAIN_SERVICE.to_owned(),
        }
    }

    pub fn set_passphrase(&self, account: &str, passphrase: &str) -> Result<(), CredentialError> {
        validate_keychain_label(account, "account")?;
        validate_passphrase(passphrase.as_bytes())?;
        let entry = keyring::Entry::new(&self.service, account)
            .map_err(|error| CredentialError::Keychain(error.to_string()))?;
        entry
            .set_password(passphrase)
            .map_err(|error| CredentialError::Keychain(error.to_string()))
    }

    pub fn get_passphrase(&self, account: &str) -> Result<Zeroizing<String>, CredentialError> {
        validate_keychain_label(account, "account")?;
        let entry = keyring::Entry::new(&self.service, account)
            .map_err(|error| CredentialError::Keychain(error.to_string()))?;
        let passphrase = entry
            .get_password()
            .map_err(|error| CredentialError::Keychain(error.to_string()))?;
        validate_passphrase(passphrase.as_bytes())?;
        Ok(Zeroizing::new(passphrase))
    }

    pub fn delete_passphrase(&self, account: &str) -> Result<(), CredentialError> {
        validate_keychain_label(account, "account")?;
        let entry = keyring::Entry::new(&self.service, account)
            .map_err(|error| CredentialError::Keychain(error.to_string()))?;
        entry
            .delete_credential()
            .map_err(|error| CredentialError::Keychain(error.to_string()))
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CredentialKind {
    #[default]
    HttpBasic,
    SshKey,
}

#[derive(Clone, PartialEq, Eq)]
pub struct Credential {
    id: String,
    kind: CredentialKind,
    username: String,
    secret: String,
    projects: BTreeSet<String>,
}

impl Credential {
    pub fn id(&self) -> &str {
        &self.id
    }

    pub fn kind(&self) -> CredentialKind {
        self.kind
    }

    pub fn username(&self) -> &str {
        &self.username
    }

    pub fn secret(&self) -> &str {
        &self.secret
    }

    /// Return the project allow-list. An empty set means the credential is
    /// available to every project, preserving the original vault behavior.
    pub fn projects(&self) -> impl Iterator<Item = &str> {
        self.projects.iter().map(String::as_str)
    }

    pub fn is_allowed_for_project(&self, project: &str) -> bool {
        self.projects.is_empty() || self.projects.contains(project)
    }
}

impl fmt::Debug for Credential {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Credential")
            .field("id", &self.id)
            .field("username", &self.username)
            .finish_non_exhaustive()
    }
}

impl Drop for Credential {
    fn drop(&mut self) {
        self.secret.zeroize();
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct CredentialSummary {
    pub id: String,
    pub kind: CredentialKind,
    pub username: String,
    /// Empty means global access; otherwise this is the explicit project
    /// allow-list. Secrets are intentionally never part of this summary.
    pub projects: Vec<String>,
}

pub struct CredentialVault {
    path: PathBuf,
    passphrase: Zeroizing<Vec<u8>>,
    credentials: BTreeMap<String, Credential>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct EncryptedVault {
    version: u8,
    kdf: String,
    salt: String,
    nonce: String,
    ciphertext: String,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct VaultPayload {
    version: u8,
    credentials: BTreeMap<String, VaultCredential>,
}

#[derive(Deserialize, Serialize)]
struct VaultCredential {
    #[serde(default)]
    kind: CredentialKind,
    username: String,
    secret: String,
    #[serde(default)]
    projects: BTreeSet<String>,
}

impl Drop for VaultCredential {
    fn drop(&mut self) {
        self.secret.zeroize();
    }
}

impl fmt::Debug for CredentialVault {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CredentialVault")
            .field("path", &self.path)
            .field(
                "credential_ids",
                &self.credentials.keys().collect::<Vec<_>>(),
            )
            .finish_non_exhaustive()
    }
}

impl CredentialVault {
    /// Open an existing encrypted vault.
    pub fn open(
        path: impl AsRef<Path>,
        passphrase: impl AsRef<[u8]>,
    ) -> Result<Self, CredentialError> {
        let path = path.as_ref().to_path_buf();
        validate_passphrase(passphrase.as_ref())?;
        let metadata = fs::symlink_metadata(&path).map_err(|error| {
            if error.kind() == std::io::ErrorKind::NotFound {
                CredentialError::VaultNotFound(path.clone())
            } else {
                CredentialError::Filesystem(error)
            }
        })?;
        if metadata.file_type().is_symlink() {
            return Err(CredentialError::SymlinkPath(path));
        }
        if !metadata.is_file() {
            return Err(CredentialError::VaultNotAFile(path));
        }
        let bytes = fs::read(&path)?;
        let encrypted: EncryptedVault = serde_json::from_slice(&bytes)?;
        let plaintext = Zeroizing::new(decrypt_vault(&encrypted, passphrase.as_ref())?);
        let payload: VaultPayload = serde_json::from_slice(&plaintext)?;
        if payload.version != VAULT_VERSION {
            return Err(CredentialError::InvalidFormat(format!(
                "unsupported payload version {}; expected {VAULT_VERSION}",
                payload.version
            )));
        }
        let credentials = payload
            .credentials
            .into_iter()
            .map(|(id, credential)| {
                validate_id(&id)?;
                validate_username(&credential.username)?;
                validate_secret(&credential.kind, &credential.secret)?;
                validate_projects(&credential.projects)?;
                Ok((
                    id.clone(),
                    Credential {
                        id,
                        kind: credential.kind,
                        username: credential.username.clone(),
                        secret: credential.secret.clone(),
                        projects: credential.projects.clone(),
                    },
                ))
            })
            .collect::<Result<BTreeMap<_, _>, CredentialError>>()?;
        Ok(Self {
            path,
            passphrase: Zeroizing::new(passphrase.as_ref().to_vec()),
            credentials,
        })
    }

    /// Open a vault, creating an encrypted empty vault when it does not exist.
    pub fn open_or_create(
        path: impl AsRef<Path>,
        passphrase: impl AsRef<[u8]>,
    ) -> Result<Self, CredentialError> {
        let path = path.as_ref().to_path_buf();
        validate_passphrase(passphrase.as_ref())?;
        match fs::symlink_metadata(&path) {
            Ok(metadata) if metadata.file_type().is_symlink() => {
                Err(CredentialError::SymlinkPath(path))
            }
            Ok(_) => Self::open(path, passphrase),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                let vault = Self {
                    path,
                    passphrase: Zeroizing::new(passphrase.as_ref().to_vec()),
                    credentials: BTreeMap::new(),
                };
                vault.save()?;
                Ok(vault)
            }
            Err(error) => Err(CredentialError::Filesystem(error)),
        }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Return metadata without exposing the credential secret.
    pub fn list(&self) -> Vec<CredentialSummary> {
        self.credentials
            .values()
            .map(|credential| CredentialSummary {
                id: credential.id.clone(),
                kind: credential.kind,
                username: credential.username.clone(),
                projects: credential.projects.iter().cloned().collect(),
            })
            .collect()
    }

    pub fn get(&self, id: &str) -> Result<Credential, CredentialError> {
        validate_id(id)?;
        self.credentials
            .get(id)
            .cloned()
            .ok_or_else(|| CredentialError::CredentialNotFound(id.to_owned()))
    }

    /// Resolve a credential for one project. Global credentials remain
    /// compatible, while scoped credentials are denied outside their list.
    pub fn get_for_project(&self, id: &str, project: &str) -> Result<Credential, CredentialError> {
        validate_project(project)?;
        let credential = self.get(id)?;
        if !credential.is_allowed_for_project(project) {
            return Err(CredentialError::CredentialNotFound(id.to_owned()));
        }
        Ok(credential)
    }

    /// Store or replace an HTTP basic credential and immediately persist it.
    pub fn set_http_basic(
        &mut self,
        id: impl Into<String>,
        username: impl Into<String>,
        secret: impl Into<String>,
    ) -> Result<(), CredentialError> {
        self.set_http_basic_for_projects(id, username, secret, std::iter::empty::<String>())
    }

    /// Store or replace an HTTP basic credential with an optional project
    /// allow-list. An empty iterator gives the credential global scope.
    pub fn set_http_basic_for_projects<I, S>(
        &mut self,
        id: impl Into<String>,
        username: impl Into<String>,
        secret: impl Into<String>,
        projects: I,
    ) -> Result<(), CredentialError>
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        let id = id.into();
        let username = username.into();
        let secret = secret.into();
        let projects = projects
            .into_iter()
            .map(|project| project.into().trim().to_owned())
            .collect::<BTreeSet<_>>();
        validate_id(&id)?;
        validate_username(&username)?;
        self.set_for_projects(id, CredentialKind::HttpBasic, username, secret, projects)
    }

    /// Store or replace an SSH private-key credential with an optional project
    /// allow-list. The key remains encrypted in the vault until Git needs it.
    pub fn set_ssh_key_for_projects<I, S>(
        &mut self,
        id: impl Into<String>,
        username: impl Into<String>,
        private_key: impl Into<String>,
        projects: I,
    ) -> Result<(), CredentialError>
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.set_for_projects(id, CredentialKind::SshKey, username, private_key, projects)
    }

    fn set_for_projects<I, S>(
        &mut self,
        id: impl Into<String>,
        kind: CredentialKind,
        username: impl Into<String>,
        secret: impl Into<String>,
        projects: I,
    ) -> Result<(), CredentialError>
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        let id = id.into();
        let username = username.into();
        let secret = secret.into();
        let projects = projects
            .into_iter()
            .map(|project| project.into().trim().to_owned())
            .collect::<BTreeSet<_>>();
        validate_id(&id)?;
        validate_username(&username)?;
        validate_secret(&kind, &secret)?;
        validate_projects(&projects)?;
        let previous = self.credentials.insert(
            id.clone(),
            Credential {
                id: id.clone(),
                kind,
                username,
                secret,
                projects,
            },
        );
        if let Err(error) = self.save() {
            if let Some(previous) = previous {
                self.credentials.insert(id, previous);
            } else {
                self.credentials.remove(&id);
            }
            return Err(error);
        }
        Ok(())
    }

    pub fn remove(&mut self, id: &str) -> Result<(), CredentialError> {
        validate_id(id)?;
        let Some(previous) = self.credentials.remove(id) else {
            return Err(CredentialError::CredentialNotFound(id.to_owned()));
        };
        if let Err(error) = self.save() {
            self.credentials.insert(id.to_owned(), previous);
            return Err(error);
        }
        Ok(())
    }

    fn save(&self) -> Result<(), CredentialError> {
        let payload = VaultPayload {
            version: VAULT_VERSION,
            credentials: self
                .credentials
                .iter()
                .map(|(id, credential)| {
                    (
                        id.clone(),
                        VaultCredential {
                            kind: credential.kind,
                            username: credential.username.clone(),
                            secret: credential.secret.clone(),
                            projects: credential.projects.clone(),
                        },
                    )
                })
                .collect(),
        };
        let plaintext = Zeroizing::new(serde_json::to_vec(&payload)?);
        let encrypted = encrypt_vault(&plaintext, &self.passphrase)?;
        let bytes = serde_json::to_vec_pretty(&encrypted)?;
        write_atomic(&self.path, &bytes)
    }
}

fn validate_passphrase(passphrase: &[u8]) -> Result<(), CredentialError> {
    if passphrase.len() < MIN_PASSPHRASE_BYTES {
        Err(CredentialError::WeakPassphrase)
    } else {
        Ok(())
    }
}

fn validate_id(id: &str) -> Result<(), CredentialError> {
    if id.is_empty()
        || id.len() > MAX_ID_BYTES
        || !id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
    {
        return Err(CredentialError::InvalidId(id.to_owned()));
    }
    Ok(())
}

fn validate_username(username: &str) -> Result<(), CredentialError> {
    if username.is_empty() || username.len() > MAX_USERNAME_BYTES || username.contains('\0') {
        Err(CredentialError::InvalidUsername)
    } else {
        Ok(())
    }
}

fn validate_secret(kind: &CredentialKind, secret: &str) -> Result<(), CredentialError> {
    if secret.is_empty() {
        return Err(CredentialError::EmptySecret);
    }
    if secret.len() > MAX_SECRET_BYTES || secret.contains('\0') {
        return Err(CredentialError::InvalidSecret);
    }
    if *kind == CredentialKind::SshKey && !secret.contains("PRIVATE KEY") {
        return Err(CredentialError::InvalidFormat(
            "SSH credential does not contain a private-key marker".into(),
        ));
    }
    Ok(())
}

fn validate_projects(projects: &BTreeSet<String>) -> Result<(), CredentialError> {
    if projects.len() > MAX_PROJECTS {
        return Err(CredentialError::TooManyProjects);
    }
    for project in projects {
        validate_project(project)?;
    }
    Ok(())
}

fn validate_project(project: &str) -> Result<(), CredentialError> {
    if project.trim().is_empty()
        || project.len() > MAX_PROJECT_BYTES
        || project.contains('/')
        || project.contains('\\')
        || project.contains('\0')
    {
        return Err(CredentialError::InvalidProject(project.to_owned()));
    }
    Ok(())
}

fn validate_keychain_label(value: &str, label: &'static str) -> Result<(), CredentialError> {
    if value.trim().is_empty() || value.len() > MAX_KEYCHAIN_LABEL_BYTES || value.contains('\0') {
        return Err(CredentialError::InvalidKeychainLabel(label));
    }
    Ok(())
}

fn derive_key(passphrase: &[u8], salt: &[u8]) -> Result<[u8; KEY_BYTES], CredentialError> {
    validate_passphrase(passphrase)?;
    let mut key = [0u8; KEY_BYTES];
    Argon2::default()
        .hash_password_into(passphrase, salt, &mut key)
        .map_err(|_| CredentialError::Cryptography)?;
    Ok(key)
}

fn encrypt_vault(plaintext: &[u8], passphrase: &[u8]) -> Result<EncryptedVault, CredentialError> {
    let mut salt = [0u8; SALT_BYTES];
    let mut nonce = [0u8; NONCE_BYTES];
    getrandom::fill(&mut salt).map_err(|error| CredentialError::Randomness(error.to_string()))?;
    getrandom::fill(&mut nonce).map_err(|error| CredentialError::Randomness(error.to_string()))?;
    let key = Zeroizing::new(derive_key(passphrase, &salt)?);
    let cipher = Aes256Gcm::new_from_slice(&key[..]).map_err(|_| CredentialError::Cryptography)?;
    let nonce = Nonce::try_from(&nonce[..]).map_err(|_| CredentialError::Cryptography)?;
    let ciphertext = cipher
        .encrypt(
            &nonce,
            Payload {
                msg: plaintext,
                aad: AAD,
            },
        )
        .map_err(|_| CredentialError::Cryptography)?;
    Ok(EncryptedVault {
        version: VAULT_VERSION,
        kdf: KDF_NAME.to_owned(),
        salt: hex::encode(salt),
        nonce: hex::encode(nonce),
        ciphertext: hex::encode(ciphertext),
    })
}

fn decrypt_vault(
    encrypted: &EncryptedVault,
    passphrase: &[u8],
) -> Result<Vec<u8>, CredentialError> {
    if encrypted.version != VAULT_VERSION {
        return Err(CredentialError::InvalidFormat(format!(
            "unsupported vault version {}; expected {VAULT_VERSION}",
            encrypted.version
        )));
    }
    if encrypted.kdf != KDF_NAME {
        return Err(CredentialError::InvalidFormat(format!(
            "unsupported key derivation function {:?}",
            encrypted.kdf
        )));
    }
    let salt = decode_fixed_hex::<SALT_BYTES>(&encrypted.salt, "salt")?;
    let nonce = decode_fixed_hex::<NONCE_BYTES>(&encrypted.nonce, "nonce")?;
    let ciphertext = hex::decode(&encrypted.ciphertext)
        .map_err(|_| CredentialError::InvalidFormat("ciphertext is not valid hex".into()))?;
    if ciphertext.is_empty() {
        return Err(CredentialError::InvalidFormat("ciphertext is empty".into()));
    }
    let key = Zeroizing::new(derive_key(passphrase, &salt)?);
    let cipher = Aes256Gcm::new_from_slice(&key[..]).map_err(|_| CredentialError::Cryptography)?;
    let nonce = Nonce::try_from(&nonce[..]).map_err(|_| CredentialError::Cryptography)?;
    cipher
        .decrypt(
            &nonce,
            Payload {
                msg: ciphertext.as_ref(),
                aad: AAD,
            },
        )
        .map_err(|_| CredentialError::Cryptography)
}

fn decode_fixed_hex<const N: usize>(value: &str, field: &str) -> Result<[u8; N], CredentialError> {
    let bytes = hex::decode(value)
        .map_err(|_| CredentialError::InvalidFormat(format!("{field} is not valid hex")))?;
    bytes.try_into().map_err(|_| {
        CredentialError::InvalidFormat(format!("{field} must contain exactly {N} bytes"))
    })
}

fn write_atomic(path: &Path, bytes: &[u8]) -> Result<(), CredentialError> {
    if let Ok(metadata) = fs::symlink_metadata(path) {
        if metadata.file_type().is_symlink() {
            return Err(CredentialError::SymlinkPath(path.to_path_buf()));
        }
        if !metadata.is_file() {
            return Err(CredentialError::VaultNotAFile(path.to_path_buf()));
        }
    }
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    fs::create_dir_all(parent)?;
    if fs::symlink_metadata(parent)
        .map(|metadata| metadata.file_type().is_symlink())
        .unwrap_or(false)
    {
        return Err(CredentialError::SymlinkPath(parent.to_path_buf()));
    }
    let filename = path
        .file_name()
        .and_then(|value| value.to_str())
        .ok_or_else(|| CredentialError::InvalidFormat("vault path has no valid filename".into()))?;
    let temporary = parent.join(format!(".{filename}.{}.tmp", Uuid::new_v4()));
    let result = (|| {
        let mut options = OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let mut file = options.open(&temporary)?;
        file.write_all(bytes)?;
        file.sync_all()?;
        drop(file);
        #[cfg(windows)]
        if path.exists() {
            fs::remove_file(path)?;
        }
        fs::rename(&temporary, path)?;
        #[cfg(unix)]
        if let Ok(directory) = File::open(parent) {
            let _ = directory.sync_all();
        }
        Ok::<(), std::io::Error>(())
    })();
    if result.is_err() {
        let _ = fs::remove_file(&temporary);
    }
    result.map_err(CredentialError::Filesystem)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    const PASSPHRASE: &str = "local-vault-passphrase";

    #[test]
    fn vault_round_trips_without_plaintext_on_disk() {
        let directory = tempdir().expect("tempdir");
        let path = directory.path().join("credentials.vault");
        let mut vault = CredentialVault::open_or_create(&path, PASSPHRASE).expect("create");
        vault
            .set_http_basic("github", "oauth2", "fixture-token-value")
            .expect("set");
        let raw = fs::read_to_string(&path).expect("read vault");
        assert!(!raw.contains("fixture-token-value"));
        assert!(!raw.contains("oauth2"));
        drop(vault);

        let vault = CredentialVault::open(&path, PASSPHRASE).expect("reopen");
        let credential = vault.get("github").expect("credential");
        assert_eq!(credential.username(), "oauth2");
        assert_eq!(credential.secret(), "fixture-token-value");
        assert_eq!(
            vault.list(),
            [CredentialSummary {
                id: "github".into(),
                kind: CredentialKind::HttpBasic,
                username: "oauth2".into(),
                projects: vec![],
            }]
        );
        assert!(matches!(
            CredentialVault::open(&path, "wrong-passphrase"),
            Err(CredentialError::Cryptography)
        ));
    }

    #[test]
    fn project_scoped_credentials_round_trip_and_deny_other_projects() {
        let directory = tempdir().expect("tempdir");
        let path = directory.path().join("credentials.vault");
        let mut vault = CredentialVault::open_or_create(&path, PASSPHRASE).expect("create");
        vault
            .set_http_basic_for_projects("github", "oauth2", "scoped-secret", ["rivet", "release"])
            .expect("set scoped credential");
        assert_eq!(
            vault.get_for_project("github", "rivet").unwrap().secret(),
            "scoped-secret"
        );
        assert!(matches!(
            vault.get_for_project("github", "other"),
            Err(CredentialError::CredentialNotFound(id)) if id == "github"
        ));
        assert_eq!(vault.list()[0].projects, vec!["release", "rivet"]);
        drop(vault);

        let vault = CredentialVault::open(&path, PASSPHRASE).expect("reopen");
        assert!(vault.get_for_project("github", "release").is_ok());
        assert!(vault.get_for_project("github", "other").is_err());
    }

    #[test]
    fn project_scopes_are_trimmed_and_bounded() {
        let directory = tempdir().expect("tempdir");
        let path = directory.path().join("credentials.vault");
        let mut vault = CredentialVault::open_or_create(&path, PASSPHRASE).expect("create");
        vault
            .set_http_basic_for_projects("github", "oauth2", "secret", [" release "])
            .expect("trim scope");
        assert_eq!(vault.list()[0].projects, vec!["release"]);
        assert!(matches!(
            vault.set_http_basic_for_projects(
                "too-many",
                "oauth2",
                "secret",
                (0..=MAX_PROJECTS).map(|index| format!("project-{index}")),
            ),
            Err(CredentialError::TooManyProjects)
        ));
        assert!(matches!(
            vault.set_http_basic_for_projects("invalid", "oauth2", "secret", ["bad/name"]),
            Err(CredentialError::InvalidProject(_))
        ));
    }

    #[test]
    fn keychain_labels_and_passphrases_are_validated_before_access() {
        assert!(CredentialKeychain::new(" ").is_err());
        assert!(
            CredentialKeychain::rivet()
                .set_passphrase("account", "short")
                .is_err()
        );
    }

    #[cfg(target_os = "macos")]
    #[test]
    #[ignore = "touches the user's OS keychain; run explicitly for local verification"]
    fn macos_keychain_round_trips_and_cleans_up() {
        let account = format!("rivet-test-{}", Uuid::new_v4());
        let keychain = CredentialKeychain::rivet();
        let passphrase = "local-keychain-fixture-passphrase";
        keychain
            .set_passphrase(&account, passphrase)
            .expect("store keychain fixture");
        let stored = keychain
            .get_passphrase(&account)
            .expect("read keychain fixture");
        let cleanup = keychain.delete_passphrase(&account);
        assert_eq!(stored.as_str(), passphrase);
        cleanup.expect("remove keychain fixture");
    }

    #[test]
    fn invalid_entries_are_rejected_before_persistence() {
        let directory = tempdir().expect("tempdir");
        let path = directory.path().join("credentials.vault");
        let mut vault = CredentialVault::open_or_create(&path, PASSPHRASE).expect("create");
        assert!(matches!(
            vault.set_http_basic("../escape", "user", "secret"),
            Err(CredentialError::InvalidId(_))
        ));
        assert!(matches!(
            vault.set_http_basic("valid", "", "secret"),
            Err(CredentialError::InvalidUsername)
        ));
        assert!(matches!(
            vault.set_http_basic("valid", "user", ""),
            Err(CredentialError::EmptySecret)
        ));
        assert!(matches!(
            vault.set_http_basic("nul", "user", "secret\0value"),
            Err(CredentialError::InvalidSecret)
        ));
        assert!(matches!(
            CredentialVault::open_or_create(directory.path().join("weak.vault"), "short"),
            Err(CredentialError::WeakPassphrase)
        ));
    }

    #[test]
    fn ssh_credentials_round_trip_with_kind_and_project_scope() {
        let directory = tempdir().expect("tempdir");
        let path = directory.path().join("credentials.vault");
        let private_key =
            "-----BEGIN OPENSSH PRIVATE KEY-----\nfixture-key\n-----END OPENSSH PRIVATE KEY-----";
        let mut vault = CredentialVault::open_or_create(&path, PASSPHRASE).expect("create");
        vault
            .set_ssh_key_for_projects("deploy", "git", private_key, ["rivet"])
            .expect("set ssh credential");
        assert_eq!(vault.list()[0].kind, CredentialKind::SshKey);
        assert_eq!(
            vault.get_for_project("deploy", "rivet").unwrap().secret(),
            private_key
        );
        assert!(matches!(
            vault.set_ssh_key_for_projects(
                "invalid",
                "git",
                "not-a-key",
                std::iter::empty::<String>()
            ),
            Err(CredentialError::InvalidFormat(_))
        ));
        drop(vault);

        let vault = CredentialVault::open(&path, PASSPHRASE).expect("reopen");
        let credential = vault.get("deploy").expect("ssh credential");
        assert_eq!(credential.kind(), CredentialKind::SshKey);
        assert_eq!(credential.username(), "git");
        assert_eq!(credential.secret(), private_key);
    }

    #[test]
    fn legacy_vault_entries_default_to_http_basic() {
        let directory = tempdir().expect("tempdir");
        let path = directory.path().join("credentials.vault");
        let mut vault = CredentialVault::open_or_create(&path, PASSPHRASE).expect("create");
        vault
            .set_http_basic("github", "oauth2", "fixture-token-value")
            .expect("set");
        let encrypted = serde_json::from_slice::<EncryptedVault>(&fs::read(&path).expect("read"))
            .expect("decode encrypted vault");
        let plaintext =
            Zeroizing::new(decrypt_vault(&encrypted, PASSPHRASE.as_bytes()).expect("decrypt"));
        let mut legacy = serde_json::from_slice::<serde_json::Value>(&plaintext).expect("payload");
        let credential = legacy["credentials"]["github"]
            .as_object_mut()
            .expect("credential object");
        credential.remove("kind");
        let reencrypted = encrypt_vault(
            &Zeroizing::new(serde_json::to_vec(&legacy).expect("serialize legacy payload")),
            PASSPHRASE.as_bytes(),
        )
        .expect("encrypt legacy payload");
        fs::write(
            &path,
            serde_json::to_vec(&reencrypted).expect("serialize vault"),
        )
        .expect("write legacy vault");

        let vault = CredentialVault::open(&path, PASSPHRASE).expect("open legacy vault");
        assert_eq!(
            vault.get("github").unwrap().kind(),
            CredentialKind::HttpBasic
        );
    }

    #[cfg(unix)]
    #[test]
    fn vault_file_is_private() {
        use std::os::unix::fs::PermissionsExt;

        let directory = tempdir().expect("tempdir");
        let path = directory.path().join("credentials.vault");
        CredentialVault::open_or_create(&path, PASSPHRASE).expect("create");
        let mode = fs::metadata(path).expect("metadata").permissions().mode() & 0o777;
        assert_eq!(mode, 0o600);
    }
}
