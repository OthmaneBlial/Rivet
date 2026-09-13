//! Versioned, bounded messages for Rivet extensions.
//!
//! The extension boundary is deliberately narrower than a plugin ABI. An
//! extension declares whether it is a WASM module or a direct subprocess,
//! receives JSON messages over a length-prefixed stream, and can only ask for
//! capabilities named in its manifest. This crate validates and frames the
//! contract; the manager below owns the subprocess lifecycle, a small
//! capability-free WASM runtime, and the host-side permission check.

use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{BTreeMap, HashSet};
use std::fs;
use std::path::{Component, Path, PathBuf};
use std::process::Stdio;
use std::sync::Arc;
use thiserror::Error;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::process::{Child, ChildStdin, ChildStdout, Command};
use tokio::sync::Mutex;
use uuid::Uuid;
use wasmi::{
    Config, Engine, ExternType, Linker, Memory, Module, Store, StoreLimits, StoreLimitsBuilder,
    TypedFunc,
};

pub const PROTOCOL_NAME: &str = "rivet-extension";
pub const PROTOCOL_VERSION: u16 = 1;
pub const MAX_FRAME_BYTES: usize = 1024 * 1024;

const MAX_EXTENSION_ID_BYTES: usize = 64;
const MAX_DISPLAY_NAME_BYTES: usize = 128;
const MAX_VERSION_BYTES: usize = 64;
const MAX_ENTRYPOINT_BYTES: usize = 512;
const MAX_METHOD_BYTES: usize = 128;
const MAX_EVENT_BYTES: usize = 128;
const MAX_ERROR_CODE_BYTES: usize = 64;
const MAX_ERROR_MESSAGE_BYTES: usize = 512;
const MAX_PERMISSIONS: usize = 32;
const MAX_CATALOG_MANIFESTS: usize = 128;
const MAX_MANIFEST_BYTES: u64 = 64 * 1024;
const MAX_WASM_MODULE_BYTES: u64 = 16 * 1024 * 1024;
const MAX_WASM_MEMORY_BYTES: usize = 16 * 1024 * 1024;
const MAX_WASM_FUEL: u64 = 5_000_000;
const MAX_WASM_OUTPUT_BYTES: usize = MAX_FRAME_BYTES;
const WASM_ABI_VERSION: u16 = 1;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ExtensionKind {
    Wasm,
    Subprocess,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum ExtensionPermission {
    ReadBuilds,
    ReadLogs,
    ReadArtifacts,
    TriggerBuilds,
    WriteAnnotations,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ExtensionManifest {
    pub protocol_version: u16,
    pub id: String,
    pub name: String,
    pub version: String,
    pub kind: ExtensionKind,
    /// A relative module path or executable name. The host never interprets
    /// this as a shell command and rejects path traversal components.
    pub entrypoint: String,
    #[serde(default)]
    pub permissions: Vec<ExtensionPermission>,
}

impl ExtensionManifest {
    pub fn validate(&self) -> Result<(), ExtensionProtocolError> {
        validate_version(self.protocol_version)?;
        validate_identifier(&self.id)?;
        validate_text(
            &self.name,
            MAX_DISPLAY_NAME_BYTES,
            ExtensionProtocolError::EmptyName,
            ExtensionProtocolError::NameTooLong,
        )?;
        validate_text(
            &self.version,
            MAX_VERSION_BYTES,
            ExtensionProtocolError::EmptyVersion,
            ExtensionProtocolError::VersionTooLong,
        )?;
        validate_entrypoint(&self.entrypoint)?;
        if self.permissions.len() > MAX_PERMISSIONS {
            return Err(ExtensionProtocolError::TooManyPermissions);
        }
        let mut unique = HashSet::with_capacity(self.permissions.len());
        if self
            .permissions
            .iter()
            .any(|permission| !unique.insert(permission))
        {
            return Err(ExtensionProtocolError::DuplicatePermission);
        }
        Ok(())
    }

    pub fn allows(&self, permission: ExtensionPermission) -> bool {
        self.permissions.contains(&permission)
    }
}

#[derive(Debug, Clone, Default)]
pub struct ExtensionCatalog {
    manifests: Vec<ExtensionManifest>,
}

impl ExtensionCatalog {
    pub fn empty() -> Self {
        Self::default()
    }

    pub fn from_directory(path: Option<&Path>) -> Result<Self, ExtensionCatalogError> {
        let Some(path) = path else {
            return Ok(Self::empty());
        };
        let metadata =
            fs::symlink_metadata(path).map_err(|error| ExtensionCatalogError::Filesystem {
                path: path.to_path_buf(),
                message: error.to_string(),
            })?;
        if metadata.file_type().is_symlink() || !metadata.is_dir() {
            return Err(ExtensionCatalogError::InvalidDirectory(path.to_path_buf()));
        }

        let mut entries = fs::read_dir(path)
            .map_err(|error| ExtensionCatalogError::Filesystem {
                path: path.to_path_buf(),
                message: error.to_string(),
            })?
            .collect::<Result<Vec<_>, _>>()
            .map_err(|error| ExtensionCatalogError::Filesystem {
                path: path.to_path_buf(),
                message: error.to_string(),
            })?;
        entries.sort_by_key(|entry| entry.path());
        let mut manifests = Vec::new();
        let mut ids = HashSet::new();
        for entry in entries {
            let manifest_path = entry.path();
            if !manifest_path
                .extension()
                .is_some_and(|extension| extension.eq_ignore_ascii_case("json"))
            {
                continue;
            }
            if manifests.len() == MAX_CATALOG_MANIFESTS {
                return Err(ExtensionCatalogError::TooManyManifests);
            }
            let metadata = fs::symlink_metadata(&manifest_path).map_err(|error| {
                ExtensionCatalogError::Filesystem {
                    path: manifest_path.clone(),
                    message: error.to_string(),
                }
            })?;
            if metadata.file_type().is_symlink() || !metadata.is_file() {
                return Err(ExtensionCatalogError::InvalidManifestPath(manifest_path));
            }
            if metadata.len() > MAX_MANIFEST_BYTES {
                return Err(ExtensionCatalogError::ManifestTooLarge(manifest_path));
            }
            let bytes =
                fs::read(&manifest_path).map_err(|error| ExtensionCatalogError::Filesystem {
                    path: manifest_path.clone(),
                    message: error.to_string(),
                })?;
            let manifest =
                serde_json::from_slice::<ExtensionManifest>(&bytes).map_err(|error| {
                    ExtensionCatalogError::InvalidManifest {
                        path: manifest_path.clone(),
                        message: error.to_string(),
                    }
                })?;
            manifest
                .validate()
                .map_err(|error| ExtensionCatalogError::InvalidManifest {
                    path: manifest_path.clone(),
                    message: error.to_string(),
                })?;
            if !ids.insert(manifest.id.clone()) {
                return Err(ExtensionCatalogError::DuplicateId(manifest.id));
            }
            manifests.push(manifest);
        }
        Ok(Self { manifests })
    }

    pub fn manifests(&self) -> &[ExtensionManifest] {
        &self.manifests
    }
}

#[derive(Debug, Error)]
pub enum ExtensionCatalogError {
    #[error("extension catalog directory is not a regular directory: {0}")]
    InvalidDirectory(std::path::PathBuf),
    #[error("extension catalog entry is not a regular manifest file: {0}")]
    InvalidManifestPath(std::path::PathBuf),
    #[error("extension manifest is larger than the 64 KiB limit: {0}")]
    ManifestTooLarge(std::path::PathBuf),
    #[error("could not read extension catalog path {path}: {message}")]
    Filesystem {
        path: std::path::PathBuf,
        message: String,
    },
    #[error("invalid extension manifest {path}: {message}")]
    InvalidManifest {
        path: std::path::PathBuf,
        message: String,
    },
    #[error("extension catalog contains duplicate ID {0}")]
    DuplicateId(String),
    #[error("extension catalog contains more than {MAX_CATALOG_MANIFESTS} manifests")]
    TooManyManifests,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ExtensionMessage {
    Hello {
        protocol_version: u16,
        manifest: ExtensionManifest,
    },
    Ready {
        protocol_version: u16,
        extension_id: String,
    },
    Request {
        protocol_version: u16,
        request_id: Uuid,
        method: String,
        payload: Value,
    },
    Result {
        protocol_version: u16,
        request_id: Uuid,
        payload: Value,
    },
    Error {
        protocol_version: u16,
        request_id: Option<Uuid>,
        code: String,
        message: String,
    },
    Event {
        protocol_version: u16,
        name: String,
        payload: Value,
    },
    Shutdown {
        protocol_version: u16,
    },
}

impl ExtensionMessage {
    pub fn validate(&self) -> Result<(), ExtensionProtocolError> {
        match self {
            Self::Hello {
                protocol_version,
                manifest,
            } => {
                validate_version(*protocol_version)?;
                if *protocol_version != manifest.protocol_version {
                    return Err(ExtensionProtocolError::ManifestVersionMismatch);
                }
                manifest.validate()
            }
            Self::Ready {
                protocol_version,
                extension_id,
            } => {
                validate_version(*protocol_version)?;
                validate_identifier(extension_id)
            }
            Self::Request {
                protocol_version,
                method,
                ..
            } => {
                validate_version(*protocol_version)?;
                validate_method(method)
            }
            Self::Result {
                protocol_version, ..
            } => validate_version(*protocol_version),
            Self::Error {
                protocol_version,
                code,
                message,
                ..
            } => {
                validate_version(*protocol_version)?;
                validate_error_text(code, message)
            }
            Self::Event {
                protocol_version,
                name,
                ..
            } => {
                validate_version(*protocol_version)?;
                validate_event_name(name)
            }
            Self::Shutdown { protocol_version } => validate_version(*protocol_version),
        }
    }

    pub fn protocol_version(&self) -> u16 {
        match self {
            Self::Hello {
                protocol_version, ..
            }
            | Self::Ready {
                protocol_version, ..
            }
            | Self::Request {
                protocol_version, ..
            }
            | Self::Result {
                protocol_version, ..
            }
            | Self::Error {
                protocol_version, ..
            }
            | Self::Event {
                protocol_version, ..
            }
            | Self::Shutdown { protocol_version } => *protocol_version,
        }
    }
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum ExtensionProtocolError {
    #[error("unsupported extension protocol version {found}; expected {expected}")]
    UnsupportedVersion { found: u16, expected: u16 },
    #[error("extension ID is empty, too long, or contains unsupported characters")]
    InvalidIdentifier,
    #[error("extension name cannot be empty")]
    EmptyName,
    #[error("extension name is too long")]
    NameTooLong,
    #[error("extension version cannot be empty")]
    EmptyVersion,
    #[error("extension version is too long")]
    VersionTooLong,
    #[error("extension entrypoint cannot be empty")]
    EmptyEntrypoint,
    #[error("extension entrypoint is too long")]
    EntrypointTooLong,
    #[error("extension entrypoint must be relative and cannot traverse parent directories")]
    InvalidEntrypoint,
    #[error("extension manifest contains too many permissions")]
    TooManyPermissions,
    #[error("extension manifest contains a duplicate permission")]
    DuplicatePermission,
    #[error("extension method cannot be empty, too long, or contain control characters")]
    InvalidMethod,
    #[error("extension event name cannot be empty, too long, or contain control characters")]
    InvalidEventName,
    #[error("extension error code or message is invalid")]
    InvalidErrorText,
    #[error("extension hello version does not match its manifest")]
    ManifestVersionMismatch,
    #[error("extension frame exceeds the {MAX_FRAME_BYTES}-byte payload limit")]
    FrameTooLarge,
    #[error("extension frame is truncated")]
    TruncatedFrame,
    #[error("extension frame length does not match its payload")]
    FrameLengthMismatch,
    #[error("extension frame contains invalid JSON: {0}")]
    InvalidJson(String),
}

pub fn encode_message(message: &ExtensionMessage) -> Result<Vec<u8>, ExtensionProtocolError> {
    message.validate()?;
    let payload = serde_json::to_vec(message)
        .map_err(|error| ExtensionProtocolError::InvalidJson(error.to_string()))?;
    if payload.len() > MAX_FRAME_BYTES {
        return Err(ExtensionProtocolError::FrameTooLarge);
    }
    let length = u32::try_from(payload.len()).map_err(|_| ExtensionProtocolError::FrameTooLarge)?;
    let mut frame = Vec::with_capacity(4 + payload.len());
    frame.extend_from_slice(&length.to_be_bytes());
    frame.extend_from_slice(&payload);
    Ok(frame)
}

pub fn decode_frame(frame: &[u8]) -> Result<ExtensionMessage, ExtensionProtocolError> {
    if frame.len() < 4 {
        return Err(ExtensionProtocolError::TruncatedFrame);
    }
    let length = u32::from_be_bytes([frame[0], frame[1], frame[2], frame[3]]) as usize;
    if length > MAX_FRAME_BYTES {
        return Err(ExtensionProtocolError::FrameTooLarge);
    }
    if frame.len() != 4 + length {
        return Err(ExtensionProtocolError::FrameLengthMismatch);
    }
    let message = serde_json::from_slice::<ExtensionMessage>(&frame[4..])
        .map_err(|error| ExtensionProtocolError::InvalidJson(error.to_string()))?;
    message.validate()?;
    Ok(message)
}

#[derive(Debug, Default)]
pub struct FrameDecoder {
    buffer: Vec<u8>,
}

impl FrameDecoder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn push(&mut self, bytes: &[u8]) -> Result<Vec<ExtensionMessage>, ExtensionProtocolError> {
        if self.buffer.len().saturating_add(bytes.len()) > MAX_FRAME_BYTES + 4 {
            return Err(ExtensionProtocolError::FrameTooLarge);
        }
        self.buffer.extend_from_slice(bytes);
        let mut messages = Vec::new();
        loop {
            if self.buffer.len() < 4 {
                break;
            }
            let length = u32::from_be_bytes([
                self.buffer[0],
                self.buffer[1],
                self.buffer[2],
                self.buffer[3],
            ]) as usize;
            if length > MAX_FRAME_BYTES {
                return Err(ExtensionProtocolError::FrameTooLarge);
            }
            let frame_length = 4 + length;
            if self.buffer.len() < frame_length {
                break;
            }
            let frame = self.buffer.drain(..frame_length).collect::<Vec<_>>();
            messages.push(decode_frame(&frame)?);
        }
        Ok(messages)
    }

    pub fn is_empty(&self) -> bool {
        self.buffer.is_empty()
    }
}

/// A direct subprocess transport for a validated subprocess extension.
///
/// The program and arguments are passed directly to `Command`; no shell is
/// involved. The host uses a one-request-at-a-time exchange, which keeps the
/// first protocol boundary deterministic while the extension manager owns
/// lifecycle and permission enforcement.
pub struct SubprocessExtension {
    manifest: ExtensionManifest,
    child: Child,
    stdin: ChildStdin,
    stdout: ChildStdout,
}

/// A capability-free in-process WASM extension.
///
/// The ABI is intentionally explicit and small. A module must export a
/// `memory`, `rivet_alloc(i32) -> i32`, and
/// `rivet_handle(i32, i32) -> i64`. The packed return value contains the
/// response length in the high 32 bits and its memory pointer in the low 32
/// bits. Modules cannot import functions, memory, tables, or globals, so the
/// host exposes no filesystem, network, clock, or process capability.
pub struct WasmExtension {
    manifest: ExtensionManifest,
    store: Store<StoreLimits>,
    memory: Memory,
    alloc: TypedFunc<i32, i32>,
    handle: TypedFunc<(i32, i32), i64>,
    free: Option<TypedFunc<(i32, i32), ()>>,
}

enum ExtensionSession {
    Subprocess(SubprocessExtension),
    Wasm(WasmExtension),
}

/// Owns the lifecycle of locally catalogued extensions.
///
/// The manager deliberately keeps the policy boundary in the host: a
/// manifest is not enough to launch arbitrary code, an entrypoint must be a
/// regular executable under the configured root, and every request goes
/// through the manifest permission check. WASM modules run with no imports,
/// bounded linear memory, and deterministic fuel metering.
pub struct ExtensionManager {
    root: PathBuf,
    manifests: BTreeMap<String, ExtensionManifest>,
    sessions: Arc<Mutex<BTreeMap<String, ExtensionSession>>>,
}

impl ExtensionManager {
    pub fn new(
        root: impl AsRef<Path>,
        catalog: &ExtensionCatalog,
    ) -> Result<Self, ExtensionManagerError> {
        let root = root.as_ref();
        let metadata =
            fs::symlink_metadata(root).map_err(|error| ExtensionManagerError::Filesystem {
                path: root.to_path_buf(),
                message: error.to_string(),
            })?;
        if metadata.file_type().is_symlink() || !metadata.is_dir() {
            return Err(ExtensionManagerError::InvalidRoot(root.to_path_buf()));
        }
        let root = fs::canonicalize(root).map_err(|error| ExtensionManagerError::Filesystem {
            path: root.to_path_buf(),
            message: error.to_string(),
        })?;
        let manifests = catalog
            .manifests()
            .iter()
            .map(|manifest| (manifest.id.clone(), manifest.clone()))
            .collect();
        Ok(Self {
            root,
            manifests,
            sessions: Arc::new(Mutex::new(BTreeMap::new())),
        })
    }

    pub fn manifest(&self, id: &str) -> Option<&ExtensionManifest> {
        self.manifests.get(id)
    }

    pub async fn active_extensions(&self) -> Vec<String> {
        self.sessions.lock().await.keys().cloned().collect()
    }

    pub async fn launch<I, S>(&self, id: &str, args: I) -> Result<(), ExtensionManagerError>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<std::ffi::OsStr>,
    {
        let manifest = self
            .manifests
            .get(id)
            .cloned()
            .ok_or_else(|| ExtensionManagerError::UnknownExtension(id.to_owned()))?;
        if self.sessions.lock().await.contains_key(id) {
            return Err(ExtensionManagerError::AlreadyRunning(id.to_owned()));
        }
        let extension = match &manifest.kind {
            ExtensionKind::Subprocess => {
                let program = resolve_entrypoint(&self.root, &manifest)?;
                ExtensionSession::Subprocess(
                    SubprocessExtension::spawn(manifest, program, args).await?,
                )
            }
            ExtensionKind::Wasm => {
                let module = resolve_entrypoint(&self.root, &manifest)?;
                ExtensionSession::Wasm(WasmExtension::load(manifest, module)?)
            }
        };
        let mut sessions = self.sessions.lock().await;
        if sessions.contains_key(id) {
            return Err(ExtensionManagerError::AlreadyRunning(id.to_owned()));
        }
        sessions.insert(id.to_owned(), extension);
        Ok(())
    }

    pub async fn request(
        &self,
        id: &str,
        permission: ExtensionPermission,
        method: impl Into<String>,
        payload: Value,
    ) -> Result<Value, ExtensionManagerError> {
        let mut sessions = self.sessions.lock().await;
        let session = sessions
            .get_mut(id)
            .ok_or_else(|| ExtensionManagerError::NotRunning(id.to_owned()))?;
        match session {
            ExtensionSession::Subprocess(extension) => extension
                .request_with_permission(permission, method, payload)
                .await
                .map_err(ExtensionManagerError::Host),
            ExtensionSession::Wasm(extension) => extension
                .request_with_permission(permission, method, payload)
                .map_err(ExtensionManagerError::Host),
        }
    }

    pub async fn shutdown(&self, id: &str) -> Result<(), ExtensionManagerError> {
        let extension = self
            .sessions
            .lock()
            .await
            .remove(id)
            .ok_or_else(|| ExtensionManagerError::NotRunning(id.to_owned()))?;
        match extension {
            ExtensionSession::Subprocess(extension) => extension
                .shutdown()
                .await
                .map_err(ExtensionManagerError::Host),
            ExtensionSession::Wasm(_) => Ok(()),
        }
    }

    pub async fn terminate(&self, id: &str) -> Result<(), ExtensionManagerError> {
        let extension = self
            .sessions
            .lock()
            .await
            .remove(id)
            .ok_or_else(|| ExtensionManagerError::NotRunning(id.to_owned()))?;
        match extension {
            ExtensionSession::Subprocess(extension) => extension
                .terminate()
                .await
                .map_err(ExtensionManagerError::Host),
            ExtensionSession::Wasm(_) => Ok(()),
        }
    }
}

fn resolve_entrypoint(
    root: &Path,
    manifest: &ExtensionManifest,
) -> Result<PathBuf, ExtensionManagerError> {
    let candidate = root.join(&manifest.entrypoint);
    let metadata =
        fs::symlink_metadata(&candidate).map_err(|error| ExtensionManagerError::Filesystem {
            path: candidate.clone(),
            message: error.to_string(),
        })?;
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        return Err(ExtensionManagerError::InvalidEntrypoint(candidate));
    }
    let resolved =
        fs::canonicalize(&candidate).map_err(|error| ExtensionManagerError::Filesystem {
            path: candidate.clone(),
            message: error.to_string(),
        })?;
    if !resolved.starts_with(root) {
        return Err(ExtensionManagerError::EntrypointOutsideRoot(resolved));
    }
    if matches!(manifest.kind, ExtensionKind::Subprocess) {
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            if metadata.permissions().mode() & 0o111 == 0 {
                return Err(ExtensionManagerError::NotExecutable(resolved));
            }
        }
    }
    Ok(resolved)
}

impl WasmExtension {
    fn load(
        manifest: ExtensionManifest,
        module_path: impl AsRef<Path>,
    ) -> Result<Self, ExtensionHostError> {
        manifest.validate()?;
        if !matches!(manifest.kind, ExtensionKind::Wasm) {
            return Err(ExtensionHostError::WrongExtensionKind);
        }
        let metadata = fs::metadata(module_path.as_ref()).map_err(ExtensionHostError::Io)?;
        if metadata.len() > MAX_WASM_MODULE_BYTES {
            return Err(ExtensionHostError::WasmModuleTooLarge);
        }
        let bytes = fs::read(module_path.as_ref()).map_err(ExtensionHostError::Io)?;
        let mut config = Config::default();
        config.consume_fuel(true);
        let engine = Engine::new(&config);
        let module = Module::new(&engine, &bytes).map_err(|error| {
            ExtensionHostError::Wasm(format!("module validation failed: {error}"))
        })?;
        if module.imports().next().is_some() {
            return Err(ExtensionHostError::WasmImportsDenied);
        }
        let Some(ExternType::Memory(memory_type)) = module.get_export("memory") else {
            return Err(ExtensionHostError::WasmMissingExport("memory"));
        };
        let initial_pages = usize::try_from(u32::from(memory_type.initial_pages()))
            .map_err(|_| ExtensionHostError::WasmMemoryTooLarge)?;
        let initial_bytes = initial_pages.saturating_mul(64 * 1024);
        if initial_bytes > MAX_WASM_MEMORY_BYTES {
            return Err(ExtensionHostError::WasmMemoryTooLarge);
        }
        let mut store = Store::new(
            &engine,
            StoreLimitsBuilder::new()
                .memory_size(MAX_WASM_MEMORY_BYTES)
                .memories(1)
                .instances(1)
                .tables(4)
                .table_elements(1024)
                .build(),
        );
        store.limiter(|limits| limits);
        store
            .set_fuel(MAX_WASM_FUEL)
            .map_err(|error| ExtensionHostError::Wasm(format!("fuel setup failed: {error}")))?;
        let linker = Linker::<StoreLimits>::new(&engine);
        let instance = linker
            .instantiate(&mut store, &module)
            .map_err(|error| {
                ExtensionHostError::Wasm(format!("module instantiation failed: {error}"))
            })?
            .start(&mut store)
            .map_err(|error| ExtensionHostError::Wasm(format!("module start failed: {error}")))?;
        let memory = instance
            .get_memory(&store, "memory")
            .ok_or(ExtensionHostError::WasmMissingExport("memory"))?;
        let alloc = instance
            .get_typed_func::<i32, i32>(&store, "rivet_alloc")
            .map_err(|_| ExtensionHostError::WasmMissingExport("rivet_alloc"))?;
        let handle = instance
            .get_typed_func::<(i32, i32), i64>(&store, "rivet_handle")
            .map_err(|_| ExtensionHostError::WasmMissingExport("rivet_handle"))?;
        let free = instance
            .get_export(&store, "rivet_free")
            .map(|_| {
                instance
                    .get_typed_func::<(i32, i32), ()>(&store, "rivet_free")
                    .map_err(|_| ExtensionHostError::WasmInvalidAbi("rivet_free"))
            })
            .transpose()?;
        Ok(Self {
            manifest,
            store,
            memory,
            alloc,
            handle,
            free,
        })
    }

    fn request_with_permission(
        &mut self,
        permission: ExtensionPermission,
        method: impl Into<String>,
        payload: Value,
    ) -> Result<Value, ExtensionHostError> {
        ensure_permission(&self.manifest, permission)?;
        let method = method.into();
        validate_method(&method)?;
        let request_id = Uuid::new_v4();
        let request = serde_json::json!({
            "abi_version": WASM_ABI_VERSION,
            "request_id": request_id,
            "method": method,
            "payload": payload,
        });
        let request = serde_json::to_vec(&request).map_err(|error| {
            ExtensionHostError::Wasm(format!("request encoding failed: {error}"))
        })?;
        if request.len() > MAX_FRAME_BYTES {
            return Err(ExtensionHostError::WasmOutputTooLarge);
        }
        let request_len = i32::try_from(request.len())
            .map_err(|_| ExtensionHostError::WasmInvalidAbi("request length"))?;
        let request_ptr = self
            .alloc
            .call(&mut self.store, request_len)
            .map_err(|error| ExtensionHostError::Wasm(format!("allocator trapped: {error}")))?;
        if request_ptr < 0 {
            return Err(ExtensionHostError::WasmInvalidPointer);
        }
        let request_ptr =
            usize::try_from(request_ptr).map_err(|_| ExtensionHostError::WasmInvalidPointer)?;
        self.memory
            .write(&mut self.store, request_ptr, &request)
            .map_err(|error| ExtensionHostError::Wasm(format!("request write failed: {error}")))?;
        let packed = self
            .handle
            .call(&mut self.store, (request_ptr as i32, request_len))
            .map_err(|error| ExtensionHostError::Wasm(format!("handler trapped: {error}")))?
            as u64;
        let response_ptr = usize::try_from((packed & u64::from(u32::MAX)) as u32)
            .map_err(|_| ExtensionHostError::WasmInvalidPointer)?;
        let response_len = usize::try_from((packed >> 32) as u32)
            .map_err(|_| ExtensionHostError::WasmInvalidPointer)?;
        if response_len > MAX_WASM_OUTPUT_BYTES {
            return Err(ExtensionHostError::WasmOutputTooLarge);
        }
        let mut response = vec![0_u8; response_len];
        self.memory
            .read(&self.store, response_ptr, &mut response)
            .map_err(|error| ExtensionHostError::Wasm(format!("response read failed: {error}")))?;
        if let Some(free) = self.free {
            free.call(&mut self.store, (response_ptr as i32, response_len as i32))
                .map_err(|error| {
                    ExtensionHostError::Wasm(format!("response free trapped: {error}"))
                })?;
        }
        let response: Value = serde_json::from_slice(&response).map_err(|error| {
            ExtensionHostError::Wasm(format!("response JSON is invalid: {error}"))
        })?;
        if response.get("abi_version").and_then(Value::as_u64) != Some(u64::from(WASM_ABI_VERSION))
        {
            return Err(ExtensionHostError::WasmInvalidResponse("ABI version"));
        }
        if let Some(error) = response.get("error") {
            let code = error
                .get("code")
                .and_then(Value::as_str)
                .unwrap_or("wasm_error")
                .to_owned();
            let message = error
                .get("message")
                .and_then(Value::as_str)
                .unwrap_or("WASM extension returned an error")
                .to_owned();
            return Err(ExtensionHostError::Remote { code, message });
        }
        response
            .get("result")
            .cloned()
            .ok_or(ExtensionHostError::WasmInvalidResponse("result"))
    }
}

impl SubprocessExtension {
    pub async fn spawn<I, S>(
        manifest: ExtensionManifest,
        program: impl AsRef<Path>,
        args: I,
    ) -> Result<Self, ExtensionHostError>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<std::ffi::OsStr>,
    {
        manifest.validate()?;
        if !matches!(manifest.kind, ExtensionKind::Subprocess) {
            return Err(ExtensionHostError::WrongExtensionKind);
        }
        let mut command = Command::new(program.as_ref());
        command
            .args(args)
            .env_clear()
            .env("RIVET_EXTENSION_ID", &manifest.id)
            .env(
                "RIVET_EXTENSION_PROTOCOL_VERSION",
                PROTOCOL_VERSION.to_string(),
            )
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit());
        command.kill_on_drop(true);
        let mut child = command.spawn().map_err(ExtensionHostError::Spawn)?;
        let stdin = match child.stdin.take() {
            Some(stdin) => stdin,
            None => {
                let _ = child.kill().await;
                return Err(ExtensionHostError::MissingPipe("stdin"));
            }
        };
        let stdout = match child.stdout.take() {
            Some(stdout) => stdout,
            None => {
                let _ = child.kill().await;
                return Err(ExtensionHostError::MissingPipe("stdout"));
            }
        };
        let mut extension = Self {
            manifest,
            child,
            stdin,
            stdout,
        };
        if let Err(error) = extension
            .write_message(&ExtensionMessage::Hello {
                protocol_version: PROTOCOL_VERSION,
                manifest: extension.manifest.clone(),
            })
            .await
        {
            let _ = extension.terminate().await;
            return Err(error);
        }
        let response = match extension.read_message().await {
            Ok(response) => response,
            Err(error) => {
                let _ = extension.terminate().await;
                return Err(error);
            }
        };
        match response {
            ExtensionMessage::Ready { extension_id, .. }
                if extension_id == extension.manifest.id =>
            {
                Ok(extension)
            }
            _ => {
                let _ = extension.terminate().await;
                Err(ExtensionHostError::UnexpectedHandshake)
            }
        }
    }

    pub fn manifest(&self) -> &ExtensionManifest {
        &self.manifest
    }

    pub async fn request(
        &mut self,
        method: impl Into<String>,
        payload: Value,
    ) -> Result<Value, ExtensionHostError> {
        let request_id = Uuid::new_v4();
        self.write_message(&ExtensionMessage::Request {
            protocol_version: PROTOCOL_VERSION,
            request_id,
            method: method.into(),
            payload,
        })
        .await?;
        match self.read_message().await? {
            ExtensionMessage::Result {
                request_id: response_id,
                payload,
                ..
            } if response_id == request_id => Ok(payload),
            ExtensionMessage::Error {
                request_id: Some(response_id),
                code,
                message,
                ..
            } if response_id == request_id => Err(ExtensionHostError::Remote { code, message }),
            _ => Err(ExtensionHostError::UnexpectedResponse),
        }
    }

    pub async fn request_with_permission(
        &mut self,
        permission: ExtensionPermission,
        method: impl Into<String>,
        payload: Value,
    ) -> Result<Value, ExtensionHostError> {
        ensure_permission(&self.manifest, permission)?;
        self.request(method, payload).await
    }

    pub async fn shutdown(mut self) -> Result<(), ExtensionHostError> {
        self.write_message(&ExtensionMessage::Shutdown {
            protocol_version: PROTOCOL_VERSION,
        })
        .await?;
        self.stdin
            .shutdown()
            .await
            .map_err(ExtensionHostError::Io)?;
        self.child.wait().await.map_err(ExtensionHostError::Io)?;
        Ok(())
    }

    pub async fn terminate(mut self) -> Result<(), ExtensionHostError> {
        self.child.kill().await.map_err(ExtensionHostError::Io)
    }

    async fn write_message(
        &mut self,
        message: &ExtensionMessage,
    ) -> Result<(), ExtensionHostError> {
        let frame = encode_message(message)?;
        self.stdin
            .write_all(&frame)
            .await
            .map_err(ExtensionHostError::Io)
    }

    async fn read_message(&mut self) -> Result<ExtensionMessage, ExtensionHostError> {
        let mut header = [0_u8; 4];
        self.stdout
            .read_exact(&mut header)
            .await
            .map_err(ExtensionHostError::Io)?;
        let length = u32::from_be_bytes(header) as usize;
        if length > MAX_FRAME_BYTES {
            return Err(ExtensionProtocolError::FrameTooLarge.into());
        }
        let mut frame = Vec::with_capacity(4 + length);
        frame.extend_from_slice(&header);
        let mut payload = vec![0_u8; length];
        self.stdout
            .read_exact(&mut payload)
            .await
            .map_err(ExtensionHostError::Io)?;
        frame.extend_from_slice(&payload);
        Ok(decode_frame(&frame)?)
    }
}

#[derive(Debug, Error)]
pub enum ExtensionHostError {
    #[error(transparent)]
    Protocol(#[from] ExtensionProtocolError),
    #[error("could not start extension subprocess: {0}")]
    Spawn(std::io::Error),
    #[error("extension subprocess I/O failed: {0}")]
    Io(std::io::Error),
    #[error("extension subprocess did not provide a {0} pipe")]
    MissingPipe(&'static str),
    #[error("extension subprocess handshake was not accepted")]
    UnexpectedHandshake,
    #[error("extension subprocess host requires a subprocess manifest")]
    WrongExtensionKind,
    #[error("extension does not declare the {0:?} permission")]
    PermissionDenied(ExtensionPermission),
    #[error("extension subprocess response did not match the request")]
    UnexpectedResponse,
    #[error("extension returned {code}: {message}")]
    Remote { code: String, message: String },
    #[error("WASM module exceeds the 16 MiB module limit")]
    WasmModuleTooLarge,
    #[error("WASM modules must not import host capabilities")]
    WasmImportsDenied,
    #[error("WASM module is missing the required `{0}` export")]
    WasmMissingExport(&'static str),
    #[error("WASM module memory exceeds the 16 MiB limit")]
    WasmMemoryTooLarge,
    #[error("WASM extension ABI is invalid for `{0}`")]
    WasmInvalidAbi(&'static str),
    #[error("WASM extension returned an invalid pointer")]
    WasmInvalidPointer,
    #[error("WASM extension response exceeds the 1 MiB limit")]
    WasmOutputTooLarge,
    #[error("WASM extension response is missing a valid `{0}` field")]
    WasmInvalidResponse(&'static str),
    #[error("WASM execution failed: {0}")]
    Wasm(String),
}

#[derive(Debug, Error)]
pub enum ExtensionManagerError {
    #[error("extension manager root is not a regular directory: {0}")]
    InvalidRoot(PathBuf),
    #[error("could not inspect extension path {path}: {message}")]
    Filesystem { path: PathBuf, message: String },
    #[error("extension {0:?} is not present in the validated catalog")]
    UnknownExtension(String),
    #[error("extension entrypoint is not a regular file: {0}")]
    InvalidEntrypoint(PathBuf),
    #[error("extension entrypoint resolves outside its configured root: {0}")]
    EntrypointOutsideRoot(PathBuf),
    #[error("extension entrypoint is not executable: {0}")]
    NotExecutable(PathBuf),
    #[error("extension {0:?} already has an active session")]
    AlreadyRunning(String),
    #[error("extension {0:?} has no active session")]
    NotRunning(String),
    #[error(transparent)]
    Host(#[from] ExtensionHostError),
}

fn ensure_permission(
    manifest: &ExtensionManifest,
    permission: ExtensionPermission,
) -> Result<(), ExtensionHostError> {
    if manifest.allows(permission) {
        Ok(())
    } else {
        Err(ExtensionHostError::PermissionDenied(permission))
    }
}

fn validate_version(version: u16) -> Result<(), ExtensionProtocolError> {
    if version != PROTOCOL_VERSION {
        return Err(ExtensionProtocolError::UnsupportedVersion {
            found: version,
            expected: PROTOCOL_VERSION,
        });
    }
    Ok(())
}

fn validate_identifier(value: &str) -> Result<(), ExtensionProtocolError> {
    if value.is_empty()
        || value.len() > MAX_EXTENSION_ID_BYTES
        || !value.bytes().all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || b"._-".contains(&byte)
        })
        || !value.as_bytes()[0].is_ascii_lowercase()
    {
        return Err(ExtensionProtocolError::InvalidIdentifier);
    }
    Ok(())
}

fn validate_text(
    value: &str,
    max_bytes: usize,
    empty: ExtensionProtocolError,
    too_long: ExtensionProtocolError,
) -> Result<(), ExtensionProtocolError> {
    if value.trim().is_empty() {
        return Err(empty);
    }
    if value.len() > max_bytes || value.chars().any(char::is_control) {
        return Err(too_long);
    }
    Ok(())
}

fn validate_entrypoint(value: &str) -> Result<(), ExtensionProtocolError> {
    if value.trim().is_empty() {
        return Err(ExtensionProtocolError::EmptyEntrypoint);
    }
    if value.len() > MAX_ENTRYPOINT_BYTES {
        return Err(ExtensionProtocolError::EntrypointTooLong);
    }
    let path = Path::new(value);
    if path.is_absolute()
        || path
            .components()
            .any(|component| matches!(component, Component::ParentDir | Component::Prefix(_)))
        || value.chars().any(char::is_control)
    {
        return Err(ExtensionProtocolError::InvalidEntrypoint);
    }
    Ok(())
}

fn validate_method(value: &str) -> Result<(), ExtensionProtocolError> {
    if value.trim().is_empty()
        || value.len() > MAX_METHOD_BYTES
        || value.chars().any(char::is_control)
    {
        return Err(ExtensionProtocolError::InvalidMethod);
    }
    Ok(())
}

fn validate_event_name(value: &str) -> Result<(), ExtensionProtocolError> {
    if value.trim().is_empty()
        || value.len() > MAX_EVENT_BYTES
        || value.chars().any(char::is_control)
    {
        return Err(ExtensionProtocolError::InvalidEventName);
    }
    Ok(())
}

fn validate_error_text(code: &str, message: &str) -> Result<(), ExtensionProtocolError> {
    if code.trim().is_empty()
        || code.len() > MAX_ERROR_CODE_BYTES
        || message.trim().is_empty()
        || message.len() > MAX_ERROR_MESSAGE_BYTES
        || code.chars().any(char::is_control)
        || message.chars().any(char::is_control)
    {
        return Err(ExtensionProtocolError::InvalidErrorText);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn manifest(kind: ExtensionKind) -> ExtensionManifest {
        ExtensionManifest {
            protocol_version: PROTOCOL_VERSION,
            id: "coverage.reporter".into(),
            name: "Coverage reporter".into(),
            version: "1.0.0".into(),
            kind,
            entrypoint: "extension.wasm".into(),
            permissions: vec![ExtensionPermission::ReadBuilds],
        }
    }

    #[test]
    fn validates_manifest_without_allowing_traversal_or_duplicate_permissions() {
        manifest(ExtensionKind::Wasm).validate().expect("manifest");

        let mut traversal = manifest(ExtensionKind::Subprocess);
        traversal.entrypoint = "../extension".into();
        assert!(matches!(
            traversal.validate(),
            Err(ExtensionProtocolError::InvalidEntrypoint)
        ));

        let mut duplicate = manifest(ExtensionKind::Wasm);
        duplicate.permissions.push(ExtensionPermission::ReadBuilds);
        assert!(matches!(
            duplicate.validate(),
            Err(ExtensionProtocolError::DuplicatePermission)
        ));
    }

    #[test]
    fn frames_round_trip_and_decode_incremental_concatenated_messages() {
        let first = ExtensionMessage::Request {
            protocol_version: PROTOCOL_VERSION,
            request_id: Uuid::new_v4(),
            method: "build.summary".into(),
            payload: serde_json::json!({"project": "rivet"}),
        };
        let second = ExtensionMessage::Event {
            protocol_version: PROTOCOL_VERSION,
            name: "build.updated".into(),
            payload: serde_json::json!({"number": 7}),
        };
        let mut bytes = encode_message(&first).expect("first frame");
        bytes.extend_from_slice(&encode_message(&second).expect("second frame"));
        let mut decoder = FrameDecoder::new();
        let mut messages = Vec::new();
        for chunk in bytes.chunks(3) {
            messages.extend(decoder.push(chunk).expect("decode chunk"));
        }
        assert_eq!(messages, vec![first.clone(), second]);
        assert!(decoder.is_empty());
        assert_eq!(
            decode_frame(&encode_message(&first).expect("frame")).expect("decoded frame"),
            first
        );
    }

    #[test]
    fn rejects_oversized_and_mismatched_frames_before_json_processing() {
        let oversized = (MAX_FRAME_BYTES as u32 + 1).to_be_bytes();
        assert!(matches!(
            decode_frame(&oversized),
            Err(ExtensionProtocolError::FrameTooLarge)
        ));

        let mut mismatched = 4_u32.to_be_bytes().to_vec();
        mismatched.extend_from_slice(b"{}");
        assert!(matches!(
            decode_frame(&mismatched),
            Err(ExtensionProtocolError::FrameLengthMismatch)
        ));

        let mut hello = manifest(ExtensionKind::Wasm);
        hello.protocol_version = PROTOCOL_VERSION + 1;
        let frame = encode_message(&ExtensionMessage::Hello {
            protocol_version: PROTOCOL_VERSION,
            manifest: hello,
        });
        assert!(matches!(
            frame,
            Err(ExtensionProtocolError::ManifestVersionMismatch)
        ));
    }

    #[test]
    fn catalog_loads_only_bounded_regular_json_manifests() {
        let directory = tempfile::tempdir().expect("catalog directory");
        let manifest_path = directory.path().join("coverage.json");
        std::fs::write(
            &manifest_path,
            serde_json::to_vec(&manifest(ExtensionKind::Wasm)).expect("manifest JSON"),
        )
        .expect("write manifest");
        std::fs::write(directory.path().join("README.txt"), "ignored").expect("write note");

        let catalog = ExtensionCatalog::from_directory(Some(directory.path())).expect("catalog");
        assert_eq!(catalog.manifests().len(), 1);
        assert_eq!(catalog.manifests()[0].id, "coverage.reporter");
        assert!(catalog.manifests()[0].allows(ExtensionPermission::ReadBuilds));
        assert!(!catalog.manifests()[0].allows(ExtensionPermission::TriggerBuilds));
    }

    #[test]
    fn catalog_rejects_duplicate_manifest_ids() {
        let directory = tempfile::tempdir().expect("catalog directory");
        let bytes = serde_json::to_vec(&manifest(ExtensionKind::Subprocess)).expect("manifest");
        std::fs::write(directory.path().join("first.json"), &bytes).expect("first manifest");
        std::fs::write(directory.path().join("second.json"), bytes).expect("second manifest");

        assert!(matches!(
            ExtensionCatalog::from_directory(Some(directory.path())),
            Err(ExtensionCatalogError::DuplicateId(id)) if id == "coverage.reporter"
        ));
    }

    #[test]
    fn manager_resolves_only_regular_executables_inside_the_root() {
        let directory = tempfile::tempdir().expect("extension root");
        let mut subprocess = manifest(ExtensionKind::Subprocess);
        subprocess.entrypoint = "runner".into();
        std::fs::write(
            directory.path().join("manifest.json"),
            serde_json::to_vec(&subprocess).expect("manifest JSON"),
        )
        .expect("write manifest");
        let runner = directory.path().join("runner");
        std::fs::write(&runner, b"#!/bin/sh\nexit 0\n").expect("write runner");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mut permissions = std::fs::metadata(&runner)
                .expect("runner metadata")
                .permissions();
            permissions.set_mode(0o755);
            std::fs::set_permissions(&runner, permissions).expect("runner permissions");
        }

        let catalog = ExtensionCatalog::from_directory(Some(directory.path())).expect("catalog");
        let manager = ExtensionManager::new(directory.path(), &catalog).expect("manager");
        let resolved = resolve_entrypoint(
            &manager.root,
            manager.manifest("coverage.reporter").unwrap(),
        )
        .expect("resolved runner");
        assert_eq!(resolved, runner.canonicalize().expect("canonical runner"));
    }

    #[cfg(unix)]
    #[test]
    fn manager_rejects_symlinked_entrypoints() {
        let directory = tempfile::tempdir().expect("extension root");
        let outside = tempfile::tempdir().expect("outside root");
        let mut subprocess = manifest(ExtensionKind::Subprocess);
        subprocess.entrypoint = "runner".into();
        std::fs::write(
            directory.path().join("manifest.json"),
            serde_json::to_vec(&subprocess).expect("manifest JSON"),
        )
        .expect("write manifest");
        let outside_runner = outside.path().join("runner");
        std::fs::write(&outside_runner, b"#!/bin/sh\nexit 0\n").expect("write runner");
        std::os::unix::fs::symlink(&outside_runner, directory.path().join("runner"))
            .expect("symlink runner");

        let catalog = ExtensionCatalog::from_directory(Some(directory.path())).expect("catalog");
        let manager = ExtensionManager::new(directory.path(), &catalog).expect("manager");
        assert!(matches!(
            resolve_entrypoint(
                &manager.root,
                manager.manifest("coverage.reporter").unwrap()
            ),
            Err(ExtensionManagerError::InvalidEntrypoint(_))
        ));
    }

    #[test]
    fn host_permission_boundary_rejects_undeclared_capabilities() {
        let manifest = manifest(ExtensionKind::Subprocess);
        assert!(ensure_permission(&manifest, ExtensionPermission::ReadBuilds).is_ok());
        assert!(matches!(
            ensure_permission(&manifest, ExtensionPermission::TriggerBuilds),
            Err(ExtensionHostError::PermissionDenied(
                ExtensionPermission::TriggerBuilds
            ))
        ));
    }

    #[tokio::test]
    async fn manager_runs_a_capability_free_wasm_extension_with_bounded_abi() {
        let directory = tempfile::tempdir().expect("extension root");
        let mut wasm = manifest(ExtensionKind::Wasm);
        wasm.entrypoint = "coverage.wasm".into();
        std::fs::write(
            directory.path().join("manifest.json"),
            serde_json::to_vec(&wasm).expect("manifest JSON"),
        )
        .expect("write manifest");
        let response = br#"{"abi_version":1,"result":{"ok":true}}"#;
        let response_data = response
            .iter()
            .map(|byte| format!("\\{byte:02x}"))
            .collect::<String>();
        let packed = (u64::try_from(response.len()).expect("response length") << 32) | 1024;
        let module = wat::parse_str(format!(
            r#"(module
                (memory (export "memory") 1 2)
                (data (i32.const 1024) "{response_data}")
                (func (export "rivet_alloc") (param i32) (result i32) i32.const 0)
                (func (export "rivet_handle") (param i32 i32) (result i64) i64.const {packed})
                (func (export "rivet_free") (param i32 i32)))"#
        ))
        .expect("WASM module");
        std::fs::write(directory.path().join("coverage.wasm"), module).expect("write module");
        let catalog = ExtensionCatalog::from_directory(Some(directory.path())).expect("catalog");
        let manager = ExtensionManager::new(directory.path(), &catalog).expect("manager");
        manager
            .launch("coverage.reporter", std::iter::empty::<&str>())
            .await
            .expect("launch WASM extension");
        let result = manager
            .request(
                "coverage.reporter",
                ExtensionPermission::ReadBuilds,
                "summary",
                serde_json::json!({"build": 7}),
            )
            .await
            .expect("WASM response");
        assert_eq!(result, serde_json::json!({"ok": true}));
        assert_eq!(manager.active_extensions().await, ["coverage.reporter"]);
        manager
            .shutdown("coverage.reporter")
            .await
            .expect("shutdown WASM extension");
        assert!(manager.active_extensions().await.is_empty());
    }

    #[tokio::test]
    async fn wasm_runtime_rejects_imported_host_capabilities() {
        let directory = tempfile::tempdir().expect("extension root");
        let mut wasm = manifest(ExtensionKind::Wasm);
        wasm.entrypoint = "imported.wasm".into();
        std::fs::write(
            directory.path().join("manifest.json"),
            serde_json::to_vec(&wasm).expect("manifest JSON"),
        )
        .expect("write manifest");
        let module = wat::parse_str(
            r#"(module
                (import "host" "read_file" (func))
                (memory (export "memory") 1 1))"#,
        )
        .expect("WASM module");
        std::fs::write(directory.path().join("imported.wasm"), module).expect("write module");
        let catalog = ExtensionCatalog::from_directory(Some(directory.path())).expect("catalog");
        let manager = ExtensionManager::new(directory.path(), &catalog).expect("manager");
        assert!(matches!(
            manager
                .launch("coverage.reporter", std::iter::empty::<&str>())
                .await,
            Err(ExtensionManagerError::Host(
                ExtensionHostError::WasmImportsDenied
            ))
        ));
    }

    #[tokio::test]
    async fn wasm_runtime_traps_when_fuel_is_exhausted() {
        let directory = tempfile::tempdir().expect("extension root");
        let mut wasm = manifest(ExtensionKind::Wasm);
        wasm.entrypoint = "fuel.wasm".into();
        std::fs::write(
            directory.path().join("manifest.json"),
            serde_json::to_vec(&wasm).expect("manifest JSON"),
        )
        .expect("write manifest");
        let module = wat::parse_str(
            r#"(module
                (memory (export "memory") 1 1)
                (func (export "rivet_alloc") (param i32) (result i32) i32.const 0)
                (func (export "rivet_handle") (param i32 i32) (result i64)
                    (local i32)
                    i32.const 6000000
                    local.set 2
                    (block
                        (loop
                            local.get 2
                            i32.eqz
                            br_if 1
                            local.get 2
                            i32.const 1
                            i32.sub
                            local.set 2
                            br 0))
                    i64.const 0))"#,
        )
        .expect("WASM module");
        std::fs::write(directory.path().join("fuel.wasm"), module).expect("write module");
        let catalog = ExtensionCatalog::from_directory(Some(directory.path())).expect("catalog");
        let manager = ExtensionManager::new(directory.path(), &catalog).expect("manager");
        manager
            .launch("coverage.reporter", std::iter::empty::<&str>())
            .await
            .expect("launch WASM extension");
        assert!(matches!(
            manager
                .request(
                    "coverage.reporter",
                    ExtensionPermission::ReadBuilds,
                    "summary",
                    serde_json::json!({}),
                )
                .await,
            Err(ExtensionManagerError::Host(ExtensionHostError::Wasm(message)))
                if message.contains("fuel")
        ));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn manager_launches_and_terminates_a_validated_subprocess() {
        let directory = tempfile::tempdir().expect("extension root");
        let mut subprocess = manifest(ExtensionKind::Subprocess);
        subprocess.entrypoint = "runner".into();
        std::fs::write(
            directory.path().join("manifest.json"),
            serde_json::to_vec(&subprocess).expect("manifest JSON"),
        )
        .expect("write manifest");

        let ready = encode_message(&ExtensionMessage::Ready {
            protocol_version: PROTOCOL_VERSION,
            extension_id: subprocess.id.clone(),
        })
        .expect("ready frame");
        let escaped = ready
            .iter()
            .map(|byte| format!("\\{byte:03o}"))
            .collect::<String>();
        let runner = directory.path().join("runner");
        std::fs::write(
            &runner,
            format!("#!/bin/sh\nprintf '{escaped}'\nsleep 60\n"),
        )
        .expect("write runner");
        use std::os::unix::fs::PermissionsExt;
        let mut permissions = std::fs::metadata(&runner)
            .expect("runner metadata")
            .permissions();
        permissions.set_mode(0o755);
        std::fs::set_permissions(&runner, permissions).expect("runner permissions");

        let catalog = ExtensionCatalog::from_directory(Some(directory.path())).expect("catalog");
        let manager = ExtensionManager::new(directory.path(), &catalog).expect("manager");
        manager
            .launch("coverage.reporter", std::iter::empty::<&str>())
            .await
            .expect("launch");
        assert_eq!(manager.active_extensions().await, ["coverage.reporter"]);
        manager
            .terminate("coverage.reporter")
            .await
            .expect("terminate");
        assert!(manager.active_extensions().await.is_empty());
    }

    #[tokio::test]
    async fn subprocess_host_rejects_wasm_manifests_before_spawning() {
        let result = SubprocessExtension::spawn(
            manifest(ExtensionKind::Wasm),
            "unused",
            std::iter::empty::<&str>(),
        )
        .await;
        assert!(matches!(
            result,
            Err(ExtensionHostError::WrongExtensionKind)
        ));
    }
}
