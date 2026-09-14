import { useCallback, useEffect, useMemo, useState } from "react";
import {
  buildDetails,
  builds,
  cancelBuild,
  credentials as fetchCredentials,
  createProject,
  createSchedule,
  deleteCredential,
  deleteSchedule,
  artifactUrl,
  analyzeJenkinsfile,
  agents as fetchAgents,
  artifacts,
  extensions as fetchExtensions,
  extensionStatuses as fetchExtensionStatuses,
  ENGINE_OFFLINE_MESSAGE,
  eventUrl,
  getEngineOrigin,
  initializeEngineOrigin,
  logs,
  pauseQueue,
  pollRepositoryChanges,
  pipelineParameters as fetchPipelineParameters,
  queueItems as fetchQueueItems,
  projects,
  queueStats as fetchQueueStats,
  queueBuild,
  readiness,
  retryBuild,
  resumeQueue,
  startExtension,
  stopExtension,
  setCredential,
  schedules,
  updateSchedule,
} from "./api";
import type {
  AgentSummary,
  ArtifactRecord,
  BuildDetails,
  BuildRecord,
  BuildStatus,
  CredentialSummary,
  LogRecord,
  MigrationResponse,
  PipelineParameter,
  Project,
  QueueItem,
  QueueStats,
  ScheduleRecord,
  StageDetails,
} from "./types";
import {
  EXTENSION_CATALOG,
  EXTENSION_PERMISSION_LABELS,
  type ExtensionPermission,
} from "./extensionModel";
import type { ExtensionManifest, ExtensionRuntimeStatus } from "./extensionModel";

const NAV_ITEMS = [
  { label: "Pipelines", mark: "↳", active: true },
  { label: "Queue", mark: "≋", active: true },
  { label: "Agents", mark: "⊙", active: true },
  { label: "Artifacts", mark: "□", active: false },
  { label: "Credentials", mark: "◈", active: true },
  { label: "Migration", mark: "⇄", active: true },
  { label: "Extensions", mark: "◇", active: true },
];

const STATUS_LABEL: Record<BuildStatus, string> = {
  pending: "Pending",
  queued: "Queued",
  running: "Running",
  passed: "Passed",
  failed: "Failed",
  cancelled: "Cancelled",
};

type Theme = "light" | "dark";

const THEME_STORAGE_KEY = "rivet-theme";

const DEFAULT_JENKINSFILE = `pipeline {
  agent any
  stages {
    stage('Test') {
      steps {
        sh 'cargo test --workspace'
      }
    }
    stage('Deploy') {
      steps {
        input message: 'Approve production deploy?'
      }
    }
  }
}`;

function initialTheme(): Theme {
  if (typeof window === "undefined") return "light";
  try {
    return window.localStorage.getItem(THEME_STORAGE_KEY) === "dark" ? "dark" : "light";
  } catch {
    return "light";
  }
}

function RivetMark({ className = "" }: { className?: string }) {
  return (
    <svg className={`rivet-mark ${className}`} viewBox="0 0 80 80" aria-hidden="true" focusable="false">
      <path className="rivet-mark-shadow" d="M22 8h30l18 18v30L55 73H22L7 58V23z" />
      <path className="rivet-mark-shell" d="M19 5h30l18 18v30L52 70H19L4 55V20z" />
      <path className="rivet-mark-edge" d="M19 5v12M49 5v18h18M67 53l-15 17M19 70V55H4" />
      <circle className="rivet-mark-core" cx="37" cy="39" r="22" />
      <circle className="rivet-mark-fastener" cx="18" cy="19" r="2.5" />
      <circle className="rivet-mark-fastener" cx="57" cy="58" r="2.5" />
      <path className="rivet-mark-r" d="M26 54V24h12.3c9.8 0 15 3.6 15 10 0 4.2-2.1 7.2-6.3 8.8L54 54h-8.5l-6.8-9H33v9zm7-15.2h5c3.3 0 5.1-1.3 5.1-4s-1.8-3.8-5.1-3.8H33z" />
      <path className="rivet-mark-rail" d="M9 45 20 56h9M53 20 65 8" />
      <circle className="rivet-mark-node" cx="68" cy="6" r="6" />
      <circle className="rivet-mark-node-core" cx="68" cy="6" r="2" />
      <path className="rivet-mark-notch" d="m64.5 2.5 7 7" />
    </svg>
  );
}

function statusClass(status: string): string {
  return `status-${status}`;
}

function formatTime(value: string | null): string {
  if (!value) return "—";
  return new Intl.DateTimeFormat(undefined, {
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
  }).format(new Date(value));
}

function duration(build: BuildRecord): string {
  if (!build.started_at) return "—";
  const end = build.finished_at ? new Date(build.finished_at) : new Date();
  const seconds = Math.max(
    0,
    Math.round((end.getTime() - new Date(build.started_at).getTime()) / 1000),
  );
  return seconds < 60 ? `${seconds}s` : `${Math.floor(seconds / 60)}m ${seconds % 60}s`;
}

function shortRevision(revision: string): string {
  return revision.slice(0, 8);
}

function App() {
  const [theme, setTheme] = useState<Theme>(initialTheme);
  const [engineOnline, setEngineOnline] = useState(false);
  const [projectsList, setProjectsList] = useState<Project[]>([]);
  const [projectName, setProjectName] = useState("");
  const [buildList, setBuildList] = useState<BuildRecord[]>([]);
  const [queueStatus, setQueueStatus] = useState<QueueStats | null>(null);
  const [queueItemList, setQueueItemList] = useState<QueueItem[]>([]);
  const [agentList, setAgentList] = useState<AgentSummary[]>([]);
  const [extensionList, setExtensionList] = useState<ExtensionManifest[]>([]);
  const [extensionStatusList, setExtensionStatusList] = useState<ExtensionRuntimeStatus[]>([]);
  const [extensionBusyId, setExtensionBusyId] = useState<string | null>(null);
  const [credentialList, setCredentialList] = useState<CredentialSummary[]>([]);
  const [credentialsReady, setCredentialsReady] = useState<boolean | null>(null);
  const [credentialError, setCredentialError] = useState<string | null>(null);
  const [credentialBusy, setCredentialBusy] = useState(false);
  const [credentialId, setCredentialId] = useState("");
  const [credentialKind, setCredentialKind] = useState<CredentialSummary["kind"]>("http_basic");
  const [credentialUsername, setCredentialUsername] = useState("");
  const [credentialSecret, setCredentialSecret] = useState("");
  const [credentialProjects, setCredentialProjects] = useState("");
  const [selectedBuild, setSelectedBuild] = useState<number | null>(null);
  const [details, setDetails] = useState<BuildDetails | null>(null);
  const [logLines, setLogLines] = useState<LogRecord[]>([]);
  const [artifactList, setArtifactList] = useState<ArtifactRecord[]>([]);
  const [scheduleList, setScheduleList] = useState<ScheduleRecord[]>([]);
  const [parameterDefinitions, setParameterDefinitions] = useState<PipelineParameter[]>([]);
  const [parameterValues, setParameterValues] = useState<Record<string, string>>({});
  const [activeNav, setActiveNav] = useState("Pipelines");
  const [showScmOptions, setShowScmOptions] = useState(false);
  const [scmRemote, setScmRemote] = useState("origin");
  const [scmFetch, setScmFetch] = useState(false);
  const [scmRevision, setScmRevision] = useState("");
  const [scmClean, setScmClean] = useState(false);
  const [scmCleanIgnored, setScmCleanIgnored] = useState(false);
  const [scmCredentialId, setScmCredentialId] = useState("");
  const [pollBusy, setPollBusy] = useState(false);
  const [pollMessage, setPollMessage] = useState<string | null>(null);
  const [showCreate, setShowCreate] = useState(false);
  const [showScheduleCreate, setShowScheduleCreate] = useState(false);
  const [migrationSource, setMigrationSource] = useState(DEFAULT_JENKINSFILE);
  const [migrationResult, setMigrationResult] = useState<MigrationResponse | null>(null);
  const [migrationBusy, setMigrationBusy] = useState(false);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const selectedProject = useMemo(
    () => projectsList.find((project) => project.name === projectName) ?? null,
    [projectName, projectsList],
  );

  const loadProjects = useCallback(async () => {
    try {
      await readiness();
      const result = await projects();
      setProjectsList(result);
      setProjectName((current) => current || result[0]?.name || "");
      setEngineOnline(true);
      setError(null);
    } catch (cause) {
      setEngineOnline(false);
      setError(cause instanceof Error ? cause.message : ENGINE_OFFLINE_MESSAGE);
    }
  }, []);

  const loadBuildList = useCallback(async () => {
    if (!projectName) return;
    try {
      const result = await builds(projectName);
      setBuildList(result);
      setSelectedBuild((current) => current ?? result[0]?.number ?? null);
      setError(null);
    } catch (cause) {
      setError(cause instanceof Error ? cause.message : "Could not load builds");
    }
  }, [projectName]);

  const loadScheduleList = useCallback(async () => {
    if (!projectName) {
      setScheduleList([]);
      return;
    }
    try {
      setScheduleList(await schedules(projectName));
      setError(null);
    } catch (cause) {
      setError(cause instanceof Error ? cause.message : "Could not load schedules");
    }
  }, [projectName]);

  const loadBuildView = useCallback(async () => {
    if (!projectName || selectedBuild === null) {
      setDetails(null);
      setLogLines([]);
      setArtifactList([]);
      return;
    }
    try {
      const [nextDetails, nextLogs, nextArtifacts] = await Promise.all([
        buildDetails(projectName, selectedBuild),
        logs(projectName, selectedBuild),
        artifacts(projectName, selectedBuild),
      ]);
      setDetails(nextDetails);
      setLogLines(nextLogs);
      setArtifactList(nextArtifacts);
      setError(null);
    } catch (cause) {
      setError(cause instanceof Error ? cause.message : "Could not load build");
    }
  }, [projectName, selectedBuild]);

  const loadQueueStatus = useCallback(async () => {
    try {
      setQueueStatus(await fetchQueueStats());
    } catch {
      setQueueStatus(null);
      setEngineOnline(false);
    }
  }, []);

  const loadQueueItemList = useCallback(async () => {
    try {
      setQueueItemList(await fetchQueueItems());
    } catch {
      setQueueItemList([]);
    }
  }, []);

  const loadAgentList = useCallback(async () => {
    try {
      setAgentList(await fetchAgents());
    } catch {
      setAgentList([]);
    }
  }, []);

  const loadExtensionList = useCallback(async () => {
    try {
      const [manifests, statuses] = await Promise.all([
        fetchExtensions(),
        fetchExtensionStatuses(),
      ]);
      setExtensionList(manifests);
      setExtensionStatusList(statuses);
    } catch {
      setExtensionList([]);
      setExtensionStatusList([]);
    }
  }, []);

  const loadCredentialList = useCallback(async (showError = activeNav === "Credentials") => {
    if (!engineOnline) return;
    try {
      setCredentialList(await fetchCredentials());
      setCredentialsReady(true);
      if (showError) setCredentialError(null);
    } catch (cause) {
      setCredentialList([]);
      setCredentialsReady(false);
      if (showError) {
        setCredentialError(cause instanceof Error ? cause.message : "Credential vault unavailable");
      }
    }
  }, [activeNav, engineOnline]);

  useEffect(() => {
    void initializeEngineOrigin();
    void loadProjects();
  }, [loadProjects]);

  useEffect(() => {
    void loadBuildList();
  }, [loadBuildList]);

  useEffect(() => {
    void loadScheduleList();
  }, [loadScheduleList]);

  useEffect(() => {
    if (!engineOnline || !projectName) {
      setParameterDefinitions([]);
      return;
    }
    let active = true;
    void fetchPipelineParameters(projectName)
      .then((definitions) => {
        if (active) setParameterDefinitions(definitions);
      })
      .catch(() => {
        if (active) setParameterDefinitions([]);
      });
    return () => {
      active = false;
    };
  }, [engineOnline, projectName]);

  useEffect(() => {
    setParameterValues((current) => {
      const declared = new Set(parameterDefinitions.map((definition) => definition.name));
      return Object.fromEntries(
        Object.entries(current).filter(([name]) => declared.has(name)),
      );
    });
  }, [parameterDefinitions]);

  useEffect(() => {
    void loadBuildView();
    if (!projectName || selectedBuild === null) return;
    const socket = new WebSocket(eventUrl(projectName, selectedBuild));
    socket.onmessage = () => {
      void Promise.all([loadBuildList(), loadBuildView()]);
    };
    socket.onerror = () => {
      socket.close();
      setEngineOnline(false);
    };
    return () => socket.close();
  }, [loadBuildList, loadBuildView, projectName, selectedBuild]);

  useEffect(() => {
    if (!engineOnline) return;
    void loadQueueStatus();
    const interval = window.setInterval(() => void loadQueueStatus(), 1000);
    return () => window.clearInterval(interval);
  }, [engineOnline, loadQueueStatus]);

  useEffect(() => {
    if (!engineOnline) {
      setAgentList([]);
      return;
    }
    void loadAgentList();
    const interval = window.setInterval(() => void loadAgentList(), 3000);
    return () => window.clearInterval(interval);
  }, [engineOnline, loadAgentList]);

  useEffect(() => {
    if (!engineOnline || activeNav !== "Queue") {
      setQueueItemList([]);
      return;
    }
    void loadQueueItemList();
    const interval = window.setInterval(() => void loadQueueItemList(), 1000);
    return () => window.clearInterval(interval);
  }, [activeNav, engineOnline, loadQueueItemList]);

  useEffect(() => {
    if (!engineOnline || activeNav !== "Extensions") {
      setExtensionList([]);
      setExtensionStatusList([]);
      return;
    }
    void loadExtensionList();
    const interval = window.setInterval(() => void loadExtensionList(), 3000);
    return () => window.clearInterval(interval);
  }, [activeNav, engineOnline, loadExtensionList]);

  useEffect(() => {
    if (!engineOnline || activeNav !== "Credentials") {
      setCredentialList([]);
      setCredentialsReady(null);
      setCredentialError(null);
      return;
    }
    void loadCredentialList();
  }, [activeNav, engineOnline, loadCredentialList]);

  useEffect(() => {
    if (engineOnline) return;
    const retry = window.setTimeout(() => void loadProjects(), 5000);
    return () => window.clearTimeout(retry);
  }, [engineOnline, loadProjects]);

  async function runSelectedPipeline() {
    if (!projectName) return;
    if (!validateParameterValues()) return;
    setBusy(true);
    setError(null);
    try {
      const queued = await queueBuild(projectName, buildRequestOptions());
      setSelectedBuild(queued.build.number);
      clearSecretParameterValues();
      await loadBuildList();
    } catch (cause) {
      setError(cause instanceof Error ? cause.message : "Could not queue build");
    } finally {
      setBusy(false);
    }
  }

  async function checkRepositoryChanges() {
    if (!projectName) return;
    setPollBusy(true);
    setError(null);
    setPollMessage(null);
    try {
      const result = await pollRepositoryChanges(projectName, {
        remote: scmRemote.trim() || "origin",
        fetch: scmFetch,
        credential_id: scmFetch && scmCredentialId.trim() ? scmCredentialId.trim() : undefined,
      });
      const revision = shortRevision(result.revision);
      if (result.build) {
        setSelectedBuild(result.build.number);
        await loadBuildList();
      }
      setPollMessage(
        result.status === "queued"
          ? `Change detected at ${revision} · run #${result.build?.number ?? "—"} queued.`
          : result.status === "unchanged"
            ? `No new commit detected · ${revision}.`
            : result.status === "already_queued"
              ? `This revision is already admitted · ${revision}.`
              : `Another poll is checking ${revision}.`,
      );
    } catch (cause) {
      setError(cause instanceof Error ? cause.message : "Could not poll repository changes");
    } finally {
      setPollBusy(false);
    }
  }

  async function stopSelectedBuild() {
    if (!projectName || selectedBuild === null) return;
    setBusy(true);
    try {
      await cancelBuild(projectName, selectedBuild);
      await loadBuildView();
    } catch (cause) {
      setError(cause instanceof Error ? cause.message : "Could not cancel build");
    } finally {
      setBusy(false);
    }
  }

  async function retrySelectedBuild() {
    if (!projectName || selectedBuild === null) return;
    if (!validateParameterValues()) return;
    setBusy(true);
    setError(null);
    try {
      const queued = await retryBuild(projectName, selectedBuild, buildRequestOptions());
      setSelectedBuild(queued.build.number);
      clearSecretParameterValues();
      await loadBuildList();
      await loadBuildView();
    } catch (cause) {
      setError(cause instanceof Error ? cause.message : "Could not retry build");
    } finally {
      setBusy(false);
    }
  }

  function buildRequestOptions() {
    const revision = scmRevision.trim();
    const credential = scmFetch ? scmCredentialId.trim() : "";
    const options: {
      scm?: {
        remote?: string;
        fetch?: boolean;
        revision?: string;
        clean?: boolean;
        clean_ignored?: boolean;
        credential_id?: string;
      };
      parameters?: Record<string, string>;
    } = {};
    const parameters = Object.fromEntries(
      Object.entries(parameterValues).filter(([name, value]) =>
        parameterDefinitions.some((definition) => definition.name === name) && value.length > 0,
      ),
    );
    if (Object.keys(parameters).length > 0) options.parameters = parameters;
    if (scmFetch || revision || scmClean) {
      options.scm = {
        remote: scmFetch ? scmRemote.trim() || "origin" : undefined,
        fetch: scmFetch,
        revision: revision || undefined,
        clean: scmClean,
        clean_ignored: scmClean && scmCleanIgnored,
        credential_id: credential || undefined,
      };
    }
    return options;
  }

  function validateParameterValues(): boolean {
    const missing = parameterDefinitions
      .filter((definition) => definition.required && !(parameterValues[definition.name] ?? "").trim())
      .map((definition) => definition.name);
    if (missing.length === 0) return true;
    setError(`Required pipeline parameter${missing.length === 1 ? "" : "s"} missing: ${missing.join(", ")}`);
    return false;
  }

  function clearSecretParameterValues() {
    setParameterValues((current) => {
      const next = { ...current };
      for (const definition of parameterDefinitions) {
        if (definition.secret) delete next[definition.name];
      }
      return next;
    });
  }

  function toggleScmOptions() {
    setShowScmOptions((current) => {
      const next = !current;
      if (next && credentialList.length === 0) void loadCredentialList(false);
      return next;
    });
  }

  async function toggleQueue() {
    if (!queueStatus) return;
    setBusy(true);
    setError(null);
    try {
      setQueueStatus(await (queueStatus.paused ? resumeQueue() : pauseQueue()));
    } catch (cause) {
      setError(cause instanceof Error ? cause.message : "Could not change queue state");
    } finally {
      setBusy(false);
    }
  }

  async function submitProject(event: React.FormEvent<HTMLFormElement>) {
    event.preventDefault();
    const form = new FormData(event.currentTarget);
    setBusy(true);
    try {
      const project = await createProject({
        name: String(form.get("name") ?? ""),
        repository_path: String(form.get("repository_path") ?? ""),
        pipeline_path: String(form.get("pipeline_path") ?? "") || undefined,
      });
      setProjectsList((current) => [...current, project]);
      setProjectName(project.name);
      setShowCreate(false);
      setError(null);
    } catch (cause) {
      setError(cause instanceof Error ? cause.message : "Could not create project");
    } finally {
      setBusy(false);
    }
  }

  async function submitSchedule(event: React.FormEvent<HTMLFormElement>) {
    event.preventDefault();
    if (!projectName) return;
    const form = new FormData(event.currentTarget);
    setBusy(true);
    setError(null);
    try {
      await createSchedule(projectName, {
        name: String(form.get("schedule_name") ?? ""),
        expression: String(form.get("schedule_expression") ?? ""),
        trigger: String(form.get("schedule_trigger") ?? "build") as "build" | "repository_poll",
      });
      await loadScheduleList();
      setShowScheduleCreate(false);
    } catch (cause) {
      setError(cause instanceof Error ? cause.message : "Could not create schedule");
    } finally {
      setBusy(false);
    }
  }

  async function toggleSchedule(schedule: ScheduleRecord) {
    if (!projectName) return;
    setBusy(true);
    setError(null);
    try {
      await updateSchedule(projectName, schedule.id, !schedule.enabled);
      await loadScheduleList();
    } catch (cause) {
      setError(cause instanceof Error ? cause.message : "Could not update schedule");
    } finally {
      setBusy(false);
    }
  }

  async function removeSchedule(schedule: ScheduleRecord) {
    if (!projectName) return;
    setBusy(true);
    setError(null);
    try {
      await deleteSchedule(projectName, schedule.id);
      await loadScheduleList();
    } catch (cause) {
      setError(cause instanceof Error ? cause.message : "Could not delete schedule");
    } finally {
      setBusy(false);
    }
  }

  async function runMigrationAnalysis() {
    if (!migrationSource.trim()) {
      setError("Paste a Jenkinsfile before running the migration analysis.");
      return;
    }
    setMigrationBusy(true);
    setError(null);
    try {
      setMigrationResult(await analyzeJenkinsfile(migrationSource, true));
      setEngineOnline(true);
    } catch (cause) {
      setError(cause instanceof Error ? cause.message : "Could not analyze Jenkinsfile");
    } finally {
      setMigrationBusy(false);
    }
  }

  async function toggleExtension(manifest: ExtensionManifest, active: boolean) {
    setExtensionBusyId(manifest.id);
    setError(null);
    try {
      await (active ? stopExtension(manifest.id) : startExtension(manifest.id));
      await loadExtensionList();
    } catch (cause) {
      setError(cause instanceof Error ? cause.message : "Could not change extension state");
    } finally {
      setExtensionBusyId(null);
    }
  }

  function editCredential(credential: CredentialSummary) {
    setCredentialId(credential.id);
    setCredentialKind(credential.kind);
    setCredentialUsername(credential.username);
    setCredentialSecret("");
    setCredentialProjects(credential.projects.join(", "));
    setCredentialError(null);
  }

  function resetCredentialForm() {
    setCredentialId("");
    setCredentialKind("http_basic");
    setCredentialUsername("");
    setCredentialSecret("");
    setCredentialProjects("");
  }

  async function submitCredential(event: React.FormEvent<HTMLFormElement>) {
    event.preventDefault();
    if (!credentialId.trim() || !credentialUsername.trim() || !credentialSecret) return;
    setCredentialBusy(true);
    setCredentialError(null);
    try {
      await setCredential(credentialId.trim(), {
        kind: credentialKind,
        username: credentialUsername.trim(),
        secret: credentialSecret,
        projects: [...new Set(credentialProjects.split(",").map((project) => project.trim()).filter(Boolean))],
      });
      setCredentialSecret("");
      resetCredentialForm();
      await loadCredentialList();
    } catch (cause) {
      setCredentialError(cause instanceof Error ? cause.message : "Could not save credential");
    } finally {
      setCredentialBusy(false);
    }
  }

  async function removeCredential(credential: CredentialSummary) {
    if (!window.confirm(`Remove credential “${credential.id}”?`)) return;
    setCredentialBusy(true);
    setCredentialError(null);
    try {
      await deleteCredential(credential.id);
      if (credentialId === credential.id) resetCredentialForm();
      await loadCredentialList();
    } catch (cause) {
      setCredentialError(cause instanceof Error ? cause.message : "Could not remove credential");
    } finally {
      setCredentialBusy(false);
    }
  }

  const latest = buildList[0] ?? null;
  const passed = buildList.filter((build) => build.status === "passed").length;
  const successRate = buildList.length ? Math.round((passed / buildList.length) * 100) : 0;
  const isRunning = details?.build.status === "running" || details?.build.status === "queued";

  useEffect(() => {
    document.documentElement.dataset.theme = theme;
    document.documentElement.style.colorScheme = theme;
    try {
      window.localStorage.setItem(THEME_STORAGE_KEY, theme);
    } catch {
      // The theme still applies when storage is unavailable.
    }
  }, [theme]);

  return (
    <div className="app-shell">
      <aside className="sidebar">
        <div className="brand-lockup">
          <div className="brand-glyph"><RivetMark /></div>
          <div>
            <div className="brand-name">Rivet</div>
            <div className="brand-caption">control room / 01</div>
          </div>
        </div>

        <div className="workspace-switcher">
          <span className="overline">Workspace</span>
          <span className="workspace-name">Local engine</span>
          <span className={`connection-dot ${engineOnline ? "online" : "offline"}`} />
        </div>

        <nav className="primary-nav" aria-label="Primary navigation">
          <span className="overline nav-overline">Operate</span>
          {NAV_ITEMS.map((item) => (
            <button
              className={`nav-item ${activeNav === item.label ? "selected" : ""}`}
              key={item.label}
              disabled={!item.active}
              onClick={() => item.active && setActiveNav(item.label)}
            >
              <span className="nav-mark">{item.mark}</span>
              <span>{item.label}</span>
              {!item.active && <span className="planned-tag">soon</span>}
            </button>
          ))}
        </nav>

        <div className="sidebar-foot">
          <div className="reference-card">
            <span className="reference-line" />
            <div>
              <span className="overline">Engine</span>
              <strong>Rust core</strong>
            <small>{getEngineOrigin().replace("http://", "")}</small>
            </div>
          </div>
          <span className="build-stamp">RIVET 0.1.0 · LOCAL</span>
        </div>
      </aside>

      <main className="main-canvas">
        <header className="topbar">
          <div className="breadcrumb">
            <span>Operate</span><span className="crumb-separator">/</span><strong>{activeNav}</strong>
          </div>
          <div className="topbar-actions">
            <div className={`engine-pill ${engineOnline ? "online" : "offline"}`}>
              <span className="pulse" />
              {engineOnline ? "Engine online" : "Engine offline"}
            </div>
            <button
              className="button button-quiet theme-toggle"
              type="button"
              aria-pressed={theme === "dark"}
              aria-label={theme === "light" ? "Switch to dark mode" : "Switch to light mode"}
              title={theme === "light" ? "Switch to dark mode" : "Switch to light mode"}
              onClick={() => setTheme((current) => current === "light" ? "dark" : "light")}
            >
              <span className="theme-toggle-icon" aria-hidden="true">{theme === "light" ? "☾" : "☀"}</span>
              <span className="theme-toggle-label">{theme === "light" ? "Dark" : "Light"}</span>
            </button>
            <button className="button button-quiet" onClick={() => setShowCreate(true)}>
              <span>＋</span> New project
            </button>
          </div>
        </header>

        {error && (
          <div className="error-banner" role="alert">
            <span className="error-symbol">!</span>
            <span>{error}</span>
            <button onClick={() => setError(null)} aria-label="Dismiss error">×</button>
          </div>
        )}

        {activeNav === "Migration" ? (
          <MigrationPanel
            source={migrationSource}
            result={migrationResult}
            busy={migrationBusy}
            online={engineOnline}
            onChange={setMigrationSource}
            onAnalyze={() => void runMigrationAnalysis()}
          />
        ) : activeNav === "Extensions" ? (
          <ExtensionsPanel
            extensions={extensionList}
            statuses={extensionStatusList}
            online={engineOnline}
            busyId={extensionBusyId}
            onToggle={toggleExtension}
          />
        ) : activeNav === "Agents" ? (
          <AgentsPanel agents={agentList} online={engineOnline} />
        ) : activeNav === "Queue" ? (
          <QueuePanel
            items={queueItemList}
            stats={queueStatus}
            online={engineOnline}
            busy={busy}
            onToggle={toggleQueue}
          />
        ) : activeNav === "Credentials" ? (
          <CredentialsPanel
            credentials={credentialList}
            ready={credentialsReady}
            online={engineOnline}
            error={credentialError}
            busy={credentialBusy}
            id={credentialId}
            kind={credentialKind}
            username={credentialUsername}
            secret={credentialSecret}
            projects={credentialProjects}
            onIdChange={setCredentialId}
            onKindChange={setCredentialKind}
            onUsernameChange={setCredentialUsername}
            onSecretChange={setCredentialSecret}
            onProjectsChange={setCredentialProjects}
            onSubmit={submitCredential}
            onEdit={editCredential}
            onRemove={removeCredential}
            onReset={resetCredentialForm}
          />
        ) : projectsList.length === 0 ? (
          <Onboarding onCreate={() => setShowCreate(true)} online={engineOnline} />
        ) : (
          <div className="content-wrap">
            <section className="page-heading">
              <div>
                <span className="eyebrow"><span className="eyebrow-line" />Pipeline operations</span>
                <h1>Ship with a clear signal.</h1>
                <p>Observe every stage, keep the history, know what moved.</p>
              </div>
              <div className="project-picker-wrap">
                <label htmlFor="project-picker" className="overline">Project</label>
                <select
                  id="project-picker"
                  value={projectName}
                  onChange={(event) => {
                    setProjectName(event.target.value);
                    setSelectedBuild(null);
                    setParameterDefinitions([]);
                    setParameterValues({});
                    setPollMessage(null);
                  }}
                >
                  {projectsList.map((project) => <option key={project.id} value={project.name}>{project.name}</option>)}
                </select>
              </div>
            </section>

            <section className="metric-grid" aria-label="Pipeline metrics">
              <MetricCard label="Latest run" value={latest ? `#${latest.number}` : "—"} detail={latest ? STATUS_LABEL[latest.status] : "No runs yet"} accent={latest?.status ?? "neutral"} />
              <MetricCard label="Success rate" value={`${successRate}%`} detail={`${buildList.length} recorded runs`} accent="cyan" />
              <MetricCard label="Queue" value={(queueStatus?.queued ?? buildList.filter((build) => build.status === "queued").length).toString().padStart(2, "0")} detail={queueStatus ? `${queueStatus.paused ? "paused · " : ""}${queueStatus.running} running / ${queueStatus.capacity} slots` : "local capacity / 01"} accent="amber" />
              <MetricCard label="Last signal" value={latest ? formatTime(latest.finished_at ?? latest.started_at) : "—"} detail={latest ? duration(latest) : "Waiting for first run"} accent="neutral" />
            </section>

            <SourcePreparationPanel
              open={showScmOptions}
              remote={scmRemote}
              fetchEnabled={scmFetch}
              revision={scmRevision}
              clean={scmClean}
              cleanIgnored={scmCleanIgnored}
              credentialId={scmCredentialId}
              credentials={credentialList}
              onToggle={toggleScmOptions}
              onRemoteChange={setScmRemote}
              onFetchChange={setScmFetch}
              onRevisionChange={setScmRevision}
              onCleanChange={setScmClean}
              onCleanIgnoredChange={setScmCleanIgnored}
              onCredentialChange={setScmCredentialId}
            />

            {pollMessage && <p className="poll-notice" role="status"><span>⌁</span>{pollMessage}</p>}

            {parameterDefinitions.length > 0 && (
              <ParameterPanel
                definitions={parameterDefinitions}
                values={parameterValues}
                onChange={(name, value) => setParameterValues((current) => ({ ...current, [name]: value }))}
              />
            )}

            <SchedulePanel
              schedules={scheduleList}
              busy={busy || !engineOnline}
              showCreate={showScheduleCreate}
              onToggleCreate={() => setShowScheduleCreate((current) => !current)}
              onSubmit={submitSchedule}
              onToggle={toggleSchedule}
              onDelete={removeSchedule}
            />

            <section className="section-head">
              <div><span className="overline">Execution map</span><h2>{selectedProject?.name ?? "Pipeline"}</h2></div>
              <div className="section-actions">
                {queueStatus && <button className="button button-quiet" disabled={busy || !engineOnline} onClick={() => void toggleQueue()}>{queueStatus.paused ? "Resume queue" : "Pause queue"}</button>}
                {isRunning && <button className="button button-danger" disabled={busy} onClick={() => void stopSelectedBuild()}>Stop run</button>}
                {details && details.build.status !== "running" && details.build.status !== "queued" && <button className="button button-quiet" disabled={busy || !engineOnline} onClick={() => void retrySelectedBuild()}>↻ Retry run</button>}
                <button className="button button-quiet" disabled={busy || pollBusy || !engineOnline} onClick={() => void checkRepositoryChanges()}>{pollBusy ? "Checking…" : "Check changes"}</button>
                <button className="button button-primary" disabled={busy || !engineOnline} onClick={() => void runSelectedPipeline()}>
                  <span className="run-icon">▶</span> {busy ? "Working…" : "Run pipeline"}
                </button>
              </div>
            </section>

            <section className="execution-card">
              <div className="execution-card-top">
                <div className="run-identity">
                  <span className="run-index">{details ? `RUN ${String(details.build.number).padStart(3, "0")}` : "NO RUN SELECTED"}</span>
                  <span className={`status-chip ${details ? statusClass(details.build.status) : "status-pending"}`}><i />{details ? STATUS_LABEL[details.build.status] : "Awaiting signal"}</span>
                </div>
                <div className="execution-context">
                  <span className="execution-time">{details ? `started ${formatTime(details.build.started_at)}` : "The map will populate after the first run"}</span>
                  {details?.build.source && <span className={`source-badge ${details.build.source.dirty ? "source-dirty" : ""}`} title={details.build.source.revision}><span>⎇</span>{shortRevision(details.build.source.revision)} · {details.build.source.reference ?? "detached"} · {details.build.source.dirty ? "dirty" : "clean"}</span>}
                </div>
              </div>
              <StageRail stages={details?.stages ?? []} />
            </section>

            <div className="lower-grid">
              <section className="panel runs-panel">
                <div className="panel-heading"><div><span className="overline">History</span><h2>Recent runs</h2></div><span className="panel-count">{buildList.length.toString().padStart(2, "0")}</span></div>
                <div className="run-list">
                  {buildList.length === 0 ? <EmptyState text="No builds have been queued." /> : buildList.slice(0, 8).map((build) => (
                    <button className={`run-row ${selectedBuild === build.number ? "current" : ""}`} key={build.id} onClick={() => setSelectedBuild(build.number)}>
                      <span className={`run-status ${statusClass(build.status)}`}><i /></span>
                      <span className="run-number">#{String(build.number).padStart(3, "0")}</span>
                      <span className="run-status-label">{STATUS_LABEL[build.status]}</span>
                      <span className="run-duration">{duration(build)}</span>
                      <span className="run-time">{formatTime(build.finished_at ?? build.queued_at)}</span>
                      <span className="row-arrow">↗</span>
                    </button>
                  ))}
                </div>
              </section>

              <LogPanel logs={logLines} build={details?.build ?? null} />
            </div>
            <ArtifactPanel artifacts={artifactList} build={details?.build ?? null} projectName={projectName} />
          </div>
        )}
      </main>

      {showCreate && <CreateProjectModal busy={busy} onClose={() => setShowCreate(false)} onSubmit={submitProject} />}
    </div>
  );
}

function MetricCard({ label, value, detail, accent }: { label: string; value: string; detail: string; accent: string }) {
  return <div className={`metric-card metric-${accent}`}><span className="overline">{label}</span><strong>{value}</strong><span className="metric-detail"><i />{detail}</span></div>;
}

function SourcePreparationPanel({
  open,
  remote,
  fetchEnabled,
  revision,
  clean,
  cleanIgnored,
  credentialId,
  credentials,
  onToggle,
  onRemoteChange,
  onFetchChange,
  onRevisionChange,
  onCleanChange,
  onCleanIgnoredChange,
  onCredentialChange,
}: {
  open: boolean;
  remote: string;
  fetchEnabled: boolean;
  revision: string;
  clean: boolean;
  cleanIgnored: boolean;
  credentialId: string;
  credentials: CredentialSummary[];
  onToggle: () => void;
  onRemoteChange: (value: string) => void;
  onFetchChange: (value: boolean) => void;
  onRevisionChange: (value: string) => void;
  onCleanChange: (value: boolean) => void;
  onCleanIgnoredChange: (value: boolean) => void;
  onCredentialChange: (value: string) => void;
}) {
  return (
    <section className="panel scm-options-panel" aria-label="Source preparation options">
      <div className="panel-heading">
        <div><span className="overline">Source admission</span><h2>Prepare SCM before run</h2></div>
        <div className="scm-heading-actions">
          <span className="scm-mode">{open ? "OPTIONAL" : "INSPECT ONLY"}</span>
          <button className="button button-quiet" type="button" onClick={onToggle}>{open ? "Close" : "Configure"}</button>
        </div>
      </div>
      {open ? (
        <div className="scm-options-form">
          <label className="scm-toggle"><input type="checkbox" checked={fetchEnabled} onChange={(event) => onFetchChange(event.target.checked)} /><span><strong>Fetch from remote</strong><small>Fetch is explicit and runs before revision checkout.</small></span></label>
          <label>Remote<input value={remote} onChange={(event) => onRemoteChange(event.target.value)} disabled={!fetchEnabled} placeholder="origin" /></label>
          <label>Revision <span className="optional">(optional)</span><input value={revision} onChange={(event) => onRevisionChange(event.target.value)} placeholder="main or commit SHA" /></label>
          <label>Vault credential ID <span className="optional">(fetch only)</span><input list="rivet-credential-ids" value={credentialId} onChange={(event) => onCredentialChange(event.target.value)} disabled={!fetchEnabled} placeholder="github-ci" autoComplete="off" /><datalist id="rivet-credential-ids">{credentials.map((credential) => <option key={credential.id} value={credential.id}>{credential.username}</option>)}</datalist><small>{credentials.length ? `${credentials.length} known vault ID${credentials.length === 1 ? "" : "s"} available` : "Type an ID configured in the encrypted server vault."}</small></label>
          <div className="scm-checks">
            <label className="scm-check"><input type="checkbox" checked={clean} onChange={(event) => onCleanChange(event.target.checked)} /><span>Remove untracked files</span></label>
            <label className="scm-check"><input type="checkbox" checked={cleanIgnored} onChange={(event) => onCleanIgnoredChange(event.target.checked)} disabled={!clean} /><span>Include ignored files</span></label>
          </div>
          <p className="scm-notice"><span>!</span>Credential values never enter this request. Only the non-secret ID is sent to the engine.</p>
        </div>
      ) : (
        <div className="scm-collapsed"><span className="scm-collapsed-mark">⎇</span><p>Rivet inspects the current checkout by default. Open this panel to fetch a revision, clean untracked files, or attach a vault credential ID.</p><span className="scm-collapsed-status">NO MUTATION</span></div>
      )}
    </section>
  );
}

function ParameterPanel({
  definitions,
  values,
  onChange,
}: {
  definitions: PipelineParameter[];
  values: Record<string, string>;
  onChange: (name: string, value: string) => void;
}) {
  return (
    <section className="panel parameter-panel" aria-label="Pipeline parameters">
      <div className="panel-heading">
        <div>
          <span className="overline">Runtime inputs</span>
          <h2>Pipeline parameters</h2>
        </div>
        <span className="panel-count">{String(definitions.length).padStart(2, "0")}</span>
      </div>
      <div className="parameter-form">
        {definitions.map((definition) => (
          <label className="parameter-field" key={definition.name}>
            <span className="parameter-label">
              <span>{definition.name}</span>
              {definition.required ? <em>required</em> : <em>optional</em>}
            </span>
            <input
              type={definition.secret ? "password" : "text"}
              value={values[definition.name] ?? ""}
              onChange={(event) => onChange(definition.name, event.target.value)}
              placeholder={definition.secret ? "enter secret at run time" : definition.default ?? "value"}
              autoComplete={definition.secret ? "new-password" : "off"}
              spellCheck={false}
              aria-required={definition.required}
            />
            <small>
              {definition.secret
                ? "Secret · required at run time; never returned or persisted in clear text."
                : definition.default
                  ? `Default: ${definition.default}`
                  : "No default · enter a value before running."}
            </small>
          </label>
        ))}
      </div>
      <p className="parameter-notice"><span>⌁</span>Values are sent only with the run request. Secret fields are cleared after a successful queue or retry.</p>
    </section>
  );
}

function QueuePanel({
  items,
  stats,
  online,
  busy,
  onToggle,
}: {
  items: QueueItem[];
  stats: QueueStats | null;
  online: boolean;
  busy: boolean;
  onToggle: () => void;
}) {
  return (
    <div className="content-wrap queue-page">
      <section className="page-heading queue-heading">
        <div>
          <span className="eyebrow"><span className="eyebrow-line" />Admission control</span>
          <h1>Make the waiting visible.</h1>
          <p>Priority decides first; sequence keeps equal-priority work fair. Pausing admission never discards queued builds.</p>
        </div>
        <div className={`queue-status ${stats?.paused ? "paused" : ""}`}>
          <span className="overline">Queue state</span>
          <strong>{!online ? "ENGINE OFFLINE" : stats?.paused ? "PAUSED" : "ADMITTING"}</strong>
          <small>{stats ? `${stats.running} running · ${stats.capacity} slots` : "waiting for telemetry"}</small>
        </div>
      </section>

      <section className="metric-grid queue-metrics" aria-label="Queue metrics">
        <MetricCard label="Waiting" value={String(stats?.queued ?? items.length).padStart(2, "0")} detail="pending admission" accent="amber" />
        <MetricCard label="Running" value={String(stats?.running ?? 0).padStart(2, "0")} detail="active executors" accent="cyan" />
        <MetricCard label="Capacity" value={String(stats?.capacity ?? 0).padStart(2, "0")} detail="global slots" accent="neutral" />
        <MetricCard label="Policy" value="PRIORITY" detail="FIFO tie-break" accent="neutral" />
      </section>

      <section className="panel queue-panel" aria-label="Queued builds">
        <div className="panel-heading">
          <div><span className="overline">Admission order</span><h2>Waiting builds</h2></div>
          <div className="queue-heading-actions"><span className="panel-count">{String(items.length).padStart(2, "0")}</span><button className="button button-quiet" type="button" disabled={busy || !online || !stats} onClick={onToggle}>{stats?.paused ? "Resume queue" : "Pause queue"}</button></div>
        </div>
        {items.length === 0 ? (
          <div className="queue-empty"><span className="queue-empty-mark">≋</span><div><strong>No builds are waiting.</strong><p>New manual, scheduled, or webhook builds will appear here before an executor admits them.</p></div></div>
        ) : (
          <div className="queue-list">
            <div className="queue-list-head"><span>Position</span><span>Project</span><span>Build</span><span>Priority</span></div>
            {items.map((item) => (
              <div className="queue-row" key={item.build_id}>
                <span className="queue-position">{String(item.position).padStart(2, "0")}</span>
                <strong>{item.project}</strong>
                <code title={item.build_id}>{item.build_id.slice(0, 8)}</code>
                <span className={`queue-priority ${item.priority > 0 ? "high" : item.priority < 0 ? "low" : "normal"}`}>{item.priority > 0 ? "+" : ""}{item.priority}</span>
              </div>
            ))}
          </div>
        )}
      </section>
      <p className="queue-boundary"><span />The queue snapshot contains identifiers, project names, and scheduling metadata only; build parameters and secrets stay in their existing scoped records.</p>
    </div>
  );
}

function SchedulePanel({
  schedules: scheduleRecords,
  busy,
  showCreate,
  onToggleCreate,
  onSubmit,
  onToggle,
  onDelete,
}: {
  schedules: ScheduleRecord[];
  busy: boolean;
  showCreate: boolean;
  onToggleCreate: () => void;
  onSubmit: (event: React.FormEvent<HTMLFormElement>) => void;
  onToggle: (schedule: ScheduleRecord) => void;
  onDelete: (schedule: ScheduleRecord) => void;
}) {
  return (
    <section className="panel schedules-panel" aria-label="Build schedules">
      <div className="panel-heading">
        <div><span className="overline">Build triggers</span><h2>Schedules</h2></div>
        <div className="schedule-heading-actions">
          <span className="panel-count">{scheduleRecords.length.toString().padStart(2, "0")}</span>
          <button className="button button-quiet" type="button" disabled={busy} onClick={onToggleCreate}>{showCreate ? "Close" : "＋ Add schedule"}</button>
        </div>
      </div>
      {showCreate && (
        <form className="schedule-form" onSubmit={onSubmit}>
          <label>Schedule name<input name="schedule_name" required placeholder="nightly" /></label>
          <label>Trigger<select name="schedule_trigger" defaultValue="build"><option value="build">Run pipeline</option><option value="repository_poll">Poll repository changes</option></select><small>Polling queues only a new Git revision.</small></label>
          <label>Cron expression<input name="schedule_expression" required placeholder="0 2 * * *" /><small>UTC · 5 fields: minute hour day month weekday</small></label>
          <button className="button button-primary" type="submit" disabled={busy}>{busy ? "Saving…" : "Save schedule"}</button>
        </form>
      )}
      {scheduleRecords.length === 0 ? (
        <EmptyState text={showCreate ? "Add a UTC cron expression to automate this pipeline." : "No schedules configured for this project."} />
      ) : (
        <div className="schedule-list">
          {scheduleRecords.map((schedule) => (
            <div className={`schedule-row ${schedule.enabled ? "" : "schedule-disabled"}`} key={schedule.id}>
              <span className={`schedule-indicator ${schedule.enabled ? "enabled" : "disabled"}`} aria-hidden="true" />
              <div className="schedule-copy"><strong>{schedule.name}</strong><small><code>{schedule.expression}</code> · {schedule.trigger === "repository_poll" ? "poll changes" : "run pipeline"} · next {formatTime(schedule.next_run_at)}</small></div>
              <span className="schedule-status">{schedule.enabled ? "active" : "paused"}</span>
              <button className="button button-quiet schedule-action" type="button" disabled={busy} onClick={() => onToggle(schedule)}>{schedule.enabled ? "Pause" : "Resume"}</button>
              <button className="schedule-delete" type="button" disabled={busy} aria-label={`Delete schedule ${schedule.name}`} onClick={() => onDelete(schedule)}>×</button>
            </div>
          ))}
        </div>
      )}
    </section>
  );
}

function StageRail({ stages }: { stages: StageDetails[] }) {
  if (!stages.length) return <div className="stage-empty"><span className="stage-empty-mark">⌁</span><div><strong>Execution map is standing by</strong><span>Queue a pipeline to see stage-level movement and live output.</span></div></div>;
  return <div className="stage-rail">{stages.map((item, index) => <div className="stage-node-wrap" key={item.stage.id}>
    <div className={`stage-node ${statusClass(item.stage.status)}`}><div className="stage-node-head"><span className="stage-number">0{index + 1}</span><span className="stage-state"><i />{item.stage.status}</span></div><strong>{item.stage.name}</strong><div className="stage-steps">{item.steps.map((step) => <span key={step.id} className={statusClass(step.status)}><i />{step.name}</span>)}</div></div>
    {index < stages.length - 1 && <span className="stage-connector" aria-hidden="true"><i /></span>}
  </div>)}</div>;
}

function LogPanel({ logs, build }: { logs: LogRecord[]; build: BuildRecord | null }) {
  return <section className="panel log-panel"><div className="panel-heading"><div><span className="overline">Live output</span><h2>Signal stream</h2></div><div className="log-meta"><span className="live-dot" />{build?.status === "running" ? "listening" : `${logs.length} lines`}</div></div><div className="log-window">{logs.length === 0 ? <EmptyState text="Output will appear here, line by line." /> : logs.slice(-160).map((log) => <div className="log-line" key={log.sequence}><span className="log-sequence">{String(log.sequence).padStart(4, "0")}</span><span className={`log-stream ${log.stream}`}>{log.stream === "stderr" ? "ERR" : log.stream === "system" ? "SYS" : "OUT"}</span><span className="log-text">{log.line || " "}</span></div>)}</div></section>;
}

function ArtifactPanel({
  artifacts: artifactRecords,
  build,
  projectName,
}: {
  artifacts: ArtifactRecord[];
  build: BuildRecord | null;
  projectName: string;
}) {
  return (
    <section className="panel artifacts-panel">
      <div className="panel-heading">
        <div><span className="overline">Build outputs</span><h2>Artifacts</h2></div>
        <span className="panel-count">{artifactRecords.length.toString().padStart(2, "0")}</span>
      </div>
      {artifactRecords.length === 0 ? (
        <EmptyState text={build ? "No artifacts were collected for this run." : "Artifacts will appear after a completed run."} />
      ) : (
        <div className="artifact-list">
          {artifactRecords.map((artifact) => (
            <a
              className="artifact-row"
              href={artifactUrl(projectName, build?.number ?? 0, artifact.id)}
              download
              key={artifact.id}
            >
              <span className="artifact-mark">□</span>
              <span className="artifact-copy"><strong>{artifact.relative_path}</strong><small>{artifact.name} · {formatBytes(artifact.size_bytes)}</small></span>
              <span className="artifact-checksum">{artifact.checksum.slice(-12)}</span>
              <span className="artifact-arrow">↗</span>
            </a>
          ))}
        </div>
      )}
    </section>
  );
}

function formatBytes(value: number): string {
  if (value < 1024) return `${value} B`;
  if (value < 1024 * 1024) return `${(value / 1024).toFixed(1)} KB`;
  return `${(value / (1024 * 1024)).toFixed(1)} MB`;
}

function EmptyState({ text }: { text: string }) { return <div className="empty-state"><span>∅</span>{text}</div>; }

function Onboarding({ onCreate, online }: { onCreate: () => void; online: boolean }) {
  return <div className="onboarding"><div className="onboarding-mark"><RivetMark /></div><span className="eyebrow"><span className="eyebrow-line" />First connection</span><h1>Give the engine<br /><em>a pipeline to watch.</em></h1><p>Rivet keeps the execution signal close: a local repository, an explicit `Rivetfile.toml`, and a durable history you can trust.</p><button className="button button-primary onboarding-button" disabled={!online} onClick={onCreate}>＋ Connect a project <span>→</span></button><div className="onboarding-note"><span className="note-rule" />{online ? "The local engine is ready for a repository." : "Start `rivet server` to connect the local engine."}</div></div>;
}

function CredentialsPanel({
  credentials,
  ready,
  online,
  error,
  busy,
  id,
  kind,
  username,
  secret,
  projects,
  onIdChange,
  onKindChange,
  onUsernameChange,
  onSecretChange,
  onProjectsChange,
  onSubmit,
  onEdit,
  onRemove,
  onReset,
}: {
  credentials: CredentialSummary[];
  ready: boolean | null;
  online: boolean;
  error: string | null;
  busy: boolean;
  id: string;
  kind: CredentialSummary["kind"];
  username: string;
  secret: string;
  projects: string;
  onIdChange: (value: string) => void;
  onKindChange: (value: CredentialSummary["kind"]) => void;
  onUsernameChange: (value: string) => void;
  onSecretChange: (value: string) => void;
  onProjectsChange: (value: string) => void;
  onSubmit: (event: React.FormEvent<HTMLFormElement>) => void;
  onEdit: (credential: CredentialSummary) => void;
  onRemove: (credential: CredentialSummary) => void;
  onReset: () => void;
}) {
  const vaultUnavailable = ready === false;
  return (
    <div className="content-wrap credentials-page">
      <section className="page-heading credentials-heading">
        <div>
          <span className="eyebrow"><span className="eyebrow-line" />Secret boundary</span>
          <h1>Keep access out of the build.</h1>
          <p>Store provider credentials once, then reference them by an opaque ID from a build or webhook.</p>
        </div>
        <div className={`credentials-status ${vaultUnavailable ? "unavailable" : ""}`}>
          <span className="overline">Vault status</span>
          <strong>{!online ? "ENGINE OFFLINE" : vaultUnavailable ? "VAULT UNAVAILABLE" : ready ? "ENCRYPTED VAULT" : "CONNECTING…"}</strong>
          <small>{!online ? "start the local engine" : vaultUnavailable ? "configure a vault on the server" : "secrets never returned by API"}</small>
        </div>
      </section>

      {error && (
        <div className="credential-alert" role="status">
          <span className="error-symbol">!</span>
          <span>{error}</span>
        </div>
      )}

      {vaultUnavailable || !online ? (
        <section className="panel credentials-empty">
          <span className="credentials-empty-mark">◈</span>
          <div>
            <span className="overline">Management is gated</span>
            <strong>{!online ? "Connect to the local engine first." : "Configure the encrypted vault first."}</strong>
            <p>{!online ? "Credential management becomes available when the Rust engine is reachable." : "Start the server with a vault file and private passphrase. Rivet keeps only encrypted ciphertext on disk."}</p>
          </div>
        </section>
      ) : (
        <>
          <section className="metric-grid credentials-metrics" aria-label="Credential vault metrics">
            <MetricCard label="Stored entries" value={String(credentials.length).padStart(2, "0")} detail="IDs only in this view" accent="cyan" />
            <MetricCard label="Secret exposure" value="ZERO" detail="not returned by API" accent="amber" />
            <MetricCard label="Access model" value="ADMIN" detail="management permission" accent="neutral" />
            <MetricCard label="Persistence" value="AES-GCM" detail="passphrase-encrypted vault" accent="neutral" />
          </section>

          <div className="credentials-grid">
            <section className="panel credential-form-panel">
              <div className="panel-heading">
                <div><span className="overline">Write access</span><h2>{id ? "Rotate credential" : "Add credential"}</h2></div>
                <span className="credential-lock" aria-hidden="true">⌁</span>
              </div>
              <form className="credential-form" onSubmit={onSubmit}>
                <label htmlFor="credential-id">Credential ID<input id="credential-id" value={id} onChange={(event) => onIdChange(event.target.value)} placeholder="github-ci" autoComplete="off" required /></label>
                <label htmlFor="credential-kind">Credential type<select id="credential-kind" value={kind} onChange={(event) => onKindChange(event.target.value as CredentialSummary["kind"])}><option value="http_basic">HTTP basic / token</option><option value="ssh_key">SSH private key</option></select><small>SSH keys are written only to a private temporary file while Git runs.</small></label>
                <label htmlFor="credential-username">Username<input id="credential-username" value={username} onChange={(event) => onUsernameChange(event.target.value)} placeholder={kind === "ssh_key" ? "git" : "automation-user"} autoComplete="username" required /></label>
                <label htmlFor="credential-secret">{kind === "ssh_key" ? "Private key" : "Secret"}<input id="credential-secret" type="password" value={secret} onChange={(event) => onSecretChange(event.target.value)} placeholder={kind === "ssh_key" ? "paste an SSH private key" : id ? "enter a new secret" : "paste a provider token"} autoComplete="new-password" required /><small>Required for every save; the field is cleared after success.</small></label>
                <label htmlFor="credential-projects">Allowed projects <span className="optional">(empty = all)</span><input id="credential-projects" value={projects} onChange={(event) => onProjectsChange(event.target.value)} placeholder="web-app, release" autoComplete="off" /><small>Comma-separated project names. Scope credentials to the repositories that need them.</small></label>
                <div className="credential-form-actions">
                  {id && <button className="button button-quiet" type="button" disabled={busy} onClick={onReset}>Clear</button>}
                  <button className="button button-primary" type="submit" disabled={busy || !id.trim() || !username.trim() || !secret}>{busy ? "Saving…" : id ? "Rotate securely" : "Store credential"}</button>
                </div>
              </form>
            </section>

            <section className="panel credential-list-panel">
              <div className="panel-heading">
                <div><span className="overline">Inventory</span><h2>Known credentials</h2></div>
                <span className="panel-count">{String(credentials.length).padStart(2, "0")}</span>
              </div>
              {credentials.length === 0 ? (
                <div className="credentials-list-empty"><span>∅</span><p>No provider credential is stored yet.</p></div>
              ) : (
                <div className="credential-list">
                  {credentials.map((credential) => (
                    <div className="credential-row" key={credential.id}>
                      <span className="credential-row-mark">◈</span>
                      <div className="credential-copy"><strong>{credential.id}</strong><small>{credential.kind === "ssh_key" ? "SSH private key" : "HTTP basic / token"} · {credential.username} · {credential.projects.length ? `scoped to ${credential.projects.join(", ")}` : "all projects"} · secret sealed</small></div>
                      <button className="button button-quiet credential-action" type="button" disabled={busy} onClick={() => onEdit(credential)}>Rotate</button>
                      <button className="credential-delete" type="button" disabled={busy} aria-label={`Remove credential ${credential.id}`} onClick={() => void onRemove(credential)}>×</button>
                    </div>
                  ))}
                </div>
              )}
            </section>
          </div>

          <p className="credential-boundary"><span />Only credential summaries are rendered here. Secret values are accepted by the admin API, encrypted by the vault, redacted from logs, and never included in responses.</p>
        </>
      )}
    </div>
  );
}

function AgentsPanel({ agents, online }: { agents: AgentSummary[]; online: boolean }) {
  const onlineAgents = agents.filter((agent) => agent.status === "online");
  const staleAgents = agents.filter((agent) => agent.status === "stale");
  const running = agents.reduce((total, agent) => total + agent.running.length, 0);
  const capacity = agents.reduce((total, agent) => total + agent.capabilities.executors, 0);
  const available = agents.reduce((total, agent) => total + agent.available_executors, 0);
  return (
    <div className="content-wrap agents-page">
      <section className="page-heading agents-heading">
        <div>
          <span className="eyebrow"><span className="eyebrow-line" />Remote fleet</span>
          <h1>Machines with a pulse.</h1>
          <p>See which workers are reachable, what they can run, and how much executor capacity is free.</p>
        </div>
        <div className="agents-protocol"><span className="overline">Protocol</span><strong>rivet-agent / v4</strong><small>{online ? "registry connected" : "engine unavailable"}</small></div>
      </section>

      <section className="metric-grid agents-metrics" aria-label="Agent metrics">
        <MetricCard label="Registered" value={String(agents.length).padStart(2, "0")} detail={`${onlineAgents.length} online / ${staleAgents.length} stale`} accent="cyan" />
        <MetricCard label="Free capacity" value={`${available}/${capacity}`} detail="available executors" accent="amber" />
        <MetricCard label="Running work" value={String(running).padStart(2, "0")} detail="agent-reported builds" accent="neutral" />
        <MetricCard label="Assignment" value="READY" detail="durable event replay verified" accent="neutral" />
      </section>

      <section className="panel agents-panel" aria-label="Connected agents">
        <div className="panel-heading">
          <div><span className="overline">Fleet registry</span><h2>Connected agents</h2></div>
          <span className="panel-count">{String(agents.length).padStart(2, "0")}</span>
        </div>
        {agents.length === 0 ? (
          <div className="agents-empty"><span className="agents-empty-mark">⊙</span><strong>{online ? "No remote agents connected" : "Agent registry unavailable"}</strong><p>{online ? "Start a Rivet agent to see its capabilities and heartbeat here." : "Reconnect the local engine to inspect the remote fleet."}</p></div>
        ) : (
          <div className="agent-list">
            {agents.map((agent) => {
              const statusLabel = agent.status === "online" ? "Online" : "Stale";
              return (
                <article className={`agent-row agent-${agent.status}`} key={agent.agent_id}>
                  <span className="agent-status-dot" aria-label={statusLabel} />
                  <div className="agent-identity"><strong>{agent.name}</strong><small>{agent.agent_id.slice(0, 8)} · heartbeat #{agent.last_sequence}</small></div>
                  <div className="agent-capability"><span className="overline">Platform</span><strong>{agent.capabilities.os} · {agent.capabilities.arch}</strong></div>
                  <div className="agent-capability agent-labels"><span className="overline">Labels</span><strong>{agent.capabilities.labels.length ? agent.capabilities.labels.join(" · ") : "none"}</strong></div>
                  <div className="agent-capacity"><span className="overline">Capacity</span><strong>{agent.available_executors} / {agent.capabilities.executors}</strong><small>{agent.running.length} running · {agent.reserved?.length ?? 0} reserved · {agent.capabilities.docker ? "Docker" : "Native"}</small></div>
                  <span className="agent-state">{statusLabel}</span>
                </article>
              );
            })}
          </div>
        )}
      </section>
      <p className="agent-boundary"><span />Capacity discovery, assignment, remote execution, artifact transfer, one bounded replacement attempt, and durable event replay are verified locally.</p>
    </div>
  );
}

function ExtensionsPanel({
  extensions,
  statuses,
  online,
  busyId,
  onToggle,
}: {
  extensions: ExtensionManifest[];
  statuses: ExtensionRuntimeStatus[];
  online: boolean;
  busyId: string | null;
  onToggle: (manifest: ExtensionManifest, active: boolean) => void;
}) {
  const permissionEntries = Object.entries(EXTENSION_PERMISSION_LABELS) as [
    ExtensionPermission,
    string,
  ][];
  return (
    <div className="content-wrap extensions-page">
      <section className="page-heading extensions-heading">
        <div>
          <span className="eyebrow"><span className="eyebrow-line" />Extension surface</span>
          <h1>Make the boundary useful.</h1>
          <p>Small, explicit capabilities for teams that want to extend the signal without inheriting a plugin runtime.</p>
        </div>
        <div className="extensions-protocol">
          <span className="overline">Wire contract</span>
          <strong>{EXTENSION_CATALOG.protocol_name} / v{EXTENSION_CATALOG.protocol_version}</strong>
          <small>bounded JSON frames · no shell dispatch</small>
        </div>
      </section>

      <section className="metric-grid extensions-metrics" aria-label="Extension protocol metrics">
        <MetricCard label="Protocol" value={`v${EXTENSION_CATALOG.protocol_version}`} detail="versioned contract" accent="cyan" />
        <MetricCard label="Runtimes" value={String(EXTENSION_CATALOG.supported_kinds.length).padStart(2, "0")} detail="WASM / subprocess" accent="amber" />
        <MetricCard label="Loaded" value={String(extensions.length).padStart(2, "0")} detail="local catalog" accent="neutral" />
        <MetricCard label="Permissions" value={String(permissionEntries.length).padStart(2, "0")} detail="declared capabilities" accent="neutral" />
      </section>

      <div className="extensions-grid">
        <section className="panel extension-boundary-card">
          <div className="panel-heading">
            <div><span className="overline">Design principle</span><h2>Capabilities before code.</h2></div>
            <span className="extension-seal">R / 01</span>
          </div>
          <div className="extension-flow" aria-label="Extension message flow">
            <div className="extension-flow-node"><span>01</span><strong>Manifest</strong><small>identity + permissions</small></div>
            <span className="extension-flow-line" aria-hidden="true">→</span>
            <div className="extension-flow-node active"><span>02</span><strong>Host</strong><small>bounded framing</small></div>
            <span className="extension-flow-line" aria-hidden="true">→</span>
            <div className="extension-flow-node"><span>03</span><strong>Signal</strong><small>request / result / event</small></div>
          </div>
          <p className="extension-card-copy">Every message carries the protocol version. Frames are length-prefixed and capped before JSON decoding; subprocess programs receive direct argument arrays, never a shell command string.</p>
        </section>

        <section className="panel extension-permissions-card">
          <div className="panel-heading">
            <div><span className="overline">Permission vocabulary</span><h2>Ask for less.</h2></div>
            <span className="panel-count">{String(permissionEntries.length).padStart(2, "0")}</span>
          </div>
          <div className="extension-permission-list">
            {permissionEntries.map(([permission, label], index) => (
              <div className="extension-permission-row" key={permission}>
                <span>{String(index + 1).padStart(2, "0")}</span>
                <strong>{label}</strong>
                <code>{permission}</code>
              </div>
            ))}
          </div>
        </section>
      </div>

      <section className="panel extension-runtime-card">
        <div className="panel-heading">
          <div><span className="overline">Runtime control</span><h2>Lifecycle</h2></div>
          <span className="panel-count">{String(statuses.filter((status) => status.active).length).padStart(2, "0")}</span>
        </div>
        {extensions.length === 0 ? (
          <EmptyState text={online ? "No validated extensions are available." : "Reconnect the local engine to inspect extensions."} />
        ) : (
          <div className="extension-runtime-list">
            {extensions.map((extension) => {
              const status = statuses.find((candidate) => candidate.id === extension.id);
              const active = status?.active ?? false;
              const canControl = status?.runtime_available === true;
              return (
                <div className="extension-runtime-row" key={extension.id}>
                  <span className={`extension-runtime-dot ${active ? "active" : ""}`} />
                  <div className="extension-runtime-copy"><strong>{extension.name}</strong><small>{extension.id} · {extension.kind}</small></div>
                  <span className="extension-runtime-state">{active ? "active" : "stopped"}</span>
                  <button className="button button-quiet extension-runtime-action" type="button" disabled={!canControl || busyId === extension.id} onClick={() => onToggle(extension, active)}>{busyId === extension.id ? "Working…" : active ? "Stop" : "Start"}</button>
                </div>
              );
            })}
          </div>
        )}
        <p className="extension-runtime-boundary"><span>!</span>Starting an extension is an administrator action. WASM runs through a capability-free ABI with bounded memory, output, and fuel; subprocesses use direct arguments and declared permissions.</p>
      </section>

      <section className="panel extension-empty-card">
        <span className="extension-empty-mark">◇</span>
        <div><span className="overline">Catalog status</span><strong>{extensions.length ? `${extensions.length} extension${extensions.length === 1 ? "" : "s"} loaded` : "No extensions loaded"}</strong><p>{extensions.length ? "These manifests were validated by the local engine. Lifecycle actions stay behind the administrator boundary, and each request must use a declared permission." : "Nothing is installed, executed, or granted by this empty view. Add validated manifests to expose the lifecycle surface."}</p></div>
        <span className="extension-gate">{extensions.length ? "MANIFESTS VALIDATED" : "CATALOG EMPTY"}</span>
      </section>
    </div>
  );
}

function MigrationPanel({
  source,
  result,
  busy,
  online,
  onChange,
  onAnalyze,
}: {
  source: string;
  result: MigrationResponse | null;
  busy: boolean;
  online: boolean;
  onChange: (source: string) => void;
  onAnalyze: () => void;
}) {
  const summary = result?.analysis.summary;
  return (
    <div className="content-wrap migration-page">
      <section className="page-heading migration-heading">
        <div>
          <span className="eyebrow"><span className="eyebrow-line" />Migration assistant</span>
          <h1>Move with evidence.</h1>
          <p>Read a Jenkinsfile, see the migration boundary, and keep every ambiguous behavior visible.</p>
        </div>
        <div className="migration-version">
          <span className="overline">Analyzer</span>
          <strong>{result ? `v${result.analysis.analyzer_version}` : "v1"}</strong>
          <small>local · no Groovy execution</small>
        </div>
      </section>

      <div className="migration-grid">
        <section className="panel migration-source-panel">
          <div className="panel-heading">
            <div><span className="overline">Source review</span><h2>Jenkinsfile</h2></div>
            <span className="panel-count">{source.split(/\r?\n/).length.toString().padStart(2, "0")} lines</span>
          </div>
          <textarea
            className="migration-editor"
            aria-label="Jenkinsfile source"
            value={source}
            onChange={(event) => onChange(event.target.value)}
            spellCheck={false}
          />
          <div className="migration-source-footer">
            <span><i className={online ? "online" : "offline"} />{online ? "engine connected" : "engine unavailable"}</span>
            <button className="button button-primary" type="button" disabled={!online || busy} onClick={onAnalyze}>
              {busy ? "Analyzing…" : "Analyze Jenkinsfile"} <span>→</span>
            </button>
          </div>
        </section>

        <section className="panel migration-report-panel" aria-live="polite">
          <div className="panel-heading">
            <div><span className="overline">Compatibility signal</span><h2>Migration report</h2></div>
            {result && <span className={`migration-status migration-${result.analysis.status}`}>{migrationStatusLabel(result.analysis.status)}</span>}
          </div>
          {!result ? (
            <div className="migration-empty">
              <span className="migration-empty-mark">⇄</span>
              <strong>Analysis is standing by</strong>
              <p>Paste a Jenkinsfile and run the local analyzer to see exact line-level findings.</p>
            </div>
          ) : (
            <>
              <div className="migration-summary" aria-label="Migration summary">
                <MigrationMetric label="Supported" value={summary?.supported ?? 0} tone="supported" />
                <MigrationMetric label="Needs review" value={summary?.partial ?? 0} tone="partial" />
                <MigrationMetric label="Blocked" value={summary?.unsupported ?? 0} tone="unsupported" />
              </div>
              <div className="finding-list">
                {result.analysis.constructs.map((finding) => (
                  <article className="finding-row" key={`${finding.kind}-${finding.line}`}>
                    <span className={`finding-line migration-${finding.status}`}>L{finding.line}</span>
                    <div className="finding-copy">
                      <div><strong>{finding.kind.replaceAll("_", " ")}</strong><span className={`finding-state migration-${finding.status}`}>{migrationStatusLabel(finding.status)}</span></div>
                      <p>{finding.message}</p>
                      {finding.rivet_mapping && <small>↳ {finding.rivet_mapping}</small>}
                    </div>
                  </article>
                ))}
              </div>
            </>
          )}
        </section>
      </div>

      {result?.draft && (
        <section className="panel migration-draft-panel">
          <div className="panel-heading">
            <div><span className="overline">Deterministic output</span><h2>Rivetfile draft</h2></div>
            <span className={`migration-status migration-${result.draft.status}`}>{result.draft.converted_steps} steps converted</span>
          </div>
          {result.draft.rivetfile_toml ? (
            <pre className="migration-draft-code">{result.draft.rivetfile_toml}</pre>
          ) : (
            <EmptyState text="No valid draft was generated from the deterministic subset." />
          )}
          {result.draft.warnings.length > 0 && (
            <div className="migration-warnings">
              <span className="overline">Review queue</span>
              {result.draft.warnings.map((warning) => <p key={warning}>⚠ {warning}</p>)}
            </div>
          )}
        </section>
      )}
    </div>
  );
}

function MigrationMetric({ label, value, tone }: { label: string; value: number; tone: string }) {
  return <div className={`migration-metric migration-${tone}`}><strong>{String(value).padStart(2, "0")}</strong><span>{label}</span></div>;
}

function migrationStatusLabel(status: "supported" | "partial" | "unsupported"): string {
  return status === "supported" ? "Supported" : status === "partial" ? "Partial" : "Unsupported";
}

function CreateProjectModal({ busy, onClose, onSubmit }: { busy: boolean; onClose: () => void; onSubmit: (event: React.FormEvent<HTMLFormElement>) => void }) {
  return <div className="modal-backdrop" role="presentation" onMouseDown={(event) => event.target === event.currentTarget && onClose()}><div className="modal-card" role="dialog" aria-modal="true" aria-labelledby="create-project-title"><div className="modal-header"><div><span className="overline">Project registry</span><h2 id="create-project-title">Connect a repository</h2></div><button className="close-button" onClick={onClose} aria-label="Close">×</button></div><p className="modal-intro">Point Rivet at a local checkout and its pipeline definition. Paths are resolved by the engine.</p><form onSubmit={onSubmit}><label>Project name<input name="name" required placeholder="payments-api" autoFocus /></label><label>Repository path<input name="repository_path" required placeholder="/Users/you/Code/payments-api" /></label><label>Pipeline path <span className="optional">optional</span><input name="pipeline_path" placeholder="/Users/you/Code/payments-api/Rivetfile.toml" /></label><div className="modal-actions"><button type="button" className="button button-quiet" onClick={onClose}>Cancel</button><button type="submit" className="button button-primary" disabled={busy}>{busy ? "Connecting…" : "Connect project"}</button></div></form></div></div>;
}

export default App;
