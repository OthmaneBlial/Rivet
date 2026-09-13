import { useCallback, useEffect, useMemo, useState } from "react";
import {
  buildDetails,
  builds,
  cancelBuild,
  createProject,
  createSchedule,
  deleteSchedule,
  artifactUrl,
  analyzeJenkinsfile,
  agents as fetchAgents,
  artifacts,
  extensions as fetchExtensions,
  ENGINE_OFFLINE_MESSAGE,
  eventUrl,
  getEngineOrigin,
  initializeEngineOrigin,
  logs,
  pauseQueue,
  projects,
  queueStats as fetchQueueStats,
  queueBuild,
  retryBuild,
  resumeQueue,
  schedules,
  updateSchedule,
} from "./api";
import type {
  AgentSummary,
  ArtifactRecord,
  BuildDetails,
  BuildRecord,
  BuildStatus,
  LogRecord,
  MigrationResponse,
  Project,
  QueueStats,
  ScheduleRecord,
  StageDetails,
} from "./types";
import {
  EXTENSION_CATALOG,
  EXTENSION_PERMISSION_LABELS,
  type ExtensionPermission,
} from "./extensionModel";
import type { ExtensionManifest } from "./extensionModel";

const NAV_ITEMS = [
  { label: "Pipelines", mark: "↳", active: true },
  { label: "Queue", mark: "≋", active: false },
  { label: "Agents", mark: "⊙", active: true },
  { label: "Artifacts", mark: "□", active: false },
  { label: "Credentials", mark: "◈", active: false },
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
    <svg className={`rivet-mark ${className}`} viewBox="0 0 72 72" aria-hidden="true" focusable="false">
      <circle className="rivet-mark-shadow" cx="36" cy="38" r="28" />
      <circle className="rivet-mark-shell" cx="34" cy="34" r="28" />
      <circle className="rivet-mark-shell-line" cx="34" cy="34" r="22" />
      <path className="rivet-mark-r" d="M21 52V17h14.7c11.2 0 17.2 4.6 17.2 12.2 0 5.4-2.8 9.3-8.1 11.5L53 52H42.1l-7.2-9.7h-4V52zm9.9-18.3h4.4c5.3 0 7.8-1.9 7.8-5.3 0-3.5-2.5-5.1-7.8-5.1h-4.4z" />
      <path className="rivet-mark-rail" d="M12 48 27 61M48 8l10 10" />
      <circle className="rivet-mark-node" cx="57" cy="13" r="6" />
      <circle className="rivet-mark-node-core" cx="57" cy="13" r="2" />
      <path className="rivet-mark-notch" d="M53.5 9.5 60.5 16.5" />
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
  const [agentList, setAgentList] = useState<AgentSummary[]>([]);
  const [extensionList, setExtensionList] = useState<ExtensionManifest[]>([]);
  const [selectedBuild, setSelectedBuild] = useState<number | null>(null);
  const [details, setDetails] = useState<BuildDetails | null>(null);
  const [logLines, setLogLines] = useState<LogRecord[]>([]);
  const [artifactList, setArtifactList] = useState<ArtifactRecord[]>([]);
  const [scheduleList, setScheduleList] = useState<ScheduleRecord[]>([]);
  const [activeNav, setActiveNav] = useState("Pipelines");
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

  const loadAgentList = useCallback(async () => {
    try {
      setAgentList(await fetchAgents());
    } catch {
      setAgentList([]);
    }
  }, []);

  const loadExtensionList = useCallback(async () => {
    try {
      setExtensionList(await fetchExtensions());
    } catch {
      setExtensionList([]);
    }
  }, []);

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
    if (!engineOnline) {
      setExtensionList([]);
      return;
    }
    void loadExtensionList();
  }, [engineOnline, loadExtensionList]);

  useEffect(() => {
    if (engineOnline) return;
    const retry = window.setTimeout(() => void loadProjects(), 5000);
    return () => window.clearTimeout(retry);
  }, [engineOnline, loadProjects]);

  async function runSelectedPipeline() {
    if (!projectName) return;
    setBusy(true);
    setError(null);
    try {
      const queued = await queueBuild(projectName);
      setSelectedBuild(queued.build.number);
      await loadBuildList();
    } catch (cause) {
      setError(cause instanceof Error ? cause.message : "Could not queue build");
    } finally {
      setBusy(false);
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
    setBusy(true);
    setError(null);
    try {
      const queued = await retryBuild(projectName, selectedBuild);
      setSelectedBuild(queued.build.number);
      await loadBuildList();
      await loadBuildView();
    } catch (cause) {
      setError(cause instanceof Error ? cause.message : "Could not retry build");
    } finally {
      setBusy(false);
    }
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
          <ExtensionsPanel extensions={extensionList} />
        ) : activeNav === "Agents" ? (
          <AgentsPanel agents={agentList} online={engineOnline} />
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
              <div className="schedule-copy"><strong>{schedule.name}</strong><small><code>{schedule.expression}</code> · next {formatTime(schedule.next_run_at)}</small></div>
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
        <div className="agents-protocol"><span className="overline">Protocol</span><strong>rivet-agent / v2</strong><small>{online ? "registry connected" : "engine unavailable"}</small></div>
      </section>

      <section className="metric-grid agents-metrics" aria-label="Agent metrics">
        <MetricCard label="Registered" value={String(agents.length).padStart(2, "0")} detail={`${onlineAgents.length} online / ${staleAgents.length} stale`} accent="cyan" />
        <MetricCard label="Free capacity" value={`${available}/${capacity}`} detail="available executors" accent="amber" />
        <MetricCard label="Running work" value={String(running).padStart(2, "0")} detail="agent-reported builds" accent="neutral" />
        <MetricCard label="Assignment" value="GATED" detail="transport semantics in progress" accent="neutral" />
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
      <p className="agent-boundary"><span />Capacity discovery, assignment, remote execution, artifact transfer, and one bounded replacement attempt are verified locally. Durable restart recovery and richer retry policy remain separately gated.</p>
    </div>
  );
}

function ExtensionsPanel({ extensions }: { extensions: ExtensionManifest[] }) {
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

      <section className="panel extension-empty-card">
        <span className="extension-empty-mark">◇</span>
        <div><span className="overline">Catalog status</span><strong>{extensions.length ? `${extensions.length} extension${extensions.length === 1 ? "" : "s"} loaded` : "No extensions loaded"}</strong><p>{extensions.length ? "These manifests were validated by the local engine. Runtime permissions are still reviewed at the host boundary." : "The protocol and desktop model are ready for a future catalog manager. Nothing is installed, executed, or granted by this empty view."}</p></div>
        <span className="extension-gate">{extensions.length ? "MANIFESTS VALIDATED" : "MANAGER GATED"}</span>
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
