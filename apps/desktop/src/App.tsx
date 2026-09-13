import { useCallback, useEffect, useMemo, useState } from "react";
import {
  buildDetails,
  builds,
  cancelBuild,
  createProject,
  createSchedule,
  deleteSchedule,
  artifactUrl,
  artifacts,
  ENGINE_OFFLINE_MESSAGE,
  eventUrl,
  getEngineOrigin,
  initializeEngineOrigin,
  logs,
  projects,
  queueStats as fetchQueueStats,
  queueBuild,
  retryBuild,
  schedules,
  updateSchedule,
} from "./api";
import type {
  ArtifactRecord,
  BuildDetails,
  BuildRecord,
  BuildStatus,
  LogRecord,
  Project,
  ScheduleRecord,
  StageDetails,
} from "./types";

const NAV_ITEMS = [
  { label: "Pipelines", mark: "↳", active: true },
  { label: "Queue", mark: "≋", active: false },
  { label: "Agents", mark: "⊙", active: false },
  { label: "Artifacts", mark: "□", active: false },
  { label: "Credentials", mark: "◈", active: false },
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
    <svg className={`rivet-mark ${className}`} viewBox="0 0 64 64" aria-hidden="true" focusable="false">
      <path className="rivet-mark-shadow" d="m32 6 21 12v24L32 54 11 42V18z" />
      <path className="rivet-mark-orbit" d="M24 7H18L7 18v6 M40 7h6l11 11v6 M57 40v6L46 57h-6 M24 57h-6L7 46v-6" />
      <path className="rivet-mark-rail" d="m14 14 11 11m25-11L39 25m11 25L39 39M14 50l11-11" />
      <circle className="rivet-mark-node" cx="14" cy="14" r="2.7" />
      <circle className="rivet-mark-node" cx="50" cy="14" r="2.7" />
      <circle className="rivet-mark-node rivet-mark-node-warm" cx="50" cy="50" r="2.7" />
      <circle className="rivet-mark-node rivet-mark-node-warm" cx="14" cy="50" r="2.7" />
      <path className="rivet-mark-core" d="m32 18 12 7v14l-12 7-12-7V25z" />
      <path className="rivet-mark-core-inner" d="m32 24 6 3.5v9L32 40l-6-3.5v-9z" />
      <circle className="rivet-mark-bolt" cx="32" cy="32" r="2.25" />
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
  const [queueStatus, setQueueStatus] = useState<{ queued: number; running: number; capacity: number } | null>(null);
  const [selectedBuild, setSelectedBuild] = useState<number | null>(null);
  const [details, setDetails] = useState<BuildDetails | null>(null);
  const [logLines, setLogLines] = useState<LogRecord[]>([]);
  const [artifactList, setArtifactList] = useState<ArtifactRecord[]>([]);
  const [scheduleList, setScheduleList] = useState<ScheduleRecord[]>([]);
  const [activeNav, setActiveNav] = useState("Pipelines");
  const [showCreate, setShowCreate] = useState(false);
  const [showScheduleCreate, setShowScheduleCreate] = useState(false);
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

        {projectsList.length === 0 ? (
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
              <MetricCard label="Queue" value={(queueStatus?.queued ?? buildList.filter((build) => build.status === "queued").length).toString().padStart(2, "0")} detail={queueStatus ? `${queueStatus.running} running / ${queueStatus.capacity} slots` : "local capacity / 01"} accent="amber" />
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

function CreateProjectModal({ busy, onClose, onSubmit }: { busy: boolean; onClose: () => void; onSubmit: (event: React.FormEvent<HTMLFormElement>) => void }) {
  return <div className="modal-backdrop" role="presentation" onMouseDown={(event) => event.target === event.currentTarget && onClose()}><div className="modal-card" role="dialog" aria-modal="true" aria-labelledby="create-project-title"><div className="modal-header"><div><span className="overline">Project registry</span><h2 id="create-project-title">Connect a repository</h2></div><button className="close-button" onClick={onClose} aria-label="Close">×</button></div><p className="modal-intro">Point Rivet at a local checkout and its pipeline definition. Paths are resolved by the engine.</p><form onSubmit={onSubmit}><label>Project name<input name="name" required placeholder="payments-api" autoFocus /></label><label>Repository path<input name="repository_path" required placeholder="/Users/you/Code/payments-api" /></label><label>Pipeline path <span className="optional">optional</span><input name="pipeline_path" placeholder="/Users/you/Code/payments-api/Rivetfile.toml" /></label><div className="modal-actions"><button type="button" className="button button-quiet" onClick={onClose}>Cancel</button><button type="submit" className="button button-primary" disabled={busy}>{busy ? "Connecting…" : "Connect project"}</button></div></form></div></div>;
}

export default App;
