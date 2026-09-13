import { useCallback, useEffect, useMemo, useState } from "react";
import {
  buildDetails,
  builds,
  cancelBuild,
  createProject,
  ENGINE_ORIGIN,
  ENGINE_OFFLINE_MESSAGE,
  eventUrl,
  logs,
  projects,
  queueStats as fetchQueueStats,
  queueBuild,
} from "./api";
import type {
  BuildDetails,
  BuildRecord,
  BuildStatus,
  LogRecord,
  Project,
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

function RivetMark({ className = "" }: { className?: string }) {
  return (
    <svg className={`rivet-mark ${className}`} viewBox="0 0 48 48" aria-hidden="true" focusable="false">
      <path className="rivet-mark-shadow" d="m11 8 27-4 6 32-27 5z" />
      <path className="rivet-mark-frame" d="m7 9 28-4 6 31-28 6z" />
      <path className="rivet-mark-inset" d="m10.5 12 22.4-3.2 4.7 24.7-22.5 3.2z" />
      <path className="rivet-mark-core" d="M14.5 13.5h16.1c4.7 0 7.5 2.5 7.5 6.4 0 2.8-1.5 5-4.3 5.9l5.5 8.4h-6.4l-4.6-7h-7.9v7h-5.9V13.5Zm5.9 5.1v3.9h9.3c1.7 0 2.7-.7 2.7-2s-1-1.9-2.7-1.9h-9.3Z" />
      <path className="rivet-mark-link" d="m29.4 30.7 5.6-5.7" />
      <circle className="rivet-mark-bolt" cx="37" cy="9.5" r="2.5" />
      <circle className="rivet-mark-pin" cx="15.1" cy="37.1" r="1.35" />
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
  const [engineOnline, setEngineOnline] = useState(false);
  const [projectsList, setProjectsList] = useState<Project[]>([]);
  const [projectName, setProjectName] = useState("");
  const [buildList, setBuildList] = useState<BuildRecord[]>([]);
  const [queueStatus, setQueueStatus] = useState<{ queued: number; running: number; capacity: number } | null>(null);
  const [selectedBuild, setSelectedBuild] = useState<number | null>(null);
  const [details, setDetails] = useState<BuildDetails | null>(null);
  const [logLines, setLogLines] = useState<LogRecord[]>([]);
  const [activeNav, setActiveNav] = useState("Pipelines");
  const [showCreate, setShowCreate] = useState(false);
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

  const loadBuildView = useCallback(async () => {
    if (!projectName || selectedBuild === null) return;
    try {
      const [nextDetails, nextLogs] = await Promise.all([
        buildDetails(projectName, selectedBuild),
        logs(projectName, selectedBuild),
      ]);
      setDetails(nextDetails);
      setLogLines(nextLogs);
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
    void loadProjects();
  }, [loadProjects]);

  useEffect(() => {
    void loadBuildList();
  }, [loadBuildList]);

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

  const latest = buildList[0] ?? null;
  const passed = buildList.filter((build) => build.status === "passed").length;
  const successRate = buildList.length ? Math.round((passed / buildList.length) * 100) : 0;
  const isRunning = details?.build.status === "running" || details?.build.status === "queued";

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
              <small>{ENGINE_ORIGIN.replace("http://", "")}</small>
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

            <section className="section-head">
              <div><span className="overline">Execution map</span><h2>{selectedProject?.name ?? "Pipeline"}</h2></div>
              <div className="section-actions">
                {isRunning && <button className="button button-danger" disabled={busy} onClick={() => void stopSelectedBuild()}>Stop run</button>}
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

function EmptyState({ text }: { text: string }) { return <div className="empty-state"><span>∅</span>{text}</div>; }

function Onboarding({ onCreate, online }: { onCreate: () => void; online: boolean }) {
  return <div className="onboarding"><div className="onboarding-mark"><RivetMark /></div><span className="eyebrow"><span className="eyebrow-line" />First connection</span><h1>Give the engine<br /><em>a pipeline to watch.</em></h1><p>Rivet keeps the execution signal close: a local repository, an explicit `Rivetfile.toml`, and a durable history you can trust.</p><button className="button button-primary onboarding-button" disabled={!online} onClick={onCreate}>＋ Connect a project <span>→</span></button><div className="onboarding-note"><span className="note-rule" />{online ? "The local engine is ready for a repository." : "Start `rivet server` to connect the local engine."}</div></div>;
}

function CreateProjectModal({ busy, onClose, onSubmit }: { busy: boolean; onClose: () => void; onSubmit: (event: React.FormEvent<HTMLFormElement>) => void }) {
  return <div className="modal-backdrop" role="presentation" onMouseDown={(event) => event.target === event.currentTarget && onClose()}><div className="modal-card" role="dialog" aria-modal="true" aria-labelledby="create-project-title"><div className="modal-header"><div><span className="overline">Project registry</span><h2 id="create-project-title">Connect a repository</h2></div><button className="close-button" onClick={onClose} aria-label="Close">×</button></div><p className="modal-intro">Point Rivet at a local checkout and its pipeline definition. Paths are resolved by the engine.</p><form onSubmit={onSubmit}><label>Project name<input name="name" required placeholder="payments-api" autoFocus /></label><label>Repository path<input name="repository_path" required placeholder="/Users/you/Code/payments-api" /></label><label>Pipeline path <span className="optional">optional</span><input name="pipeline_path" placeholder="/Users/you/Code/payments-api/Rivetfile.toml" /></label><div className="modal-actions"><button type="button" className="button button-quiet" onClick={onClose}>Cancel</button><button type="submit" className="button button-primary" disabled={busy}>{busy ? "Connecting…" : "Connect project"}</button></div></form></div></div>;
}

export default App;
