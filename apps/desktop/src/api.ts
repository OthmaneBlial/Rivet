import type {
  BuildDetails,
  BuildRecord,
  AgentSummary,
  ArtifactRecord,
  LogRecord,
  MigrationResponse,
  Project,
  QueueResponse,
  QueueStats,
  ScheduleRecord,
} from "./types";
import { invoke } from "@tauri-apps/api/core";

export const ENGINE_ORIGIN =
  import.meta.env.VITE_RIVET_ENGINE_URL ?? "http://127.0.0.1:7878";

let activeEngineOrigin = ENGINE_ORIGIN;
let engineOriginPromise: Promise<string> | null = null;

export const ENGINE_OFFLINE_MESSAGE =
  "Engine offline — start the local engine to connect.";

export interface HealthResponse {
  status: string;
  service: string;
  timestamp: string;
}

export interface ScmPrepareOptions {
  remote?: string;
  fetch?: boolean;
  revision?: string;
  clean?: boolean;
  clean_ignored?: boolean;
}

export interface BuildRequestOptions {
  scm?: ScmPrepareOptions;
  parameters?: Record<string, string>;
}

function runningInsideTauri(): boolean {
  return typeof window !== "undefined" && "__TAURI_INTERNALS__" in window;
}

export async function initializeEngineOrigin(): Promise<string> {
  if (!runningInsideTauri()) return activeEngineOrigin;
  if (!engineOriginPromise) {
    engineOriginPromise = invoke<string>("engine_origin")
      .then((origin) => {
        const parsed = new URL(origin);
        if (parsed.protocol !== "http:" || parsed.hostname !== "127.0.0.1") {
          throw new Error("The embedded engine must use a loopback HTTP origin.");
        }
        activeEngineOrigin = parsed.origin;
        return activeEngineOrigin;
      })
      .catch(() => activeEngineOrigin);
  }
  return engineOriginPromise;
}

export function getEngineOrigin(): string {
  return activeEngineOrigin;
}

async function request<T>(path: string, init?: RequestInit): Promise<T> {
  const engineOrigin = await initializeEngineOrigin();
  let response: Response | undefined;
  for (let attempt = 0; attempt < 8; attempt += 1) {
    try {
      response = await fetch(`${engineOrigin}${path}`, {
        ...init,
        headers: {
          "content-type": "application/json",
          ...init?.headers,
        },
      });
      break;
    } catch (cause) {
      if (cause instanceof DOMException && cause.name === "AbortError") {
        throw cause;
      }
      if (attempt === 7) throw new Error(ENGINE_OFFLINE_MESSAGE);
      await new Promise((resolve) =>
        window.setTimeout(resolve, Math.min(150 * 2 ** attempt, 1_000)),
      );
    }
  }
  if (!response) throw new Error(ENGINE_OFFLINE_MESSAGE);
  if (!response.ok) {
    let message = `${response.status} ${response.statusText}`;
    try {
      const body = (await response.json()) as { error?: string };
      message = body.error ?? message;
    } catch {
      // Preserve the useful HTTP status if the response is not JSON.
    }
    throw new Error(message);
  }
  const payload = await response.text();
  return (payload ? JSON.parse(payload) : undefined) as T;
}

export function health(): Promise<HealthResponse> {
  return request<HealthResponse>("/api/v1/health");
}

export function projects(): Promise<Project[]> {
  return request<Project[]>("/api/v1/projects");
}

export function queueStats(): Promise<QueueStats> {
  return request<QueueStats>("/api/v1/queue");
}

export function agents(): Promise<AgentSummary[]> {
  return request<AgentSummary[]>("/api/v1/agents");
}

export function analyzeJenkinsfile(
  source: string,
  draft = true,
): Promise<MigrationResponse> {
  return request<MigrationResponse>("/api/v1/migration/jenkinsfile", {
    method: "POST",
    body: JSON.stringify({ source, draft }),
  });
}

export function builds(project: string): Promise<BuildRecord[]> {
  return request<BuildRecord[]>(
    `/api/v1/projects/${encodeURIComponent(project)}/builds`,
  );
}

export function buildDetails(
  project: string,
  number: number,
): Promise<BuildDetails> {
  return request<BuildDetails>(
    `/api/v1/projects/${encodeURIComponent(project)}/builds/${number}`,
  );
}

export function logs(project: string, number: number): Promise<LogRecord[]> {
  return request<LogRecord[]>(
    `/api/v1/projects/${encodeURIComponent(project)}/builds/${number}/logs`,
  );
}

export function artifacts(project: string, number: number): Promise<ArtifactRecord[]> {
  return request<ArtifactRecord[]>(
    `/api/v1/projects/${encodeURIComponent(project)}/builds/${number}/artifacts`,
  );
}

export function schedules(project: string): Promise<ScheduleRecord[]> {
  return request<ScheduleRecord[]>(
    `/api/v1/projects/${encodeURIComponent(project)}/schedules`,
  );
}

export function createSchedule(
  project: string,
  input: { name: string; expression: string; enabled?: boolean },
): Promise<ScheduleRecord> {
  return request<ScheduleRecord>(
    `/api/v1/projects/${encodeURIComponent(project)}/schedules`,
    { method: "POST", body: JSON.stringify(input) },
  );
}

export function updateSchedule(
  project: string,
  id: string,
  enabled: boolean,
): Promise<ScheduleRecord> {
  return request<ScheduleRecord>(
    `/api/v1/projects/${encodeURIComponent(project)}/schedules/${encodeURIComponent(id)}`,
    { method: "PATCH", body: JSON.stringify({ enabled }) },
  );
}

export function deleteSchedule(project: string, id: string): Promise<void> {
  return request<void>(
    `/api/v1/projects/${encodeURIComponent(project)}/schedules/${encodeURIComponent(id)}`,
    { method: "DELETE" },
  );
}

export function queueBuild(
  project: string,
  options: BuildRequestOptions = {},
): Promise<QueueResponse> {
  return request<QueueResponse>(
    `/api/v1/projects/${encodeURIComponent(project)}/builds`,
    { method: "POST", body: JSON.stringify(options) },
  );
}

export function retryBuild(
  project: string,
  number: number,
  options: BuildRequestOptions = {},
): Promise<QueueResponse> {
  return request<QueueResponse>(
    `/api/v1/projects/${encodeURIComponent(project)}/builds/${number}/retry`,
    { method: "POST", body: JSON.stringify(options) },
  );
}

export function cancelBuild(project: string, number: number): Promise<void> {
  return request<void>(
    `/api/v1/projects/${encodeURIComponent(project)}/builds/${number}/cancel`,
    { method: "POST", body: "{}" },
  );
}

export function createProject(input: {
  name: string;
  repository_path: string;
  pipeline_path?: string;
}): Promise<Project> {
  return request<Project>("/api/v1/projects", {
    method: "POST",
    body: JSON.stringify(input),
  });
}

export function eventUrl(project: string, number: number): string {
  const url = new URL(
    `/api/v1/projects/${encodeURIComponent(project)}/builds/${number}/events`,
    activeEngineOrigin,
  );
  url.protocol = url.protocol === "https:" ? "wss:" : "ws:";
  return url.toString();
}

export function artifactUrl(project: string, number: number, artifactId: string): string {
  return `${activeEngineOrigin}/api/v1/projects/${encodeURIComponent(project)}/builds/${number}/artifacts/${encodeURIComponent(artifactId)}`;
}
