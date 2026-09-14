import type {
  BuildDetails,
  BuildRecord,
  AgentSummary,
  ArtifactRecord,
  CredentialSummary,
  LogRecord,
  MigrationResponse,
  PipelineParameter,
  Project,
  QueueItem,
  QueueResponse,
  QueueStats,
  RepositoryPollResponse,
  ScheduleRecord,
  ScheduleTrigger,
} from "./types";
import type { ExtensionManifest, ExtensionRuntimeStatus } from "./extensionModel";
import { invoke } from "@tauri-apps/api/core";

export const ENGINE_ORIGIN =
  import.meta.env.VITE_RIVET_ENGINE_URL ?? "http://127.0.0.1:7878";

let activeEngineOrigin = ENGINE_ORIGIN;
let engineOriginPromise: Promise<string> | null = null;

export const ENGINE_OFFLINE_MESSAGE =
  "Engine offline — start the local engine to connect.";

const RETRYABLE_METHODS = new Set(["GET", "HEAD", "OPTIONS"]);
const MAX_READ_ATTEMPTS = 8;

export class EngineRequestError extends Error {
  constructor(
    message: string,
    readonly requestId: string,
    readonly status: number | null,
    readonly networkFailure: boolean,
  ) {
    super(message);
    this.name = "EngineRequestError";
  }
}

export interface HealthResponse {
  status: string;
  service: string;
  timestamp: string;
}

export interface ReadinessResponse {
  status: string;
  service: string;
  storage: string;
  timestamp: string;
}

export interface ScmPrepareOptions {
  remote?: string;
  fetch?: boolean;
  revision?: string;
  fetch_ref?: string;
  clean?: boolean;
  clean_ignored?: boolean;
  credential_id?: string;
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
  const method = (init?.method ?? "GET").toUpperCase();
  const retryable = RETRYABLE_METHODS.has(method);
  let response: Response | undefined;
  let requestId = "";
  for (let attempt = 0; attempt < (retryable ? MAX_READ_ATTEMPTS : 1); attempt += 1) {
    requestId = createRequestId();
    try {
      response = await fetch(`${engineOrigin}${path}`, {
        ...init,
        headers: {
          "content-type": "application/json",
          ...init?.headers,
          "x-request-id": requestId,
        },
      });
      break;
    } catch (cause) {
      if (cause instanceof DOMException && cause.name === "AbortError") {
        throw cause;
      }
      if (!retryable || attempt === MAX_READ_ATTEMPTS - 1) {
        throw new EngineRequestError(
          retryable
            ? ENGINE_OFFLINE_MESSAGE
            : "The engine connection was lost before this mutation returned; it was not retried automatically to prevent duplicate work.",
          requestId,
          null,
          true,
        );
      }
      await new Promise((resolve) =>
        window.setTimeout(resolve, Math.min(150 * 2 ** attempt, 1_000)),
      );
    }
  }
  if (!response) {
    throw new EngineRequestError(ENGINE_OFFLINE_MESSAGE, requestId, null, true);
  }
  if (!response.ok) {
    let message = `${response.status} ${response.statusText}`;
    try {
      const body = (await response.json()) as { error?: string };
      message = body.error ?? message;
    } catch {
      // Preserve the useful HTTP status if the response is not JSON.
    }
    throw new EngineRequestError(
      message,
      response.headers.get("x-request-id") ?? requestId,
      response.status,
      false,
    );
  }
  const payload = await response.text();
  return (payload ? JSON.parse(payload) : undefined) as T;
}

function createRequestId(): string {
  if (typeof crypto !== "undefined" && typeof crypto.randomUUID === "function") {
    return crypto.randomUUID();
  }
  return `rivet-${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 12)}`;
}

export function health(): Promise<HealthResponse> {
  return request<HealthResponse>("/api/v1/health");
}

export function readiness(): Promise<ReadinessResponse> {
  return request<ReadinessResponse>("/api/v1/ready");
}

export function projects(): Promise<Project[]> {
  return request<Project[]>("/api/v1/projects");
}

export function queueStats(): Promise<QueueStats> {
  return request<QueueStats>("/api/v1/queue");
}

export function queueItems(): Promise<QueueItem[]> {
  return request<QueueItem[]>("/api/v1/queue/items");
}

export function pollRepositoryChanges(
  project: string,
  input: { remote?: string; fetch?: boolean; credential_id?: string } = {},
): Promise<RepositoryPollResponse> {
  return request<RepositoryPollResponse>(
    `/api/v1/projects/${encodeURIComponent(project)}/repository-changes`,
    { method: "POST", body: JSON.stringify(input) },
  );
}

export function pipelineParameters(project: string): Promise<PipelineParameter[]> {
  return request<PipelineParameter[]>(
    `/api/v1/projects/${encodeURIComponent(project)}/parameters`,
  );
}

export function pauseQueue(): Promise<QueueStats> {
  return request<QueueStats>("/api/v1/queue/pause", { method: "POST" });
}

export function resumeQueue(): Promise<QueueStats> {
  return request<QueueStats>("/api/v1/queue/resume", { method: "POST" });
}

export function agents(): Promise<AgentSummary[]> {
  return request<AgentSummary[]>("/api/v1/agents");
}

export function extensions(): Promise<ExtensionManifest[]> {
  return request<ExtensionManifest[]>("/api/v1/extensions");
}

export function extensionStatuses(): Promise<ExtensionRuntimeStatus[]> {
  return request<ExtensionRuntimeStatus[]>("/api/v1/extensions/status");
}

export function startExtension(id: string): Promise<ExtensionRuntimeStatus> {
  return request<ExtensionRuntimeStatus>(
    `/api/v1/extensions/${encodeURIComponent(id)}/start`,
    { method: "POST" },
  );
}

export function stopExtension(id: string): Promise<ExtensionRuntimeStatus> {
  return request<ExtensionRuntimeStatus>(
    `/api/v1/extensions/${encodeURIComponent(id)}/stop`,
    { method: "POST" },
  );
}

export function requestExtension(
  id: string,
  permission: ExtensionManifest["permissions"][number],
  method: string,
  payload: unknown,
): Promise<unknown> {
  return request<unknown>(
    `/api/v1/extensions/${encodeURIComponent(id)}/request`,
    {
      method: "POST",
      body: JSON.stringify({ permission, method, payload }),
    },
  );
}

export function credentials(): Promise<CredentialSummary[]> {
  return request<CredentialSummary[]>("/api/v1/credentials");
}

export function setCredential(
  id: string,
  input: {
    kind: "http_basic" | "ssh_key";
    username: string;
    secret: string;
    projects?: string[];
  },
): Promise<CredentialSummary> {
  return request<CredentialSummary>(
    `/api/v1/credentials/${encodeURIComponent(id)}`,
    { method: "PUT", body: JSON.stringify(input) },
  );
}

export function deleteCredential(id: string): Promise<void> {
  return request<void>(
    `/api/v1/credentials/${encodeURIComponent(id)}`,
    { method: "DELETE" },
  );
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
  input: { name: string; expression: string; trigger?: ScheduleTrigger; enabled?: boolean },
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
