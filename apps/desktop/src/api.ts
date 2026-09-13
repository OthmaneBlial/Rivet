import type {
  BuildDetails,
  BuildRecord,
  LogRecord,
  Project,
  QueueResponse,
} from "./types";

export const ENGINE_ORIGIN =
  import.meta.env.VITE_RIVET_ENGINE_URL ?? "http://127.0.0.1:7878";

export interface HealthResponse {
  status: string;
  service: string;
  timestamp: string;
}

async function request<T>(path: string, init?: RequestInit): Promise<T> {
  const response = await fetch(`${ENGINE_ORIGIN}${path}`, {
    ...init,
    headers: {
      "content-type": "application/json",
      ...init?.headers,
    },
  });
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
  return (await response.json()) as T;
}

export function health(): Promise<HealthResponse> {
  return request<HealthResponse>("/api/v1/health");
}

export function projects(): Promise<Project[]> {
  return request<Project[]>("/api/v1/projects");
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

export function queueBuild(project: string): Promise<QueueResponse> {
  return request<QueueResponse>(
    `/api/v1/projects/${encodeURIComponent(project)}/builds`,
    { method: "POST", body: "{}" },
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
    ENGINE_ORIGIN,
  );
  url.protocol = url.protocol === "https:" ? "wss:" : "ws:";
  return url.toString();
}
