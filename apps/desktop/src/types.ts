export type BuildStatus =
  | "pending"
  | "queued"
  | "running"
  | "passed"
  | "failed"
  | "cancelled";

export type StageStatus =
  | "pending"
  | "running"
  | "passed"
  | "failed"
  | "cancelled"
  | "skipped";

export type StepStatus = StageStatus;
export type LogStream = "stdout" | "stderr" | "system";

export interface Project {
  id: string;
  name: string;
  repository_path: string;
  pipeline_path: string;
  created_at: string;
}

export interface SourceSnapshot {
  provider: string;
  revision: string;
  reference: string | null;
  remote: string | null;
  dirty: boolean;
}

export interface BuildRecord {
  id: string;
  project_id: string;
  number: number;
  status: BuildStatus;
  queued_at: string;
  started_at: string | null;
  finished_at: string | null;
  source: SourceSnapshot | null;
}

export interface StepRecord {
  id: string;
  stage_id: string;
  position: number;
  name: string;
  status: StepStatus;
  exit_code: number | null;
  started_at: string | null;
  finished_at: string | null;
}

export interface StageRecord {
  id: string;
  build_id: string;
  position: number;
  name: string;
  status: StageStatus;
  started_at: string | null;
  finished_at: string | null;
}

export interface StageDetails {
  stage: StageRecord;
  steps: StepRecord[];
}

export interface BuildDetails {
  build: BuildRecord;
  stages: StageDetails[];
}

export interface LogRecord {
  sequence: number;
  build_id: string;
  timestamp: string;
  stream: LogStream;
  line: string;
}

export interface QueueResponse {
  build: BuildRecord;
  status: BuildStatus;
}

export interface QueueStats {
  queued: number;
  running: number;
  capacity: number;
}
