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

export interface ArtifactRecord {
  id: string;
  build_id: string;
  name: string;
  relative_path: string;
  size_bytes: number;
  checksum: string;
  created_at: string;
}

export interface ScheduleRecord {
  id: string;
  project_id: string;
  name: string;
  expression: string;
  trigger: ScheduleTrigger;
  poll: SchedulePollConfig | null;
  enabled: boolean;
  next_run_at: string;
  last_run_at: string | null;
  created_at: string;
}

export type ScheduleTrigger = "build" | "repository_poll";

export interface SchedulePollConfig {
  remote: string;
  fetch: boolean;
  credential_id: string | null;
}

export interface QueueResponse {
  build: BuildRecord;
  status: BuildStatus;
}

export type RepositoryPollStatus =
  | "queued"
  | "unchanged"
  | "already_queued"
  | "already_checking";

export interface RepositoryPollResponse {
  status: RepositoryPollStatus;
  changed: boolean;
  deduplicated: boolean;
  revision: string;
  reference: string | null;
  build: BuildRecord | null;
}

export interface QueueStats {
  queued: number;
  running: number;
  capacity: number;
  paused: boolean;
}

export interface QueueItem {
  build_id: string;
  project_id: string;
  project: string;
  priority: number;
  position: number;
}

export interface PipelineParameter {
  name: string;
  kind: "string" | "text" | "boolean" | "choice" | "password";
  secret: boolean;
  default: string | null;
  required: boolean;
  choices: string[];
}

export interface CredentialSummary {
  id: string;
  kind: "http_basic" | "ssh_key";
  username: string;
  /** Empty means the credential is available to every project. */
  projects: string[];
}

export type AgentStatus = "online" | "stale";

export interface AgentCapabilities {
  os: string;
  arch: string;
  docker: boolean;
  labels: string[];
  executors: number;
}

export interface AgentSummary {
  agent_id: string;
  name: string;
  protocol_version: number;
  capabilities: AgentCapabilities;
  connected_at: string;
  last_heartbeat: string;
  last_sequence: number;
  running: string[];
  reserved?: string[];
  available_executors: number;
  status: AgentStatus;
}

export type MigrationSupportLevel = "supported" | "partial" | "unsupported";

export interface MigrationFinding {
  kind: string;
  status: MigrationSupportLevel;
  line: number;
  evidence: string;
  message: string;
  rivet_mapping?: string;
}

export interface MigrationAnalysis {
  analyzer_version: number;
  status: MigrationSupportLevel;
  summary: {
    supported: number;
    partial: number;
    unsupported: number;
  };
  constructs: MigrationFinding[];
  recommendations: string[];
}

export interface RivetfileDraft {
  status: MigrationSupportLevel;
  converted_steps: number;
  skipped_stages: string[];
  warnings: string[];
  rivetfile_toml?: string;
}

export interface MigrationResponse {
  analysis: MigrationAnalysis;
  draft?: RivetfileDraft;
}
