// The JSON the screens read. Scheduler types mirror crates/downbeat-api/src/types.rs; console
// types mirror docs/console-contract.md. Times are RFC 3339 strings, dates are "YYYY-MM-DD".

export type Json =
  null | boolean | number | string | Json[] | { [key: string]: Json };

export type Scope = "read" | "operate" | "administer";

export interface SchedulerAccess {
  name: string;
  label: string;
  scope: Scope | null;
  reachable: boolean;
  api_version: number;
}

export interface Me {
  person: { email: string; name: string };
  csrf_token: string;
  local: boolean;
  schedulers: SchedulerAccess[];
}

export interface Preferences {
  time_zone: string | null;
  filters: Record<string, unknown>;
}

export interface Grant {
  grant_id: number | string;
  to_person: string | null;
  to_group: string | null;
  scheduler: string;
  scope: Scope;
  pipelines: string[] | null;
  created_by: string;
  created_at: string;
}

export interface Person {
  email: string;
  name: string;
  groups: string[];
  last_seen_at: string | null;
}

export interface ConsoleAuditEntry {
  audit_id: number;
  at: string;
  actor: string;
  action: string;
  detail: Json;
}

export interface Page<T> {
  items: T[];
  next_cursor?: string | null;
}

export type RunStatus =
  | "queued"
  | "running"
  | "succeeded"
  | "skipped"
  | "failed"
  | "cancelled"
  | "expired";

export interface RunView {
  run_id: string;
  job: string;
  pipeline: string | null;
  chain_id: string;
  status: string;
  queued_because: string | null;
  skipped_because: string | null;
  failed_because: string | null;
  reason: string;
  started_by: string;
  actor: string | null;
  schedule: string | null;
  queue: string;
  priority: number;
  due_at: string;
  created_at: string;
  started_at: string | null;
  finished_at: string | null;
  attempt: number;
  attempts: number;
  calls: number;
  lost_count: number;
  interruptions: number;
  dry_run: boolean;
  progress: string | null;
  has_checkpoint: boolean;
  slow: boolean;
  expected_seconds: number | null;
  input: Json | null;
  output: Json | null;
  log_link: string | null;
  marked_by: string | null;
  marked_at?: string | null;
  note: string | null;
  detail?: RunDetail | null;
}

export interface RunDetail {
  error_chain: string[];
  stderr_tail: string | null;
  call_log: CallView[];
  parents: string[];
  children: string[];
}

export interface CallView {
  worker: string;
  build: string;
  started_at: string;
  ended_at: string;
  ended: string;
}

export interface FieldShape {
  name: string;
  type_name: string;
  compared: boolean;
}

export interface Shape {
  type_name: string;
  fields?: FieldShape[];
}

export interface JobView {
  name: string;
  pipeline: string;
  kind: string;
  queue: string;
  triggers: string[];
  policies: string[];
  input: Json;
  output: Json;
  /** The input as a JSON Schema, for callers in other languages. */
  input_schema: unknown;
  supports_dry_run: boolean;
  last_run: RunSummary | null;
  next_occurrence: string | null;
  expected_seconds: number | null;
  paused: PauseView | null;
  overdue: boolean;
}

export interface RunSummary {
  run_id: string;
  status: string;
  reason: string;
  at: string;
}

export interface PauseView {
  by: string;
  note: string;
  until: string | null;
  since: string;
}

export interface PipelineView {
  name: string;
  jobs: PipelineNode[];
  recent_chains: ChainSummary[];
}

export interface PipelineNode {
  job: string;
  link: string | null;
  parents: string[];
  schedules: string[];
}

export interface ChainSummary {
  chain_id: string;
  first_job: string;
  started_at: string;
  statuses: Record<string, string>;
  cancelled: boolean;
}

export interface ChainView {
  chain_id: string;
  pipeline: string;
  first_job: string;
  started_at: string;
  cancelled_at: string | null;
  cancelled_by: string | null;
  nodes: ChainNode[];
}

export interface ChainNode {
  job: string;
  parents: string[];
  runs: RunSummary[];
  why_not: string | null;
}

export interface ScheduleView {
  name: string;
  job: string;
  times: string;
  next_occurrence: string | null;
  paused: PauseView | null;
}

export interface CalendarDay {
  date: string;
  entries: CalendarEntry[];
  days_off: DayOff[];
}

export interface CalendarEntry {
  schedule: string;
  job: string;
  at: string;
  source: string;
  note: string | null;
  paused: boolean;
  run: RunSummary | null;
}

export interface DayOff {
  schedule: string;
  reason: string;
}

export interface QueueView {
  name: string;
  queued: number;
  running: number;
  queued_because: Record<string, number>;
  longest_wait_seconds: number | null;
  workers: WorkerView[];
  paused: PauseView | null;
}

export interface WorkerView {
  worker_id: string;
  slots: number;
  build: string;
  last_seen_at: string;
}

export interface BackfillView {
  backfill_id: number;
  job: string;
  schedule: string;
  from: string;
  to: string;
  reached: string;
  state: string;
  reason: string | null;
  continue_on_failure: boolean;
  created_by: string;
  created_at: string;
}

export interface AuditEntry {
  audit_id: number;
  at: string;
  actor: string;
  action: string;
  job: string | null;
  run_id: string | null;
  schedule: string | null;
  detail: Json;
}

export interface OverrideView {
  override_id: number;
  schedule: string;
  date: string;
  change: string;
  times: string[];
  note: string;
  created_by: string;
}

export interface Preview {
  effects: string[];
  false_conditions?: string[];
}

export interface Done {
  message: string;
  run_id?: string;
  id?: number;
}

export interface StartResponse {
  run_id: string;
  status: string;
  joined?: boolean;
  repeated?: boolean;
}
