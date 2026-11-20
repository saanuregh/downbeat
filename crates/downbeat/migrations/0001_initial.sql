-- The engine's tables. Every table lives in the `downbeat` schema of the product's database.
-- Rule: a run is in exactly one of `run` (unfinished) and `run_history` (finished).

CREATE TABLE downbeat.chain (
    chain_id     uuid PRIMARY KEY,
    pipeline     text NOT NULL,
    first_job    text NOT NULL,
    started_at   timestamptz NOT NULL DEFAULT now(),
    started_by   text NOT NULL,
    backfill_id  bigint,
    cancelled_at timestamptz,
    cancelled_by text
);

-- A run's call log keeps its last ten calls, oldest first.
CREATE FUNCTION downbeat.append_call(log jsonb, call jsonb) RETURNS jsonb
LANGUAGE sql IMMUTABLE AS $$
    SELECT COALESCE(jsonb_agg(e ORDER BY n), '[]'::jsonb)
      FROM (SELECT e, n FROM jsonb_array_elements(log || jsonb_build_array(call)) WITH ORDINALITY AS t(e, n)
             ORDER BY n DESC LIMIT 10) newest
$$;

CREATE TABLE downbeat.run (
    run_id              uuid PRIMARY KEY,
    job_name            text NOT NULL,
    queue               text NOT NULL,
    priority            smallint NOT NULL DEFAULT 0,
    state               text NOT NULL DEFAULT 'queued' CHECK (state IN ('queued', 'running')),
    run_at              timestamptz NOT NULL DEFAULT now(),
    due_at              timestamptz NOT NULL,
    expires_at          timestamptz,
    started_by          text NOT NULL CHECK (started_by IN ('schedule', 'link', 'person', 'program', 'backfill', 'job')),
    schedule            text,
    chain_id            uuid NOT NULL REFERENCES downbeat.chain ON DELETE CASCADE,
    parents             uuid[] NOT NULL DEFAULT '{}',
    input               jsonb,
    input_hash          text,
    same_work_key       text,
    turn_key            text,
    dry_run             boolean NOT NULL DEFAULT false,
    ignore_conditions   boolean NOT NULL DEFAULT false,
    without_children    boolean NOT NULL DEFAULT false,
    -- A person's longer timeout for this run; zero means no timeout.
    timeout_override    interval,
    starts              integer NOT NULL DEFAULT 0,
    failures            smallint NOT NULL DEFAULT 0,
    lost_count          smallint NOT NULL DEFAULT 0,
    interruptions       smallint NOT NULL DEFAULT 0,
    fence               bigint NOT NULL DEFAULT 0,
    lease_owner         text,
    lease_expires_at    timestamptz,
    function_started_at timestamptz,
    first_started_at    timestamptz,
    queued_because      text,
    queued_reason       text,
    progress            text,
    checkpoint          jsonb,
    -- Who asked for the running function to stop; null when nobody has.
    cancel_requested_by text,
    slow_reported       boolean NOT NULL DEFAULT false,
    call_log            jsonb NOT NULL DEFAULT '[]',
    actor               text,
    created_at          timestamptz NOT NULL DEFAULT now(),
    claimed_at          timestamptz
);

-- The claim: the most urgent due run in a queue.
CREATE INDEX run_claim ON downbeat.run (queue, priority DESC, run_at) WHERE state = 'queued';
-- Rule 1: the same work has at most one run that has not begun.
CREATE UNIQUE INDEX run_same_work ON downbeat.run (same_work_key) WHERE starts = 0 AND same_work_key IS NOT NULL;
-- Rule 2: one run at a time holds a turn, from its first call until it ends.
CREATE UNIQUE INDEX run_turn ON downbeat.run (turn_key) WHERE starts > 0;
-- "Has this child already been created in this chain?"
CREATE INDEX run_job_chain ON downbeat.run (job_name, chain_id);
-- Reading and cancelling a chain.
CREATE INDEX run_chain ON downbeat.run (chain_id);

CREATE TABLE downbeat.run_history (
    run_id           uuid PRIMARY KEY,
    job_name         text NOT NULL,
    queue            text NOT NULL,
    priority         smallint NOT NULL,
    due_at           timestamptz NOT NULL,
    started_by       text NOT NULL,
    schedule         text,
    chain_id         uuid NOT NULL REFERENCES downbeat.chain ON DELETE CASCADE,
    parents          uuid[] NOT NULL DEFAULT '{}',
    input            jsonb,
    input_hash       text,
    turn_key         text,
    dry_run          boolean NOT NULL DEFAULT false,
    ignore_conditions boolean NOT NULL DEFAULT false,
    without_children boolean NOT NULL DEFAULT false,
    starts           integer NOT NULL DEFAULT 0,
    failures         smallint NOT NULL DEFAULT 0,
    lost_count       smallint NOT NULL DEFAULT 0,
    interruptions    smallint NOT NULL DEFAULT 0,
    call_log         jsonb NOT NULL DEFAULT '[]',
    actor            text,
    created_at       timestamptz NOT NULL,
    status           text NOT NULL CHECK (status IN ('succeeded', 'skipped', 'failed', 'cancelled', 'expired')),
    skipped_because  text,
    failed_because   text,
    reason           text NOT NULL,
    error_chain      text[],
    output           jsonb,
    stderr_tail      text,
    started_at       timestamptz,
    finished_at      timestamptz NOT NULL DEFAULT now(),
    -- Who recorded the outcome when it was not the job ("mark as succeeded"), and their note.
    marked_by        text,
    note             text,
    -- For a request that joined a queued run: that run.
    joined_run       uuid,
    notify_after     timestamptz
);

-- A job's runs; "last success" for needs, if_changed and expect_success_every.
CREATE INDEX run_history_job ON downbeat.run_history (job_name, finished_at DESC);
-- The runs list.
CREATE INDEX run_history_finished ON downbeat.run_history (finished_at DESC);
-- Reading a chain; how a child's parents ended.
CREATE INDEX run_history_chain ON downbeat.run_history (chain_id);
-- Failures.
CREATE INDEX run_history_failed ON downbeat.run_history (finished_at DESC) WHERE status = 'failed';
-- The calendar: what each occurrence of a schedule became.
CREATE INDEX run_history_schedule ON downbeat.run_history (schedule, due_at) WHERE schedule IS NOT NULL;
-- Delivering on_finish.
CREATE INDEX run_history_notify ON downbeat.run_history (notify_after) WHERE notify_after IS NOT NULL;

CREATE TABLE downbeat.schedule_state (
    schedule    text PRIMARY KEY,
    watermark   timestamptz NOT NULL,
    next_due_at timestamptz,
    seen_at     timestamptz NOT NULL DEFAULT now(),
    -- A digest of the schedule's times as last registered; a change starts it again from now.
    times_hash  text NOT NULL DEFAULT ''
);

CREATE TABLE downbeat.schedule_override (
    override_id  bigserial PRIMARY KEY,
    schedule     text NOT NULL,
    on_date      date NOT NULL,
    change       text NOT NULL CHECK (change IN ('off', 'times', 'window')),
    times        time[] NOT NULL DEFAULT '{}',
    note         text NOT NULL,
    created_by   text NOT NULL,
    created_at   timestamptz NOT NULL DEFAULT now(),
    UNIQUE (schedule, on_date)
);

CREATE TABLE downbeat.pause (
    kind       text NOT NULL CHECK (kind IN ('schedule', 'job', 'queue')),
    name       text NOT NULL,
    until      timestamptz,
    paused_by  text NOT NULL,
    note       text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (kind, name)
);

CREATE TABLE downbeat.worker (
    worker_id    text PRIMARY KEY,
    role         text NOT NULL,
    queues       jsonb NOT NULL DEFAULT '{}',
    jobs         text[] NOT NULL DEFAULT '{}',
    build        text NOT NULL,
    started_at   timestamptz NOT NULL DEFAULT now(),
    last_seen_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE downbeat.backfill (
    backfill_id         bigserial PRIMARY KEY,
    job_name            text NOT NULL,
    schedule            text NOT NULL,
    from_date           date NOT NULL,
    to_date             date NOT NULL,
    next_date           date NOT NULL,
    current_chain       uuid,
    continue_on_failure boolean NOT NULL DEFAULT false,
    state               text NOT NULL DEFAULT 'running' CHECK (state IN ('running', 'done', 'failed', 'cancelled')),
    reason              text,
    created_by          text NOT NULL,
    created_at          timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE downbeat.idempotency (
    actor      text NOT NULL,
    job_name   text NOT NULL,
    key        text NOT NULL,
    run_id     uuid NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (actor, job_name, key)
);

CREATE TABLE downbeat.used_token (
    caller     text NOT NULL,
    jti        text NOT NULL,
    expires_at timestamptz NOT NULL,
    PRIMARY KEY (caller, jti)
);

CREATE TABLE downbeat.audit (
    audit_id     bigserial PRIMARY KEY,
    at           timestamptz NOT NULL DEFAULT now(),
    actor        text NOT NULL,
    action       text NOT NULL,
    job_name     text,
    run_id       uuid,
    schedule     text,
    detail       jsonb NOT NULL DEFAULT '{}',
    notify_after timestamptz
);

CREATE INDEX audit_at ON downbeat.audit (at DESC);
CREATE INDEX audit_notify ON downbeat.audit (notify_after) WHERE notify_after IS NOT NULL;
