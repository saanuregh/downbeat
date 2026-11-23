//! What a list of queued and running runs says about each one: the reason worked out for every
//! queued state, the running sentences, and the numbers taken from the job and its history. The
//! list and the single-run view must agree.

mod support;

use downbeat_client::types::RunView;
use support::{Database, without_worker};
use uuid::Uuid;

fn id(n: u128) -> Uuid {
    Uuid::from_u128(0x1000 + n)
}

/// Queued and running runs in every state the views work out, newest first by id. Times that
/// sweeps or due checks read are relative to now; the rest are fixed. Worker `w1` serves `default`.
const SEED: &str = "
INSERT INTO downbeat.chain (chain_id, pipeline, first_job, started_by)
VALUES ('00000000-0000-0000-0000-000000000c00', 'basics', 'echo', 'program');

INSERT INTO downbeat.worker (worker_id, role, queues, jobs, build)
VALUES ('w1', 'worker', '{\"default\": 2}',
        '{echo,noop,single,per_account,counts,flaky,waits_for_file,sleeps}', 'test');

INSERT INTO downbeat.pause (kind, name, until, paused_by, note) VALUES
    ('job', 'echo', NULL, 'alice', 'deploy'),
    ('queue', 'quick', '2999-01-01 00:00:00+00', 'bob', 'maintenance'),
    ('job', 'noop', '2020-01-01 00:00:00+00', 'carol', 'ended long ago');

INSERT INTO downbeat.run_history (run_id, job_name, queue, priority, due_at, started_by, chain_id, created_at,
                                  status, reason, started_at, finished_at, dry_run)
SELECT gen_random_uuid(), 'noop', 'default', 0, now(), 'program', '00000000-0000-0000-0000-000000000c00'::uuid, now(),
       'succeeded', 'Succeeded.', now() - interval '10 seconds', now(), false
  FROM generate_series(1, 5) AS n
UNION ALL
SELECT gen_random_uuid(), 'counts', 'default', 0, now(), 'program', '00000000-0000-0000-0000-000000000c00'::uuid, now(),
       'succeeded', 'Succeeded.', now() - interval '10 seconds', now(), false
  FROM generate_series(1, 4);

INSERT INTO downbeat.run (run_id, job_name, queue, state, run_at, due_at, started_by, chain_id, input, turn_key,
                          starts, failures, queued_because, queued_reason, lease_owner, lease_expires_at,
                          function_started_at, first_started_at, progress, checkpoint, slow_reported, created_at)
SELECT run_id::uuid, job, queue, state, run_at, '2026-01-01 00:00:00+00', 'program',
       '00000000-0000-0000-0000-000000000c00'::uuid, '{\"n\": 1}'::jsonb, turn_key, starts, failures, because, reason, owner,
       CASE WHEN state = 'running' THEN now() + interval '1 hour' END, function_started, first_started, progress,
       checkpoint, slow, now() - interval '1 second' * n
  FROM (VALUES
    (1, '00000000-0000-0000-0000-000000001001', 'echo', 'default', 'queued', '2026-01-01 00:00:00+00'::timestamptz,
        NULL, 0, 0, NULL, NULL, NULL, NULL::timestamptz, NULL::timestamptz, NULL, NULL::jsonb, false),
    (2, '00000000-0000-0000-0000-000000001002', 'noop', 'quick', 'queued', '2999-01-01 00:00:00+00',
        NULL, 0, 0, 'scheduled_later', NULL, NULL, NULL, NULL, NULL, NULL, false),
    (3, '00000000-0000-0000-0000-000000001003', 'single', 'default', 'queued', '2026-01-01 00:00:00+00',
        'single', 0, 0, NULL, NULL, NULL, NULL, NULL, NULL, NULL, false),
    (4, '00000000-0000-0000-0000-000000001004', 'per_account', 'default', 'queued', '2026-01-01 00:00:00+00',
        'per_account:acme', 0, 0, 'waiting_turn', NULL, NULL, NULL, NULL, NULL, NULL, false),
    (5, '00000000-0000-0000-0000-000000001005', 'counts', 'default', 'queued', '2999-01-01 00:00:00+00',
        'counts:x', 0, 0, 'not_ready', 'told', NULL, NULL, NULL, NULL, NULL, false),
    (6, '00000000-0000-0000-0000-000000001006', 'flaky', 'default', 'queued', '2999-01-01 00:00:00+00',
        'flaky:alone', 1, 1, 'waiting_turn', 'stale', NULL, NULL, '2026-01-01 00:00:00+00', NULL, NULL, false),
    (7, '00000000-0000-0000-0000-000000001007', 'waits_for_file', 'default', 'queued', '2999-01-01 00:00:00+00',
        NULL, 0, 0, 'not_ready', 'told to wait', NULL, NULL, NULL, NULL, NULL, false),
    (8, '00000000-0000-0000-0000-000000001008', 'waits_for_file', 'default', 'queued', '2999-01-01 12:34:56+00',
        NULL, 0, 0, 'scheduled_later', NULL, NULL, NULL, NULL, NULL, NULL, false),
    (9, '00000000-0000-0000-0000-000000001009', 'single', 'default', 'running', '2026-01-01 00:00:00+00',
        'single', 1, 0, NULL, NULL, 'w1', now() - interval '2 hours', '2026-01-01 00:00:00+00', 'half', '{}', true),
    (10, '00000000-0000-0000-0000-00000000100a', 'per_account', 'default', 'running', '2026-01-01 00:00:00+00',
        'per_account:acme', 1, 0, NULL, NULL, 'w1', NULL, '2026-01-01 00:00:00+00', NULL, NULL, false),
    (11, '00000000-0000-0000-0000-00000000100b', 'counts', 'default', 'queued', '2026-01-01 00:00:00+00',
        'counts:x', 1, 0, NULL, NULL, NULL, NULL, '2026-01-01 00:00:00+00', NULL, NULL, false),
    (12, '00000000-0000-0000-0000-00000000100c', 'sleeps', 'nobody_serves', 'queued', '2026-01-01 00:00:00+00',
        NULL, 0, 0, 'not_ready', 'told', NULL, NULL, NULL, NULL, NULL, false),
    (13, '00000000-0000-0000-0000-00000000100d', 'noop', 'default', 'running', '2026-01-01 00:00:00+00',
        NULL, 1, 0, NULL, NULL, NULL, NULL, '2026-01-01 00:00:00+00', NULL, NULL, false),
    (14, '00000000-0000-0000-0000-00000000100e', 'noop', 'default', 'queued', '2026-01-01 00:00:00+00',
        NULL, 0, 0, NULL, NULL, NULL, NULL, NULL, NULL, NULL, false)
  ) AS seed (n, run_id, job, queue, state, run_at, turn_key, starts, failures, because, reason, owner,
             function_started, first_started, progress, checkpoint, slow);
";

/// What a view says, with how long something has waited or run written as `{elapsed}`.
#[derive(Debug, PartialEq)]
struct Said {
    run_id: Uuid,
    status: String,
    queued_because: Option<String>,
    reason: String,
    attempt: i32,
    attempts: i32,
    calls: i32,
    expected_seconds: Option<i64>,
    pipeline: Option<String>,
    slow: bool,
    has_checkpoint: bool,
    progress: Option<String>,
}

fn said(view: &RunView) -> Said {
    Said {
        run_id: view.run_id,
        status: view.status.clone(),
        queued_because: view.queued_because.clone(),
        reason: masked(&view.reason),
        attempt: view.attempt,
        attempts: view.attempts,
        calls: view.calls,
        expected_seconds: view.expected_seconds,
        pipeline: view.pipeline.clone(),
        slow: view.slow,
        has_checkpoint: view.has_checkpoint,
        progress: view.progress.clone(),
    }
}

/// The sentence with its elapsed time, such as `2h0m3s` after "Queued " or "for ", as `{elapsed}`.
fn masked(reason: &str) -> String {
    for lead in ["Queued ", " for "] {
        if let Some(at) = reason.find(lead) {
            let start = at + lead.len();
            let rest = &reason[start..];
            let end = rest.find(|c: char| !c.is_ascii_alphanumeric()).unwrap_or(rest.len());
            let token = &rest[..end];
            if !token.is_empty()
                && token.starts_with(|c: char| c.is_ascii_digit())
                && token.chars().all(|c| c.is_ascii_digit() || "dhms".contains(c))
            {
                return format!("{}{{elapsed}}{}", &reason[..start], &rest[end..]);
            }
        }
    }
    reason.to_owned()
}

#[allow(clippy::too_many_arguments)]
fn expect(
    n: u128,
    status: &str,
    because: Option<&str>,
    reason: &str,
    (attempt, attempts, calls): (i32, i32, i32),
    expected_seconds: Option<i64>,
    pipeline: &str,
    (slow, has_checkpoint, progress): (bool, bool, Option<&str>),
) -> Said {
    Said {
        run_id: id(n),
        status: status.to_owned(),
        queued_because: because.map(str::to_owned),
        reason: reason.to_owned(),
        attempt,
        attempts,
        calls,
        expected_seconds,
        pipeline: Some(pipeline.to_owned()),
        slow,
        has_checkpoint,
        progress: progress.map(str::to_owned),
    }
}

/// Our runs in a page, in its order; the scheduler may have started others meanwhile.
async fn listed(client: &downbeat_client::Client) -> Vec<RunView> {
    let page = client.runs("limit=200").await.unwrap();
    page.items.into_iter().filter(|view| (1..=14).any(|n| view.run_id == id(n))).collect()
}

/// What the list says about each seeded run, in its order.
fn expected() -> Vec<Said> {
    let plain = (false, false, None);
    let echo_held = "Held: paused by alice until resumed (deploy).";
    let busy = "Queued {elapsed}: every slot on queue default is busy.";
    vec![
        expect(1, "queued", Some("paused"), echo_held, (1, 1, 0), None, "basics", plain),
        expect(
            2,
            "queued",
            Some("paused"),
            "Held: paused by bob until 2999-01-01 00:00 UTC (maintenance).",
            (1, 1, 0),
            Some(10),
            "basics",
            plain,
        ),
        expect(
            3,
            "queued",
            Some("waiting_turn"),
            "Queued: waiting for run 00001009; this job runs one at a time.",
            (1, 1, 0),
            None,
            "failures",
            plain,
        ),
        expect(
            4,
            "queued",
            Some("waiting_turn"),
            "Queued: waiting for run 0000100a; this job runs one at a time per account.",
            (1, 1, 0),
            None,
            "failures",
            plain,
        ),
        expect(
            5,
            "queued",
            Some("waiting_turn"),
            "Queued: waiting for run 0000100b, which is doing the same work.",
            (1, 1, 0),
            None,
            "failures",
            plain,
        ),
        expect(6, "queued", Some("no_free_worker"), busy, (2, 3, 1), None, "failures", plain),
        expect(7, "queued", Some("not_ready"), "told to wait", (1, 1, 0), None, "failures", plain),
        expect(8, "queued", Some("scheduled_later"), "Queued until 12:34:56 UTC.", (1, 1, 0), None, "failures", plain),
        expect(
            9,
            "running",
            None,
            "Running on w1 for {elapsed}.",
            (1, 1, 1),
            None,
            "failures",
            (true, true, Some("half")),
        ),
        expect(10, "running", None, "Claimed by w1; preparing.", (1, 1, 1), None, "failures", plain),
        expect(11, "queued", Some("no_free_worker"), busy, (1, 1, 1), None, "failures", plain),
        expect(
            12,
            "queued",
            Some("no_free_worker"),
            "Queued {elapsed}: no running worker serves queue nobody_serves.",
            (1, 1, 0),
            None,
            "failures",
            plain,
        ),
        expect(13, "running", None, "Running.", (1, 1, 1), Some(10), "basics", plain),
        expect(14, "queued", Some("no_free_worker"), busy, (1, 1, 0), Some(10), "basics", plain),
    ]
}

#[tokio::test]
async fn a_list_says_why_each_unfinished_run_waits_and_agrees_with_the_single_run_view() {
    let database = Database::fresh().await;
    sqlx::raw_sql(SEED).execute(&database.pool).await.unwrap();
    let (_app, client) = without_worker(&database).await;

    let views = listed(&client).await;
    assert_eq!(views.iter().map(said).collect::<Vec<_>>(), expected());
    for view in &views {
        let mut single = client.run(view.run_id).await.unwrap();
        assert!(single.detail.take().is_some(), "the single-run view has its detail");
        single.reason = masked(&single.reason);
        let mut listed = view.clone();
        listed.reason = masked(&listed.reason);
        assert_eq!(single, listed, "the list and the single-run view agree on {}", view.run_id);
    }

    let queues = client.as_caller().queues().await.unwrap();
    let counts = |name: &str| queues.iter().find(|queue| queue.name == name).unwrap().queued_because.clone();
    assert_eq!(counts("quick"), [("paused".to_owned(), 1)].into());
    assert_eq!(counts("nobody_serves"), [("no_free_worker".to_owned(), 1)].into());

    client
        .as_caller()
        .pause_everything(&downbeat_client::types::PauseRequest {
            note: "all stop".into(),
            until: None,
            until_resumed: true,
        })
        .await
        .unwrap();
    let everything = "Held: paused by local until resumed (all stop).";
    for view in listed(&client).await {
        if view.status == "queued" {
            assert_eq!(
                (view.queued_because.as_deref(), view.reason.as_str()),
                (Some("paused"), everything),
                "{}",
                view.run_id
            );
        }
    }
}
