// One job: what starts it, its conditions and policies in words, its runs, and its actions.
import { ActionButton } from "../components/action";
import {
  BackfillButton,
  PauseButton,
  ResumeButton,
} from "../components/action-forms";
import { RunTable } from "../components/run-table";
import { StartButton } from "../components/start-dialog";
import {
  Empty,
  ErrorNote,
  Facts,
  Loading,
  PageHeader,
  PauseNote,
  Section,
  StatusBadge,
  Tag,
  Time,
} from "../components/ui";
import { formatDuration, secondsBetween } from "../format";
import { useLoad } from "../load";
import { Link } from "../router";
import { useScheduler } from "../session";
import { asShape } from "../shape-form";
import type {
  BackfillView,
  JobView,
  Json,
  Page,
  RunView,
  ScheduleView,
} from "../types";

export function JobScreen({ params }: { params: Record<string, string> }) {
  const name = params.name ?? "";
  const { api, href, can } = useScheduler();
  const job = useLoad<JobView>(api(`/jobs/${encodeURIComponent(name)}`));
  const runs = useLoad<Page<RunView>>(api("/runs", { job: name, limit: 20 }));
  const schedules = useLoad<ScheduleView[]>(api("/schedules"));
  const backfills = useLoad<BackfillView[]>(api("/backfills"));
  const reload = () => {
    job.reload();
    runs.reload();
    schedules.reload();
    backfills.reload();
  };

  if (job.error) return <ErrorNote error={job.error} onRetry={job.reload} />;
  if (!job.data) return <Loading />;
  const view = job.data;
  const ownSchedules = (schedules.data ?? []).filter(
    (schedule) => schedule.job === name,
  );
  const ownBackfills = (backfills.data ?? []).filter(
    (backfill) => backfill.job === name,
  );

  return (
    <>
      <PageHeader
        title={view.name}
        subtitle={
          <>
            {view.kind === "command" ? "Command" : "Function"} job in pipeline{" "}
            <Link to={href(`/pipelines/${encodeURIComponent(view.pipeline)}`)}>
              {view.pipeline}
            </Link>
            , on queue {view.queue}
          </>
        }
        actions={
          <>
            {can("operate") && <StartButton job={view} />}
            {can("administer") &&
              (view.paused ? (
                <ResumeButton kind="job" name={view.name} onDone={reload} />
              ) : (
                <PauseButton kind="job" name={view.name} onDone={reload} />
              ))}
            {can("administer") && ownSchedules.length > 0 && (
              <BackfillButton
                job={view.name}
                schedules={ownSchedules}
                onDone={reload}
              />
            )}
          </>
        }
      />
      {view.paused && <PauseNote pause={view.paused} what="This job is" />}
      {view.overdue && (
        <p className="note note-bad">
          This job is overdue: it has not succeeded as often as it is expected
          to.
        </p>
      )}

      <Facts
        items={[
          [
            "Last run",
            view.last_run ? (
              <span className="inline-status">
                <StatusBadge status={view.last_run.status} />
                <Link to={href(`/runs/${view.last_run.run_id}`)}>
                  <Time at={view.last_run.at} />
                </Link>
              </span>
            ) : (
              "Never"
            ),
          ],
          [
            "Next",
            view.next_occurrence ? (
              <Time at={view.next_occurrence} />
            ) : (
              "Not scheduled"
            ),
          ],
          ["Expected duration", formatDuration(view.expected_seconds)],
          ["Dry run", view.supports_dry_run ? "Supported" : "Not supported"],
        ]}
      />
      {view.last_run && <p className="reason">{view.last_run.reason}</p>}

      <div className="columns">
        <Section title="What starts it">
          {view.triggers.length === 0 ? (
            <Empty>Only a person or a program starts it.</Empty>
          ) : (
            <Sentences items={view.triggers} />
          )}
        </Section>
        <Section title="Policies">
          {view.policies.length === 0 ? (
            <Empty>The defaults.</Empty>
          ) : (
            <Sentences items={view.policies} />
          )}
        </Section>
      </div>

      <div className="columns">
        <Section title="Input">
          <ShapeView value={view.input} />
        </Section>
        <Section title="Output">
          <ShapeView value={view.output} />
        </Section>
      </div>

      <Section
        title="Recent runs"
        actions={
          <Link to={href(`/runs?job=${encodeURIComponent(name)}`)}>
            All runs of {name}
          </Link>
        }
      >
        {runs.error && <ErrorNote error={runs.error} onRetry={runs.reload} />}
        {runs.data && runs.data.items.length === 0 && (
          <Empty>No runs yet.</Empty>
        )}
        {runs.data && runs.data.items.length > 0 && (
          <>
            <DurationBars
              runs={runs.data.items}
              expected={view.expected_seconds}
            />
            <RunTable runs={runs.data.items} showJob={false} />
          </>
        )}
      </Section>

      <Section title="Schedules">
        {ownSchedules.length === 0 ? (
          <Empty>No schedule starts this job.</Empty>
        ) : (
          <ul className="cards">
            {ownSchedules.map((schedule) => (
              <li key={schedule.name} className="card">
                <p>
                  <strong>{schedule.name}</strong>: {schedule.times}
                  {schedule.paused && <Tag tone="wait">paused</Tag>}
                </p>
                <p className="card-meta">
                  Next:{" "}
                  {schedule.next_occurrence ? (
                    <Time at={schedule.next_occurrence} />
                  ) : (
                    "none"
                  )}
                  {can("administer") &&
                    (schedule.paused ? (
                      <ResumeButton
                        kind="schedule"
                        name={schedule.name}
                        onDone={reload}
                      />
                    ) : (
                      <PauseButton
                        kind="schedule"
                        name={schedule.name}
                        onDone={reload}
                      />
                    ))}
                </p>
                {schedule.paused && (
                  <PauseNote pause={schedule.paused} what="This schedule is" />
                )}
              </li>
            ))}
          </ul>
        )}
      </Section>

      {ownBackfills.length > 0 && (
        <Section title="Backfills">
          <ul className="cards">
            {ownBackfills.map((backfill) => (
              <li key={backfill.backfill_id} className="card">
                <p className="reason">
                  {backfill.reason ??
                    `${backfill.from} to ${backfill.to}, ${backfill.state}; reached ${backfill.reached}.`}
                </p>
                <p className="card-meta">
                  <Tag tone={backfill.state === "running" ? "run" : "neutral"}>
                    {backfill.state}
                  </Tag>
                  {backfill.from} to {backfill.to} · reached {backfill.reached}{" "}
                  · by {backfill.created_by}
                  {can("administer") && backfill.state === "running" && (
                    <ActionButton
                      label="Cancel backfill"
                      small
                      spec={{
                        title: `Cancel backfill of ${backfill.job}`,
                        confirm: "Cancel backfill",
                        path: api(`/backfills/${backfill.backfill_id}/cancel`),
                        danger: true,
                      }}
                      onDone={reload}
                    />
                  )}
                </p>
              </li>
            ))}
          </ul>
        </Section>
      )}
    </>
  );
}

function Sentences({ items }: { items: string[] }) {
  return (
    <ul className="sentences">
      {items.map((item, index) => (
        <li key={index}>{item}</li>
      ))}
    </ul>
  );
}

function ShapeView({ value }: { value: Json }) {
  const shape = asShape(value);
  if (!shape) return <p className="muted">Not described.</p>;
  return (
    <>
      <p className="code-inline">{shape.type_name}</p>
      {shape.fields && shape.fields.length > 0 && (
        <ul className="fields">
          {shape.fields.map((field) => (
            <li key={field.name}>
              <span className="code-inline">{field.name}</span>{" "}
              <span className="muted">{field.type_name}</span>
              {!field.compared && <Tag>not compared</Tag>}
            </li>
          ))}
        </ul>
      )}
    </>
  );
}

/** Duration over time: one bar per finished run, oldest on the left, with the expectation drawn. */
function DurationBars({
  runs,
  expected,
}: {
  runs: RunView[];
  expected: number | null;
}) {
  const finished = runs
    .filter((run) => run.started_at && run.finished_at)
    .map((run) => ({
      run,
      seconds: secondsBetween(run.started_at, run.finished_at) ?? 0,
    }))
    .toReversed();
  // Under a second each, the bars would all be the same sliver and say nothing.
  if (finished.length < 2 || finished.every((item) => item.seconds < 1))
    return null;
  const height = 64;
  const bar = 12;
  const gap = 4;
  const most = Math.max(
    expected ?? 0,
    ...finished.map((item) => item.seconds),
    1,
  );
  const width = finished.length * (bar + gap);
  const expectedY = expected ? height - (expected / most) * height : null;
  return (
    <figure className="bars">
      <svg
        width={width}
        height={height}
        role="img"
        aria-label="Duration of recent runs, oldest first"
      >
        {finished.map(({ run, seconds }, index) => {
          const barHeight = Math.max(2, (seconds / most) * height);
          return (
            <rect
              key={run.run_id}
              x={index * (bar + gap)}
              y={height - barHeight}
              width={bar}
              height={barHeight}
              rx={2}
              className={`bar tone-${run.status === "succeeded" ? "ok" : run.status === "failed" ? "bad" : "neutral"}`}
            >
              <title>{`${formatDuration(seconds)}, ${run.status}`}</title>
            </rect>
          );
        })}
        {expectedY !== null && (
          <line
            x1={0}
            x2={width}
            y1={expectedY}
            y2={expectedY}
            className="bar-expected"
          />
        )}
      </svg>
      <figcaption className="muted">
        Duration of recent runs, oldest first
        {expected
          ? `; the line is the expected ${formatDuration(expected)}`
          : ""}
        .
      </figcaption>
    </figure>
  );
}
