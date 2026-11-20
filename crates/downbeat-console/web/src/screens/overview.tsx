// Is anything failing, stuck, slow, overdue or held right now?
import { RunTable } from "../components/run-table";
import {
  Empty,
  ErrorNote,
  Loading,
  PageHeader,
  Section,
  StatusBadge,
  Time,
} from "../components/ui";
import { humanize } from "../format";
import { useLoad } from "../load";
import { Link } from "../router";
import { useScheduler } from "../session";
import type {
  JobView,
  Page,
  PauseView,
  QueueView,
  RunView,
  ScheduleView,
} from "../types";

export function OverviewScreen() {
  const { scheduler, api, href } = useScheduler();
  const jobs = useLoad<JobView[]>(api("/jobs"));
  const queues = useLoad<QueueView[]>(api("/queues"));
  const schedules = useLoad<ScheduleView[]>(api("/schedules"));
  const running = useLoad<Page<RunView>>(
    api("/runs", { status: "running", limit: 200 }),
  );

  const failing = (jobs.data ?? []).filter(
    (job) => job.last_run?.status === "failed",
  );
  const overdue = (jobs.data ?? []).filter((job) => job.overdue);
  const slow = (running.data?.items ?? []).filter((run) => run.slow);
  const queuedBecause = new Map<string, number>();
  for (const queue of queues.data ?? []) {
    for (const [code, count] of Object.entries(queue.queued_because)) {
      queuedBecause.set(code, (queuedBecause.get(code) ?? 0) + count);
    }
  }
  const pauses: {
    kind: string;
    name: string;
    to: string | null;
    pause: PauseView;
  }[] = [
    ...(jobs.data ?? []).flatMap((job) =>
      job.paused
        ? [
            {
              kind: "Job",
              name: job.name,
              to: href(`/jobs/${encodeURIComponent(job.name)}`),
              pause: job.paused,
            },
          ]
        : [],
    ),
    ...(queues.data ?? []).flatMap((queue) =>
      queue.paused
        ? [
            {
              kind: "Queue",
              name: queue.name,
              to: href("/queues"),
              pause: queue.paused,
            },
          ]
        : [],
    ),
    ...(schedules.data ?? []).flatMap((schedule) =>
      schedule.paused
        ? [
            {
              kind: "Schedule",
              name: schedule.name,
              to: href(`/jobs/${encodeURIComponent(schedule.job)}`),
              pause: schedule.paused,
            },
          ]
        : [],
    ),
  ];

  const error = jobs.error ?? queues.error ?? schedules.error ?? running.error;
  const loading =
    !jobs.data || !queues.data || !schedules.data || !running.data;
  const reload = () => {
    jobs.reload();
    queues.reload();
    schedules.reload();
    running.reload();
  };

  return (
    <>
      <PageHeader
        title="Overview"
        subtitle={`What needs attention on ${scheduler.label} right now.`}
        actions={
          <button type="button" className="button" onClick={reload}>
            Refresh
          </button>
        }
      />
      {error && <ErrorNote error={error} onRetry={reload} />}
      {loading && !error && <Loading />}
      {!loading && (
        <>
          <Section title="Failing">
            {failing.length === 0 ? (
              <Empty>No job's latest run failed.</Empty>
            ) : (
              <ul className="cards">
                {failing.map((job) => (
                  <li key={job.name} className="card">
                    <p className="reason">{job.last_run?.reason}</p>
                    <p className="card-meta">
                      <StatusBadge status="failed" />
                      <Link to={href(`/jobs/${encodeURIComponent(job.name)}`)}>
                        {job.name}
                      </Link>
                      <Time at={job.last_run?.at} />
                      {job.last_run && (
                        <Link to={href(`/runs/${job.last_run.run_id}`)}>
                          Open run
                        </Link>
                      )}
                    </p>
                  </li>
                ))}
              </ul>
            )}
          </Section>
          <Section title="Queued, by why">
            {queuedBecause.size === 0 ? (
              <Empty>Nothing is queued.</Empty>
            ) : (
              <ul className="counts">
                {[...queuedBecause.entries()]
                  .toSorted((a, b) => b[1] - a[1])
                  .map(([code, count]) => (
                    <li key={code}>
                      <Link
                        to={href(
                          `/runs?status=queued&queued_because=${encodeURIComponent(code)}`,
                        )}
                      >
                        <span className="count">{count}</span> {humanize(code)}
                      </Link>
                    </li>
                  ))}
              </ul>
            )}
          </Section>
          <Section title="Slow">
            {slow.length === 0 ? (
              <Empty>No running run is slower than expected.</Empty>
            ) : (
              <RunTable runs={slow} />
            )}
          </Section>
          <Section title="Overdue">
            {overdue.length === 0 ? (
              <Empty>No job is overdue.</Empty>
            ) : (
              <ul className="cards">
                {overdue.map((job) => (
                  <li key={job.name} className="card">
                    <p className="reason">
                      {job.name} has not succeeded as often as expected.
                      {job.last_run
                        ? ` Last run: ${job.last_run.reason}`
                        : " It has never run."}
                    </p>
                    <p className="card-meta">
                      <Link to={href(`/jobs/${encodeURIComponent(job.name)}`)}>
                        {job.name}
                      </Link>
                    </p>
                  </li>
                ))}
              </ul>
            )}
          </Section>
          <Section title="Paused">
            {pauses.length === 0 ? (
              <Empty>Nothing is paused.</Empty>
            ) : (
              <div className="table-scroll">
                <table className="table">
                  <thead>
                    <tr>
                      <th scope="col">What</th>
                      <th scope="col">Why</th>
                      <th scope="col">By</th>
                      <th scope="col">Ends</th>
                    </tr>
                  </thead>
                  <tbody>
                    {pauses.map((item) => (
                      <tr key={`${item.kind}:${item.name}`}>
                        <td>
                          {item.kind}{" "}
                          {item.to ? (
                            <Link to={item.to}>{item.name}</Link>
                          ) : (
                            item.name
                          )}
                        </td>
                        <td>{item.pause.note}</td>
                        <td>{item.pause.by}</td>
                        <td className="nowrap">
                          {item.pause.until ? (
                            <Time at={item.pause.until} />
                          ) : (
                            "When resumed"
                          )}
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            )}
          </Section>
        </>
      )}
    </>
  );
}
