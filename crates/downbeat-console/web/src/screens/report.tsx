// How each job did over the last days (failures, failure rate, timing), then yesterday's failures.
import { ApiError, queryString } from "../api";
import {
  Empty,
  ErrorNote,
  Loading,
  PageHeader,
  Section,
  Tag,
  Time,
} from "../components/ui";
import { addDays, formatDuration, shortId, wallTimeToUtc } from "../format";
import { useLoad } from "../load";
import { Link, useLocation } from "../router";
import { useScheduler } from "../session";
import type { DayReport, JobReport, Report } from "../types";

const spans = [7, 14, 30] as const;

export function ReportScreen() {
  const { scheduler, api, href } = useScheduler();
  const { query } = useLocation();
  const days = spans.find((span) => String(span) === query.get("days")) ?? 7;
  const report = useLoad<Report>(api("/report", { days }));
  const base = href("/report");
  const tooOld =
    report.error instanceof ApiError && report.error.status === 404;

  return (
    <>
      <PageHeader
        title="Report"
        subtitle={
          report.data
            ? `How each job did on ${scheduler.label}, by day in ${report.data.zone}.`
            : `How each job did on ${scheduler.label}.`
        }
        actions={
          <button type="button" className="button" onClick={report.reload}>
            Refresh
          </button>
        }
      />
      <nav className="tabs" aria-label="How many days">
        {spans.map((span) => (
          <Link
            key={span}
            to={span === 7 ? base : `${base}?days=${span}`}
            aria-current={span === days ? "page" : undefined}
          >
            {span} days
          </Link>
        ))}
      </nav>
      {tooOld && (
        <Empty>
          This scheduler is older than the report; upgrade it to see this
          screen.
        </Empty>
      )}
      {report.error && !tooOld && (
        <ErrorNote error={report.error} onRetry={report.reload} />
      )}
      {!report.data && !report.error && <Loading />}
      {report.data && <ReportBody report={report.data} />}
    </>
  );
}

function ReportBody({ report }: { report: Report }) {
  const { href } = useScheduler();
  const yesterday = report.days.length - 2;
  return (
    <>
      <Section title="Jobs">
        {report.jobs.length === 0 ? (
          <Empty>No jobs.</Empty>
        ) : (
          <div className="table-scroll">
            <table className="table report-table">
              <thead>
                <tr>
                  <th scope="col">Job</th>
                  <th scope="col">Failure rate</th>
                  <th scope="col">Yesterday's time</th>
                  {newestFirst(report.days).map((date, index) => (
                    <th key={date} scope="col" className="report-day">
                      {(["Today", "Yesterday"] as const)[index] ??
                        dayLabel(date)}
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {report.jobs.map((job) => (
                  <JobRow
                    key={job.job}
                    job={job}
                    yesterday={yesterday}
                    zone={report.zone}
                  />
                ))}
              </tbody>
            </table>
          </div>
        )}
      </Section>
      <Section title="Failed yesterday">
        {report.failed_yesterday.length === 0 ? (
          <Empty>No failed run from yesterday is in the history.</Empty>
        ) : (
          <div className="table-scroll">
            <table className="table">
              <thead>
                <tr>
                  <th scope="col">Reason</th>
                  <th scope="col">Job</th>
                  <th scope="col">Ended</th>
                  <th scope="col">Run</th>
                </tr>
              </thead>
              <tbody>
                {report.failed_yesterday.map((run) => (
                  <tr key={run.run_id}>
                    <td className="cell-reason">{run.reason}</td>
                    <td>
                      <Link to={href(`/jobs/${encodeURIComponent(run.job)}`)}>
                        {run.job}
                      </Link>
                    </td>
                    <td className="nowrap">
                      <Time at={run.finished_at} />
                    </td>
                    <td className="nowrap">
                      <Link
                        className="code-inline"
                        to={href(`/runs/${run.run_id}`)}
                      >
                        {shortId(run.run_id)}
                      </Link>
                      {run.log_link && (
                        <>
                          {" "}
                          <a
                            href={run.log_link}
                            target="_blank"
                            rel="noreferrer"
                          >
                            Logs
                          </a>
                        </>
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
  );
}

function JobRow({
  job,
  yesterday,
  zone,
}: {
  job: JobReport;
  yesterday: number;
  zone: string;
}) {
  const { href } = useScheduler();
  const failed = job.days.reduce((sum, day) => sum + day.failed, 0);
  const decided = job.days.reduce(
    (sum, day) => sum + day.succeeded + day.failed,
    0,
  );
  const timing = job.days[yesterday];
  /** That day's failed runs, the day being the scheduler's, as the report counts it. */
  const failedRuns = (date: string) =>
    href("/runs") +
    queryString({
      job: job.job,
      status: "failed",
      from: wallTimeToUtc(`${date}T00:00`, zone),
      to: wallTimeToUtc(`${addDays(date, 1)}T00:00`, zone),
    });
  return (
    <tr>
      <td>
        <Link to={href(`/jobs/${encodeURIComponent(job.job)}`)}>{job.job}</Link>
      </td>
      <td className="nowrap">
        {decided === 0 ? (
          <span className="muted">–</span>
        ) : (
          <>
            {percent(failed / decided)}{" "}
            <span className="muted">
              ({failed} of {decided})
            </span>
          </>
        )}
      </td>
      <td className="nowrap">
        {!timing || timing.median_seconds === null ? (
          <span className="muted">–</span>
        ) : (
          <span
            title={`90th percentile ${formatDuration(timing.p90_seconds)}, longest ${formatDuration(timing.longest_seconds)}`}
          >
            {formatDuration(timing.median_seconds)}{" "}
            <span className="muted">
              typical, {formatDuration(timing.longest_seconds)} longest
            </span>
          </span>
        )}
      </td>
      {newestFirst(job.days).map((day) => (
        <td key={day.date} className="report-day" title={describe(day)}>
          <DayCell day={day} failedRuns={failedRuns(day.date)} />
        </td>
      ))}
    </tr>
  );
}

function DayCell({ day, failedRuns }: { day: DayReport; failedRuns: string }) {
  const total = runsOn(day);
  if (total === 0) return <span className="muted">–</span>;
  if (day.failed === 0) return <span>{total}</span>;
  return (
    <Link to={failedRuns}>
      <Tag tone="bad">{day.failed} failed</Tag>
    </Link>
  );
}

/** The newest day first, so the latest days stay in view when the table scrolls. */
function newestFirst<T>(days: T[]): T[] {
  return days.toReversed();
}

function runsOn(day: DayReport): number {
  return day.succeeded + day.failed + day.skipped + day.cancelled + day.expired;
}

/** Everything about one cell, for its tooltip. */
function describe(day: DayReport): string {
  const total = runsOn(day);
  if (total === 0) return `${day.date}: no runs`;
  const counts = (
    [
      ["succeeded", day.succeeded],
      ["failed", day.failed],
      ["skipped", day.skipped],
      ["cancelled", day.cancelled],
      ["expired", day.expired],
    ] as const
  )
    .filter(([, count]) => count > 0)
    .map(([word, count]) => `${count} ${word}`)
    .join(", ");
  const rate =
    day.failure_rate === null ? "" : `; ${percent(day.failure_rate)} failed`;
  const time =
    day.median_seconds === null
      ? ""
      : `; typical ${formatDuration(day.median_seconds)}, 90th percentile ${formatDuration(day.p90_seconds)}`;
  return `${day.date}: ${total} runs (${counts})${rate}${time}`;
}

function percent(rate: number): string {
  if (rate > 0 && rate < 0.01) return "<1%";
  return `${Math.round(rate * 100)}%`;
}

/** "Thu 19" for "2026-11-19". */
function dayLabel(date: string): string {
  const instant = new Date(`${date}T00:00:00Z`);
  const weekday = instant.toLocaleDateString("en", {
    weekday: "short",
    timeZone: "UTC",
  });
  return `${weekday} ${instant.getUTCDate()}`;
}
