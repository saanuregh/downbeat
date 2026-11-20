// What exists: each job, when it last ran and how, when it runs next, whether it is held.
import { useState } from "react";
import {
  Empty,
  ErrorNote,
  Loading,
  PageHeader,
  StatusBadge,
  Tag,
  Time,
} from "../components/ui";
import { useLoad } from "../load";
import { Link } from "../router";
import { useScheduler } from "../session";
import type { JobView } from "../types";

export function JobsScreen() {
  const { api, href } = useScheduler();
  const jobs = useLoad<JobView[]>(api("/jobs"));
  const [filter, setFilter] = useState("");
  const needle = filter.trim().toLowerCase();
  const shown = (jobs.data ?? []).filter(
    (job) =>
      !needle ||
      job.name.toLowerCase().includes(needle) ||
      job.pipeline.toLowerCase().includes(needle),
  );

  return (
    <>
      <PageHeader
        title="Jobs"
        actions={
          <label className="search">
            <span className="visually-hidden">Find a job</span>
            <input
              type="search"
              placeholder="Find a job or pipeline"
              value={filter}
              onChange={(event) => setFilter(event.target.value)}
            />
          </label>
        }
      />
      {jobs.error && <ErrorNote error={jobs.error} onRetry={jobs.reload} />}
      {!jobs.data && !jobs.error && <Loading />}
      {jobs.data && shown.length === 0 && <Empty>No jobs match.</Empty>}
      {shown.length > 0 && (
        <div className="table-scroll">
          <table className="table">
            <thead>
              <tr>
                <th scope="col">Job</th>
                <th scope="col">Last run</th>
                <th scope="col">Next</th>
                <th scope="col">Pipeline</th>
                <th scope="col">Queue</th>
              </tr>
            </thead>
            <tbody>
              {shown.map((job) => (
                <tr key={job.name}>
                  <td>
                    <Link to={href(`/jobs/${encodeURIComponent(job.name)}`)}>
                      {job.name}
                    </Link>
                    {job.paused && <Tag tone="wait">paused</Tag>}
                    {job.overdue && <Tag tone="bad">overdue</Tag>}
                  </td>
                  <td className="cell-reason">
                    {job.last_run ? (
                      <>
                        <span className="inline-status">
                          <StatusBadge status={job.last_run.status} />{" "}
                          <Time at={job.last_run.at} />
                        </span>
                        <span className="muted">{job.last_run.reason}</span>
                      </>
                    ) : (
                      <span className="muted">Never ran</span>
                    )}
                  </td>
                  <td className="nowrap">
                    {job.next_occurrence ? (
                      <Time at={job.next_occurrence} />
                    ) : (
                      <span className="muted">Not scheduled</span>
                    )}
                  </td>
                  <td>
                    <Link
                      to={href(
                        `/pipelines/${encodeURIComponent(job.pipeline)}`,
                      )}
                    >
                      {job.pipeline}
                    </Link>
                  </td>
                  <td>{job.queue}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </>
  );
}
