// Every pipeline, with how its latest chains went.
import {
  Empty,
  ErrorNote,
  Loading,
  PageHeader,
  StatusBadge,
  Time,
} from "../components/ui";
import { latestStatuses } from "../graph";
import { useLoad } from "../load";
import { Link } from "../router";
import { useScheduler } from "../session";
import type { PipelineView } from "../types";

export function PipelinesScreen() {
  const { api, href } = useScheduler();
  const pipelines = useLoad<PipelineView[]>(api("/pipelines"));

  return (
    <>
      <PageHeader
        title="Pipelines"
        subtitle="Jobs linked to run one after another, and their latest chains."
      />
      {pipelines.error && (
        <ErrorNote error={pipelines.error} onRetry={pipelines.reload} />
      )}
      {!pipelines.data && !pipelines.error && <Loading />}
      {pipelines.data && pipelines.data.length === 0 && (
        <Empty>No pipelines.</Empty>
      )}
      <ul className="cards cards-grid">
        {(pipelines.data ?? []).map((pipeline) => {
          const latest = latestStatuses(pipeline.recent_chains);
          const counts = new Map<string, number>();
          for (const status of latest.values())
            counts.set(status, (counts.get(status) ?? 0) + 1);
          const newest = pipeline.recent_chains.toSorted(
            (a, b) => Date.parse(b.started_at) - Date.parse(a.started_at),
          )[0];
          return (
            <li key={pipeline.name} className="card">
              <h2 className="card-title">
                <Link
                  to={href(`/pipelines/${encodeURIComponent(pipeline.name)}`)}
                >
                  {pipeline.name}
                </Link>
              </h2>
              <p className="muted">
                {pipeline.jobs.length}{" "}
                {pipeline.jobs.length === 1 ? "job" : "jobs"}
                {newest ? (
                  <>
                    ; latest chain began <Time at={newest.started_at} />
                  </>
                ) : (
                  "; no recent chains"
                )}
              </p>
              {counts.size > 0 && (
                <p className="card-meta" aria-label="Latest status of its jobs">
                  {[...counts.entries()].map(([status, count]) => (
                    <span key={status} className="inline-status">
                      <StatusBadge status={status} /> {count}
                    </span>
                  ))}
                </p>
              )}
            </li>
          );
        })}
      </ul>
    </>
  );
}
