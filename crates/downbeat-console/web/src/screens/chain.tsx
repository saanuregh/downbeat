// One chain, job by job: each job's runs with their reasons, or why it did not run.
import { useMemo } from "react";
import { ActionButton } from "../components/action";
import { PipelineGraph } from "../components/pipeline-graph";
import {
  ErrorNote,
  Loading,
  PageHeader,
  Section,
  StatusBadge,
  Time,
} from "../components/ui";
import { shortId } from "../format";
import { layers } from "../graph";
import { useLoad } from "../load";
import { Link } from "../router";
import { useScheduler } from "../session";
import type { ChainView } from "../types";

export function ChainScreen({ params }: { params: Record<string, string> }) {
  const id = params.id ?? "";
  const { api, href, can } = useScheduler();
  const chain = useLoad<ChainView>(api(`/chains/${encodeURIComponent(id)}`));
  const statuses = useMemo(
    () =>
      new Map(
        (chain.data?.nodes ?? []).flatMap((node) => {
          const newest = node.runs[0];
          return newest ? [[node.job, newest.status] as const] : [];
        }),
      ),
    [chain.data],
  );

  if (chain.error)
    return <ErrorNote error={chain.error} onRetry={chain.reload} />;
  if (!chain.data) return <Loading />;
  const view = chain.data;
  const order = layers(view.nodes).flat();
  const nodes = order.flatMap((job) =>
    view.nodes.filter((node) => node.job === job),
  );

  return (
    <>
      <PageHeader
        title={
          <>
            Chain <span className="code-inline">{shortId(view.chain_id)}</span>
          </>
        }
        subtitle={
          <>
            Pipeline{" "}
            <Link to={href(`/pipelines/${encodeURIComponent(view.pipeline)}`)}>
              {view.pipeline}
            </Link>
            , began with {view.first_job} at <Time at={view.started_at} />
          </>
        }
        actions={
          can("operate") &&
          !view.cancelled_at &&
          view.nodes.some((node) =>
            node.runs.some(
              (run) => run.status === "queued" || run.status === "running",
            ),
          ) && (
            <ActionButton
              label="Cancel chain"
              spec={{
                title: `Cancel chain ${shortId(view.chain_id)}`,
                confirm: "Cancel chain",
                path: api(
                  `/chains/${encodeURIComponent(view.chain_id)}/cancel`,
                ),
                danger: true,
              }}
              onDone={chain.reload}
            />
          )
        }
      />
      {view.cancelled_at && (
        <p className="note note-neutral">
          Cancelled by {view.cancelled_by ?? "someone"} at{" "}
          <Time at={view.cancelled_at} />; nothing more starts in it.
        </p>
      )}
      <Section title="Jobs and links">
        <PipelineGraph
          nodes={view.nodes}
          statuses={statuses}
          hrefOf={(job) => href(`/jobs/${encodeURIComponent(job)}`)}
          label={`Chain ${shortId(view.chain_id)}`}
        />
      </Section>
      <Section title="Job by job">
        <ol className="steps">
          {nodes.map((node) => {
            const newest = node.runs[0];
            return (
              <li key={node.job} className="step">
                <div className="step-header">
                  <h3>
                    <Link to={href(`/jobs/${encodeURIComponent(node.job)}`)}>
                      {node.job}
                    </Link>
                  </h3>
                  {node.parents.length > 0 && (
                    <span className="muted">
                      after {node.parents.join(", ")}
                    </span>
                  )}
                  {can("operate") &&
                    !view.cancelled_at &&
                    newest?.status === "failed" && (
                      <ActionButton
                        label="Retry this step"
                        small
                        spec={{
                          title: `Retry ${node.job} in this chain`,
                          confirm: "Retry",
                          path: api(
                            `/runs/${encodeURIComponent(newest.run_id)}/retry`,
                          ),
                        }}
                        onDone={chain.reload}
                      />
                    )}
                </div>
                {node.runs.length === 0 ? (
                  <p className="reason">{node.why_not ?? "Has not run."}</p>
                ) : (
                  <ul className="plain-list">
                    {node.runs.map((run) => (
                      <li key={run.run_id} className="step-run">
                        <p className="reason">{run.reason}</p>
                        <p className="card-meta">
                          <StatusBadge status={run.status} />
                          <Time at={run.at} />
                          <Link
                            className="code-inline"
                            to={href(`/runs/${run.run_id}`)}
                          >
                            run {shortId(run.run_id)}
                          </Link>
                        </p>
                      </li>
                    ))}
                  </ul>
                )}
              </li>
            );
          })}
        </ol>
      </Section>
    </>
  );
}
