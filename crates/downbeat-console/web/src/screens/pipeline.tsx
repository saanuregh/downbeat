// One pipeline drawn as a graph, coloured by its latest chains, and the chains themselves.
import { useMemo } from "react";
import { PipelineGraph } from "../components/pipeline-graph";
import {
  Empty,
  ErrorNote,
  Loading,
  PageHeader,
  Section,
  StatusBadge,
  Tag,
  Time,
} from "../components/ui";
import { shortId } from "../format";
import { latestStatuses } from "../graph";
import { useLoad } from "../load";
import { Link } from "../router";
import { useScheduler } from "../session";
import type { PipelineView } from "../types";

export function PipelineScreen({ params }: { params: Record<string, string> }) {
  const name = params.name ?? "";
  const { api, href } = useScheduler();
  const pipeline = useLoad<PipelineView>(
    api(`/pipelines/${encodeURIComponent(name)}`),
  );
  const statuses = useMemo(
    () => latestStatuses(pipeline.data?.recent_chains ?? []),
    [pipeline.data],
  );

  if (pipeline.error)
    return <ErrorNote error={pipeline.error} onRetry={pipeline.reload} />;
  if (!pipeline.data) return <Loading />;
  const view = pipeline.data;
  const chains = view.recent_chains.toSorted(
    (a, b) => Date.parse(b.started_at) - Date.parse(a.started_at),
  );

  return (
    <>
      <PageHeader
        title={view.name}
        subtitle="Each job is coloured by its status in the newest chain that ran it."
      />
      <Section title="Jobs and links">
        <PipelineGraph
          nodes={view.jobs}
          statuses={statuses}
          hrefOf={(job) => href(`/jobs/${encodeURIComponent(job)}`)}
          label={`Pipeline ${view.name}`}
        />
      </Section>
      <Section title="Recent chains">
        {chains.length === 0 ? (
          <Empty>No chains yet.</Empty>
        ) : (
          <div className="table-scroll">
            <table className="table">
              <thead>
                <tr>
                  <th scope="col">Began</th>
                  <th scope="col">First job</th>
                  <th scope="col">Jobs</th>
                  <th scope="col">Chain</th>
                </tr>
              </thead>
              <tbody>
                {chains.map((chain) => (
                  <tr key={chain.chain_id}>
                    <td className="nowrap">
                      <Time at={chain.started_at} />
                    </td>
                    <td>{chain.first_job}</td>
                    <td>
                      <span className="status-list">
                        {Object.entries(chain.statuses).map(([job, status]) => (
                          <span key={job} className="inline-status">
                            {job} <StatusBadge status={status} />
                          </span>
                        ))}
                        {chain.cancelled && <Tag>cancelled</Tag>}
                      </span>
                    </td>
                    <td>
                      <Link
                        className="code-inline"
                        to={href(`/chains/${chain.chain_id}`)}
                      >
                        {shortId(chain.chain_id)}
                      </Link>
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
