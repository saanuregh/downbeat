// Runs in a table, the reason sentence first in each row.
import { formatDuration, secondsBetween, shortId } from "../format";
import { Link } from "../router";
import { useScheduler } from "../session";
import type { RunView } from "../types";
import { StatusBadge, Tag, Time } from "./ui";

export function RunTable({
  runs,
  showJob = true,
}: {
  runs: RunView[];
  showJob?: boolean;
}) {
  const { href } = useScheduler();
  return (
    <div className="table-scroll">
      <table className="table">
        <thead>
          <tr>
            <th scope="col">Reason</th>
            <th scope="col">Status</th>
            {showJob && <th scope="col">Job</th>}
            <th scope="col">Created</th>
            <th scope="col">Took</th>
            <th scope="col">Run</th>
          </tr>
        </thead>
        <tbody>
          {runs.map((run) => (
            <tr key={run.run_id}>
              <td className="cell-reason">
                {run.reason}
                {run.slow && <Tag tone="wait">slow</Tag>}
                {run.dry_run && <Tag>dry run</Tag>}
              </td>
              <td>
                <StatusBadge status={run.status} />
              </td>
              {showJob && (
                <td>
                  <Link to={href(`/jobs/${encodeURIComponent(run.job)}`)}>
                    {run.job}
                  </Link>
                </td>
              )}
              <td className="nowrap">
                <Time at={run.created_at} />
              </td>
              <td className="nowrap">
                {run.started_at
                  ? formatDuration(
                      secondsBetween(run.started_at, run.finished_at),
                    )
                  : "–"}
              </td>
              <td>
                <Link className="code-inline" to={href(`/runs/${run.run_id}`)}>
                  {shortId(run.run_id)}
                </Link>
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
