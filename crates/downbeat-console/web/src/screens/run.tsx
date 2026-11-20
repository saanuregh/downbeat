import type { ReactNode } from "react";
// One run: the reason first, then everything about it, and what can be done to it.
import { ActionButton } from "../components/action";
import { MarkSucceededButton } from "../components/action-forms";
import {
  ErrorNote,
  ExternalLink,
  Facts,
  JsonBlock,
  Loading,
  PageHeader,
  Reason,
  Section,
  StatusBadge,
  Tag,
  Time,
} from "../components/ui";
import {
  formatDuration,
  humanize,
  secondsBetween,
  shortId,
  timingAgainstExpected,
  formatStartedBy,
  formatActor,
} from "../format";
import { useLoad } from "../load";
import { Link } from "../router";
import { useScheduler } from "../session";
import type { RunView } from "../types";

export function RunScreen({ params }: { params: Record<string, string> }) {
  const id = params.id ?? "";
  const { api, href, can } = useScheduler();
  const run = useLoad<RunView>(api(`/runs/${encodeURIComponent(id)}`));

  if (run.error) return <ErrorNote error={run.error} onRetry={run.reload} />;
  if (!run.data) return <Loading />;
  const view = run.data;
  const detail = view.detail;
  const unfinished = view.status === "queued" || view.status === "running";
  const waitingOnCheck =
    view.status === "queued" &&
    (view.queued_because === "not_ready" ||
      view.queued_because === "need_not_met");
  const runPath = (action: string) =>
    api(`/runs/${encodeURIComponent(view.run_id)}/${action}`);
  const because =
    view.queued_because ?? view.skipped_because ?? view.failed_because;

  // Who recorded the outcome by hand, and when; why is in the reason above.
  const marked: [string, ReactNode][] = view.marked_by
    ? [
        [
          "Marked as succeeded",
          <span key="marked">
            {formatActor(view.marked_by)}
            {view.marked_at && (
              <>
                , <Time at={view.marked_at} />
              </>
            )}
          </span>,
        ],
      ]
    : [];

  return (
    <>
      <Reason large>{view.reason}</Reason>
      <PageHeader
        title={
          <>
            <Link to={href(`/jobs/${encodeURIComponent(view.job)}`)}>
              {view.job}
            </Link>{" "}
            <span className="muted code-inline">
              run {shortId(view.run_id)}
            </span>
          </>
        }
        subtitle={
          <span className="inline-status">
            <StatusBadge status={view.status} />
            {because && <span>{humanize(because)}</span>}
            {view.slow && <Tag tone="wait">slow</Tag>}
            {view.dry_run && <Tag>dry run</Tag>}
          </span>
        }
        actions={
          <>
            {can("operate") && unfinished && (
              <ActionButton
                label="Cancel"
                spec={{
                  title: `Cancel run ${shortId(view.run_id)}`,
                  confirm: "Cancel run",
                  path: runPath("cancel"),
                  danger: true,
                }}
                onDone={run.reload}
              />
            )}
            {can("operate") && waitingOnCheck && (
              <ActionButton
                label="Check now"
                spec={{
                  title: `Check run ${shortId(view.run_id)} now`,
                  confirm: "Check now",
                  path: runPath("check-now"),
                }}
                onDone={run.reload}
              />
            )}
            {can("operate") && view.status === "failed" && (
              <ActionButton
                label="Retry this step"
                spec={{
                  title: `Retry ${view.job} in its chain`,
                  confirm: "Retry",
                  path: runPath("retry"),
                }}
                onDone={run.reload}
              />
            )}
            {can("operate") && !unfinished && (
              <ActionButton
                label="Restart from here"
                spec={{
                  title: `Restart a new chain from ${view.job}`,
                  confirm: "Restart",
                  path: runPath("restart"),
                }}
                onDone={run.reload}
              />
            )}
            {can("administer") && view.status === "failed" && (
              <MarkSucceededButton
                runId={view.run_id}
                output={view.output}
                onDone={run.reload}
              />
            )}
          </>
        }
      />

      <Facts
        items={[
          [
            "Timing",
            timingAgainstExpected(
              view.started_at
                ? secondsBetween(view.started_at, view.finished_at)
                : null,
              view.expected_seconds,
              view.slow,
            ),
          ],
          ["Due", <Time key="due" at={view.due_at} />],
          ["Created", <Time key="created" at={view.created_at} />],
          ["Started", <Time key="started" at={view.started_at} />],
          ["Finished", <Time key="finished" at={view.finished_at} />],
          ["Attempt", `${view.attempt} of ${view.attempts}`],
          [
            "Calls",
            `${view.calls}${view.lost_count ? `, ${view.lost_count} lost` : ""}${view.interruptions ? `, ${view.interruptions} interrupted` : ""}`,
          ],
          ["Started by", formatStartedBy(view)],
          ...marked,
          ["Queue and priority", `${view.queue}, priority ${view.priority}`],
          [
            "Chain",
            <Link key="chain" to={href(`/chains/${view.chain_id}`)}>
              {shortId(view.chain_id)}
            </Link>,
          ],
          [
            "Pipeline",
            view.pipeline ? (
              <Link
                key="pipeline"
                to={href(`/pipelines/${encodeURIComponent(view.pipeline)}`)}
              >
                {view.pipeline}
              </Link>
            ) : (
              "–"
            ),
          ],
          ["Checkpoint", view.has_checkpoint ? "Has one" : "None"],
          [
            "Logs",
            view.log_link ? (
              <ExternalLink key="logs" href={view.log_link}>
                Open logs
              </ExternalLink>
            ) : (
              "No link"
            ),
          ],
        ]}
      />

      {view.progress && (
        <Section title="Progress">
          <p>{view.progress}</p>
        </Section>
      )}

      {detail && detail.error_chain.length > 0 && (
        <Section title="Error">
          <ol className="error-chain">
            {detail.error_chain.map((cause, index) => (
              <li key={index}>{cause}</li>
            ))}
          </ol>
        </Section>
      )}
      {detail?.stderr_tail && (
        <Section title="Last lines of stderr">
          <pre className="code">{detail.stderr_tail}</pre>
        </Section>
      )}

      <div className="columns">
        <Section title="Input">
          <JsonBlock value={view.input} />
        </Section>
        <Section title="Output">
          <JsonBlock value={view.output} />
        </Section>
      </div>

      {detail && (
        <Section title="Calls">
          {detail.call_log.length === 0 ? (
            <p className="muted">The function has not been called yet.</p>
          ) : (
            <div className="table-scroll">
              <table className="table">
                <thead>
                  <tr>
                    <th scope="col">Ended</th>
                    <th scope="col">Worker</th>
                    <th scope="col">Build</th>
                    <th scope="col">Started</th>
                    <th scope="col">Took</th>
                  </tr>
                </thead>
                <tbody>
                  {detail.call_log.map((call, index) => (
                    <tr key={index}>
                      <td>{call.ended}</td>
                      <td className="code-inline">{call.worker}</td>
                      <td className="code-inline">{call.build}</td>
                      <td className="nowrap">
                        <Time at={call.started_at} />
                      </td>
                      <td className="nowrap">
                        {formatDuration(
                          secondsBetween(call.started_at, call.ended_at),
                        )}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </Section>
      )}

      {detail && (detail.parents.length > 0 || detail.children.length > 0) && (
        <div className="columns">
          <Section title="Parents">
            <RunLinks
              ids={detail.parents}
              empty="None: this run began its chain."
            />
          </Section>
          <Section title="Children">
            <RunLinks ids={detail.children} empty="None yet." />
          </Section>
        </div>
      )}
    </>
  );
}

function RunLinks({ ids, empty }: { ids: string[]; empty: string }) {
  const { href } = useScheduler();
  if (ids.length === 0) return <p className="muted">{empty}</p>;
  return (
    <ul className="plain-list">
      {ids.map((id) => (
        <li key={id}>
          <Link className="code-inline" to={href(`/runs/${id}`)}>
            run {shortId(id)}
          </Link>
        </li>
      ))}
    </ul>
  );
}
