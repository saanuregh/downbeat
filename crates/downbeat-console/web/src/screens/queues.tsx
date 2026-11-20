// Queued and running per queue, why the queued ones wait, the workers serving each, and pauses.
import { PauseButton, ResumeButton } from "../components/action-forms";
import {
  Empty,
  ErrorNote,
  Loading,
  PageHeader,
  PauseNote,
  Section,
  Time,
} from "../components/ui";
import { formatDuration, humanize } from "../format";
import { useLoad } from "../load";
import { Link } from "../router";
import { useScheduler } from "../session";
import type { PauseView, QueueView } from "../types";

export function QueuesScreen() {
  const { api, href, can } = useScheduler();
  const queues = useLoad<QueueView[]>(api("/queues"));
  const everything = useLoad<PauseView | null>(api("/pause"));
  const reload = () => {
    queues.reload();
    everything.reload();
  };

  return (
    <>
      <PageHeader
        title="Queues"
        actions={
          <>
            {can("administer") &&
              (everything.data ? (
                <ResumeButton kind="everything" name="all" onDone={reload} />
              ) : (
                <PauseButton kind="everything" name="all" onDone={reload} />
              ))}
            <button type="button" className="button" onClick={reload}>
              Refresh
            </button>
          </>
        }
      />
      {everything.data && (
        <PauseNote pause={everything.data} what="Every job is" />
      )}
      {queues.error && (
        <ErrorNote error={queues.error} onRetry={queues.reload} />
      )}
      {!queues.data && !queues.error && <Loading />}
      {queues.data && queues.data.length === 0 && <Empty>No queues.</Empty>}
      {(queues.data ?? []).map((queue) => (
        <Section
          key={queue.name}
          title={queue.name}
          actions={
            can("administer") &&
            (queue.paused ? (
              <ResumeButton
                kind="queue"
                name={queue.name}
                onDone={queues.reload}
              />
            ) : (
              <PauseButton
                kind="queue"
                name={queue.name}
                onDone={queues.reload}
              />
            ))
          }
        >
          {queue.paused && (
            <PauseNote pause={queue.paused} what="This queue is" />
          )}
          <p className="queue-counts">
            <Link
              to={href(
                `/runs?queue=${encodeURIComponent(queue.name)}&status=queued`,
              )}
            >
              <span className="count">{queue.queued}</span> queued
            </Link>
            <Link
              to={href(
                `/runs?queue=${encodeURIComponent(queue.name)}&status=running`,
              )}
            >
              <span className="count">{queue.running}</span> running
            </Link>
            {queue.longest_wait_seconds !== null && (
              <span className="muted">
                longest wait {formatDuration(queue.longest_wait_seconds)}
              </span>
            )}
          </p>
          {Object.keys(queue.queued_because).length > 0 && (
            <ul className="counts">
              {Object.entries(queue.queued_because).map(([code, count]) => (
                <li key={code}>
                  <Link
                    to={href(
                      `/runs?queue=${encodeURIComponent(queue.name)}&status=queued&queued_because=${encodeURIComponent(code)}`,
                    )}
                  >
                    <span className="count">{count}</span> {humanize(code)}
                  </Link>
                </li>
              ))}
            </ul>
          )}
          {queue.workers.length === 0 ? (
            <p className="note note-bad">
              No running worker serves this queue.
            </p>
          ) : (
            <div className="table-scroll">
              <table className="table">
                <thead>
                  <tr>
                    <th scope="col">Worker</th>
                    <th scope="col">Slots</th>
                    <th scope="col">Build</th>
                    <th scope="col">Last seen</th>
                  </tr>
                </thead>
                <tbody>
                  {queue.workers.map((worker) => (
                    <tr key={worker.worker_id}>
                      <td className="code-inline">{worker.worker_id}</td>
                      <td>{worker.slots}</td>
                      <td className="code-inline">{worker.build}</td>
                      <td className="nowrap">
                        <Time at={worker.last_seen_at} />
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </Section>
      ))}
    </>
  );
}
