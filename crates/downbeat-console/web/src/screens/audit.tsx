// Who did what: to jobs, from the scheduler's audit; and sign-ins and grants, from the console's.
import { queryString } from "../api";
import { Empty, ErrorNote, Loading, PageHeader, Time } from "../components/ui";
import { shortId } from "../format";
import { useLoad } from "../load";
import { Link, useLocation } from "../router";
import { useScheduler } from "../session";
import type { AuditEntry, ConsoleAuditEntry, Json, Page } from "../types";

export function AuditScreen() {
  const { scheduler, href } = useScheduler();
  const { query } = useLocation();
  const tab = query.get("of") === "console" ? "console" : "scheduler";
  const before = query.get("before");
  const base = href("/audit");

  return (
    <>
      <PageHeader title="Audit" />
      <nav className="tabs" aria-label="Which audit">
        <Link to={base} aria-current={tab === "scheduler" ? "page" : undefined}>
          Jobs, on {scheduler.label}
        </Link>
        <Link
          to={`${base}?of=console`}
          aria-current={tab === "console" ? "page" : undefined}
        >
          Sign-ins and access, in the console
        </Link>
      </nav>
      {tab === "scheduler" ? (
        <SchedulerAudit before={before} />
      ) : (
        <ConsoleAudit before={before} />
      )}
    </>
  );
}

function SchedulerAudit({ before }: { before: string | null }) {
  const { api, href } = useScheduler();
  const page = useLoad<Page<AuditEntry>>(api("/audit", { before, limit: 50 }));
  return (
    <AuditTable
      page={page}
      firstHref={before ? href("/audit") : null}
      nextHref={
        page.data?.next_cursor
          ? href("/audit") + queryString({ before: page.data.next_cursor })
          : null
      }
      showTarget
    />
  );
}

function ConsoleAudit({ before }: { before: string | null }) {
  const { href } = useScheduler();
  const page = useLoad<Page<ConsoleAuditEntry>>(
    `/api/audit${queryString({ before })}`,
  );
  return (
    <AuditTable
      page={page}
      firstHref={before ? `${href("/audit")}?of=console` : null}
      nextHref={
        page.data?.next_cursor
          ? href("/audit") +
            queryString({ of: "console", before: page.data.next_cursor })
          : null
      }
      showTarget={false}
    />
  );
}

/** What a scheduler audit entry was done to: a job, a schedule, a run. */
function Target({ entry }: { entry: AuditEntry | ConsoleAuditEntry }) {
  const { href } = useScheduler();
  if (!("job" in entry)) return null;
  return (
    <>
      {entry.job && (
        <Link to={href(`/jobs/${encodeURIComponent(entry.job)}`)}>
          {entry.job}
        </Link>
      )}
      {entry.schedule && (
        <span className="muted"> schedule {entry.schedule}</span>
      )}
      {entry.run_id && (
        <>
          {" "}
          <Link className="code-inline" to={href(`/runs/${entry.run_id}`)}>
            run {shortId(entry.run_id)}
          </Link>
        </>
      )}
    </>
  );
}

function AuditTable(props: {
  page: {
    data: Page<AuditEntry | ConsoleAuditEntry> | undefined;
    error: Error | undefined;
    reload: () => void;
  };
  firstHref: string | null;
  nextHref: string | null;
  showTarget: boolean;
}) {
  const { page } = props;
  if (page.error) return <ErrorNote error={page.error} onRetry={page.reload} />;
  if (!page.data) return <Loading />;
  return (
    <>
      {page.data.items.length === 0 ? (
        <Empty>Nothing recorded.</Empty>
      ) : (
        <div className="table-scroll">
          <table className="table">
            <thead>
              <tr>
                <th scope="col">When</th>
                <th scope="col">Who</th>
                <th scope="col">Did</th>
                {props.showTarget && <th scope="col">To</th>}
                <th scope="col">Detail</th>
              </tr>
            </thead>
            <tbody>
              {page.data.items.map((entry) => (
                <tr key={entry.audit_id}>
                  <td className="nowrap">
                    <Time at={entry.at} />
                  </td>
                  <td>{entry.actor}</td>
                  <td>{entry.action.replaceAll("_", " ")}</td>
                  {props.showTarget && (
                    <td>
                      <Target entry={entry} />
                    </td>
                  )}
                  <td>
                    <DetailText detail={entry.detail} />
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
      <nav className="pager" aria-label="Pages">
        {props.firstHref && (
          <Link className="button" to={props.firstHref}>
            Newest
          </Link>
        )}
        {props.nextHref && (
          <Link className="button" to={props.nextHref}>
            Next page
          </Link>
        )}
      </nav>
    </>
  );
}

/** An audit entry's detail as short text: "key: value" pairs for an object, JSON otherwise. */
function DetailText({ detail }: { detail: Json }) {
  if (detail === null) return null;
  if (typeof detail !== "object" || Array.isArray(detail))
    return <span>{JSON.stringify(detail)}</span>;
  const pairs = Object.entries(detail);
  if (pairs.length === 0) return null;
  return (
    <span className="detail">
      {pairs.map(([key, value]) => (
        <span key={key}>
          <span className="muted">{key.replaceAll("_", " ")}:</span>{" "}
          {typeof value === "string" ? value : JSON.stringify(value)}
        </span>
      ))}
    </span>
  );
}
