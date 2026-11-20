// One list of runs, filtered; the filters and the page live in the address, so a view can be
// shared and the browser's Back button returns to the previous page.
import { useId, useState, type FormEvent } from "react";
import { queryString } from "../api";
import { RunTable } from "../components/run-table";
import { Empty, ErrorNote, Loading, PageHeader } from "../components/ui";
import { humanize } from "../format";
import { useLoad } from "../load";
import { Link, navigate, useLocation } from "../router";
import { useScheduler } from "../session";
import type { Page, RunView } from "../types";

const statuses = [
  "queued",
  "running",
  "succeeded",
  "skipped",
  "failed",
  "cancelled",
  "expired",
];
const reasonKinds = [
  "queued_because",
  "skipped_because",
  "failed_because",
] as const;
const queuedCodes = [
  "no_free_worker",
  "waiting_turn",
  "paused",
  "need_not_met",
  "not_ready",
  "backing_off",
  "scheduled_later",
];
const textFilters = [
  ["job", "Job"],
  ["pipeline", "Pipeline"],
  ["queue", "Queue"],
  ["started_by", "Started by"],
  ["input", "Input field"],
] as const;

type Filters = Record<string, string>;

function filtersFrom(query: URLSearchParams): Filters {
  const filters: Filters = {};
  for (const name of [
    "status",
    ...reasonKinds,
    ...textFilters.map(([key]) => key),
  ]) {
    const value = query.get(name);
    if (value) filters[name] = value;
  }
  return filters;
}

export function RunsScreen() {
  const { api, href } = useScheduler();
  const { query } = useLocation();
  const applied = filtersFrom(query);
  const cursor = query.get("cursor");
  const runs = useLoad<Page<RunView>>(
    api("/runs", { ...applied, cursor, limit: 50 }),
  );
  const appliedKey = JSON.stringify(applied);

  return (
    <>
      <PageHeader title="Runs" />
      <FilterForm
        key={appliedKey}
        applied={applied}
        onApply={(filters) => navigate(href("/runs") + queryString(filters))}
      />
      {runs.error && <ErrorNote error={runs.error} onRetry={runs.reload} />}
      {!runs.data && !runs.error && <Loading />}
      {runs.data && runs.data.items.length === 0 && (
        <Empty>No runs match these filters.</Empty>
      )}
      {runs.data && runs.data.items.length > 0 && (
        <RunTable runs={runs.data.items} />
      )}
      <nav className="pager" aria-label="Pages">
        {cursor && (
          <Link className="button" to={href("/runs") + queryString(applied)}>
            First page
          </Link>
        )}
        {runs.data?.next_cursor && (
          <Link
            className="button"
            to={
              href("/runs") +
              queryString({ ...applied, cursor: runs.data.next_cursor })
            }
          >
            Next page
          </Link>
        )}
      </nav>
    </>
  );
}

function FilterForm({
  applied,
  onApply,
}: {
  applied: Filters;
  onApply: (filters: Filters) => void;
}) {
  const initialKind =
    reasonKinds.find((kind) => applied[kind]) ?? "queued_because";
  const [status, setStatus] = useState(applied.status ?? "");
  const [reasonKind, setReasonKind] = useState<string>(initialKind);
  const [reasonCode, setReasonCode] = useState(applied[initialKind] ?? "");
  const [text, setText] = useState<Filters>(() =>
    Object.fromEntries(textFilters.map(([key]) => [key, applied[key] ?? ""])),
  );
  const ids = {
    status: useId(),
    kind: useId(),
    code: useId(),
    codes: useId(),
    text: useId(),
  };

  function submit(event: FormEvent) {
    event.preventDefault();
    const filters: Filters = { ...text, status };
    if (reasonCode.trim()) filters[reasonKind] = reasonCode.trim();
    const cleaned = Object.fromEntries(
      Object.entries(filters)
        .map(([key, value]) => [key, value.trim()] as const)
        .filter(([, value]) => value !== ""),
    );
    onApply(cleaned);
  }

  return (
    <form className="filters" onSubmit={submit} aria-label="Filter runs">
      <div className="filter">
        <label htmlFor={ids.status}>Status</label>
        <select
          id={ids.status}
          value={status}
          onChange={(event) => setStatus(event.target.value)}
        >
          <option value="">Any</option>
          {statuses.map((item) => (
            <option key={item} value={item}>
              {item}
            </option>
          ))}
        </select>
      </div>
      <div className="filter">
        <label htmlFor={ids.kind}>Why</label>
        <div className="joined">
          <select
            id={ids.kind}
            value={reasonKind}
            onChange={(event) => setReasonKind(event.target.value)}
          >
            <option value="queued_because">Queued because</option>
            <option value="skipped_because">Skipped because</option>
            <option value="failed_because">Failed because</option>
          </select>
          <input
            id={ids.code}
            aria-label="Reason code"
            list={reasonKind === "queued_because" ? ids.codes : undefined}
            value={reasonCode}
            placeholder="any"
            onChange={(event) => setReasonCode(event.target.value)}
          />
          <datalist id={ids.codes}>
            {queuedCodes.map((code) => (
              <option key={code} value={code}>
                {humanize(code)}
              </option>
            ))}
          </datalist>
        </div>
      </div>
      {textFilters.map(([key, label]) => (
        <div className="filter" key={key}>
          <label htmlFor={`${ids.text}-${key}`}>{label}</label>
          <input
            id={`${ids.text}-${key}`}
            value={text[key] ?? ""}
            placeholder={key === "input" ? "day=2026-11-08" : undefined}
            onChange={(event) =>
              setText((current) => ({ ...current, [key]: event.target.value }))
            }
          />
        </div>
      ))}
      <div className="filter filter-actions">
        <button type="submit" className="button button-primary">
          Apply
        </button>
        <button
          type="button"
          className="button button-quiet"
          onClick={() => onApply({})}
        >
          Clear
        </button>
      </div>
    </form>
  );
}
