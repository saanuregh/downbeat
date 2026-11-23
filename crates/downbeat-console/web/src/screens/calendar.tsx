// What runs on a date, and what ran: one timeline, in the person's time zone, with days off,
// overrides and pauses marked.
import { useId, useState } from "react";
import { ActionButton } from "../components/action";
import { OverrideButton } from "../components/action-forms";
import {
  Empty,
  ErrorNote,
  Loading,
  PageHeader,
  Pager,
  Section,
  StatusBadge,
  Tag,
} from "../components/ui";
import { messageOf, queryString } from "../api";
import {
  addDays,
  dateInZone,
  formatClock,
  formatUtc,
  knownTimeZones,
} from "../format";
import { useLoad, useNow } from "../load";
import { Link, navigate, useLocation } from "../router";
import { useScheduler, useSession } from "../session";
import type {
  CalendarDay,
  CalendarEntry,
  OverrideView,
  Page,
  ScheduleView,
} from "../types";

const sourceLabel: Record<string, string> = {
  rule: "day rule",
  override: "override",
};

export function CalendarScreen() {
  const { api, href, can } = useScheduler();
  const { zone } = useSession();
  const { query } = useLocation();
  const now = useNow();
  const today = dateInZone(new Date(now), zone);
  const date = /^\d{4}-\d{2}-\d{2}$/.test(query.get("date") ?? "")
    ? (query.get("date") as string)
    : today;
  const day = useLoad<CalendarDay>(api("/calendar", { date }));
  const overridesCursor = query.get("overrides");
  const overrides = useLoad<Page<OverrideView>>(
    api("/overrides", { date, cursor: overridesCursor }),
  );
  const schedules = useLoad<ScheduleView[]>(
    can("administer") ? api("/schedules") : null,
  );
  const dateId = useId();
  const go = (next: string) => navigate(href(`/calendar?date=${next}`));
  const reload = () => {
    day.reload();
    overrides.reload();
  };
  const todaysOverrides = overrides.data?.items ?? [];
  const dateHref = href(`/calendar${queryString({ date })}`);
  const entries = (day.data?.entries ?? []).toSorted(
    (a, b) => Date.parse(a.at) - Date.parse(b.at),
  );
  const firstFuture = entries.findIndex((entry) => Date.parse(entry.at) > now);

  return (
    <>
      <PageHeader
        title="Calendar"
        subtitle={`Every occurrence of every schedule on ${date} (the schedules' date), shown in ${zone}. Hover a time for UTC.`}
        actions={
          can("administer") &&
          schedules.data && (
            <OverrideButton
              date={date}
              schedules={schedules.data}
              onDone={reload}
            />
          )
        }
      />
      <div className="toolbar">
        <button
          type="button"
          className="button"
          onClick={() => go(addDays(date, -1))}
        >
          Previous day
        </button>
        <label htmlFor={dateId} className="visually-hidden">
          Date
        </label>
        <input
          id={dateId}
          type="date"
          value={date}
          onChange={(event) => event.target.value && go(event.target.value)}
        />
        <button
          type="button"
          className="button"
          onClick={() => go(addDays(date, 1))}
        >
          Next day
        </button>
        {date !== today && (
          <button
            type="button"
            className="button button-quiet"
            onClick={() => go(today)}
          >
            Today
          </button>
        )}
        <ZonePicker />
      </div>

      {day.error && <ErrorNote error={day.error} onRetry={day.reload} />}
      {!day.data && !day.error && <Loading />}

      {day.data && day.data.days_off.length > 0 && (
        <Section title="Days off">
          <ul className="plain-list">
            {day.data.days_off.map((off) => (
              <li key={off.schedule} className="day-off">
                <p className="reason">{off.reason}</p>
                <p className="card-meta">
                  <Tag>day off</Tag> {off.schedule}
                </p>
              </li>
            ))}
          </ul>
        </Section>
      )}

      {(todaysOverrides.length > 0 || overridesCursor) && (
        <Section title="Overrides on this date">
          <ul className="plain-list">
            {todaysOverrides.map((item) => (
              <li key={item.override_id} className="day-off">
                <p className="reason">
                  {item.schedule}:{" "}
                  {item.change === "off"
                    ? "no runs"
                    : `${item.change === "window" ? "window" : "times"} ${item.times.join(", ")}`}{" "}
                  ({item.note})
                </p>
                <p className="card-meta">
                  <Tag tone="wait">override</Tag> by {item.created_by}
                  {can("administer") && (
                    <ActionButton
                      label="Remove"
                      small
                      spec={{
                        title: `Remove the override of ${item.schedule} on ${item.date}`,
                        confirm: "Remove override",
                        method: "DELETE",
                        path: api(`/overrides/${item.override_id}`),
                        danger: true,
                      }}
                      onDone={reload}
                    />
                  )}
                </p>
              </li>
            ))}
          </ul>
          <Pager
            label="Pages of overrides"
            firstHref={overridesCursor ? dateHref : null}
            nextHref={
              overrides.data?.next_cursor
                ? href(
                    `/calendar${queryString({ date, overrides: overrides.data.next_cursor })}`,
                  )
                : null
            }
          />
        </Section>
      )}

      {day.data && (
        <Section title="Timeline">
          {entries.length === 0 ? (
            <Empty>No occurrences on this date.</Empty>
          ) : (
            <ol className="timeline">
              {entries.map((entry, index) => (
                <TimelineEntry
                  key={`${entry.schedule}@${entry.at}`}
                  entry={entry}
                  zone={zone}
                  now={now}
                  nowBefore={index === firstFuture && date === today}
                  jobHref={href(`/jobs/${encodeURIComponent(entry.job)}`)}
                  runHref={entry.run ? href(`/runs/${entry.run.run_id}`) : null}
                />
              ))}
            </ol>
          )}
        </Section>
      )}
    </>
  );
}

function TimelineEntry(props: {
  entry: CalendarEntry;
  zone: string;
  now: number;
  nowBefore: boolean;
  jobHref: string;
  runHref: string | null;
}) {
  const { entry } = props;
  const past = Date.parse(entry.at) <= props.now;
  const source = sourceLabel[entry.source];
  return (
    <li
      className={`timeline-entry${past ? " is-past" : ""}${props.nowBefore ? " now-before" : ""}`}
    >
      <time
        className="timeline-time"
        dateTime={entry.at}
        title={formatUtc(entry.at)}
      >
        {formatClock(entry.at, props.zone)}
      </time>
      <div className="timeline-body">
        {entry.run ? (
          <p className="reason">{entry.run.reason}</p>
        ) : past ? (
          <p className="reason">
            {entry.note ?? "No run for this occurrence."}
          </p>
        ) : entry.note ? (
          <p className="reason">{entry.note}</p>
        ) : null}
        <p className="card-meta">
          <Link to={props.jobHref}>{entry.job}</Link>
          {entry.schedule !== entry.job && (
            <span className="muted">{entry.schedule}</span>
          )}
          {source && <Tag tone="wait">{source}</Tag>}
          {entry.paused && <Tag tone="wait">paused</Tag>}
          {entry.run && <StatusBadge status={entry.run.status} />}
          {props.runHref && <Link to={props.runHref}>Open run</Link>}
          {!past && !entry.run && <span className="muted">upcoming</span>}
        </p>
      </div>
    </li>
  );
}

function ZonePicker() {
  const { zone, setZone } = useSession();
  const [error, setError] = useState<string | null>(null);
  const id = useId();
  const zones = knownTimeZones();
  if (!zones.includes(zone)) zones.unshift(zone);
  return (
    <span className="zone-picker">
      <label htmlFor={id}>Time zone</label>
      <select
        id={id}
        value={zone}
        onChange={(event) => {
          setError(null);
          setZone(event.target.value).catch((caught: unknown) =>
            setError(messageOf(caught)),
          );
        }}
      >
        {zones.map((item) => (
          <option key={item} value={item}>
            {item}
          </option>
        ))}
      </select>
      {error && (
        <span className="field-error" role="alert">
          Not saved: {error}
        </span>
      )}
    </span>
  );
}
