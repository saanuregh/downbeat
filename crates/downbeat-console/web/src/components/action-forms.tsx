// Actions that need a few answers first: pause, backfill, mark as succeeded, a dated override.
// Each builds its request body from a small form and hands it to the preview-and-confirm dialog.
import { useId, useState } from "react";
import { schedulerPath } from "../api";
import { wallTimeToUtc } from "../format";
import { useScheduler, useZone } from "../session";
import type { Json, ScheduleView } from "../types";
import { ActionDialog } from "./action";

function useOpen(): [boolean, () => void, () => void] {
  const [open, setOpen] = useState(false);
  return [open, () => setOpen(true), () => setOpen(false)];
}

export function PauseButton(props: {
  kind: "job" | "queue" | "schedule";
  name: string;
  onDone: () => void;
}) {
  const { scheduler } = useScheduler();
  const zone = useZone();
  const [open, show, hide] = useOpen();
  const [note, setNote] = useState("");
  const [untilResumed, setUntilResumed] = useState(false);
  const [until, setUntil] = useState("");
  const noteId = useId();
  const untilId = useId();
  const untilUtc = until ? wallTimeToUtc(until, zone) : null;
  const invalid =
    note.trim() === ""
      ? "Write a note saying why."
      : !untilResumed && !untilUtc
        ? "Choose when the pause ends, or pause until resumed."
        : null;
  const body = untilResumed
    ? { note, until_resumed: true }
    : { note, until: untilUtc };
  const plural =
    props.kind === "queue"
      ? "every run on queue"
      : `every run of ${props.kind}`;
  return (
    <>
      <button type="button" className="button" onClick={show}>
        Pause
      </button>
      {open && (
        <ActionDialog
          spec={{
            title: `Pause ${props.kind} ${props.name}`,
            confirm: "Pause",
            path: schedulerPath(
              scheduler.name,
              `/${props.kind}s/${encodeURIComponent(props.name)}/pause`,
            ),
            body,
            invalid,
          }}
          onClose={hide}
          onDone={props.onDone}
        >
          <p className="muted">
            Holds {plural} {props.name} until it is resumed or the pause ends.
          </p>
          <label htmlFor={noteId}>Why</label>
          <input
            id={noteId}
            value={note}
            onChange={(event) => setNote(event.target.value)}
            required
          />
          <label className="check">
            <input
              type="checkbox"
              checked={untilResumed}
              onChange={(event) => setUntilResumed(event.target.checked)}
            />
            Until someone resumes it
          </label>
          {!untilResumed && (
            <>
              <label htmlFor={untilId}>Ends at ({zone})</label>
              <input
                id={untilId}
                type="datetime-local"
                value={until}
                onChange={(event) => setUntil(event.target.value)}
              />
            </>
          )}
        </ActionDialog>
      )}
    </>
  );
}

export function ResumeButton(props: {
  kind: "job" | "queue" | "schedule";
  name: string;
  onDone: () => void;
}) {
  const { scheduler } = useScheduler();
  const [open, show, hide] = useOpen();
  return (
    <>
      <button type="button" className="button" onClick={show}>
        Resume
      </button>
      {open && (
        <ActionDialog
          spec={{
            title: `Resume ${props.kind} ${props.name}`,
            confirm: "Resume",
            path: schedulerPath(
              scheduler.name,
              `/${props.kind}s/${encodeURIComponent(props.name)}/resume`,
            ),
          }}
          onClose={hide}
          onDone={props.onDone}
        />
      )}
    </>
  );
}

export function BackfillButton(props: {
  job: string;
  schedules: ScheduleView[];
  onDone: () => void;
}) {
  const { scheduler } = useScheduler();
  const [open, show, hide] = useOpen();
  const [schedule, setSchedule] = useState("");
  const [from, setFrom] = useState("");
  const [to, setTo] = useState("");
  const [continueOnFailure, setContinueOnFailure] = useState(false);
  const ids = { schedule: useId(), from: useId(), to: useId() };
  const invalid =
    !from || !to
      ? "Choose the first and last date."
      : from > to
        ? "The first date must not be after the last."
        : null;
  return (
    <>
      <button type="button" className="button" onClick={show}>
        Backfill
      </button>
      {open && (
        <ActionDialog
          spec={{
            title: `Backfill ${props.job}`,
            confirm: "Start backfill",
            path: schedulerPath(scheduler.name, "/backfills"),
            body: {
              job: props.job,
              schedule: schedule || null,
              from,
              to,
              continue_on_failure: continueOnFailure,
            },
            invalid,
          }}
          onClose={hide}
          onDone={props.onDone}
        >
          <p className="muted">
            Runs the job once for each date, one date at a time, oldest first.
          </p>
          {props.schedules.length > 1 && (
            <>
              <label htmlFor={ids.schedule}>
                Schedule that supplies the input
              </label>
              <select
                id={ids.schedule}
                value={schedule}
                onChange={(event) => setSchedule(event.target.value)}
              >
                <option value="">The job&apos;s own</option>
                {props.schedules.map((item) => (
                  <option key={item.name} value={item.name}>
                    {item.name}
                  </option>
                ))}
              </select>
            </>
          )}
          <div className="form-row">
            <div>
              <label htmlFor={ids.from}>First date</label>
              <input
                id={ids.from}
                type="date"
                value={from}
                onChange={(event) => setFrom(event.target.value)}
              />
            </div>
            <div>
              <label htmlFor={ids.to}>Last date</label>
              <input
                id={ids.to}
                type="date"
                value={to}
                onChange={(event) => setTo(event.target.value)}
              />
            </div>
          </div>
          <label className="check">
            <input
              type="checkbox"
              checked={continueOnFailure}
              onChange={(event) => setContinueOnFailure(event.target.checked)}
            />
            Continue past a failed date
          </label>
        </ActionDialog>
      )}
    </>
  );
}

export function MarkSucceededButton(props: {
  runId: string;
  output: Json | null;
  onDone: () => void;
}) {
  const { scheduler } = useScheduler();
  const [open, show, hide] = useOpen();
  const [note, setNote] = useState("");
  const [outputText, setOutputText] = useState(
    props.output === null ? "null" : JSON.stringify(props.output, null, 2),
  );
  const noteId = useId();
  const outputId = useId();
  let output: Json | undefined;
  try {
    output = JSON.parse(outputText) as Json;
  } catch {
    output = undefined;
  }
  const invalid =
    output === undefined
      ? "The output is not valid JSON."
      : note.trim() === ""
        ? "Write a note saying why."
        : null;
  return (
    <>
      <button type="button" className="button" onClick={show}>
        Mark as succeeded
      </button>
      {open && (
        <ActionDialog
          spec={{
            title: "Mark the run as succeeded",
            confirm: "Mark as succeeded",
            path: schedulerPath(
              scheduler.name,
              `/runs/${encodeURIComponent(props.runId)}/mark-succeeded`,
            ),
            body: { output: output ?? null, note },
            invalid,
          }}
          onClose={hide}
          onDone={props.onDone}
        >
          <p className="muted">
            Records the run as succeeded with this output; what follows it then
            starts.
          </p>
          <label htmlFor={outputId}>
            Output (JSON, in the job&apos;s output type)
          </label>
          <textarea
            id={outputId}
            className="code-input"
            rows={8}
            value={outputText}
            onChange={(event) => setOutputText(event.target.value)}
            spellCheck={false}
          />
          <label htmlFor={noteId}>Why</label>
          <input
            id={noteId}
            value={note}
            onChange={(event) => setNote(event.target.value)}
          />
        </ActionDialog>
      )}
    </>
  );
}

export function OverrideButton(props: {
  date: string;
  schedules: ScheduleView[];
  onDone: () => void;
}) {
  const { scheduler } = useScheduler();
  const [open, show, hide] = useOpen();
  const [chosen, setChosen] = useState<string[]>([]);
  const [date, setDate] = useState(props.date);
  const [change, setChange] = useState("off");
  const [timesText, setTimesText] = useState("");
  const [note, setNote] = useState("");
  const ids = { date: useId(), change: useId(), times: useId(), note: useId() };
  const times = timesText
    .split(/[\s,]+/)
    .map((time) => time.trim())
    .filter(Boolean);
  const invalid =
    chosen.length === 0
      ? "Choose at least one schedule."
      : !date
        ? "Choose a date."
        : change === "times" && times.length === 0
          ? "Write at least one time."
          : change === "window" && times.length !== 2
            ? "Write the window's start and end."
            : note.trim() === ""
              ? "Write a note saying why."
              : null;
  function toggle(name: string, on: boolean) {
    setChosen((current) =>
      on ? [...current, name] : current.filter((item) => item !== name),
    );
  }
  return (
    <>
      <button type="button" className="button" onClick={show}>
        Add override
      </button>
      {open && (
        <ActionDialog
          spec={{
            title: "Add a dated override",
            confirm: "Add override",
            path: schedulerPath(scheduler.name, "/overrides"),
            body: {
              schedules: chosen,
              date,
              change,
              times: change === "off" ? [] : times,
              note,
            },
            invalid,
          }}
          onClose={hide}
          onDone={props.onDone}
        >
          <fieldset>
            <legend>Schedules</legend>
            <div className="check-list">
              {props.schedules.map((schedule) => (
                <label key={schedule.name} className="check">
                  <input
                    type="checkbox"
                    checked={chosen.includes(schedule.name)}
                    onChange={(event) =>
                      toggle(schedule.name, event.target.checked)
                    }
                  />
                  {schedule.name}{" "}
                  <span className="muted">({schedule.times})</span>
                </label>
              ))}
            </div>
          </fieldset>
          <div className="form-row">
            <div>
              <label htmlFor={ids.date}>Date</label>
              <input
                id={ids.date}
                type="date"
                value={date}
                onChange={(event) => setDate(event.target.value)}
              />
            </div>
            <div>
              <label htmlFor={ids.change}>Change</label>
              <select
                id={ids.change}
                value={change}
                onChange={(event) => setChange(event.target.value)}
              >
                <option value="off">No runs that day</option>
                <option value="times">Other times that day</option>
                <option value="window">Another window that day</option>
              </select>
            </div>
          </div>
          {change !== "off" && (
            <>
              <label htmlFor={ids.times}>
                {change === "times"
                  ? "Times, HH:MM, separated by commas"
                  : "Start and end, HH:MM, HH:MM"}{" "}
                (in the schedule&apos;s zone)
              </label>
              <input
                id={ids.times}
                value={timesText}
                onChange={(event) => setTimesText(event.target.value)}
                placeholder={
                  change === "times" ? "09:30, 15:00" : "09:15, 15:30"
                }
              />
            </>
          )}
          <label htmlFor={ids.note}>Why</label>
          <input
            id={ids.note}
            value={note}
            onChange={(event) => setNote(event.target.value)}
          />
        </ActionDialog>
      )}
    </>
  );
}
