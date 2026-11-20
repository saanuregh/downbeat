// Starting a job by hand: a form built from the job's input shape, prefilled from its last run,
// with the start options the person's scope allows.
import { useEffect, useId, useMemo, useState } from "react";
import { schedulerPath } from "../api";
import { useLoad } from "../load";
import { navigate } from "../router";
import { useScheduler } from "../session";
import {
  asShape,
  formFields,
  inputFromJsonText,
  inputFromValues,
  valuesFromInput,
  type FormField,
} from "../shape-form";
import type {
  Json,
  JobView,
  Page,
  PipelineView,
  RunView,
  StartResponse,
} from "../types";
import { ActionDialog } from "./action";

export function StartButton({ job }: { job: JobView }) {
  const [open, setOpen] = useState(false);
  return (
    <>
      <button
        type="button"
        className="button button-primary"
        onClick={() => setOpen(true)}
      >
        Start…
      </button>
      {open && <StartDialog job={job} onClose={() => setOpen(false)} />}
    </>
  );
}

function StartDialog({ job, onClose }: { job: JobView; onClose: () => void }) {
  const { scheduler, can } = useScheduler();
  const administer = can("administer");
  const shape = useMemo(() => asShape(job.input), [job.input]);
  const fields = useMemo(() => formFields(shape), [shape]);
  const [values, setValues] = useState<Record<string, string>>(() =>
    valuesFromInput(fields, null),
  );
  const [jsonText, setJsonText] = useState("");
  const [touched, setTouched] = useState(false);
  const [priority, setPriority] = useState("");
  const [dryRun, setDryRun] = useState(false);
  const [withoutChildren, setWithoutChildren] = useState(false);
  const [evenIfQueued, setEvenIfQueued] = useState(false);
  const [ignoreConditions, setIgnoreConditions] = useState(false);
  const [timeout, setTimeoutText] = useState("");
  const ids = { priority: useId(), timeout: useId(), json: useId() };

  const lastRun = useLoad<RunView>(
    job.last_run
      ? schedulerPath(
          scheduler.name,
          `/runs/${encodeURIComponent(job.last_run.run_id)}`,
        )
      : null,
  );

  function fill(input: Json | null) {
    if (fields.length > 0) setValues(valuesFromInput(fields, input));
    else setJsonText(input === null ? "" : JSON.stringify(input, null, 2));
  }

  useEffect(() => {
    if (!touched && lastRun.data) fill(lastRun.data.input);
    // Prefill once, when the last run arrives, and never over the person's own typing.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [lastRun.data]);

  const built =
    fields.length > 0
      ? inputFromValues(fields, values)
      : inputFromJsonText(jsonText);
  const priorityValue = priority.trim() === "" ? null : Number(priority);
  const timeoutValue = timeout.trim() === "" ? null : Number(timeout);
  const invalid = !built.ok
    ? "Some fields need a value they can be read as (see above)."
    : priorityValue !== null && !Number.isInteger(priorityValue)
      ? "Priority is a whole number."
      : timeoutValue !== null &&
          !(Number.isInteger(timeoutValue) && timeoutValue >= 0)
        ? "The timeout is a whole number of seconds."
        : null;

  const body = {
    input: built.ok ? built.input : null,
    priority: priorityValue,
    dry_run: dryRun,
    without_children: withoutChildren,
    even_if_queued: evenIfQueued,
    ...(administer
      ? { ignore_conditions: ignoreConditions, timeout_seconds: timeoutValue }
      : {}),
  };
  const errors = built.ok ? {} : built.errors;

  return (
    <ActionDialog
      spec={{
        title: `Start ${job.name}`,
        confirm: dryRun ? "Start dry run" : "Start",
        path: schedulerPath(
          scheduler.name,
          `/jobs/${encodeURIComponent(job.name)}/runs`,
        ),
        body,
        invalid,
      }}
      onClose={onClose}
      onDone={(result) => {
        const started = result as StartResponse;
        if (started.run_id)
          navigate(
            `/s/${encodeURIComponent(scheduler.name)}/runs/${started.run_id}`,
          );
      }}
    >
      <fieldset>
        <legend>Input ({shape?.type_name ?? "JSON"})</legend>
        <p className="muted">
          {lastRun.data
            ? "Filled in from the last run."
            : job.last_run
              ? "Loading the last run's input…"
              : ""}
        </p>
        {fields.length > 0 ? (
          fields.map((field) => (
            <InputField
              key={field.name}
              field={field}
              value={values[field.name] ?? ""}
              error={errors[field.name]}
              onChange={(value) => {
                setTouched(true);
                setValues((current) => ({ ...current, [field.name]: value }));
              }}
            />
          ))
        ) : (
          <>
            <label htmlFor={ids.json}>Input as JSON</label>
            <textarea
              id={ids.json}
              className="code-input"
              rows={8}
              spellCheck={false}
              value={jsonText}
              onChange={(event) => {
                setTouched(true);
                setJsonText(event.target.value);
              }}
            />
            {errors.input && <p className="field-error">{errors.input}</p>}
          </>
        )}
        <ParentOutputs
          job={job}
          onUse={(output) => {
            setTouched(true);
            fill(output);
          }}
        />
      </fieldset>
      <fieldset>
        <legend>Options</legend>
        <label htmlFor={ids.priority}>
          Priority (optional; higher runs sooner)
        </label>
        <input
          id={ids.priority}
          inputMode="numeric"
          value={priority}
          onChange={(event) => setPriority(event.target.value)}
        />
        {job.supports_dry_run && (
          <label className="check">
            <input
              type="checkbox"
              checked={dryRun}
              onChange={(event) => setDryRun(event.target.checked)}
            />
            Dry run
          </label>
        )}
        <label className="check">
          <input
            type="checkbox"
            checked={withoutChildren}
            onChange={(event) => setWithoutChildren(event.target.checked)}
          />
          Without children (nothing linked after it starts)
        </label>
        <label className="check">
          <input
            type="checkbox"
            checked={evenIfQueued}
            onChange={(event) => setEvenIfQueued(event.target.checked)}
          />
          Even if the same work is already queued
        </label>
        {administer && (
          <>
            <label className="check">
              <input
                type="checkbox"
                checked={ignoreConditions}
                onChange={(event) => setIgnoreConditions(event.target.checked)}
              />
              Ignore conditions
            </label>
            <label htmlFor={ids.timeout}>
              Timeout for this run, in seconds (optional; 0 for none)
            </label>
            <input
              id={ids.timeout}
              inputMode="numeric"
              value={timeout}
              onChange={(event) => setTimeoutText(event.target.value)}
            />
          </>
        )}
      </fieldset>
    </ActionDialog>
  );
}

function InputField(props: {
  field: FormField;
  value: string;
  error: string | undefined;
  onChange: (value: string) => void;
}) {
  const { field } = props;
  const id = useId();
  const label = (
    <>
      {field.name} <span className="muted">{field.typeName}</span>
    </>
  );
  if (field.kind === "boolean") {
    return (
      <label className="check">
        <input
          type="checkbox"
          checked={props.value === "true"}
          onChange={(event) =>
            props.onChange(event.target.checked ? "true" : "false")
          }
        />
        {label}
      </label>
    );
  }
  const type = field.kind === "date" ? "date" : "text";
  return (
    <div className="field">
      <label htmlFor={id}>{label}</label>
      {field.kind === "json" ? (
        <textarea
          id={id}
          className="code-input"
          rows={3}
          spellCheck={false}
          value={props.value}
          aria-invalid={props.error !== undefined}
          onChange={(event) => props.onChange(event.target.value)}
        />
      ) : (
        <input
          id={id}
          type={type}
          inputMode={
            field.kind === "integer" || field.kind === "number"
              ? "decimal"
              : undefined
          }
          placeholder={
            field.kind === "datetime"
              ? "2026-11-20T08:00:00Z"
              : field.optional
                ? "(none)"
                : undefined
          }
          value={props.value}
          aria-invalid={props.error !== undefined}
          onChange={(event) => props.onChange(event.target.value)}
        />
      )}
      {props.error && <p className="field-error">{props.error}</p>}
    </div>
  );
}

/** A linked job takes its parent's output as input, so offer each parent's latest output. */
function ParentOutputs({
  job,
  onUse,
}: {
  job: JobView;
  onUse: (output: Json | null) => void;
}) {
  const { scheduler } = useScheduler();
  const pipeline = useLoad<PipelineView>(
    schedulerPath(
      scheduler.name,
      `/pipelines/${encodeURIComponent(job.pipeline)}`,
    ),
  );
  const parents =
    pipeline.data?.jobs.find((node) => node.job === job.name)?.parents ?? [];
  if (parents.length === 0) return null;
  return (
    <div className="parent-outputs">
      {parents.map((parent) => (
        <ParentOutput key={parent} parent={parent} onUse={onUse} />
      ))}
    </div>
  );
}

function ParentOutput({
  parent,
  onUse,
}: {
  parent: string;
  onUse: (output: Json | null) => void;
}) {
  const { scheduler } = useScheduler();
  const runs = useLoad<Page<RunView>>(
    schedulerPath(scheduler.name, "/runs", {
      job: parent,
      status: "succeeded",
      limit: 1,
    }),
  );
  const latest = runs.data?.items[0];
  if (!latest) return null;
  return (
    <button
      type="button"
      className="button button-small"
      onClick={() => onUse(latest.output)}
    >
      Use the latest output of {parent}
    </button>
  );
}
