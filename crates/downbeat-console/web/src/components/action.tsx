// Every action is previewed before it is confirmed: the dialog asks the API what the action would
// do (`?preview=true`), shows it, and only then offers the button that does it.
import { useEffect, useRef, useState, type ReactNode } from "react";
import { messageOf, previewPath, request } from "../api";
import type { Preview } from "../types";
import { Dialog } from "./dialog";

export interface ActionSpec {
  /** The dialog's title, e.g. "Cancel run 7f3a12bc". */
  title: string;
  /** The label of the confirming button, e.g. "Cancel run". */
  confirm: string;
  method?: "POST" | "DELETE";
  path: string;
  /** The JSON body; undefined sends none. */
  body?: unknown;
  /** A sentence saying why the form cannot be sent yet, or null when it can. */
  invalid?: string | null;
  danger?: boolean;
  /**
   * Effects written by the screen instead of asked of the API, for console endpoints that have
   * no preview (grants).
   */
  localPreview?: string[];
}

type PreviewState =
  | { state: "idle" }
  | { state: "loading"; key: string }
  | { state: "ready"; key: string; preview: Preview }
  | { state: "failed"; key: string; message: string };

export function ActionDialog(props: {
  spec: ActionSpec;
  onClose: () => void;
  /** Called with the API's answer after the action is done. */
  onDone: (result: unknown) => void;
  /** Form fields, shown above the preview. */
  children?: ReactNode;
}) {
  const { spec } = props;
  const method = spec.method ?? "POST";
  const key = JSON.stringify(spec.body ?? null);
  const [preview, setPreview] = useState<PreviewState>({ state: "idle" });
  const [busy, setBusy] = useState(false);
  const [failure, setFailure] = useState<string | null>(null);
  const [doneMessage, setDoneMessage] = useState<string | null>(null);
  const invalid = spec.invalid ?? null;
  const local = spec.localPreview;
  const closeButton = useRef<HTMLButtonElement>(null);

  // The confirm button is replaced by Close when the action is done; keep focus in the dialog.
  useEffect(() => {
    if (doneMessage) closeButton.current?.focus();
  }, [doneMessage]);

  useEffect(() => {
    if (local || invalid) {
      setPreview({ state: "idle" });
      return undefined;
    }
    const abort = new AbortController();
    // Wait for typing to settle before asking, so a form is not previewed on every key.
    const timer = window.setTimeout(
      () => {
        setPreview({ state: "loading", key });
        request<Preview>(
          method,
          previewPath(spec.path),
          spec.body,
          abort.signal,
        )
          .then((answer) => {
            if (!Array.isArray(answer?.effects)) {
              throw new Error(
                "The answer was not a preview, so this action cannot be confirmed here.",
              );
            }
            setPreview({ state: "ready", key, preview: answer });
          })
          .catch((error: unknown) => {
            if (!abort.signal.aborted)
              setPreview({ state: "failed", key, message: messageOf(error) });
          });
      },
      props.children ? 350 : 0,
    );
    return () => {
      abort.abort();
      window.clearTimeout(timer);
    };
    // The body is compared through its JSON text (`key`), not by identity.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [key, spec.path, method, invalid, local]);

  const previewed =
    local !== undefined || (preview.state === "ready" && preview.key === key);

  async function confirm() {
    setBusy(true);
    setFailure(null);
    try {
      const result = await request<unknown>(method, spec.path, spec.body);
      const message =
        typeof result === "object" && result !== null && "message" in result
          ? String((result as { message: unknown }).message)
          : "Done.";
      setDoneMessage(message);
      props.onDone(result);
    } catch (error) {
      setFailure(messageOf(error));
    } finally {
      setBusy(false);
    }
  }

  const footer = doneMessage ? (
    <button
      type="button"
      className="button"
      onClick={props.onClose}
      ref={closeButton}
    >
      Close
    </button>
  ) : (
    <>
      <button
        type="button"
        className="button button-quiet"
        onClick={props.onClose}
      >
        Back
      </button>
      <button
        type="button"
        className={
          spec.danger ? "button button-danger" : "button button-primary"
        }
        disabled={!previewed || busy || invalid !== null}
        onClick={confirm}
      >
        {busy ? "Working…" : spec.confirm}
      </button>
    </>
  );

  return (
    <Dialog
      title={spec.title}
      onClose={props.onClose}
      footer={footer}
      wide={props.children !== undefined}
    >
      {doneMessage ? (
        <output className="note note-ok">{doneMessage}</output>
      ) : (
        <>
          {props.children && <div className="form">{props.children}</div>}
          <PreviewPanel preview={preview} local={local} invalid={invalid} />
          {failure && (
            <p className="note note-bad" role="alert">
              {failure}
            </p>
          )}
        </>
      )}
    </Dialog>
  );
}

function PreviewPanel(props: {
  preview: PreviewState;
  local: string[] | undefined;
  invalid: string | null;
}) {
  if (props.invalid) return <p className="muted">{props.invalid}</p>;
  if (props.local)
    return <Effects effects={props.local} falseConditions={[]} />;
  const { preview } = props;
  if (preview.state === "idle" || preview.state === "loading") {
    return <output className="muted">Asking what this would do…</output>;
  }
  if (preview.state === "failed") {
    return (
      <p className="note note-bad" role="alert">
        {preview.message}
      </p>
    );
  }
  return (
    <Effects
      effects={preview.preview.effects}
      falseConditions={preview.preview.false_conditions ?? []}
    />
  );
}

function Effects({
  effects,
  falseConditions,
}: {
  effects: string[];
  falseConditions: string[];
}) {
  return (
    <div className="preview" aria-live="polite">
      <h3>What this will do</h3>
      {effects.length === 0 ? (
        <p className="muted">Nothing would change.</p>
      ) : (
        <ul>
          {effects.map((effect, index) => (
            <li key={index}>{effect}</li>
          ))}
        </ul>
      )}
      {falseConditions.length > 0 && (
        <>
          <h3>Conditions that are false right now</h3>
          <ul className="false-conditions">
            {falseConditions.map((condition, index) => (
              <li key={index}>{condition}</li>
            ))}
          </ul>
        </>
      )}
    </div>
  );
}

/** A button that opens a preview-and-confirm dialog for an action without a form. */
export function ActionButton(props: {
  label: string;
  spec: ActionSpec;
  onDone: (result: unknown) => void;
  small?: boolean;
}) {
  const [open, setOpen] = useState(false);
  return (
    <>
      <button
        type="button"
        className={`button${props.spec.danger ? " button-danger-quiet" : ""}${props.small ? " button-small" : ""}`}
        onClick={() => setOpen(true)}
      >
        {props.label}
      </button>
      {open && (
        <ActionDialog
          spec={props.spec}
          onClose={() => setOpen(false)}
          onDone={props.onDone}
        />
      )}
    </>
  );
}
