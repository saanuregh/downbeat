// Small building blocks shared by every screen. Everything is rendered as text.
import type { ReactNode } from "react";
import { formatDateTime, formatUtc, statusTone } from "../format";
import { useZone } from "../session";
import type { Json, PauseView } from "../types";

export function StatusBadge({ status }: { status: string }) {
  return (
    <span className={`badge tone-${statusTone(status)}`}>
      <span className="badge-dot" aria-hidden="true" />
      {status}
    </span>
  );
}

/** A tag such as "paused" or "slow", always with its words. */
export function Tag({
  tone = "neutral",
  children,
}: {
  tone?: string;
  children: ReactNode;
}) {
  return <span className={`tag tone-${tone}`}>{children}</span>;
}

/** A time in the person's zone, with the UTC instant on hover. */
export function Time({ at }: { at: string | null | undefined }) {
  const zone = useZone();
  if (!at) return <span className="muted">–</span>;
  return (
    <time dateTime={at} title={formatUtc(at)}>
      {formatDateTime(at, zone)}
    </time>
  );
}

/** The reason sentence, which comes first wherever a run or gap is shown. */
export function Reason({
  children,
  large = false,
}: {
  children: ReactNode;
  large?: boolean;
}) {
  return <p className={large ? "reason reason-large" : "reason"}>{children}</p>;
}

export function Loading() {
  return <output className="muted">Loading…</output>;
}

export function ErrorNote({
  error,
  onRetry,
}: {
  error: Error;
  onRetry?: () => void;
}) {
  return (
    <div className="note note-bad" role="alert">
      <span>{error.message}</span>
      {onRetry && (
        <button type="button" className="button button-small" onClick={onRetry}>
          Try again
        </button>
      )}
    </div>
  );
}

export function Empty({ children }: { children: ReactNode }) {
  return <p className="empty">{children}</p>;
}

export function PageHeader(props: {
  title: ReactNode;
  subtitle?: ReactNode;
  actions?: ReactNode;
}) {
  return (
    <header className="page-header">
      <div>
        <h1>{props.title}</h1>
        {props.subtitle && <p className="muted">{props.subtitle}</p>}
      </div>
      {props.actions && <div className="actions">{props.actions}</div>}
    </header>
  );
}

export function Section(props: {
  title: string;
  actions?: ReactNode;
  children: ReactNode;
}) {
  return (
    <section className="section">
      <div className="section-header">
        <h2>{props.title}</h2>
        {props.actions && <div className="actions">{props.actions}</div>}
      </div>
      {props.children}
    </section>
  );
}

/** A two-column list of labels and values. */
/** A list of labelled facts; each child is a `Fact`. */
export function Facts({ children }: { children: ReactNode }) {
  return <dl className="facts">{children}</dl>;
}

export function Fact({
  label,
  children,
}: {
  label: string;
  children: ReactNode;
}) {
  return (
    <div className="fact">
      <dt>{label}</dt>
      <dd>{children}</dd>
    </div>
  );
}

export function JsonBlock({ value }: { value: Json | null | undefined }) {
  if (value === undefined || value === null)
    return <p className="muted">None</p>;
  return <pre className="code">{JSON.stringify(value, null, 2)}</pre>;
}

/** A link from the data, kept only when it is http(s); anything else is shown as plain text. */
export function ExternalLink({
  href,
  children,
}: {
  href: string;
  children: ReactNode;
}) {
  let safe = false;
  try {
    const url = new URL(href, window.location.href);
    safe = url.protocol === "https:" || url.protocol === "http:";
  } catch {
    safe = false;
  }
  if (!safe) return <span className="code-inline">{href}</span>;
  return (
    <a href={href} target="_blank" rel="noopener noreferrer">
      {children}
    </a>
  );
}

export function PauseNote({ pause, what }: { pause: PauseView; what: string }) {
  return (
    <div className="note note-wait">
      <span>
        {what} paused by {pause.by} since <Time at={pause.since} />
        {pause.until ? (
          <>
            {" "}
            until <Time at={pause.until} />
          </>
        ) : (
          " until resumed"
        )}
        {pause.note ? ` (${pause.note})` : ""}.
      </span>
    </div>
  );
}
