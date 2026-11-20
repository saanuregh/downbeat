// Pure formatting helpers: durations, times in a zone, status tones. No React here, so they can be
// tested on their own.

/** "45 s", "12 min", "1 h 5 min", "2 d 3 h". Negative or unknown durations are shown as "–". */
export function formatDuration(seconds: number | null | undefined): string {
  if (
    seconds === null ||
    seconds === undefined ||
    !Number.isFinite(seconds) ||
    seconds < 0
  ) {
    return "–";
  }
  const whole = Math.round(seconds);
  if (whole < 60) return `${whole} s`;
  const minutes = Math.floor(whole / 60);
  if (minutes < 60) {
    const rest = whole % 60;
    return minutes < 10 && rest > 0
      ? `${minutes} min ${rest} s`
      : `${minutes} min`;
  }
  const hours = Math.floor(minutes / 60);
  if (hours < 24) {
    const rest = minutes % 60;
    return rest > 0 ? `${hours} h ${rest} min` : `${hours} h`;
  }
  const days = Math.floor(hours / 24);
  const rest = hours % 24;
  return rest > 0 ? `${days} d ${rest} h` : `${days} d`;
}

/** Seconds from `start` to `end`, or to `now` when the run has not ended. */
export function secondsBetween(
  start: string | null | undefined,
  end: string | null | undefined,
  now: number = Date.now(),
): number | null {
  if (!start) return null;
  const from = Date.parse(start);
  const to = end ? Date.parse(end) : now;
  if (Number.isNaN(from) || Number.isNaN(to)) return null;
  return Math.max(0, (to - from) / 1000);
}

interface ZonedParts {
  year: string;
  month: string;
  day: string;
  hour: string;
  minute: string;
  second: string;
}

const formatters = new Map<string, Intl.DateTimeFormat>();

function zonedParts(instant: Date, zone: string): ZonedParts {
  let formatter = formatters.get(zone);
  if (!formatter) {
    formatter = new Intl.DateTimeFormat("en-CA", {
      timeZone: zone,
      year: "numeric",
      month: "2-digit",
      day: "2-digit",
      hour: "2-digit",
      minute: "2-digit",
      second: "2-digit",
      hourCycle: "h23",
    });
    formatters.set(zone, formatter);
  }
  const parts: Record<string, string> = {};
  for (const part of formatter.formatToParts(instant))
    parts[part.type] = part.value;
  return {
    year: parts.year ?? "",
    month: parts.month ?? "",
    day: parts.day ?? "",
    hour: parts.hour ?? "",
    minute: parts.minute ?? "",
    second: parts.second ?? "",
  };
}

/** "2026-11-20 14:05" in the zone, or "–" when there is no time. */
export function formatDateTime(
  iso: string | null | undefined,
  zone: string,
): string {
  if (!iso) return "–";
  const instant = new Date(iso);
  if (Number.isNaN(instant.getTime())) return iso;
  const p = zonedParts(instant, zone);
  return `${p.year}-${p.month}-${p.day} ${p.hour}:${p.minute}`;
}

/** "14:05" in the zone. */
export function formatClock(iso: string, zone: string): string {
  const instant = new Date(iso);
  if (Number.isNaN(instant.getTime())) return iso;
  const p = zonedParts(instant, zone);
  return `${p.hour}:${p.minute}`;
}

/** The exact instant in UTC, for a hover title: "2026-11-20 08:35:00 UTC". */
export function formatUtc(iso: string | null | undefined): string {
  if (!iso) return "";
  const instant = new Date(iso);
  if (Number.isNaN(instant.getTime())) return iso;
  return `${instant.toISOString().slice(0, 19).replace("T", " ")} UTC`;
}

/** Today's date ("YYYY-MM-DD") in the zone. */
export function dateInZone(now: Date, zone: string): string {
  const p = zonedParts(now, zone);
  return `${p.year}-${p.month}-${p.day}`;
}

/** Minutes since midnight of the instant's own date in the zone (0 to 1439). */
export function minuteOfDay(iso: string, zone: string): number {
  const p = zonedParts(new Date(iso), zone);
  return Number(p.hour) * 60 + Number(p.minute);
}

/** The date `days` after a "YYYY-MM-DD" date. */
export function addDays(date: string, days: number): string {
  const instant = new Date(`${date}T00:00:00Z`);
  instant.setUTCDate(instant.getUTCDate() + days);
  return instant.toISOString().slice(0, 10);
}

/** "in 5 min", "12 min ago", "just now". */
export function formatRelative(
  iso: string | null | undefined,
  now: number = Date.now(),
): string {
  if (!iso) return "–";
  const at = Date.parse(iso);
  if (Number.isNaN(at)) return iso;
  const seconds = (at - now) / 1000;
  if (Math.abs(seconds) < 30) return "just now";
  const span = formatDuration(Math.abs(seconds));
  return seconds > 0 ? `in ${span}` : `${span} ago`;
}

export type Tone = "ok" | "bad" | "skip" | "run" | "wait" | "neutral";

/** The colour family of a run status; the label is always shown beside it. */
export function statusTone(status: string): Tone {
  switch (status) {
    case "succeeded":
      return "ok";
    case "failed":
      return "bad";
    case "skipped":
      return "skip";
    case "running":
      return "run";
    case "queued":
      return "wait";
    default:
      return "neutral";
  }
}

/** "no_free_worker" → "no free worker". */
export function humanize(code: string): string {
  return code.replaceAll("_", " ");
}

/** A short form of a UUID for tables: its first eight characters. */
export function shortId(id: string): string {
  // Ids are time-ordered (UUIDv7): their start is shared by everything made in the same moment,
  // their end is random. People tell runs apart by the end.
  return id.replace(/-/g, "").slice(-8);
}

/** The run's duration against its expectation, in words. */
export function timingAgainstExpected(
  seconds: number | null,
  expected: number | null,
  slow: boolean,
): string {
  if (seconds === null) return "Not started";
  const took = formatDuration(seconds);
  if (expected === null || expected <= 0)
    return `${took}; no expected duration yet`;
  const ratio = seconds / expected;
  const comparison =
    ratio < 0.9
      ? "shorter than"
      : ratio <= 1.1
        ? "about"
        : `${ratio.toFixed(1)} times`;
  const sentence = `${took}, ${comparison} the expected ${formatDuration(expected)}`;
  return slow ? `${sentence}; slow` : sentence;
}

/** Every time zone the browser knows, with UTC first; a short fallback on old engines. */
export function knownTimeZones(): string[] {
  const all =
    typeof Intl.supportedValuesOf === "function"
      ? Intl.supportedValuesOf("timeZone")
      : [];
  return ["UTC", ...all.filter((zone) => zone !== "UTC")];
}

export function browserTimeZone(): string {
  return Intl.DateTimeFormat().resolvedOptions().timeZone || "UTC";
}

/**
 * The UTC instant of a wall-clock time ("YYYY-MM-DDTHH:MM", as a datetime-local input gives it)
 * in the zone, as an RFC 3339 string; null when the text is not such a time. A time skipped by a
 * clock change resolves to the instant just after the change.
 */
export function wallTimeToUtc(local: string, zone: string): string | null {
  const match = /^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2})$/.exec(local);
  if (!match) return null;
  const [, year, month, day, hour, minute] = match.map(Number) as [
    number,
    number,
    number,
    number,
    number,
    number,
  ];
  const wanted = Date.UTC(year, month - 1, day, hour, minute);
  let guess = wanted;
  for (let step = 0; step < 2; step += 1) {
    const p = zonedParts(new Date(guess), zone);
    const shown = Date.UTC(
      Number(p.year),
      Number(p.month) - 1,
      Number(p.day),
      Number(p.hour),
      Number(p.minute),
    );
    guess += wanted - shown;
  }
  return new Date(guess).toISOString();
}

/** An actor as the scheduler writes it, "asha (via console)", in words: "asha via console". */
export function formatActor(actor: string): string {
  return actor.replace(/ \(via (.+)\)$/, " via $1");
}

/** Who or what started a run: "asha via console", or "schedule market_open". */
export function formatStartedBy(view: {
  started_by: string;
  actor?: string | null;
  schedule?: string | null;
}): string {
  if (view.schedule) return `schedule ${view.schedule}`;
  if (view.actor) return formatActor(view.actor);
  return view.started_by;
}
