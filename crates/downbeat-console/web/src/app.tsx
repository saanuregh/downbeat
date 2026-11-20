// The frame every screen sits in: navigation on the left, the scheduler picker and the person at
// the top, and the screen the path names.
import { useEffect, useRef, type ComponentType } from "react";
import { csrfToken, messageOf } from "./api";
import { Link, matchPath, navigate, useLocation } from "./router";
import { allows, SchedulerProvider, screenPath, useSession } from "./session";
import type { SchedulerAccess } from "./types";
import { AccessScreen } from "./screens/access";
import { AuditScreen } from "./screens/audit";
import { CalendarScreen } from "./screens/calendar";
import { ChainScreen } from "./screens/chain";
import { JobScreen } from "./screens/job";
import { JobsScreen } from "./screens/jobs";
import { OverviewScreen } from "./screens/overview";
import { PipelineScreen } from "./screens/pipeline";
import { PipelinesScreen } from "./screens/pipelines";
import { QueuesScreen } from "./screens/queues";
import { RunScreen } from "./screens/run";
import { RunsScreen } from "./screens/runs";

type Screen = ComponentType<{ params: Record<string, string> }>;

/** Screens of one scheduler, under /s/<scheduler>. */
const schedulerRoutes: [pattern: string, screen: Screen, section: string][] = [
  ["", OverviewScreen, "Overview"],
  ["/calendar", CalendarScreen, "Calendar"],
  ["/pipelines", PipelinesScreen, "Pipelines"],
  ["/pipelines/:name", PipelineScreen, "Pipelines"],
  ["/chains/:id", ChainScreen, "Pipelines"],
  ["/jobs", JobsScreen, "Jobs"],
  ["/jobs/:name", JobScreen, "Jobs"],
  ["/runs", RunsScreen, "Runs"],
  ["/runs/:id", RunScreen, "Runs"],
  ["/queues", QueuesScreen, "Queues"],
  ["/audit", AuditScreen, "Audit"],
];

const navigation: [path: string, section: string][] = [
  ["", "Overview"],
  ["/calendar", "Calendar"],
  ["/pipelines", "Pipelines"],
  ["/jobs", "Jobs"],
  ["/runs", "Runs"],
  ["/queues", "Queues"],
  ["/audit", "Audit"],
];

const lastSchedulerKey = "downbeat.scheduler";

function rememberScheduler(name: string) {
  try {
    window.localStorage.setItem(lastSchedulerKey, name);
  } catch {
    // Storage can be off; the picker then starts from the first scheduler.
  }
}

/** The scheduler to show when the path names none: the last one used, or the first with a grant. */
function defaultScheduler(
  schedulers: SchedulerAccess[],
): SchedulerAccess | undefined {
  let last: string | null = null;
  try {
    last = window.localStorage.getItem(lastSchedulerKey);
  } catch {
    last = null;
  }
  return (
    schedulers.find((item) => item.name === last && item.scope) ??
    schedulers.find((item) => item.scope) ??
    schedulers[0]
  );
}

function resolve(path: string, schedulers: SchedulerAccess[]) {
  const segments = path.split("/").filter(Boolean);
  if (segments[0] === "access" && segments.length === 1) {
    return { kind: "access" as const, scheduler: defaultScheduler(schedulers) };
  }
  if (segments[0] !== "s" || segments[1] === undefined)
    return { kind: "home" as const };
  let name: string;
  try {
    name = decodeURIComponent(segments[1]);
  } catch {
    return { kind: "missing" as const, scheduler: undefined };
  }
  const scheduler = schedulers.find((item) => item.name === name);
  const rest = `/${segments.slice(2).join("/")}`;
  for (const [pattern, screen, section] of schedulerRoutes) {
    const params = matchPath(pattern || "/", rest);
    if (params)
      return { kind: "screen" as const, scheduler, screen, section, params };
  }
  return { kind: "missing" as const, scheduler };
}

export function Shell() {
  const { me } = useSession();
  const { path } = useLocation();
  const resolved = resolve(path, me.schedulers);
  const main = useRef<HTMLElement>(null);
  const firstPath = useRef(path);

  useEffect(() => {
    if (resolved.kind !== "home") return;
    const target = defaultScheduler(me.schedulers);
    if (target) navigate(screenPath(target.name), { replace: true });
  }, [resolved.kind, me.schedulers]);

  // Move focus to the new screen after navigating, so keyboard and screen-reader users land on it.
  useEffect(() => {
    if (path === firstPath.current) return;
    firstPath.current = path;
    main.current?.focus();
  }, [path]);

  const scheduler = resolved.kind === "home" ? undefined : resolved.scheduler;
  const section =
    resolved.kind === "screen"
      ? resolved.section
      : resolved.kind === "access"
        ? "Access"
        : "Not found";

  useEffect(() => {
    if (scheduler && resolved.kind === "screen")
      rememberScheduler(scheduler.name);
    document.title = [section, scheduler?.label, "downbeat"]
      .filter(Boolean)
      .join(" · ");
  }, [section, scheduler, resolved.kind]);

  const administersAny = me.schedulers.some((item) =>
    allows(item.scope, "administer"),
  );

  return (
    <div className="shell">
      <a className="skip-link" href="#main">
        Skip to content
      </a>
      <header className="topbar">
        <Link to="/" className="brand">
          downbeat
        </Link>
        {me.schedulers.length > 1 && scheduler && (
          <SchedulerPicker current={scheduler} path={path} />
        )}
        {me.schedulers.length === 1 && scheduler && (
          <span className="scheduler-name">{scheduler.label}</span>
        )}
        <div className="topbar-end">
          <span className="person" title={me.person.email}>
            {me.person.name || me.person.email}
          </span>
          <SignOut />
        </div>
      </header>
      <nav className="sidebar" aria-label="Screens">
        {scheduler?.scope && (
          <ul>
            {navigation.map(([to, label]) => (
              <li key={label}>
                <Link
                  to={screenPath(scheduler.name, to)}
                  aria-current={
                    resolved.kind === "screen" && section === label
                      ? "page"
                      : undefined
                  }
                >
                  {label}
                </Link>
              </li>
            ))}
          </ul>
        )}
        {administersAny && (
          <ul className="sidebar-console">
            <li>
              <Link
                to="/access"
                aria-current={resolved.kind === "access" ? "page" : undefined}
              >
                Access
              </Link>
            </li>
          </ul>
        )}
      </nav>
      <main id="main" ref={main} tabIndex={-1} className="content">
        <Content resolved={resolved} administersAny={administersAny} />
      </main>
    </div>
  );
}

function Content({
  resolved,
  administersAny,
}: {
  resolved: ReturnType<typeof resolve>;
  administersAny: boolean;
}) {
  const { me } = useSession();
  if (resolved.kind === "home") {
    return me.schedulers.length === 0 ? (
      <Message title="No schedulers">
        This console is not connected to any scheduler yet.
      </Message>
    ) : null;
  }
  if (resolved.kind === "access") {
    return administersAny ? (
      <AccessScreen />
    ) : (
      <Message title="Access">
        Only people who administer a scheduler can see grants.
      </Message>
    );
  }
  const { scheduler } = resolved;
  if (!scheduler) {
    return (
      <Message title="Not found">
        This console has no scheduler by that name.
      </Message>
    );
  }
  if (!scheduler.scope) {
    return (
      <Message title={scheduler.label}>
        You have no grant on this scheduler, so nothing in it is shown. Ask
        someone who administers it.
      </Message>
    );
  }
  if (resolved.kind === "missing") {
    return (
      <Message title="Not found">There is no screen at this address.</Message>
    );
  }
  const Screen = resolved.screen;
  return (
    <SchedulerProvider scheduler={scheduler}>
      {!scheduler.reachable && (
        <p className="note note-bad" role="alert">
          {scheduler.label} is not reachable right now; what is shown may fail
          to load.
        </p>
      )}
      <Screen key={JSON.stringify(resolved.params)} params={resolved.params} />
    </SchedulerProvider>
  );
}

function Message({ title, children }: { title: string; children: string }) {
  return (
    <>
      <h1>{title}</h1>
      <p className="muted">{children}</p>
    </>
  );
}

function SchedulerPicker({
  current,
  path,
}: {
  current: SchedulerAccess;
  path: string;
}) {
  const { me } = useSession();
  // Keep the section (Runs, Jobs…) when switching, but not an id that belongs to the other one.
  const segment = path.split("/").filter(Boolean)[2];
  const section = navigation.some(([to]) => to === `/${segment}`)
    ? `/${segment}`
    : "";
  return (
    <label className="picker">
      <span className="visually-hidden">Scheduler</span>
      <select
        value={current.name}
        onChange={(event) => navigate(screenPath(event.target.value, section))}
      >
        {me.schedulers.map((item) => (
          <option key={item.name} value={item.name}>
            {item.label}
            {item.scope ? "" : " (no access)"}
            {item.reachable ? "" : " (unreachable)"}
          </option>
        ))}
      </select>
    </label>
  );
}

async function signOut() {
  try {
    // The answer may be a redirect or an empty body, so only its status matters.
    const response = await fetch("/auth/logout", {
      method: "POST",
      headers: { "x-csrf-token": csrfToken() },
      credentials: "same-origin",
      redirect: "manual",
    });
    if (response.status >= 400)
      throw new Error(`the console answered ${response.status}`);
  } catch (error) {
    window.alert(`Signing out failed: ${messageOf(error)}`);
    return;
  }
  window.location.assign("/");
}

function SignOut() {
  return (
    <button type="button" className="button button-quiet" onClick={signOut}>
      Sign out
    </button>
  );
}
