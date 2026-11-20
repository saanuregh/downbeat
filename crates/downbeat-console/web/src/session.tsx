// Who is signed in, which scheduler is in view, what they may do there, and their time zone.
import {
  createContext,
  useCallback,
  useContext,
  useMemo,
  useState,
  type ReactNode,
} from "react";
import { request, schedulerPath } from "./api";
import { browserTimeZone } from "./format";
import type { Me, Preferences, SchedulerAccess, Scope } from "./types";

const rank: Record<Scope, number> = { read: 1, operate: 2, administer: 3 };

/** Whether a person holding `have` may do what needs `need`. No grant allows nothing. */
export function allows(have: Scope | null | undefined, need: Scope): boolean {
  return have ? rank[have] >= rank[need] : false;
}

interface Session {
  me: Me;
  zone: string;
  preferences: Preferences;
  setZone: (zone: string) => Promise<void>;
}

const SessionContext = createContext<Session | null>(null);

export function SessionProvider(props: {
  me: Me;
  preferences: Preferences;
  children: ReactNode;
}) {
  const [preferences, setPreferences] = useState(props.preferences);
  const setZone = useCallback(
    async (zone: string) => {
      const next = { ...preferences, time_zone: zone };
      await request<unknown>("PUT", "/api/preferences", next);
      setPreferences(next);
    },
    [preferences],
  );
  const value = useMemo<Session>(
    () => ({
      me: props.me,
      preferences,
      zone: preferences.time_zone ?? browserTimeZone(),
      setZone,
    }),
    [props.me, preferences, setZone],
  );
  return (
    <SessionContext.Provider value={value}>
      {props.children}
    </SessionContext.Provider>
  );
}

export function useSession(): Session {
  const session = useContext(SessionContext);
  if (!session) throw new Error("useSession is used outside SessionProvider");
  return session;
}

/** The time zone times are shown in. */
export function useZone(): string {
  return useSession().zone;
}

type Query = Record<string, string | number | boolean | null | undefined>;

interface SchedulerInView {
  scheduler: SchedulerAccess;
  can: (need: Scope) => boolean;
  /** A screen of this scheduler: href("/runs") is "/s/<name>/runs". */
  href: (path: string) => string;
  /** An endpoint of this scheduler's control API: api("/runs") is ".../v1/runs". */
  api: (path: string, query?: Query) => string;
}

/** The path of a screen of a scheduler. */
export function screenPath(scheduler: string, path = ""): string {
  return `/s/${encodeURIComponent(scheduler)}${path}`;
}

const SchedulerContext = createContext<SchedulerInView | null>(null);

export function SchedulerProvider(props: {
  scheduler: SchedulerAccess;
  children: ReactNode;
}) {
  const value = useMemo<SchedulerInView>(
    () => ({
      scheduler: props.scheduler,
      can: (need) => allows(props.scheduler.scope, need),
      href: (path) => screenPath(props.scheduler.name, path),
      api: (path, query) => schedulerPath(props.scheduler.name, path, query),
    }),
    [props.scheduler],
  );
  return (
    <SchedulerContext.Provider value={value}>
      {props.children}
    </SchedulerContext.Provider>
  );
}

export function useScheduler(): SchedulerInView {
  const value = useContext(SchedulerContext);
  if (!value) throw new Error("useScheduler is used outside SchedulerProvider");
  return value;
}
