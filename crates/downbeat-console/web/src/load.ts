import { useCallback, useEffect, useState } from "react";
import { getJson } from "./api";

export interface Loaded<T> {
  data: T | undefined;
  error: Error | undefined;
  loading: boolean;
  reload: () => void;
}

/**
 * Fetches JSON from `path` and fetches again when the path changes or `reload` is called.
 * A null path fetches nothing. Data stays shown while the same path reloads, but data for another
 * path is never shown.
 */
export function useLoad<T>(path: string | null): Loaded<T> {
  const [generation, setGeneration] = useState(0);
  const [data, setData] = useState<{ path: string; value: T }>();
  const [settled, setSettled] = useState<{ request: string; error?: Error }>();
  const request = path === null ? null : `${generation} ${path}`;

  useEffect(() => {
    if (path === null || request === null) return undefined;
    const abort = new AbortController();
    getJson<T>(path, abort.signal)
      .then((value) => {
        setData({ path, value });
        setSettled({ request });
      })
      .catch((caught: unknown) => {
        if (abort.signal.aborted) return;
        setSettled({
          request,
          error: caught instanceof Error ? caught : new Error(String(caught)),
        });
      });
    return () => abort.abort();
  }, [path, request]);

  const reload = useCallback(() => setGeneration((value) => value + 1), []);
  return {
    data: data !== undefined && data.path === path ? data.value : undefined,
    error:
      settled !== undefined && settled.request === request
        ? settled.error
        : undefined,
    loading: request !== null && settled?.request !== request,
    reload,
  };
}

/** The current time, refreshed every `everyMs` so "now" markers and running durations move on. */
export function useNow(everyMs = 30_000): number {
  const [now, setNow] = useState(() => Date.now());
  useEffect(() => {
    const timer = window.setInterval(() => setNow(Date.now()), everyMs);
    return () => window.clearInterval(timer);
  }, [everyMs]);
  return now;
}
