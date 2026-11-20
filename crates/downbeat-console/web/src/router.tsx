// A small router over the History API: the path is state, links push it, and screens match it.
import {
  useSyncExternalStore,
  type AnchorHTMLAttributes,
  type MouseEvent,
} from "react";

const listeners = new Set<() => void>();

function subscribe(listener: () => void): () => void {
  listeners.add(listener);
  window.addEventListener("popstate", listener);
  return () => {
    listeners.delete(listener);
    window.removeEventListener("popstate", listener);
  };
}

function snapshot(): string {
  return window.location.pathname + window.location.search;
}

export function navigate(
  to: string,
  options: { replace?: boolean } = {},
): void {
  if (to === snapshot()) return;
  if (options.replace) window.history.replaceState(null, "", to);
  else window.history.pushState(null, "", to);
  window.scrollTo(0, 0);
  for (const listener of listeners) listener();
}

/** The current path and query, e.g. "/s/axon/runs?status=failed". */
export function useLocation(): { path: string; query: URLSearchParams } {
  const location = useSyncExternalStore(subscribe, snapshot);
  const [path = "/", search = ""] = location.split("?", 2);
  return { path, query: new URLSearchParams(search) };
}

/**
 * Matches a path against a pattern such as "/s/:scheduler/runs/:id". Returns the decoded
 * parameters, or null when it does not match.
 */
export function matchPath(
  pattern: string,
  path: string,
): Record<string, string> | null {
  const want = pattern.split("/").filter(Boolean);
  const have = path.split("/").filter(Boolean);
  if (want.length !== have.length) return null;
  const params: Record<string, string> = {};
  for (let index = 0; index < want.length; index += 1) {
    const part = want[index] ?? "";
    const value = have[index] ?? "";
    if (part.startsWith(":")) {
      try {
        params[part.slice(1)] = decodeURIComponent(value);
      } catch {
        return null;
      }
    } else if (part !== value) {
      return null;
    }
  }
  return params;
}

type LinkProps = Omit<AnchorHTMLAttributes<HTMLAnchorElement>, "href"> & {
  to: string;
};

/** An ordinary link that changes screens without reloading the page. */
export function Link({ to, onClick, children, ...rest }: LinkProps) {
  function follow(event: MouseEvent<HTMLAnchorElement>) {
    onClick?.(event);
    if (event.defaultPrevented || event.button !== 0) return;
    if (event.metaKey || event.ctrlKey || event.shiftKey || event.altKey)
      return;
    event.preventDefault();
    navigate(to);
  }
  return (
    <a href={to} onClick={follow} {...rest}>
      {children}
    </a>
  );
}
