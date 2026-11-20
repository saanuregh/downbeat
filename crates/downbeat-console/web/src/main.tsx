import { StrictMode, useEffect, useState } from "react";
import { createRoot } from "react-dom/client";
import { ApiError, getJson, loginPath, messageOf, setCsrfToken } from "./api";
import { Shell } from "./app";
import { SessionProvider } from "./session";
import type { Me, Preferences } from "./types";

type Boot =
  | { state: "loading" }
  | { state: "ready"; me: Me; preferences: Preferences }
  | { state: "failed"; message: string };

const defaultPreferences: Preferences = { time_zone: null, filters: {} };

/** Loads who is signed in, or sends the browser to sign in and come back here. */
function Root() {
  const [boot, setBoot] = useState<Boot>({ state: "loading" });

  useEffect(() => {
    async function load() {
      try {
        const me = await getJson<Me>("/api/me");
        setCsrfToken(me.csrf_token);
        // Preferences only change how times are shown; the screens work without them.
        const preferences = await getJson<Preferences>(
          "/api/preferences",
        ).catch(() => defaultPreferences);
        setBoot({ state: "ready", me, preferences });
      } catch (error) {
        if (error instanceof ApiError && error.status === 401) {
          window.location.assign(
            loginPath(window.location.pathname + window.location.search),
          );
          return;
        }
        setBoot({ state: "failed", message: messageOf(error) });
      }
    }
    void load();
  }, []);

  if (boot.state === "loading") {
    return <output className="boot muted">Loading…</output>;
  }
  if (boot.state === "failed") {
    return (
      <div className="boot">
        <h1>The console could not be reached</h1>
        <p className="note note-bad" role="alert">
          {boot.message}
        </p>
        <button
          type="button"
          className="button"
          onClick={() => window.location.reload()}
        >
          Try again
        </button>
      </div>
    );
  }
  return (
    <SessionProvider me={boot.me} preferences={boot.preferences}>
      <Shell />
    </SessionProvider>
  );
}

const container = document.getElementById("root");
if (container) {
  createRoot(container).render(
    <StrictMode>
      <Root />
    </StrictMode>,
  );
}
