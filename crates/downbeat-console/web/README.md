# downbeat console screens

The web screens of the downbeat console: React and TypeScript, built with Bun. The console server
serves the built files and the HTTP API they use; the API is described in
[docs/console-contract.md](../../../docs/console-contract.md), and the screens' rules in the console
design chapter.

## Run it locally

1. Start the console on its local address, `http://127.0.0.1:7401` (for example with
   `mise run demo` from the repository root).
2. In this directory:

   ```sh
   bun install
   bun run dev
   ```

3. Open the address it prints (`http://127.0.0.1:5173`). Use `127.0.0.1`, not `localhost`, so the
   browser sends the console's session cookie to both.

`bun run dev` serves the screens with live reload and forwards `/api` and `/auth` to the console.
Set `CONSOLE_URL` to forward elsewhere, and `PORT` to listen on another port.

## Checks

```sh
bun run check   # types (strict)
bun run lint    # oxlint
bun run test    # unit tests of the pure helpers
bun run fmt     # format; `bun fmt --check` fails on unformatted files
bun run build   # release bundle in dist/, entry page dist/index.html
```

## Layout

- `src/main.tsx` loads who is signed in; `src/app.tsx` is the frame and the routes.
- `src/screens/`: one file per screen. `src/components/`: shared pieces, including the
  preview-and-confirm dialog every action goes through (`action.tsx`).
- `src/api.ts` (requests, CSRF header), `src/load.ts` (fetch hook), `src/router.tsx` (History API
  router), `src/session.tsx` (person, scheduler in view, scope, time zone).
- Pure helpers with tests beside them: `format.ts`, `graph.ts`, `shape-form.ts`.
- `src/types.ts` mirrors `crates/downbeat-api/src/types.rs`; change both together.

## Rules the screens keep

- Text only: never `dangerouslySetInnerHTML`. Scripts, styles and fonts come from the app's own
  origin.
- Every action asks the API for its preview (`?preview=true`) and shows it before it is confirmed.
- Actions the person's scope does not allow are not shown.
- Plain CSS with the variables in `src/styles/base.css`; no CSS framework, router, state or chart
  library.
