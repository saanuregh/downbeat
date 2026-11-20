// Serves the screens with live reload and forwards /api and /auth to a running console, so the
// screens talk to a real console server the same way they do in production (same origin).
import index from "./index.html";

const consoleAddress = process.env.CONSOLE_URL ?? "http://127.0.0.1:7401";
const port = Number(process.env.PORT ?? 5173);

async function forward(request: Request): Promise<Response> {
  const incoming = new URL(request.url);
  const target = new URL(incoming.pathname + incoming.search, consoleAddress);
  const headers = new Headers(request.headers);
  headers.delete("host");
  const hasBody = request.method !== "GET" && request.method !== "HEAD";
  const answer = await fetch(target, {
    method: request.method,
    headers,
    body: hasBody ? await request.arrayBuffer() : undefined,
    redirect: "manual",
  });
  // fetch has already decompressed the body, so the encoding headers no longer describe it.
  const responseHeaders = new Headers(answer.headers);
  responseHeaders.delete("content-encoding");
  responseHeaders.delete("content-length");
  return new Response(answer.body, {
    status: answer.status,
    headers: responseHeaders,
  });
}

const server = Bun.serve({
  port,
  hostname: "127.0.0.1",
  development: true,
  routes: {
    "/api/*": forward,
    "/auth/*": forward,
    "/*": index,
  },
});

console.log(
  `Screens on ${server.url}, forwarding /api and /auth to ${consoleAddress}`,
);
