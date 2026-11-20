// Every request the screens make goes through here: same-origin JSON with the session cookie,
// and the CSRF token on anything that is not a GET.

export class ApiError extends Error {
  readonly status: number;
  readonly code: string;

  constructor(status: number, code: string, message: string) {
    super(message);
    this.name = "ApiError";
    this.status = status;
    this.code = code;
  }
}

let token = "";

export function setCsrfToken(value: string): void {
  token = value;
}

export function csrfToken(): string {
  return token;
}

export function loginPath(returnTo: string): string {
  return `/auth/login?return=${encodeURIComponent(returnTo)}`;
}

async function errorFrom(response: Response): Promise<ApiError> {
  const text = await response.text();
  try {
    const body = JSON.parse(text) as { error?: unknown; message?: unknown };
    if (typeof body.message === "string") {
      return new ApiError(
        response.status,
        typeof body.error === "string" ? body.error : "error",
        body.message,
      );
    }
  } catch {
    // Not JSON (a proxy's error page, say): fall through to a message built from the status.
  }
  const message =
    `The console answered ${response.status} ${response.statusText}`.trim();
  return new ApiError(response.status, "http", `${message}.`);
}

export async function request<T>(
  method: "GET" | "POST" | "PUT" | "DELETE",
  path: string,
  body?: unknown,
  signal?: AbortSignal,
): Promise<T> {
  const headers: Record<string, string> = { accept: "application/json" };
  if (method !== "GET") headers["x-csrf-token"] = token;
  if (body !== undefined) headers["content-type"] = "application/json";
  const init: RequestInit = {
    method,
    headers,
    credentials: "same-origin",
    signal,
  };
  if (body !== undefined) init.body = JSON.stringify(body);
  const response = await fetch(path, init);
  if (!response.ok) throw await errorFrom(response);
  if (response.status === 204) return undefined as T;
  return (await response.json()) as T;
}

export function getJson<T>(path: string, signal?: AbortSignal): Promise<T> {
  return request<T>("GET", path, undefined, signal);
}

/** The path of a scheduler control API endpoint, through the console. */
export function schedulerPath(
  scheduler: string,
  path: string,
  query?: Record<string, string | number | boolean | null | undefined>,
): string {
  const base = `/api/schedulers/${encodeURIComponent(scheduler)}/v1${path}`;
  return base + queryString(query);
}

export function queryString(
  query?: Record<string, string | number | boolean | null | undefined>,
): string {
  if (!query) return "";
  const params = new URLSearchParams();
  for (const [key, value] of Object.entries(query)) {
    if (value === undefined || value === null || value === "") continue;
    params.set(key, String(value));
  }
  const text = params.toString();
  return text ? `?${text}` : "";
}

/** Adds `preview=true` to a path that may already have a query. */
export function previewPath(path: string): string {
  return path + (path.includes("?") ? "&" : "?") + "preview=true";
}

export function messageOf(error: unknown): string {
  if (error instanceof Error) return error.message;
  return String(error);
}
