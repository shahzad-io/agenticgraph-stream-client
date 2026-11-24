/** Sets (or, with `null`, leaves out) query parameters. A relative URL stays relative. */
export function withQuery(baseUrl: string, params: Record<string, string | null>): string {
  const relative = !/^[a-z][a-z0-9+.-]*:/i.test(baseUrl);
  const url = new URL(baseUrl, typeof window !== 'undefined' ? window.location.href : 'http://localhost');

  for (const [key, value] of Object.entries(params)) {
    if (value !== null) {
      url.searchParams.set(key, value);
    }
  }

  return relative ? `${url.pathname}${url.search}${url.hash}` : url.toString();
}

/** The `error` code of a core 403 JSON body (`stream_token_expired`, `stream_token_invalid`, ...), else null. */
export async function streamErrorCode(response: Response): Promise<string | null> {
  try {
    const body = (await response.json()) as { error?: unknown };

    return typeof body.error === 'string' ? body.error : null;
  } catch {
    return null;
  }
}
