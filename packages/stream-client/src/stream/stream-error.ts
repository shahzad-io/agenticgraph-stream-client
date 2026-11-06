/**
 * Error detail surfaced on {@link AgenticStream#error} when status === 'error'.
 * `canReconnect` says whether `reconnect()` can resume from the last event: true only for a lost
 * connection. An ended run, a server error or an expired cursor cannot be resumed.
 */
export type StreamErrorDetail =
  | {
      kind: 'connection';
      message: string;
      canReconnect: boolean;
    }
  | {
      kind: 'stream_error';
      error_type: string;
      message: string;
      stage: string;
      canReconnect: false;
    }
  | {
      kind: 'terminal_error';
      source: 'stream_close' | 'execution_terminal';
      /** `meta.message` when the server sends it (error details on), else a fixed sentence. */
      message: string;
      /** `meta.error_type` when the server sends it: the exception class, or a provider category. */
      error_type?: string;
      meta?: unknown;
      canReconnect: false;
    }
  | {
      kind: 'replay_unavailable';
      cursor: number;
      final_state?: Record<string, unknown> | null;
      canReconnect: false;
    };

/**
 * Parse cursor from stream_replay_unavailable payload (PHP may send string or number).
 */
export function parseReplayCursor(payload: unknown): number {
  if (payload === null || typeof payload !== 'object' || Array.isArray(payload)) {
    return 0;
  }

  const cursor = (payload as { cursor?: unknown }).cursor;

  if (typeof cursor === 'number' && Number.isFinite(cursor)) {
    return cursor;
  }

  if (typeof cursor === 'string' && cursor !== '') {
    const parsed = Number(cursor);

    if (Number.isFinite(parsed)) {
      return parsed;
    }
  }

  return 0;
}
