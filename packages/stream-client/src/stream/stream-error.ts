/**
 * Error detail surfaced on {@link AgenticStream#error} when status === 'error'.
 */
export type StreamErrorDetail =
  | {
      kind: 'connection';
      message: string;
    }
  | {
      kind: 'stream_error';
      error_type: string;
      message: string;
      stage: string;
    }
  | {
      kind: 'terminal_error';
      source: 'stream_close' | 'execution_terminal';
      message: string;
      meta?: unknown;
    }
  | {
      kind: 'replay_unavailable';
      cursor: number;
      final_state?: unknown;
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
