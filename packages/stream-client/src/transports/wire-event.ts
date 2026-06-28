import { FROZEN_WIRE_BODY_KEYS } from '../types/wire-body.js';
import type { StreamClosePayload } from '../types/payloads/framework.js';
import type { StreamEvent } from '../types/events.js';
import type { StreamWireBody } from '../types/wire-body.js';

export function isStreamWireBody(body: unknown): body is StreamWireBody {
  if (body === null || typeof body !== 'object') {
    return false;
  }

  const record = body as Record<string, unknown>;

  return FROZEN_WIRE_BODY_KEYS.every((key) => key in record);
}

/**
 * Reconstruct a {@link StreamEvent} from transport delivery parts.
 * Broadcast: event name + wire body. SSE: event line + JSON data (+ optional seq on body).
 */
export function wireToStreamEvent(eventType: string, body: unknown): StreamEvent | null {
  const normalizedType = normalizeEventName(eventType);

  if (normalizedType === 'stream_close') {
    if (body === null || typeof body !== 'object') {
      return null;
    }

    const payload = body as StreamClosePayload;

    if (
      payload.status !== 'success' &&
      payload.status !== 'cancelled' &&
      payload.status !== 'error'
    ) {
      return null;
    }

    return {
      eventType: 'stream_close',
      payload: {
        status: payload.status,
        meta: 'meta' in payload ? payload.meta : null,
      },
    };
  }

  if (!isStreamWireBody(body)) {
    return null;
  }

  return {
    eventType: normalizedType,
    ...body,
  } as StreamEvent;
}

export function normalizeEventName(eventName: string): string {
  return eventName.startsWith('.') ? eventName.slice(1) : eventName;
}

/**
 * Parse a raw SSE frame block into event type, id, and data JSON.
 */
export function parseSseFrame(block: string): {
  id: string | null;
  eventType: string | null;
  data: unknown;
} | null {
  const lines = block.split('\n');
  let id: string | null = null;
  let eventType: string | null = null;
  const dataLines: string[] = [];

  for (const line of lines) {
    if (line.startsWith('id:')) {
      id = line.slice(3).trim();
    } else if (line.startsWith('event:')) {
      eventType = line.slice(6).trim();
    } else if (line.startsWith('data:')) {
      dataLines.push(line.slice(5).trimStart());
    }
  }

  if (eventType === null || dataLines.length === 0) {
    return null;
  }

  const raw = dataLines.join('\n');

  try {
    return {
      id,
      eventType,
      data: JSON.parse(raw) as unknown,
    };
  } catch {
    return null;
  }
}
