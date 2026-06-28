import { describe, expect, it, vi } from 'vitest';
import { normalizeEventName, parseSseFrame, wireToStreamEvent } from './wire-event.js';

describe('wireToStreamEvent', () => {
  it('reconstructs a 9-field wire event with external eventType', () => {
    const body = {
      stream_id: 's1',
      execution_id: 'e1',
      node_name: 'alpha',
      span_id: null,
      parent_span_id: null,
      seq: 1,
      node_seq: 1,
      occurred_at: '2024-01-01T00:00:00+00:00',
      payload: { delta: 'hi', message_id: 'm', timestamp: 1 },
    };

    const event = wireToStreamEvent('text_delta', body);

    expect(event).not.toBeNull();
    expect(event!.eventType).toBe('text_delta');
    expect(event).toMatchObject(body);
  });

  it('reconstructs stream_close without wire envelope', () => {
    const event = wireToStreamEvent('stream_close', { status: 'success', meta: null });

    expect(event).toEqual({
      eventType: 'stream_close',
      payload: { status: 'success', meta: null },
    });
  });

  it('strips dot prefix from broadcast event names', () => {
    expect(normalizeEventName('.text_delta')).toBe('text_delta');
  });
});

describe('parseSseFrame', () => {
  it('parses id, event, and data lines', () => {
    const frame = parseSseFrame(
      'id: 42\nevent: text_delta\ndata: {"stream_id":"s","execution_id":"e","node_name":null,"span_id":null,"parent_span_id":null,"seq":42,"node_seq":1,"occurred_at":"2024-01-01T00:00:00+00:00","payload":{"delta":"x","message_id":"m","timestamp":1}}',
    );

    expect(frame?.id).toBe('42');
    expect(frame?.eventType).toBe('text_delta');
    expect(frame?.data).toMatchObject({ seq: 42 });
  });
});
