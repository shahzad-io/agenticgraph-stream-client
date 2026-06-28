import { describe, expect, it, vi } from 'vitest';
import { EchoTransport } from './echo-transport.js';
import type { EchoChannel, EchoInstance } from './echo-types.js';

function createMockEcho(): {
  echo: EchoInstance;
  channel: EchoChannel;
  trigger: (eventName: string, payload: unknown) => void;
} {
  const listeners = new Map<string, (payload: unknown) => void>();
  let listenToAllHandler: ((eventName: string, payload: unknown) => void) | null = null;

  const channel: EchoChannel = {
    listen(event: string, callback: (payload: unknown) => void) {
      listeners.set(event, callback);

      return channel;
    },
    stopListening(event: string) {
      listeners.delete(event);

      return channel;
    },
    listenToAll(callback: (eventName: string, payload: unknown) => void) {
      listenToAllHandler = callback;

      return channel;
    },
    unsubscribe: vi.fn(),
  };

  const echo: EchoInstance = {
    channel: vi.fn(() => channel),
    leave: vi.fn(),
    disconnect: vi.fn(),
  };

  return {
    echo,
    channel,
    trigger(eventName: string, payload: unknown) {
      if (listenToAllHandler) {
        listenToAllHandler(eventName, payload);

        return;
      }

      const key = eventName.startsWith('.') ? eventName : `.${eventName}`;
      listeners.get(key)?.(payload);
    },
  };
}

describe('EchoTransport', () => {
  it('reconstructs StreamEvent from broadcast event name + wire body', async () => {
    const { echo, trigger } = createMockEcho();
    const transport = new EchoTransport({ echo, channel: 'agenticgraph.stream.abc' });
    const received: unknown[] = [];

    transport.onEvent((event) => {
      received.push(event);
    });

    await transport.connect();

    trigger('.text_delta', {
      stream_id: 's',
      execution_id: 'e',
      node_name: 'alpha',
      span_id: null,
      parent_span_id: null,
      seq: 1,
      node_seq: 1,
      occurred_at: '2024-01-01T00:00:00+00:00',
      payload: { delta: 'hi', message_id: 'm', timestamp: 1 },
    });

    expect(received).toHaveLength(1);
    expect(received[0]).toMatchObject({
      eventType: 'text_delta',
      payload: { delta: 'hi' },
    });

    await transport.disconnect();
  });

  it('handles stream_close terminal payload', async () => {
    const { echo, trigger } = createMockEcho();
    const transport = new EchoTransport({ echo, channel: 'ch' });
    const received: unknown[] = [];

    transport.onEvent((event) => received.push(event));
    await transport.connect();

    trigger('stream_close', { status: 'success', meta: null });

    expect(received[0]).toEqual({
      eventType: 'stream_close',
      payload: { status: 'success', meta: null },
    });

    await transport.disconnect();
  });

  it('reconnect while connected is a no-op', async () => {
    const { echo } = createMockEcho();
    const transport = new EchoTransport({ echo, channel: 'ch' });

    await transport.connect();
    await transport.reconnect();

    expect(transport.getConnectionState()).toBe('connected');
    expect(echo.channel).toHaveBeenCalledTimes(1);

    await transport.disconnect();
  });
});
