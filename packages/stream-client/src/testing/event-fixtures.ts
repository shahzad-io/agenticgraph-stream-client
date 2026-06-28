import type { StreamCloseEvent } from '../types/events.js';
import type { WireEvent } from '../types/wire-body.js';
import type { TextDeltaPayload } from '../types/payloads/prism.js';
import type { NodeInterruptPayload } from '../types/payloads/framework.js';

const BASE_WIRE = {
  stream_id: 'stream-test',
  execution_id: 'exec-test',
  node_name: 'alpha' as string | null,
  span_id: null as string | null,
  parent_span_id: null as string | null,
  occurred_at: '2024-01-01T00:00:00+00:00',
};

let seqCounter = 0;

export function resetSeqCounter(): void {
  seqCounter = 0;
}

function nextSeq(): number {
  seqCounter += 1;

  return seqCounter;
}

export function makeTextDeltaEvent(
  delta: string,
  overrides: Partial<WireEvent<'text_delta', TextDeltaPayload>> = {},
): WireEvent<'text_delta', TextDeltaPayload> {
  return {
    eventType: 'text_delta',
    ...BASE_WIRE,
    seq: nextSeq(),
    node_seq: 1,
    payload: {
      delta,
      message_id: 'msg-1',
      timestamp: 1,
    },
    ...overrides,
  };
}

export function makeNodeInterruptEvent(
  overrides: Partial<WireEvent<'node_interrupt', NodeInterruptPayload>> = {},
): WireEvent<'node_interrupt', NodeInterruptPayload> {
  return {
    eventType: 'node_interrupt',
    ...BASE_WIRE,
    seq: nextSeq(),
    node_seq: null,
    node_name: 'alpha',
    payload: {
      node_name: 'alpha',
      reason: 'Needs approval',
      checkpoint_id: 'cp-123',
    },
    ...overrides,
  };
}

export function makeStreamCloseEvent(
  status: 'success' | 'cancelled' | 'error',
  meta: unknown = null,
): StreamCloseEvent {
  return {
    eventType: 'stream_close',
    payload: { status, meta },
  };
}

export function makeExecutionTerminalEvent(
  status: 'completed' | 'interrupted' | 'failed',
  meta?: Record<string, unknown>,
): WireEvent<
  'execution_terminal',
  {
    status: typeof status;
    final_state: Record<string, unknown>;
    meta?: Record<string, unknown>;
  }
> {
  return {
    eventType: 'execution_terminal',
    ...BASE_WIRE,
    seq: nextSeq(),
    node_seq: null,
    node_name: null,
    payload: {
      status,
      final_state: {},
      ...(meta ? { meta } : {}),
    },
  };
}

export function makeStreamErrorEvent(): WireEvent<
  'stream_error',
  {
    error_type: string;
    message: string;
    stage: 'transport';
    event_type: null;
    event_seq: null;
    event_node_name: null;
    event_node_seq: null;
  }
> {
  return {
    eventType: 'stream_error',
    ...BASE_WIRE,
    seq: nextSeq(),
    node_seq: null,
    payload: {
      error_type: 'RuntimeException',
      message: 'boom',
      stage: 'transport',
      event_type: null,
      event_seq: null,
      event_node_name: null,
      event_node_seq: null,
    },
  };
}

export function makeStreamReplayUnavailableEvent(cursor = 5): WireEvent<
  'stream_replay_unavailable',
  { cursor: number }
> {
  return {
    eventType: 'stream_replay_unavailable',
    ...BASE_WIRE,
    seq: nextSeq(),
    node_seq: null,
    payload: { cursor },
  } as WireEvent<'stream_replay_unavailable', { cursor: number }>;
}

export function makeThinkingDeltaEvent(
  delta: string,
): WireEvent<'thinking_delta', { id: string; delta: string; summary: null; timestamp: number }> {
  return {
    eventType: 'thinking_delta',
    ...BASE_WIRE,
    seq: nextSeq(),
    node_seq: 2,
    payload: { id: 'r1', delta, summary: null, timestamp: 1 },
  };
}

export function makeNodeStartEvent(
  nodeName: string,
): WireEvent<'node_start', { node_name: string }> {
  return {
    eventType: 'node_start',
    ...BASE_WIRE,
    node_name: nodeName,
    seq: nextSeq(),
    node_seq: 1,
    payload: { node_name: nodeName },
  };
}

export function makeNodeCompleteEvent(
  nodeName: string,
): WireEvent<'node_complete', { node_name: string; duration_ms: number }> {
  return {
    eventType: 'node_complete',
    ...BASE_WIRE,
    node_name: nodeName,
    seq: nextSeq(),
    node_seq: 2,
    payload: { node_name: nodeName, duration_ms: 1 },
  };
}

export function makeStreamResumeEvent(): WireEvent<
  'stream_resume',
  {
    from_seq: number;
    resumed_at_seq: number;
    gap_events: [];
    checkpoint_id: string;
  }
> {
  return {
    eventType: 'stream_resume',
    ...BASE_WIRE,
    seq: nextSeq(),
    node_seq: null,
    payload: {
      from_seq: 1,
      resumed_at_seq: 2,
      gap_events: [],
      checkpoint_id: 'cp-123',
    },
  };
}

export function makeToolCallEvent(
  toolName: string,
): WireEvent<
  'tool_call',
  {
    tool_id: string;
    tool_name: string;
    arguments: Record<string, unknown>;
    message_id: string;
    reasoning_id: null;
    timestamp: number;
  }
> {
  return {
    eventType: 'tool_call',
    ...BASE_WIRE,
    seq: nextSeq(),
    node_seq: 3,
    payload: {
      tool_id: 't1',
      tool_name: toolName,
      arguments: {},
      message_id: 'msg-1',
      reasoning_id: null,
      timestamp: 1,
    },
  };
}
