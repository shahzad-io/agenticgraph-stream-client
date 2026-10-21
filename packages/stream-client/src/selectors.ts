import type {
  NodeCompleteEvent,
  NodeErrorEvent,
  NodeStartEvent,
  StreamEvent,
  TextDeltaEvent,
  ThinkingDeltaEvent,
  ToolCallEvent,
} from './types/events.js';
import type { ToolCallPayload } from './types/payloads/prism.js';
import type { StreamInterrupt } from './types/interrupt.js';
import type { AgenticStream } from './agentic-stream.js';

export type ResumeHandler = (
  decision: unknown,
  checkpoint: StreamInterrupt,
) => Promise<void> | void;

/**
 * Append text_delta payload deltas only (hook `text` field semantics).
 */
export function appendTextDelta(current: string, event: StreamEvent): string {
  if (event.eventType === 'text_delta') {
    return current + (event as TextDeltaEvent).payload.delta;
  }

  return current;
}

/**
 * Text of the current or last step (hook `finalText` semantics): `step_start` resets it, `text_delta` appends.
 * After a tool run it is the final step's text, as the server's `LlmResponse::text`.
 */
export function appendFinalTextDelta(current: string, event: StreamEvent): string {
  if (event.eventType === 'step_start') {
    return '';
  }

  return appendTextDelta(current, event);
}

/**
 * Resume orchestration (Option C): consumer owns HTTP via onResume; transport stays connected.
 * Post-resume events (stream_resume onward) arrive on the existing connection - no reconnect.
 */
export async function executeResume(
  client: AgenticStream,
  decision: unknown,
  onResume: ResumeHandler | undefined,
): Promise<void> {
  if (onResume === undefined) {
    throw new Error(
      'useAgenticStream: resume() requires onResume - provide your app resume HTTP call in hook options',
    );
  }

  const checkpoint = client.interrupt;

  if (checkpoint === null) {
    throw new Error('useAgenticStream: resume() called but stream has no interrupt');
  }

  await onResume(decision, checkpoint);
}

export type StreamSelector<T> = (events: readonly StreamEvent[]) => T;

/** Nodes started but not yet completed or errored. */
export function activeNodeSelector(events: readonly StreamEvent[]): string[] {
  const active = new Set<string>();

  for (const event of events) {
    if (event.eventType === 'node_start') {
      active.add((event as NodeStartEvent).payload.node_name);
    } else if (event.eventType === 'node_complete') {
      active.delete((event as NodeCompleteEvent).payload.node_name);
    } else if (event.eventType === 'node_error') {
      active.delete((event as NodeErrorEvent).payload.node_name);
    }
  }

  return [...active];
}

/** Accumulated thinking_delta content (parallel to hook `text`, but for thinking). */
export function thinkingSelector(events: readonly StreamEvent[]): string {
  let thinking = '';

  for (const event of events) {
    if (event.eventType === 'thinking_delta') {
      thinking += (event as ThinkingDeltaEvent).payload.delta;
    }
  }

  return thinking;
}

/**
 * Accumulated text_delta for a single node (multi-segment flows).
 * Hook `text` still spans all nodes; use this selector for per-node segments.
 */
export function nodeTextSelector(nodeName: string): StreamSelector<string> {
  return (events: readonly StreamEvent[]): string => {
    let text = '';

    for (const event of events) {
      if (event.eventType === 'text_delta' && event.node_name === nodeName) {
        text += (event as TextDeltaEvent).payload.delta;
      }
    }

    return text;
  };
}

/** All tool_call payloads in order. */
export function toolCallSelector(events: readonly StreamEvent[]): ToolCallPayload[] {
  const calls: ToolCallPayload[] = [];

  for (const event of events) {
    if (event.eventType === 'tool_call') {
      calls.push((event as ToolCallEvent).payload);
    }
  }

  return calls;
}
