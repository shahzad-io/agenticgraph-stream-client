import type { StreamWireBody, WireEvent } from './wire-body.js';
import type {
  ExecutionTerminalPayload,
  GraphCompletePayload,
  GraphFailedPayload,
  GraphStartPayload,
  NodeCompletePayload,
  NodeErrorPayload,
  NodeInterruptPayload,
  NodeStartPayload,
  StreamClosePayload,
  StreamErrorPayload,
  StreamReplayUnavailablePayload,
  StreamResumePayload,
} from './payloads/framework.js';
import type {
  ArtifactPayload,
  PrismErrorPayload,
  ProviderToolEventPayload,
  StepFinishPayload,
  StepStartPayload,
  StreamEndPayload,
  StreamStartPayload,
  TextCompletePayload,
  TextDeltaPayload,
  TextStartPayload,
  ThinkingCompletePayload,
  ThinkingDeltaPayload,
  ThinkingStartPayload,
  ToolCallPayload,
  ToolResultPayload,
} from './payloads/prism.js';

/** Framework/lifecycle types that use the 9-field wire envelope. */
export type FrameworkWireEventType =
  | 'graph_start'
  | 'graph_complete'
  | 'graph_failed'
  | 'node_start'
  | 'node_complete'
  | 'node_error'
  | 'node_interrupt'
  | 'stream_resume'
  | 'execution_terminal'
  | 'stream_error';

/** All closed framework type strings including broadcast-only `stream_close`. */
export type FrameworkEventType = FrameworkWireEventType | 'stream_close';

/**
 * Semi-closed Prism content types (pinned to Prism 0.102). One model request is `stream_start`, then
 * `step_start` ... `step_finish` per step (tool runs have several), then `stream_end`.
 * Future Prism versions may add types - handled by {@link UnknownStreamEvent}.
 */
export type PrismContentEventType =
  | 'text_start'
  | 'text_delta'
  | 'text_complete'
  | 'tool_call'
  | 'tool_result'
  | 'thinking_start'
  | 'thinking_delta'
  | 'thinking_complete'
  | 'stream_start'
  | 'step_start'
  | 'step_finish'
  | 'stream_end'
  | 'provider_tool_event'
  | 'artifact'
  | 'error';

export type KnownEventType = FrameworkWireEventType | PrismContentEventType;

// --- Closed framework / lifecycle events (9-field envelope) ---

export type GraphStartEvent = WireEvent<'graph_start', GraphStartPayload>;
export type GraphCompleteEvent = WireEvent<'graph_complete', GraphCompletePayload>;
export type GraphFailedEvent = WireEvent<'graph_failed', GraphFailedPayload>;
export type NodeStartEvent = WireEvent<'node_start', NodeStartPayload>;
export type NodeCompleteEvent = WireEvent<'node_complete', NodeCompletePayload>;
export type NodeErrorEvent = WireEvent<'node_error', NodeErrorPayload>;
export type NodeInterruptEvent = WireEvent<'node_interrupt', NodeInterruptPayload>;
export type StreamResumeEvent = WireEvent<'stream_resume', StreamResumePayload>;
export type ExecutionTerminalEvent = WireEvent<'execution_terminal', ExecutionTerminalPayload>;
export type StreamErrorEvent = WireEvent<'stream_error', StreamErrorPayload>;

/**
 * Broadcast terminal - NOT the 9-field wire envelope.
 * @see WIRE-CONTRACT.md § Terminals
 */
export type StreamCloseEvent = {
  eventType: 'stream_close';
  payload: StreamClosePayload;
};

/**
 * SSE control event: NOT the 9-field wire envelope, no seq. Delivered by SSETransport to
 * AgenticStream, which turns it into `error.kind === 'replay_unavailable'`; never dispatched
 * to `.on()` listeners.
 */
export type StreamReplayUnavailableEvent = {
  eventType: 'stream_replay_unavailable';
  payload: StreamReplayUnavailablePayload;
};

export type FrameworkWireEvent =
  | GraphStartEvent
  | GraphCompleteEvent
  | GraphFailedEvent
  | NodeStartEvent
  | NodeCompleteEvent
  | NodeErrorEvent
  | NodeInterruptEvent
  | StreamResumeEvent
  | ExecutionTerminalEvent
  | StreamErrorEvent;

// --- Semi-closed Prism content events ---

export type TextStartEvent = WireEvent<'text_start', TextStartPayload>;
export type TextDeltaEvent = WireEvent<'text_delta', TextDeltaPayload>;
export type TextCompleteEvent = WireEvent<'text_complete', TextCompletePayload>;
export type ToolCallEvent = WireEvent<'tool_call', ToolCallPayload>;
export type ToolResultEvent = WireEvent<'tool_result', ToolResultPayload>;
export type ThinkingStartEvent = WireEvent<'thinking_start', ThinkingStartPayload>;
export type ThinkingDeltaEvent = WireEvent<'thinking_delta', ThinkingDeltaPayload>;
export type ThinkingCompleteEvent = WireEvent<'thinking_complete', ThinkingCompletePayload>;
export type StreamStartEvent = WireEvent<'stream_start', StreamStartPayload>;
export type StepStartEvent = WireEvent<'step_start', StepStartPayload>;
export type StepFinishEvent = WireEvent<'step_finish', StepFinishPayload>;
export type StreamEndEvent = WireEvent<'stream_end', StreamEndPayload>;
export type ProviderToolEvent = WireEvent<'provider_tool_event', ProviderToolEventPayload>;
export type ArtifactEvent = WireEvent<'artifact', ArtifactPayload>;
export type PrismErrorEvent = WireEvent<'error', PrismErrorPayload>;

export type PrismContentEvent =
  | TextStartEvent
  | TextDeltaEvent
  | TextCompleteEvent
  | ToolCallEvent
  | ToolResultEvent
  | ThinkingStartEvent
  | ThinkingDeltaEvent
  | ThinkingCompleteEvent
  | StreamStartEvent
  | StepStartEvent
  | StepFinishEvent
  | StreamEndEvent
  | ProviderToolEvent
  | ArtifactEvent
  | PrismErrorEvent;

/** Closed + Prism events without the open author fallback. */
export type KnownStreamEvent =
  | FrameworkWireEvent
  | StreamCloseEvent
  | PrismContentEvent;

/**
 * Author / unknown future Prism types - same 9-field wire envelope, opaque payload.
 * `AbstractNode::streamEvent(string $type, array $payload)` and future Prism additions.
 *
 * Cannot participate in a discriminated union without breaking payload narrowing on known
 * literals (TypeScript treats `eventType: string` as overlapping all string literals).
 * Consumers narrow known types via {@link KnownStreamEvent} or per-type handlers (Phase 1).
 */
export type OpaqueStreamEvent = StreamWireBody & {
  eventType: string;
  payload: unknown;
};

/**
 * Full wire ingress type: frozen framework + Prism types, plus opaque author/unknown events.
 */
export type StreamEvent = KnownStreamEvent | StreamReplayUnavailableEvent | OpaqueStreamEvent;
