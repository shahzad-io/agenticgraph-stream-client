/**
 * Framework telemetry injected into lifecycle event payloads by StreamLifecycleEventEmitter.
 * Clients may ignore `payload.stream` - not part of the frozen public promise.
 */
export interface LifecycleStreamTelemetry {
  seq: number;
  node_seq: number | null;
  event_type: string;
  bytes: number;
}

export interface GraphStartPayload {
  graph_name: string;
  stream?: LifecycleStreamTelemetry;
}

export interface GraphCompletePayload {
  duration_ms: number;
  stream?: LifecycleStreamTelemetry;
}

/**
 * `error_type` on the wire. By default a generic value (`ExecutionFailed`) or, for a failed model
 * provider call, a fixed category. With `AGENTICGRAPH_STREAM_EXPOSE_ERROR_DETAILS` on the server it
 * is the exception class (any string). Always a string; the union is for autocomplete.
 */
export type ErrorType = ProviderErrorCategory | 'ExecutionFailed' | (string & {});

/** Fixed categories the server picks for a failed model provider call (never the provider's text). */
export type ProviderErrorCategory = 'rate_limited' | 'provider_unavailable' | 'provider_error';

/** Why an execution was failed from outside the run. */
export type GraphFailedReason = 'interrupt_expired' | 'stale_queued' | 'stale_running';

export interface GraphFailedPayload {
  /** See {@link ErrorType}. */
  error_type: ErrorType;
  /** A fixed sentence by default; the exception message when the server exposes error details. */
  message: string;
  reason?: GraphFailedReason;
  stream?: LifecycleStreamTelemetry;
}

export interface NodeStartPayload {
  node_name: string;
  stream?: LifecycleStreamTelemetry;
}

export interface StateDiffModifiedEntry {
  from: unknown;
  to: unknown;
}

export interface StateDiff {
  added: Record<string, unknown>;
  modified: Record<string, StateDiffModifiedEntry>;
  removed: Record<string, unknown>;
}

export interface AgenticGraphStateCapture {
  __agenticgraph_state: {
    data: Record<string, unknown>;
    metadata: Record<string, unknown>;
  };
}

/**
 * Base `node_complete` payload. `state_diff` / `state` are config-gated
 * (agenticgraph.streaming.capture.state: none | diff | full).
 */
export interface NodeCompletePayload {
  node_name: string;
  duration_ms: number;
  stream?: LifecycleStreamTelemetry;
  state_diff?: StateDiff;
  state?: AgenticGraphStateCapture;
}

export interface NodeErrorPayload {
  node_name: string;
  /** See {@link ErrorType}. */
  error_type: ErrorType;
  message: string;
  stream?: LifecycleStreamTelemetry;
}

export interface NodeInterruptPayload {
  node_name: string;
  reason: string;
  checkpoint_id: string;
  /** Present when the node returns non-null from getInterruptPayload() - author-defined review content. */
  interrupt_payload?: unknown;
  stream?: LifecycleStreamTelemetry;
}

/**
 * Gap replay item inside `stream_resume.gap_events`.
 * Exception to external-type rule: each item carries `type` inline.
 */
export interface StreamResumeGapEvent {
  seq: number;
  node_seq: number | null;
  node_name: string | null;
  type: string;
  payload: unknown;
  occurred_at: string;
}

export interface StreamResumePayload {
  from_seq: number;
  resumed_at_seq: number;
  gap_events: StreamResumeGapEvent[];
  checkpoint_id: string;
  stream?: LifecycleStreamTelemetry;
}

export type ExecutionTerminalStatus = 'completed' | 'interrupted' | 'failed';

export interface ExecutionTerminalPayload {
  status: ExecutionTerminalStatus;
  /** null unless the server sets AGENTICGRAPH_STREAM_EXPOSE_FINAL_STATE=true (wire contract v1.1). */
  final_state: Record<string, unknown> | null;
  /** For `failed`: `error_type`, `message` and `reason` (generic unless the server exposes error details). */
  meta?: ExecutionTerminalMeta;
  stream?: LifecycleStreamTelemetry;
}

export type ExecutionTerminalMeta = Record<string, unknown> & {
  error_type?: ErrorType;
  message?: string;
  reason?: GraphFailedReason;
};

/**
 * Broadcast terminal - minimal payload, NOT the 9-field wire envelope.
 * @see WIRE-CONTRACT.md § Terminals
 */
export interface StreamClosePayload {
  status: 'success' | 'cancelled' | 'error';
  meta: unknown;
}

/**
 * SSE control event `replay_unavailable`: the cursor is gone from the store. No wire body,
 * no seq. `cursor` may arrive as a string from PHP; read it with parseReplayCursor().
 */
export interface StreamReplayUnavailablePayload {
  cursor: number | string;
  /** Present only when the stream URL carries `execution_id`; null unless the server exposes it. */
  final_state?: Record<string, unknown> | null;
}

export type StreamErrorStage = 'store' | 'transport' | 'observer';

export interface StreamErrorPayload {
  error_type: string;
  message: string;
  stage: StreamErrorStage;
  event_type: string | null;
  event_seq: number | null;
  event_node_name: string | null;
  event_node_seq: number | null;
}
