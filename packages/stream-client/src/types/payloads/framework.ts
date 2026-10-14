/**
 * Framework telemetry injected into lifecycle event payloads by StreamLifecycleEventEmitter.
 * Clients may ignore `payload.stream` — not part of the frozen public promise.
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

export interface GraphFailedPayload {
  error_type: string;
  message: string;
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
  error_type: string;
  message: string;
  stream?: LifecycleStreamTelemetry;
}

export interface NodeInterruptPayload {
  node_name: string;
  reason: string;
  checkpoint_id: string;
  /** Present when the node returns non-null from getInterruptPayload() — author-defined review content. */
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
  meta?: Record<string, unknown>;
  stream?: LifecycleStreamTelemetry;
}

/**
 * Broadcast terminal — minimal payload, NOT the 9-field wire envelope.
 * @see WIRE-CONTRACT.md § Terminals
 */
export interface StreamClosePayload {
  status: 'success' | 'cancelled' | 'error';
  meta: unknown;
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
