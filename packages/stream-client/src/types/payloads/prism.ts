/**
 * Semi-closed Prism content event payloads.
 * Version-pinned to Prism 0.102; a future Prism upgrade may add types.
 * Field types match PrismStreamAdapter::resolvePayload emission (agenticgraph).
 */

export interface TextStartPayload {
  message_id: string;
  timestamp: number;
}

export interface TextDeltaPayload {
  delta: string;
  message_id: string;
  timestamp: number;
}

export interface TextCompletePayload {
  message_id: string;
  timestamp: number;
}

export interface ToolCallPayload {
  tool_id: string;
  tool_name: string;
  /** Record<string, mixed> from ToolCall::arguments() */
  arguments: Record<string, unknown>;
  message_id: string;
  reasoning_id: string | null;
  timestamp: number;
}

/** Matches ToolResult::$result (int|float|string|array|null on the PHP side). */
export type ToolResultValue =
  | number
  | string
  | Record<string, unknown>
  | unknown[]
  | null;

export interface ToolResultPayload {
  tool_id: string;
  tool_name: string;
  result: ToolResultValue;
  message_id: string;
  success: boolean;
  error: string | null;
  timestamp: number;
}

export interface ThinkingStartPayload {
  id: string;
  timestamp: number;
}

export interface ThinkingDeltaPayload {
  id: string;
  delta: string;
  /** array<string, mixed> | null — ThinkingEvent::$summary */
  summary: Record<string, unknown> | null;
  timestamp: number;
}

export interface ThinkingCompletePayload {
  id: string;
  /** array<string, mixed> | null — ThinkingCompleteEvent::$summary */
  summary: Record<string, unknown> | null;
  timestamp: number;
}

/** Once per model request, before the first step (Prism StreamStartEvent). */
export interface StreamStartPayload {
  id: string;
  timestamp: number;
  model: string;
  provider: string;
  /** array<string, mixed> | null */
  metadata: Record<string, unknown> | null;
}

/** Once per step: one model call of a tool run (Prism StepStartEvent). */
export interface StepStartPayload {
  id: string;
  timestamp: number;
}

export interface StepFinishUsage {
  prompt_tokens: number;
  completion_tokens: number;
  cache_write_input_tokens: number | null;
  cache_read_input_tokens: number | null;
  thought_tokens: number | null;
}

/** Once per step, with that step's finish reason and usage (Prism StepFinishEvent). */
export interface StepFinishPayload {
  id: string;
  timestamp: number;
  /** Enum name, e.g. `ToolCalls` or `Stop`; null when the provider gives none. */
  finish_reason: string | null;
  usage: StepFinishUsage | null;
}

/** Once per model request, after the last step, with the cumulative usage (Prism StreamEndEvent). */
export interface StreamEndPayload {
  id: string;
  timestamp: number;
  /** Enum name, e.g. `Stop`. */
  finish_reason: string;
  /** Sum of every step's usage. */
  usage: StepFinishUsage | null;
  /** Provider-specific citation parts; inner shape varies. */
  citations: unknown[] | null;
  /** array<string, mixed> */
  additional_content: Record<string, unknown>;
}

export interface ProviderToolEventPayload {
  event_key: string;
  tool_type: string;
  status: string;
  item_id: string;
  /** array<string, mixed> */
  data: Record<string, unknown>;
  timestamp: number;
}

/**
 * Citation value object JSON-encoded by PrismStreamAdapter (camelCase keys).
 * @see WIRE-CONTRACT.md § artifact.artifact
 */
export interface CitationArtifact {
  sourceType: string;
  source: string | number;
  sourceText: string | null;
  sourceTitle: string | null;
  sourcePositionType: string | null;
  sourceStartIndex: number | null;
  sourceEndIndex: number | null;
  additionalContent: Record<string, unknown>;
}

/**
 * CitationEvent mapping — adapter hardcodes tool_call_id/tool_name as null.
 */
export interface ArtifactPayload {
  tool_call_id: null;
  tool_name: null;
  message_id: string;
  artifact: CitationArtifact;
  block_index: number | null;
  metadata: Record<string, unknown> | null;
  timestamp: number;
}

export interface PrismErrorPayload {
  error_type: string;
  message: string;
  recoverable: boolean;
  metadata: Record<string, unknown> | null;
  timestamp: number;
}
