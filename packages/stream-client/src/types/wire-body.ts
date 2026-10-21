/**
 * Frozen v1.0 wire-body field names emitted by StreamEventEnvelope::toWireArray().
 * `type` is intentionally excluded - it is carried by the transport event name.
 */
export const FROZEN_WIRE_BODY_KEYS = [
  'stream_id',
  'execution_id',
  'node_name',
  'span_id',
  'parent_span_id',
  'seq',
  'node_seq',
  'occurred_at',
  'payload',
] as const;

export type FrozenWireBodyKey = (typeof FROZEN_WIRE_BODY_KEYS)[number];

/**
 * The 9 frozen snake_case fields in every standard wire envelope.
 * `type` / `eventType` is external - NOT a key in this body.
 */
export interface StreamWireBody {
  stream_id: string;
  execution_id: string;
  node_name: string | null;
  span_id: string | null;
  parent_span_id: string | null;
  seq: number;
  node_seq: number | null;
  occurred_at: string;
  payload: unknown;
}

/**
 * Helper for building discriminated wire events with a narrowed payload.
 * Fields mirror {@link StreamWireBody} verbatim; defined explicitly so
 * `switch (event.eventType)` narrows `payload` correctly.
 */
export type WireEvent<TEventType extends string, TPayload> = {
  eventType: TEventType;
  stream_id: string;
  execution_id: string;
  node_name: string | null;
  span_id: string | null;
  parent_span_id: string | null;
  seq: number;
  node_seq: number | null;
  occurred_at: string;
  payload: TPayload;
};
