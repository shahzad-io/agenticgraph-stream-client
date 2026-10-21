/**
 * Interrupt surfaced by the SDK from `node_interrupt` / `execution_terminal` payloads.
 * CamelCase public shape - wire payloads use snake_case (`checkpoint_id`, `node_name`).
 */
export interface StreamInterrupt {
  checkpointId: string;
  reason: string;
  node: string;
  /** Full wire interrupt payload (baseline fields plus author-defined review content). */
  payload: Record<string, unknown>;
}
