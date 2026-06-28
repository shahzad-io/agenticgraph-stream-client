/**
 * Compile-time assertions that KnownStreamEvent discriminated union narrows correctly.
 * This file is typechecked but not executed.
 */
import type { KnownStreamEvent } from '../events.js';

function assertKnownNarrowing(e: KnownStreamEvent): void {
  switch (e.eventType) {
    case 'text_delta':
      e.payload.delta satisfies string;
      break;
    case 'node_interrupt':
      e.payload.checkpoint_id satisfies string;
      break;
    case 'stream_close':
      e.payload.status satisfies 'success' | 'cancelled' | 'error';
      break;
    case 'graph_start':
      e.payload.graph_name satisfies string;
      break;
    case 'execution_terminal':
      e.payload.status satisfies 'completed' | 'interrupted' | 'failed';
      break;
    case 'tool_result':
      e.payload.success satisfies boolean;
      e.payload.error satisfies string | null;
      break;
    case 'thinking_delta':
      e.payload.summary satisfies Record<string, unknown> | null;
      break;
    default:
      e.payload satisfies unknown;
      break;
  }
}

void assertKnownNarrowing;
