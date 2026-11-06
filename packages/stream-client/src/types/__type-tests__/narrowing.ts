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

import type { ProviderErrorCategory } from '../payloads/framework.js';

function assertErrorTypes(e: KnownStreamEvent): void {
  if (e.eventType === 'graph_failed') {
    e.payload.reason satisfies 'interrupt_expired' | 'stale_queued' | 'stale_running' | undefined;
    e.payload.error_type satisfies string;
  }

  if (e.eventType === 'error') {
    e.payload.metadata satisfies Record<string, unknown> | null;
  }

  if (e.eventType === 'execution_terminal') {
    e.payload.meta?.error_type satisfies unknown;
  }

  const category: ProviderErrorCategory = 'rate_limited';
  category satisfies 'rate_limited' | 'provider_unavailable' | 'provider_error';
}

void assertErrorTypes;
