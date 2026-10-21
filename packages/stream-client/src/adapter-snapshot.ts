import type { StreamStatus } from './types/status.js';
import type { StreamInterrupt } from './types/interrupt.js';
import type { StreamErrorDetail } from './stream/stream-error.js';
import { appendTextDelta } from './selectors.js';
import type { StreamEvent } from './types/events.js';

export interface AdapterSnapshot {
  text: string;
  status: StreamStatus;
  error: StreamErrorDetail | null;
  interrupt: StreamInterrupt | null;
  lastEvent: StreamEvent | null;
}

export function createInitialAdapterSnapshot(): AdapterSnapshot {
  return {
    text: '',
    status: 'idle',
    error: null,
    interrupt: null,
    lastEvent: null,
  };
}

/**
 * Pure reducer for mirror tests - applies one event + core getters semantics.
 * Adapters sync status/error/interrupt from the core client, not by reimplementing rules.
 */
export function reduceAdapterEvent(
  snapshot: AdapterSnapshot,
  event: StreamEvent,
  coreStatus: StreamStatus,
  coreError: StreamErrorDetail | null,
  coreInterrupt: StreamInterrupt | null,
): AdapterSnapshot {
  return {
    text: appendTextDelta(snapshot.text, event),
    status: coreStatus,
    error: coreError,
    interrupt: coreInterrupt,
    lastEvent: event,
  };
}
