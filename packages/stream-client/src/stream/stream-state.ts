import type { StreamInterrupt } from '../types/interrupt.js';
import type { ExecutionTerminalPayload } from '../types/payloads/framework.js';
import type { NodeInterruptPayload } from '../types/payloads/framework.js';
import type { StreamClosePayload } from '../types/payloads/framework.js';
import type { StreamErrorPayload } from '../types/payloads/framework.js';
import type { StreamEvent } from '../types/events.js';
import type { StreamStatus } from '../types/status.js';
import type { StreamErrorDetail } from './stream-error.js';

export interface StreamStateSnapshot {
  status: StreamStatus;
  interrupt: StreamInterrupt | null;
  error: StreamErrorDetail | null;
}

export function createInitialStreamState(): StreamStateSnapshot {
  return {
    status: 'idle',
    interrupt: null,
    error: null,
  };
}

function wireRecord(value: unknown): Record<string, unknown> {
  if (value !== null && typeof value === 'object' && !Array.isArray(value)) {
    return value as Record<string, unknown>;
  }

  return {};
}

export function parseInterruptFromEvent(event: StreamEvent): StreamInterrupt | null {
  if (event.eventType === 'node_interrupt') {
    const payload = event.payload as NodeInterruptPayload;
    const fullPayload = wireRecord(event.payload);

    return {
      checkpointId: payload.checkpoint_id,
      reason: payload.reason,
      node: payload.node_name,
      payload: fullPayload,
    };
  }

  if (event.eventType === 'execution_terminal') {
    const payload = event.payload as ExecutionTerminalPayload;

    if (payload.status === 'interrupted') {
      const meta = payload.meta;

      if (meta && typeof meta.checkpoint_id === 'string') {
        const fullPayload = wireRecord(meta);

        return {
          checkpointId: meta.checkpoint_id,
          reason: typeof meta.reason === 'string' ? meta.reason : '',
          node:
            event.node_name ??
            (typeof meta.node_name === 'string' ? meta.node_name : ''),
          payload: fullPayload,
        };
      }
    }
  }

  return null;
}

/**
 * Apply a wire event to stream state (status, interrupt, error).
 */
export function applyEventToStreamState(
  event: StreamEvent,
  current: StreamStateSnapshot,
): StreamStateSnapshot {
  const next: StreamStateSnapshot = {
    status: current.status === 'idle' ? 'streaming' : current.status,
    interrupt: current.interrupt,
    error: current.error,
  };

  const interrupt = parseInterruptFromEvent(event);
  if (interrupt !== null) {
    next.interrupt = interrupt;
  }

  switch (event.eventType) {
    case 'stream_close': {
      const payload = event.payload as StreamClosePayload;

      if (payload.status === 'success') {
        next.status = 'complete';
        next.error = null;
      } else if (payload.status === 'cancelled') {
        next.status = 'interrupted';
        next.error = null;
      } else {
        next.status = 'error';
        next.error = {
          kind: 'terminal_error',
          source: 'stream_close',
          message: 'Stream closed with error',
          meta: payload.meta,
        };
      }

      break;
    }
    case 'execution_terminal': {
      const payload = event.payload as ExecutionTerminalPayload;

      if (payload.status === 'completed') {
        next.status = 'complete';
        next.error = null;
      } else if (payload.status === 'interrupted') {
        next.status = 'interrupted';
        next.error = null;
      } else {
        next.status = 'error';
        next.error = {
          kind: 'terminal_error',
          source: 'execution_terminal',
          message: 'Execution failed',
          meta: payload.meta,
        };
      }

      break;
    }
    case 'stream_resume':
      next.status = 'streaming';
      next.interrupt = null;
      next.error = null;
      break;
    case 'node_interrupt':
      next.status = 'interrupted';
      next.error = null;
      break;
    case 'stream_error': {
      const payload = event.payload as StreamErrorPayload;
      next.status = 'error';
      next.error = {
        kind: 'stream_error',
        error_type: payload.error_type,
        message: payload.message,
        stage: payload.stage,
      };
      break;
    }
    default:
      if (next.status !== 'complete' && next.status !== 'interrupted' && next.status !== 'error') {
        next.status = 'streaming';
      }

      break;
  }

  return next;
}

export function applyConnectionDrop(
  current: StreamStateSnapshot,
  message = 'Connection lost',
): StreamStateSnapshot {
  if (current.status === 'complete' || current.status === 'interrupted' || current.status === 'error') {
    return current;
  }

  return {
    ...current,
    status: 'error',
    error: { kind: 'connection', message },
  };
}
