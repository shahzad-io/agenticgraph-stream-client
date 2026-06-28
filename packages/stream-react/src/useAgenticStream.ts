import {
  AgenticStream,
  executeResume,
  appendTextDelta,
  type ResumeHandler,
  type StreamErrorDetail,
  type StreamEvent,
  type StreamInterrupt,
  type StreamStatus,
  type StreamTransport,
} from '@agenticgraph/stream-client';
import { useCallback, useEffect, useRef, useState } from 'react';

export interface UseAgenticStreamOptions {
  transport: StreamTransport;
  onResume?: ResumeHandler;
}

export interface UseAgenticStreamReturn {
  text: string;
  status: StreamStatus;
  error: StreamErrorDetail | null;
  interrupt: StreamInterrupt | null;
  resume: (decision: unknown) => Promise<void>;
  lastEvent: StreamEvent | null;
  client: AgenticStream | null;
}

/**
 * React hook bridging {@link AgenticStream} into reactive state.
 *
 * Text accumulation uses functional `setText` updates (one state slot, O(1) per delta).
 * Status/error/interrupt sync from the core via `onStatusChange` — not reimplemented here.
 * `lastEvent` updates on every event (intentional; use selectors for derived state).
 */
export function useAgenticStream(options: UseAgenticStreamOptions): UseAgenticStreamReturn {
  const { transport, onResume } = options;

  const [text, setText] = useState('');
  const [status, setStatus] = useState<StreamStatus>('idle');
  const [error, setError] = useState<StreamErrorDetail | null>(null);
  const [interrupt, setInterrupt] = useState<StreamInterrupt | null>(null);
  const [lastEvent, setLastEvent] = useState<StreamEvent | null>(null);
  const [client, setClient] = useState<AgenticStream | null>(null);

  const onResumeRef = useRef(onResume);
  onResumeRef.current = onResume;

  useEffect(() => {
    let stream: AgenticStream | null = null;
    let unsubEvents: (() => void) | undefined;
    let unsubStatus: (() => void) | undefined;
    let cancelled = false;

    setText('');
    setStatus('idle');
    setError(null);
    setInterrupt(null);
    setLastEvent(null);
    setClient(null);

    void (async () => {
      stream = await AgenticStream.connect({ transport });

      if (cancelled) {
        await stream.disconnect();

        return;
      }

      setClient(stream);
      setStatus(stream.status);
      setError(stream.error);
      setInterrupt(stream.interrupt);

      unsubEvents = stream.on((event) => {
        setLastEvent(event);
        setText((current) => appendTextDelta(current, event));
      });

      unsubStatus = stream.onStatusChange((nextStatus, snapshot) => {
        setStatus(nextStatus);
        setError(snapshot.error);
        setInterrupt(snapshot.interrupt);
      });
    })();

    return () => {
      cancelled = true;
      unsubEvents?.();
      unsubStatus?.();
      void stream?.disconnect();
    };
  }, [transport]);

  const resume = useCallback(
    async (decision: unknown) => {
      if (client === null) {
        throw new Error('useAgenticStream: stream not connected');
      }

      await executeResume(client, decision, onResumeRef.current);
    },
    [client],
  );

  return {
    text,
    status,
    error,
    interrupt,
    resume,
    lastEvent,
    client,
  };
}
