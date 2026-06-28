import type { AgenticStream, StreamEvent, StreamSelector } from '@agenticgraph/stream-client';
import { useEffect, useMemo, useRef, useState } from 'react';

/**
 * Derive reactive state from the stream's event history via a selector function.
 * Selector functions should be stable (imported, not inline) to avoid redundant recomputation.
 */
export function useStreamSelector<T>(
  client: AgenticStream | null,
  selector: StreamSelector<T>,
): T {
  const [events, setEvents] = useState<StreamEvent[]>([]);
  const selectorRef = useRef(selector);
  selectorRef.current = selector;

  useEffect(() => {
    if (client === null) {
      setEvents([]);

      return;
    }

    const unsub = client.on((event) => {
      setEvents((current) => [...current, event]);
    });

    return unsub;
  }, [client]);

  return useMemo(() => selectorRef.current(events), [events]);
}
