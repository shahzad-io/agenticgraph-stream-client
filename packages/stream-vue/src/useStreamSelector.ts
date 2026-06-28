import type { AgenticStream, StreamEvent, StreamSelector } from '@agenticgraph/stream-client';
import { computed, ref, shallowRef, watch, type Ref } from 'vue';

/**
 * Derive reactive state from the stream's event history via a selector function.
 */
export function useStreamSelector<T>(
  client: Ref<AgenticStream | null>,
  selector: StreamSelector<T>,
): Ref<T> {
  const events = shallowRef<StreamEvent[]>([]);

  watch(
    client,
    (nextClient, _prev, onCleanup) => {
      events.value = [];

      if (nextClient === null) {
        return;
      }

      const unsub = nextClient.on((event) => {
        events.value = [...events.value, event];
      });

      onCleanup(unsub);
    },
    { immediate: true },
  );

  return computed(() => selector(events.value));
}
