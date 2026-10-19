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
import { onUnmounted, shallowRef, ref, watch } from 'vue';

export interface UseAgenticStreamOptions {
  transport: StreamTransport;
  onResume?: ResumeHandler;
}

/**
 * Vue composable — mirror of the React hook (same fields, same semantics, Vue refs).
 */
export function useAgenticStream(options: UseAgenticStreamOptions): {
  text: ReturnType<typeof ref<string>>;
  status: ReturnType<typeof ref<StreamStatus>>;
  error: ReturnType<typeof ref<StreamErrorDetail | null>>;
  interrupt: ReturnType<typeof ref<StreamInterrupt | null>>;
  resume: (decision: unknown) => Promise<void>;
  lastEvent: ReturnType<typeof shallowRef<StreamEvent | null>>;
  client: ReturnType<typeof shallowRef<AgenticStream | null>>;
} {
  const text = ref('');
  const status = ref<StreamStatus>('idle');
  const error = ref<StreamErrorDetail | null>(null);
  const interrupt = ref<StreamInterrupt | null>(null);
  const lastEvent = shallowRef<StreamEvent | null>(null);
  const client = shallowRef<AgenticStream | null>(null);

  let unsubEvents: (() => void) | undefined;
  let unsubStatus: (() => void) | undefined;
  let stream: AgenticStream | null = null;
  let cancelled = false;

  async function connect(transport: StreamTransport): Promise<void> {
    try {
      stream = await AgenticStream.connect({ transport });
    } catch (failure) {
      if (!cancelled) {
        status.value = 'error';
        error.value = { kind: 'connection', message: failure instanceof Error ? failure.message : String(failure) };
      }

      return;
    }

    if (cancelled) {
      await stream.disconnect();

      return;
    }

    client.value = stream;
    status.value = stream.status;
    error.value = stream.error;
    interrupt.value = stream.interrupt;

    unsubEvents = stream.on((event) => {
      lastEvent.value = event;
      text.value = appendTextDelta(text.value, event);
    });

    unsubStatus = stream.onStatusChange((nextStatus, snapshot) => {
      status.value = nextStatus;
      error.value = snapshot.error;
      interrupt.value = snapshot.interrupt;
    });
  }

  function cleanup(): void {
    unsubEvents?.();
    unsubStatus?.();
    unsubEvents = undefined;
    unsubStatus = undefined;
    void stream?.disconnect();
    stream = null;
    client.value = null;
    text.value = '';
    status.value = 'idle';
    error.value = null;
    interrupt.value = null;
    lastEvent.value = null;
  }

  watch(
    () => options.transport,
    (transport) => {
      cleanup();
      cancelled = false;
      void connect(transport);
    },
    { immediate: true },
  );

  onUnmounted(() => {
    cancelled = true;
    cleanup();
  });

  async function resume(decision: unknown): Promise<void> {
    if (client.value === null) {
      throw new Error('useAgenticStream: stream not connected');
    }

    await executeResume(client.value, decision, options.onResume);
  }

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
