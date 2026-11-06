import {
  AgenticStream,
  executeResume,
  appendFinalTextDelta,
  appendTextDelta,
  type ResumeHandler,
  type StreamErrorDetail,
  type StreamEvent,
  type StreamInterrupt,
  type StreamStatus,
  type StreamTransport,
} from '@agenticgraph/stream-client';
import { onUnmounted, shallowRef, ref, watch, type Ref, type ShallowRef } from 'vue';

export interface UseAgenticStreamOptions {
  transport: StreamTransport;
  onResume?: ResumeHandler;
}

/**
 * Vue composable - mirror of the React hook (same fields, same semantics, Vue refs).
 */
export function useAgenticStream(options: UseAgenticStreamOptions): {
  /** Every text_delta of the run, every step (the narration). */
  text: Ref<string>;
  /** Text of the current or last step; after a tool run, the final answer (LlmResponse::text on the server). */
  finalText: Ref<string>;
  status: Ref<StreamStatus>;
  error: Ref<StreamErrorDetail | null>;
  interrupt: Ref<StreamInterrupt | null>;
  resume: (decision: unknown) => Promise<void>;
  /** Resume from the last event after a lost connection (`error.canReconnect` is true). No-op when connected. */
  reconnect: () => Promise<void>;
  lastEvent: ShallowRef<StreamEvent | null>;
  client: ShallowRef<AgenticStream | null>;
} {
  const text = ref('');
  const finalText = ref('');
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
        error.value = { kind: 'connection', message: failure instanceof Error ? failure.message : String(failure), canReconnect: false };
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
      finalText.value = appendFinalTextDelta(finalText.value, event);
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
    finalText.value = '';
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

  async function reconnect(): Promise<void> {
    await client.value?.reconnect();
  }

  return {
    text,
    finalText,
    status,
    error,
    interrupt,
    resume,
    reconnect,
    lastEvent,
    client,
  };
}
