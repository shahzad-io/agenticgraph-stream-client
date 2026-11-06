/**
 * Compile-time assertions on the composable's return types. Typechecked, never executed.
 */
import type { Ref, ShallowRef } from 'vue';
import type { AgenticStream, StreamErrorDetail, StreamEvent, StreamInterrupt, StreamStatus } from '@agenticgraph/stream-client';
import type { useAgenticStream } from './useAgenticStream.js';

type Result = ReturnType<typeof useAgenticStream>;

export const typeChecks = {
  text: null as unknown as Result['text'] satisfies Ref<string>,
  finalText: null as unknown as Result['finalText'] satisfies Ref<string>,
  status: null as unknown as Result['status'] satisfies Ref<StreamStatus>,
  error: null as unknown as Result['error'] satisfies Ref<StreamErrorDetail | null>,
  interrupt: null as unknown as Result['interrupt'] satisfies Ref<StreamInterrupt | null>,
  lastEvent: null as unknown as Result['lastEvent'] satisfies ShallowRef<StreamEvent | null>,
  client: null as unknown as Result['client'] satisfies ShallowRef<AgenticStream | null>,
  reconnect: null as unknown as Result['reconnect'] satisfies () => Promise<void>,
};
