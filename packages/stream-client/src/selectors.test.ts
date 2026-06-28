import { describe, expect, it } from 'vitest';
import { appendTextDelta, nodeTextSelector } from './selectors.js';
import {
  makeTextDeltaEvent,
  resetSeqCounter,
} from './testing/event-fixtures.js';
import type { StreamEvent } from './types/events.js';

describe('nodeTextSelector', () => {
  resetSeqCounter();

  it('accumulates text_delta per node while hook text spans all nodes', async () => {
    const events: StreamEvent[] = [
      makeTextDeltaEvent('A1', { node_name: 'a' }),
      makeTextDeltaEvent('B1', { node_name: 'b' }),
      makeTextDeltaEvent('A2', { node_name: 'a' }),
      makeTextDeltaEvent('B2', { node_name: 'b' }),
    ];

    const selectA = nodeTextSelector('a');
    const selectB = nodeTextSelector('b');

    expect(selectA(events)).toBe('A1A2');
    expect(selectB(events)).toBe('B1B2');

    let hookText = '';
    for (const event of events) {
      hookText = appendTextDelta(hookText, event);
    }

    expect(hookText).toBe('A1B1A2B2');
  });
});
