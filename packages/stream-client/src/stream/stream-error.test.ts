import { describe, expect, it } from 'vitest';
import { parseReplayCursor } from './stream-error.js';

describe('parseReplayCursor', () => {
  it('parses numeric cursor', () => {
    expect(parseReplayCursor({ cursor: 5 })).toBe(5);
  });

  it('parses string cursor from PHP wire payloads', () => {
    expect(parseReplayCursor({ cursor: '0' })).toBe(0);
    expect(parseReplayCursor({ cursor: '5' })).toBe(5);
  });

  it('returns 0 for missing or invalid cursor', () => {
    expect(parseReplayCursor({})).toBe(0);
    expect(parseReplayCursor({ cursor: 'not-a-number' })).toBe(0);
    expect(parseReplayCursor(null)).toBe(0);
  });
});
