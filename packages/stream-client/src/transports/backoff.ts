/** Reconnect backoff of both transports: exponential from `baseDelayMs`, capped at `maxDelayMs`, with jitter. */
export const BACKOFF_BASE_DELAY_MS = 500;
export const BACKOFF_MAX_DELAY_MS = 10000;

/** Delay before retry number `failures` (1 = first retry): a random value between half and all of the capped exponential. */
export function backoffDelayMs(failures: number, baseDelayMs = BACKOFF_BASE_DELAY_MS, maxDelayMs = BACKOFF_MAX_DELAY_MS): number {
  const exponential = Math.min(maxDelayMs, baseDelayMs * 2 ** (failures - 1));

  return exponential / 2 + Math.random() * (exponential / 2);
}
