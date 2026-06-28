/**
 * Normalized public stream status.
 * @see stream-client-foundational-spec.md § Status normalization
 */
export type StreamStatus = 'idle' | 'streaming' | 'complete' | 'interrupted' | 'error';
