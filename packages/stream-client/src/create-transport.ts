import type { StreamTransport } from './transport.js';
import { BroadcastTransport, streamChannelAuth } from './transports/broadcast-transport.js';
import type { BroadcastEchoInstance } from './transports/broadcast-types.js';
import { SSETransport } from './transports/sse-transport.js';

/** `$handle->stream()->toArray()` from core 0.3. */
export interface StreamDescriptor {
  transport: 'sse' | 'broadcast' | null;
  stream_id: string | null;
  stream_url: string | null;
  replay_url: string | null;
  auth_url: string | null;
  token: string | null;
  channel: string | null;
}

export interface CreateStreamTransportOptions {
  /** A configured Laravel Echo instance. Needed for a broadcast descriptor. */
  echo?: BroadcastEchoInstance;
  /** Returns a fresh stream token (`$handle->streamToken()`) after it expired, or null. */
  refreshToken?: () => Promise<string | null>;
  /** Passed to both transports: consecutive failures before state `failed` (default 5). */
  maxAttempts?: number;
}

/** Builds `SSETransport` or `BroadcastTransport` from the descriptor the server sent. */
export function createStreamTransport(
  descriptor: StreamDescriptor,
  options: CreateStreamTransportOptions = {},
): StreamTransport {
  const { refreshToken, maxAttempts } = options;

  if (descriptor.transport === 'sse' && descriptor.stream_url) {
    return new SSETransport({ url: descriptor.stream_url, refreshToken, maxAttempts });
  }

  if (descriptor.transport === 'broadcast' && descriptor.channel && descriptor.replay_url) {
    if (!options.echo) {
      throw new Error('createStreamTransport: a broadcast descriptor needs { echo }, a configured Laravel Echo instance');
    }

    return new BroadcastTransport({
      echo: options.echo,
      channel: descriptor.channel,
      replayUrl: descriptor.replay_url,
      authUrl: descriptor.auth_url ?? undefined,
      token: descriptor.token,
      refreshToken,
      maxAttempts,
    });
  }

  throw new Error(`createStreamTransport: unusable descriptor (transport ${String(descriptor.transport)})`);
}

/** Echo `channelAuthorization.customHandler` signature (pusher-js 8). */
export type ChannelAuthorizationHandler = (
  params: { socketId: string; channelName: string },
  callback: (error: Error | null, authData: unknown) => void,
) => void;

/**
 * Echo `channelAuthorization.customHandler` for stream channels: answers with the token and `auth_url` of the
 * BroadcastTransport that subscribed. Other channels go to `fallback` (Echo has one handler per instance).
 */
export function streamChannelAuthorizer(options: { fallback?: ChannelAuthorizationHandler } = {}): ChannelAuthorizationHandler {
  return (params, callback) => {
    const authorize = streamChannelAuth.get(params.channelName);

    if (authorize) {
      authorize(params.socketId).then(
        (authData) => callback(null, authData),
        (error: unknown) => callback(error instanceof Error ? error : new Error(String(error)), null),
      );
    } else if (options.fallback) {
      options.fallback(params, callback);
    } else {
      callback(new Error(`No channel authorizer for ${params.channelName}`), null);
    }
  };
}
