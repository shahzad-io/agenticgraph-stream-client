import type { EchoChannel, EchoInstance } from './echo-types.js';

/** The Echo channel surface BroadcastTransport needs (laravel-echo 1.16+ with pusher-js or Reverb). */
export interface BroadcastEchoChannel extends EchoChannel {
  listenToAll(callback: (eventName: string, payload: unknown) => void): EchoChannel;
  /** Subscription confirmed (`pusher:subscription_succeeded`). Fires again after a resubscribe. */
  subscribed(callback: () => void): EchoChannel;
  /** Subscription failed (`pusher:subscription_error`), for example channel auth refused. */
  error(callback: (error: unknown) => void): EchoChannel;
}

export interface BroadcastEchoInstance extends EchoInstance {
  channel(name: string): BroadcastEchoChannel;
  /** Echo adds `private-` itself. */
  private(name: string): BroadcastEchoChannel;
}

export interface BroadcastTransportOptions {
  echo: BroadcastEchoInstance;
  /** Channel name from the descriptor, for example `private-agenticgraph.stream.{id}`. */
  channel: string;
  /** Replay route URL from the descriptor (`replay_url`). */
  replayUrl: string;
  /** Channel auth route URL from the descriptor (`auth_url`), used by `streamChannelAuthorizer()`. */
  authUrl?: string;
  /** Stream token. Sent as `token` on replay and channel auth requests (it replaces a `token` already in the URL). */
  token?: string | null;
  /** Called once on 403 `stream_token_expired`. Return a fresh token (`$handle->streamToken()`), or null to stop. */
  refreshToken?: () => Promise<string | null>;
  /**
   * Called before each replay request with the confirmed cursor. Return the replay URL to use, or null to stop
   * (stop reason `resolver`). `after` and `token` are added to it.
   */
  resolveReplayUrl?: (cursor: number | null) => Promise<string | null> | string | null;
  /** No frame for this long while streaming starts one replay (then 2x, 4x, then every 6x). Default 5000. */
  idleMs?: number;
}

/** One page of the core replay route (`GET {prefix}/{streamId}/events`). */
export interface ReplayPage {
  events: { type: string; body: unknown }[];
  last_seq: number;
  more: boolean;
  gone: boolean;
  final_state?: Record<string, unknown> | null;
}
