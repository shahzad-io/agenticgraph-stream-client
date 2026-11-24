import type { StreamEvent } from '../types/events.js';
import type {
  StreamTransport,
  StreamTransportConnectionState,
  StreamTransportStopReason,
} from '../transport.js';
import { backoffDelayMs } from './backoff.js';
import type { BroadcastEchoChannel, BroadcastTransportOptions, ReplayPage } from './broadcast-types.js';
import { streamErrorCode, withQuery } from './url.js';
import { normalizeEventName, wireToStreamEvent } from './wire-event.js';

/** A hole in seq starts one replay after this delay, so a filtered run makes one replay per burst. */
const GAP_DEBOUNCE_MS = 250;

/** Channel auth for stream channels, keyed by full channel name. Read by `streamChannelAuthorizer()`. */
export const streamChannelAuth = new Map<string, (socketId: string) => Promise<unknown>>();

interface Held {
  type: string;
  body: Record<string, unknown>;
}

function seqOf(body: unknown): number | null {
  const seq = (body as { seq?: unknown } | null)?.seq;

  return typeof seq === 'number' ? seq : null;
}

function terminalStatus(type: string, body: Record<string, unknown>): unknown {
  return type === 'execution_terminal' ? (body.payload as { status?: unknown } | null)?.status : undefined;
}

/**
 * Broadcast transport for core 0.3: the store is the source of truth, the channel is the fast path.
 *
 * Start: subscribe, wait for the subscription to be confirmed, buffer live frames, replay from the start (or the
 * confirmed cursor), merge by seq. Steady state: unpack `batch` frames, drop seen seq, hold frames above a hole and
 * replay after the confirmed cursor (debounced). End: a completed or failed `execution_terminal` stops the transport,
 * after a replay when a hole is open; `stream_close` without the terminal replays at once; no frame for `idleMs`
 * while connected replays with backoff; while interrupted it replays at the maximum interval. A failed replay is retried
 * with the SSETransport reconnect backoff (not at the idle timer); more than `maxAttempts` failed replays in a row stop
 * it with `failed`. `interrupted` keeps it connected. `stream_close` is never passed on (SSE has none), so handlers see
 * the same events as on SSE.
 */
export class BroadcastTransport implements StreamTransport {
  private readonly options: BroadcastTransportOptions;

  private readonly idleMs: number;

  private readonly maxAttempts: number;

  /** Consecutive failed replays. */
  private failures = 0;

  private token: string | null;

  private channel: BroadcastEchoChannel | null = null;

  private eventCallback: ((event: StreamEvent) => void) | null = null;

  private connectionState: StreamTransportConnectionState = 'disconnected';

  private readonly connectionStateListeners = new Set<
    (state: StreamTransportConnectionState, stopReason: StreamTransportStopReason | null) => void
  >();

  private stopReason: StreamTransportStopReason | null = null;

  private stopped = true;

  /** Last seq applied with no hole before it, or the `last_seq` of the last replay. Null: nothing confirmed. */
  private cursor: number | null = null;

  /** Live frames not applied yet: before the first replay, or above a hole. */
  private readonly held = new Map<number, Held>();

  /** The first replay after a subscribe has merged. Until then live frames are only held. */
  private started = false;

  private interrupted = false;

  private replaying = false;

  private replayAgain = false;

  private gapTimer: ReturnType<typeof setTimeout> | null = null;

  private idleTimer: ReturnType<typeof setTimeout> | null = null;

  private retryTimer: ReturnType<typeof setTimeout> | null = null;

  private idleRound = 0;

  constructor(options: BroadcastTransportOptions) {
    this.options = options;
    this.idleMs = options.idleMs ?? 5000;
    this.maxAttempts = options.maxAttempts ?? 5;
    this.token = options.token ?? null;
  }

  async connect(): Promise<void> {
    if (this.connectionState === 'connected') {
      return;
    }

    this.subscribe('connecting');
  }

  async disconnect(): Promise<void> {
    this.stop('disconnected', 'manual');
    this.eventCallback = null;
    this.cursor = null;
  }

  /** Subscribe again and replay from the confirmed cursor (also after a stop). No-op when connected. */
  async reconnect(): Promise<void> {
    if (this.connectionState === 'connected') {
      return;
    }

    this.subscribe('reconnecting');
  }

  onEvent(callback: (event: StreamEvent) => void): void {
    this.eventCallback = callback;
  }

  getConnectionState(): StreamTransportConnectionState {
    return this.connectionState;
  }

  getStopReason(): StreamTransportStopReason | null {
    return this.stopReason;
  }

  onConnectionStateChange(
    callback: (state: StreamTransportConnectionState, stopReason: StreamTransportStopReason | null) => void,
  ): () => void {
    this.connectionStateListeners.add(callback);

    return () => {
      this.connectionStateListeners.delete(callback);
    };
  }

  /** The confirmed cursor. */
  getLastSeq(): number | null {
    return this.cursor;
  }

  private subscribe(state: 'connecting' | 'reconnecting'): void {
    this.leaveChannel();
    this.stopped = false;
    this.stopReason = null;
    this.started = false;
    this.failures = 0;
    this.setConnectionState(state);

    const { echo, channel: name } = this.options;
    streamChannelAuth.set(name, this.authorize);
    const channel = name.startsWith('private-') ? echo.private(name.slice('private-'.length)) : echo.channel(name);
    this.channel = channel;
    channel.listenToAll((eventName, payload) => this.onFrame(normalizeEventName(eventName), payload));
    channel.error(() => this.stop('disconnected', 'unauthorized'));
    // Confirmed, not called: a private channel subscribes after an auth round trip, and a replay that answered
    // before that would miss a frame sent in between. A resubscribe after a socket drop lands here again.
    channel.subscribed(() => {
      if (this.stopped) {
        return;
      }

      this.setConnectionState('connected');
      this.armIdle(true);
      void this.replay();
    });
  }

  private readonly authorize = async (socketId: string): Promise<unknown> => {
    const { authUrl, channel } = this.options;

    if (authUrl === undefined) {
      throw new Error(`BroadcastTransport: no authUrl for channel ${channel}`);
    }

    const response = await this.request((token) => fetch(authUrl, {
      method: 'POST',
      headers: { Accept: 'application/json', 'Content-Type': 'application/x-www-form-urlencoded' },
      body: new URLSearchParams({ socket_id: socketId, channel_name: channel, ...(token === null ? {} : { token }) }),
    }));

    if (response === null || !response.ok) {
      throw new Error(`Stream channel auth failed${response ? ` (${response.status})` : ''}`);
    }

    return response.json();
  };

  /**
   * One request with the token rule of spec 5.3: on 403 `stream_token_expired`, ask `refreshToken` once and retry.
   * Null: the transport stopped with `unauthorized` (another 403 code, no new token, or a second 403).
   */
  private async request(send: (token: string | null) => Promise<Response>): Promise<Response | null> {
    let response = await send(this.token);

    if (response.status !== 403) {
      return response;
    }

    if ((await streamErrorCode(response)) === 'stream_token_expired' && this.options.refreshToken) {
      const next = await this.options.refreshToken().catch(() => null);

      if (next) {
        this.token = next;
        response = await send(next);

        if (response.status !== 403) {
          return response;
        }
      }
    }

    this.stop('disconnected', 'unauthorized');

    return null;
  }

  private onFrame(type: string, payload: unknown): void {
    if (this.stopped) {
      return;
    }

    this.armIdle(true);

    if (type === 'stream_close') {
      // The run ended. Without the terminal applied, the store has it: replay now, not at the idle timer.
      void this.replay();

      return;
    }

    if (type === 'batch') {
      const events = (payload as { events?: unknown } | null)?.events;

      for (const inner of Array.isArray(events) ? events : []) {
        const { type: innerType, ...body } = inner as Record<string, unknown>;

        if (typeof innerType === 'string') {
          this.hold(innerType, body);
        }
      }
    } else {
      this.hold(type, payload);
    }

    if (this.started) {
      this.drain();
    }
  }

  private hold(type: string, body: unknown): void {
    const seq = seqOf(body);

    if (seq !== null && (this.cursor === null || seq > this.cursor)) {
      this.held.set(seq, { type, body: body as Record<string, unknown> });
    }
  }

  /** Apply held frames in seq order while they are contiguous with the cursor; a hole left over starts a replay. */
  private drain(): void {
    while (!this.stopped && this.held.size > 0) {
      const seq = Math.min(...this.held.keys());
      const next = this.held.get(seq) as Held;

      if (this.cursor !== null && seq <= this.cursor) {
        this.held.delete(seq);
      } else if (seq === (this.cursor ?? 0) + 1) {
        this.held.delete(seq);
        this.apply(next.type, next.body);
      } else {
        break;
      }
    }

    if (this.stopped || this.held.size === 0) {
      this.clearGapTimer();

      return;
    }

    if ([...this.held.values()].some((frame) => frame.type === 'execution_terminal')) {
      // Terminal rule: the run is over, read the hole now.
      void this.replay();
    } else if (this.gapTimer === null) {
      this.gapTimer = setTimeout(() => {
        this.gapTimer = null;
        void this.replay();
      }, GAP_DEBOUNCE_MS);
    }
  }

  private apply(type: string, body: Record<string, unknown>): void {
    const event = wireToStreamEvent(type, body);
    this.cursor = seqOf(body) ?? this.cursor;

    if (event === null) {
      return;
    }

    const status = terminalStatus(type, body);

    if (status === 'interrupted' || type === 'node_interrupt') {
      this.interrupted = true;
      this.armIdle(true);
    } else if (type === 'stream_resume') {
      this.interrupted = false;
      this.armIdle(true);
    }

    this.eventCallback?.(event);

    if (status === 'completed' || status === 'failed') {
      this.stop('disconnected', 'terminal');
    }
  }

  /** Replay after the confirmed cursor until `more` is false, merge, then drain. Calls during a replay run one more. */
  private async replay(): Promise<void> {
    if (this.replaying) {
      this.replayAgain = true;

      return;
    }

    this.replaying = true;
    this.clearGapTimer();
    this.clearRetryTimer();

    try {
      do {
        this.replayAgain = false;
        let more = true;

        while (more && !this.stopped) {
          const page = await this.fetchPage();

          if (page === null || this.stopped) {
            return;
          }

          if (page.gone) {
            this.eventCallback?.({
              eventType: 'stream_replay_unavailable',
              payload: { cursor: this.cursor ?? 0, ...('final_state' in page ? { final_state: page.final_state ?? null } : {}) },
            });
            this.stop('disconnected', 'replay_unavailable');

            return;
          }

          for (const { type, body } of page.events) {
            const seq = seqOf(body);

            if (this.stopped) {
              return;
            }

            if (seq !== null && (this.cursor === null || seq > this.cursor)) {
              this.apply(type, body as Record<string, unknown>);
            }
          }

          // A seq the store covered but did not return does not exist (filtered, or its store write failed).
          if (page.last_seq > 0 && (this.cursor === null || page.last_seq > this.cursor)) {
            this.cursor = page.last_seq;
          }

          more = page.more;
        }

        this.started = true;
        this.drain();
      } while (this.replayAgain && !this.stopped);
    } finally {
      this.replaying = false;
    }
  }

  /** One replay page. Null: stopped (resolver, unauthorized, failed) or failed with a retry scheduled. */
  private async fetchPage(): Promise<ReplayPage | null> {
    try {
      const base = this.options.resolveReplayUrl
        ? await this.options.resolveReplayUrl(this.cursor)
        : this.options.replayUrl;

      if (this.stopped) {
        return null;
      }

      if (base === null) {
        this.stop('disconnected', 'resolver');

        return null;
      }

      // No cursor: no `after`, read from the start. `after=0` would be gone on a stream whose first seq is above 1.
      const after = this.cursor === null ? null : String(this.cursor);
      const response = await this.request((token) => fetch(withQuery(base, { after, token }), {
        headers: { Accept: 'application/json' },
      }));

      if (response === null) {
        return null;
      }

      if (!response.ok) {
        throw new Error(`replay ${response.status}`);
      }

      const page = (await response.json()) as ReplayPage;
      this.failures = 0;

      return page;
    } catch {
      // A failed replay (network, 5xx) is retried with the SSETransport backoff, up to maxAttempts in a row.
      this.failures += 1;

      if (this.failures > this.maxAttempts) {
        this.stop('failed', 'failed');
      } else if (!this.stopped) {
        this.clearRetryTimer();
        this.retryTimer = setTimeout(() => {
          this.retryTimer = null;
          void this.replay();
        }, backoffDelayMs(this.failures));
      }

      return null;
    }
  }

  /**
   * Idle replay: no frame for idleMs, then 2x, 4x, then every 6x (5, 10, 20, 30 s by default). Reset on a frame.
   * While interrupted: every 6x, so a resume the socket did not deliver is found in the store.
   */
  private armIdle(reset: boolean): void {
    if (reset) {
      this.idleRound = 0;
    }

    this.clearIdle();

    if (this.stopped || this.connectionState !== 'connected') {
      return;
    }

    const max = this.idleMs * 6;
    const delay = this.interrupted ? max : Math.min(this.idleMs * 2 ** this.idleRound, max);
    this.idleTimer = setTimeout(() => {
      this.idleTimer = null;
      this.idleRound += 1;
      this.armIdle(false);
      void this.replay();
    }, delay);
  }

  private clearIdle(): void {
    if (this.idleTimer !== null) {
      clearTimeout(this.idleTimer);
      this.idleTimer = null;
    }
  }

  private clearRetryTimer(): void {
    if (this.retryTimer !== null) {
      clearTimeout(this.retryTimer);
      this.retryTimer = null;
    }
  }

  private clearGapTimer(): void {
    if (this.gapTimer !== null) {
      clearTimeout(this.gapTimer);
      this.gapTimer = null;
    }
  }

  private leaveChannel(): void {
    this.clearIdle();
    this.clearGapTimer();
    this.clearRetryTimer();
    this.held.clear();

    if (streamChannelAuth.get(this.options.channel) === this.authorize) {
      streamChannelAuth.delete(this.options.channel);
    }

    if (this.channel) {
      this.channel = null;
      this.options.echo.leave(this.options.channel.replace(/^private-/, ''));
    }
  }

  /** The first stop holds until the next connect()/reconnect(). */
  private stop(state: 'disconnected' | 'failed', reason: StreamTransportStopReason): void {
    if (this.stopped) {
      return;
    }

    this.stopped = true;
    this.stopReason = reason;
    this.leaveChannel();
    this.setConnectionState(state);
  }

  private setConnectionState(state: StreamTransportConnectionState): void {
    if (this.connectionState === state) {
      return;
    }

    this.connectionState = state;

    for (const listener of this.connectionStateListeners) {
      listener(state, this.stopReason);
    }
  }
}
