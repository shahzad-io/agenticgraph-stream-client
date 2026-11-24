// Test fakes for the broadcast path: recorded core 0.3 runs, a fake Echo, a fake replay and channel auth route.
import type { ChannelAuthorizationHandler } from '../create-transport.js';
import type { BroadcastEchoChannel, BroadcastEchoInstance } from '../transports/broadcast-types.js';
import recorded from './frames-0.3.json';

export interface Frame {
  event: string;
  payload: Record<string, unknown> & { seq?: number; events?: Record<string, unknown>[] };
}

export interface StoredEvent {
  type: string;
  body: Record<string, unknown> & { seq: number };
}

export interface RecordedRun {
  name: string;
  /** All text_delta text of the run, from the store. */
  text: string;
  /** What the core BroadcastSink sent, in order: frames, `batch` frames, the terminal, `stream_close`. */
  broadcast: Frame[];
  /** Interrupted run: index in `broadcast` where the resumed segment starts. */
  interrupt_at: number | null;
  stored: StoredEvent[];
  /** The real SSE route answer (custom_events=wrap). */
  sse: string;
}

/** Runs dumped from core 0.3 (scratch core/DumpFrames03Test.php): streamed, tool, held, interrupted, failed, filter. */
export function loadRuns(): Record<string, RecordedRun> {
  return structuredClone((recorded as unknown as { runs: Record<string, RecordedRun> }).runs);
}

/** The seq of a frame, or of the first event of a batch frame. */
export function frameSeq(frame: Frame): number | null {
  return frame.event === 'batch' ? ((frame.payload.events?.[0]?.seq as number | undefined) ?? null) : frame.payload.seq ?? null;
}

export interface FakeEchoOptions {
  /** Called like Echo calls `channelAuthorization.customHandler` before a private subscription. */
  authorizer?: ChannelAuthorizationHandler;
  /** Do not confirm the subscription by itself: call `confirm()`. */
  manualConfirm?: boolean;
}

/** One channel, as laravel-echo with pusher-js: listenToAll gets `.name`; frames before confirmation are lost. */
export function fakeEcho(options: FakeEchoOptions = {}) {
  let onAll: ((eventName: string, payload: unknown) => void) | null = null;
  let onSubscribed: (() => void) | null = null;
  let onError: ((error: unknown) => void) | null = null;
  let live = false;
  const log: string[] = [];

  const channel: BroadcastEchoChannel = {
    listen: () => channel,
    stopListening: () => channel,
    listenToAll(callback) {
      onAll = callback;

      return channel;
    },
    subscribed(callback) {
      onSubscribed = callback;

      return channel;
    },
    error(callback) {
      onError = callback;

      return channel;
    },
    unsubscribe() {
      live = false;
    },
  };

  const fake = {
    log,
    get live() {
      return live;
    },
    /** Subscription succeeded (again after a resubscribe). */
    confirm() {
      live = true;
      onSubscribed?.();
    },
    fail(status = 403) {
      onError?.({ status });
    },
    /** The socket dropped: frames are lost until `confirm()`. */
    drop() {
      live = false;
    },
    push(frame: Frame) {
      if (live) {
        onAll?.(`.${frame.event}`, frame.payload);
      }
    },
    echo: null as unknown as BroadcastEchoInstance,
  };

  const subscribe = (name: string): BroadcastEchoChannel => {
    log.push(`subscribe ${name}`);
    live = false;
    queueMicrotask(() => {
      if (options.authorizer) {
        options.authorizer({ socketId: '1234.5678', channelName: name }, (error) => (error ? fake.fail(403) : fake.confirm()));
      } else if (!options.manualConfirm) {
        fake.confirm();
      }
    });

    return channel;
  };

  fake.echo = {
    channel: (name) => subscribe(name),
    private: (name) => subscribe(`private-${name}`),
    leave: (name) => {
      log.push(`leave ${name}`);
      live = false;
    },
    disconnect: () => undefined,
  };

  return fake;
}

export interface FakeRoutesOptions {
  /** How many stored events the store holds now (a run still going). Default: all. */
  visible?: number;
  pageSize?: number;
  /** Replay with `after` answers `gone` (truncated store). */
  gone?: boolean;
  /** Tokens the routes accept. Default: any. */
  validTokens?: string[];
  /** Tokens the routes answer with 403 `stream_token_expired`. */
  expiredTokens?: string[];
  /** Channel auth answers 403 `stream_forbidden`. */
  authForbidden?: boolean;
  /** The first N replays answer 503 (Infinity: every replay). */
  failReplays?: number;
}

/** The core replay route and channel auth route over one recorded run, as a `fetch`. */
export function fakeRoutes(run: RecordedRun, options: FakeRoutesOptions = {}) {
  const requests: { method: string; url: URL; token: string | null; at: number }[] = [];
  let release: (() => void) | null = null;
  let gate: Promise<void> | null = null;

  const routes = {
    requests,
    visible: options.visible ?? run.stored.length,
    /** Replays made (requests to the replay route). */
    get replays() {
      return requests.filter((r) => r.method === 'GET').length;
    },
    /** Hold the next replay answer until `release()`. */
    hold() {
      gate = new Promise((done) => {
        release = done;
      });
    },
    release() {
      release?.();
      gate = null;
    },
    fetch: async (input: string | URL | Request, init?: RequestInit): Promise<Response> => {
      const url = new URL(String(input), 'http://app.test');
      const method = init?.method ?? 'GET';
      const form = method === 'POST' ? new URLSearchParams(String(init?.body ?? '')) : null;
      const token = form ? form.get('token') : url.searchParams.get('token');
      requests.push({ method, url, token, at: Date.now() });

      if (token !== null && options.expiredTokens?.includes(token)) {
        return Response.json({ error: 'stream_token_expired' }, { status: 403 });
      }

      if (options.validTokens && (token === null || !options.validTokens.includes(token))) {
        return Response.json({ error: 'stream_token_invalid' }, { status: 403 });
      }

      if (method === 'POST') {
        return options.authForbidden
          ? Response.json({ error: 'stream_forbidden' }, { status: 403 })
          : Response.json({ auth: `key:sig-${form?.get('socket_id')}` });
      }

      if (gate) {
        await gate;
      }

      if (routes.replays <= (options.failReplays ?? 0)) {
        return new Response('', { status: 503 });
      }

      const after = url.searchParams.has('after') ? Number(url.searchParams.get('after')) : null;

      if (after !== null && options.gone) {
        return Response.json({ events: [], last_seq: after, more: false, gone: true });
      }

      const pageSize = options.pageSize ?? 200;
      const page = run.stored.slice(0, routes.visible).filter((e) => e.body.seq > (after ?? 0)).slice(0, pageSize);

      return Response.json({
        events: page.map((e) => ({ type: e.type, body: e.body })),
        last_seq: page.at(-1)?.body.seq ?? after ?? 0,
        more: page.length === pageSize,
        gone: false,
      });
    },
  };

  return routes;
}
