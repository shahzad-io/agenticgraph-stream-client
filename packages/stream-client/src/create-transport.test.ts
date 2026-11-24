import { afterEach, describe, expect, it, vi } from 'vitest';
import { AgenticStream } from './agentic-stream.js';
import { createStreamTransport, streamChannelAuthorizer, type StreamDescriptor } from './create-transport.js';
import { fakeEcho, fakeRoutes, loadRuns } from './testing/broadcast-fakes.js';
import { BroadcastTransport } from './transports/broadcast-transport.js';
import { SSETransport } from './transports/sse-transport.js';

const run = loadRuns().streamed!;
const streamId = run.stored[0]!.body.stream_id as string;
const broadcast: StreamDescriptor = {
  transport: 'broadcast',
  stream_id: streamId,
  stream_url: null,
  replay_url: `/agentic-graph/streams/${streamId}/events?token=t1`,
  auth_url: `/agentic-graph/streams/${streamId}/auth`,
  token: 't1',
  channel: `private-agenticgraph.stream.${streamId}`,
};

afterEach(() => {
  vi.unstubAllGlobals();
});

describe('createStreamTransport', () => {
  it('builds SSETransport from an sse descriptor and BroadcastTransport from a broadcast one', () => {
    const sse = createStreamTransport({ ...broadcast, transport: 'sse', stream_url: '/agentic-graph/streams/s?token=t1' });

    expect(sse).toBeInstanceOf(SSETransport);
    expect(createStreamTransport(broadcast, { echo: fakeEcho().echo })).toBeInstanceOf(BroadcastTransport);
  });

  it('throws a clear error for a broadcast descriptor without echo', () => {
    expect(() => createStreamTransport(broadcast)).toThrow(/needs \{ echo \}/);
  });
});

describe('streamChannelAuthorizer', () => {
  it('signs the stream channel with the token and auth_url of the transport, then the run completes', async () => {
    const routes = fakeRoutes(run);
    vi.stubGlobal('fetch', routes.fetch);
    const echo = fakeEcho({ authorizer: streamChannelAuthorizer() });
    const stream = await AgenticStream.connect({ transport: createStreamTransport(broadcast, { echo: echo.echo }) });
    await vi.waitFor(() => expect(stream.status).toBe('complete'));

    const auth = routes.requests.find((r) => r.method === 'POST');
    expect(auth?.url.pathname).toBe(`/agentic-graph/streams/${streamId}/auth`);
    expect(auth?.token).toBe('t1');
    expect(echo.log[0]).toBe(`subscribe ${broadcast.channel}`);
  });

  it('passes any other channel to fallback, and refuses it without one', () => {
    const fallback = vi.fn((_params, callback: (error: Error | null, data: unknown) => void) => callback(null, { auth: 'app' }));
    const answers: unknown[] = [];

    streamChannelAuthorizer({ fallback })({ socketId: '1.2', channelName: 'private-App.User.1' }, (error, data) => answers.push(error ?? data));
    streamChannelAuthorizer()({ socketId: '1.2', channelName: 'private-App.User.1' }, (error, data) => answers.push(error?.message ?? data));

    expect(fallback).toHaveBeenCalledOnce();
    expect(answers).toEqual([{ auth: 'app' }, 'No channel authorizer for private-App.User.1']);
  });
});
