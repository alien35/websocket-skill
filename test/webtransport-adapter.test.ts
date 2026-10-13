import test from 'node:test';
import assert from 'node:assert/strict';
import { AdaptiveMultiLaneTransport } from '../src/webtransport-adapter.ts';
import { TwoLaneWebSocketServer } from '../src/dual-lane.ts';
import type { TransportMessage } from '../src/types.ts';

test('AdaptiveMultiLaneTransport falls back to Dual-Lane WebSocket gracefully', async () => {
  const fastPort = 9401;
  const slowPort = 9402;

  const server = new TwoLaneWebSocketServer({ fastPort, slowPort });
  await server.start();

  const transport = new AdaptiveMultiLaneTransport({
    webSocketFallback: {
      fastUrl: `ws://localhost:${fastPort}?clientId=adapt-client`,
      slowUrl: `ws://localhost:${slowPort}?clientId=adapt-client`,
    },
    preferWebTransport: false,
  });

  const received: TransportMessage[] = [];
  server.on('fastMessage', ({ message }) => received.push(message));

  await transport.connect();
  assert.equal(transport.getActiveTransport(), 'dual-lane-websocket');

  await transport.sendCritical({
    payload: { alert: 'FAILOVER_TEST' },
  });

  await new Promise((r) => setTimeout(r, 100));
  assert.equal(received.length, 1);
  assert.equal((received[0].payload as any).alert, 'FAILOVER_TEST');

  transport.close();
  await server.stop();
});
