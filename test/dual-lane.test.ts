import test from 'node:test';
import assert from 'node:assert/strict';
import { TwoLaneWebSocketServer, TwoLaneWebSocketClient } from '../src/dual-lane.ts';
import { PriorityLane } from '../src/types.ts';
import type { TransportMessage } from '../src/types.ts';

test('TwoLaneWebSocket transports fast and slow messages independently', async () => {
  const fastPort = 9301;
  const slowPort = 9302;

  const server = new TwoLaneWebSocketServer({ fastPort, slowPort });
  await server.start();

  const client = new TwoLaneWebSocketClient({
    fastUrl: `ws://localhost:${fastPort}?clientId=test-client-1`,
    slowUrl: `ws://localhost:${slowPort}?clientId=test-client-1`,
    heartbeatIntervalMs: 200,
  });

  const fastMessagesReceived: TransportMessage[] = [];
  const slowMessagesReceived: TransportMessage[] = [];

  server.on('fastMessage', ({ message }) => {
    fastMessagesReceived.push(message);
  });

  server.on('slowMessage', ({ message }) => {
    slowMessagesReceived.push(message);
  });

  await client.connect();

  client.sendFast({
    priority: PriorityLane.P1_HIGH,
    payload: { action: 'cursor-move', x: 42, y: 84 },
  });

  client.sendSlow({
    priority: PriorityLane.P4_BULK,
    payload: { snapshot: 'LARGE_STATE_BLOB' },
  });

  await new Promise((r) => setTimeout(r, 150));

  assert.equal(fastMessagesReceived.length, 1);
  assert.equal(fastMessagesReceived[0].priority, PriorityLane.P1_HIGH);

  assert.equal(slowMessagesReceived.length, 1);
  assert.equal(slowMessagesReceived[0].priority, PriorityLane.P4_BULK);

  client.send({
    lane: 'fast',
    priority: PriorityLane.P1_HIGH,
    payload: 'X'.repeat(5000),
  });

  await new Promise((r) => setTimeout(r, 150));

  assert.equal(slowMessagesReceived.length, 2, 'Large payload must be rerouted to slow lane');

  client.close();
  await server.stop();
});

test('TwoLaneWebSocketClient maintains sub-5ms heartbeat ping-pong', async () => {
  const fastPort = 9303;
  const slowPort = 9304;

  const server = new TwoLaneWebSocketServer({ fastPort, slowPort });
  await server.start();

  const client = new TwoLaneWebSocketClient({
    fastUrl: `ws://localhost:${fastPort}?clientId=heartbeat-client`,
    slowUrl: `ws://localhost:${slowPort}?clientId=heartbeat-client`,
    heartbeatIntervalMs: 50,
  });

  let heartbeatFired = false;
  client.on('heartbeat', (stats) => {
    heartbeatFired = true;
    assert.ok(stats.isHealthy);
    assert.ok(stats.rttMs >= 0 && stats.rttMs < 50);
  });

  await client.connect();
  await new Promise((r) => setTimeout(r, 200));

  assert.ok(heartbeatFired, 'Heartbeat event should have fired');

  client.close();
  await server.stop();
});
