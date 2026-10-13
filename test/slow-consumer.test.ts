import test from 'node:test';
import assert from 'node:assert/strict';
import { BoundedRingBuffer, IsolatedBroadcaster } from '../src/slow-consumer.ts';
import { PriorityLane } from '../src/types.ts';

test('BoundedRingBuffer conflates intermediate messages with identical conflateKey', () => {
  const ring = new BoundedRingBuffer({ capacity: 10, dropPolicy: 'conflate-by-key' });

  // Push 5 updates for the same cursor key
  for (let i = 1; i <= 5; i++) {
    ring.push({
      id: `cursor-msg-${i}`,
      lane: 'fast',
      priority: PriorityLane.P1_HIGH,
      conflateKey: 'user:alex:cursor',
      timestamp: Date.now(),
      payload: { x: i * 10, y: i * 20 },
    });
  }

  // Size should remain 1 because each subsequent update conflates the slot
  assert.equal(ring.size(), 1);

  const item = ring.pop();
  assert.notEqual(item, null);
  assert.equal((item!.payload as any).x, 50, 'Latest state (x: 50) should be retained');
  assert.equal(ring.getStats().totalConflated, 4, '4 intermediate frames should be marked conflated');
});

test('BoundedRingBuffer drops oldest when capacity is exceeded without conflateKey', () => {
  const ring = new BoundedRingBuffer({ capacity: 4, dropPolicy: 'conflate-by-key' });

  // Push 6 distinct non-conflated messages
  for (let i = 1; i <= 6; i++) {
    ring.push({
      id: `msg-${i}`,
      lane: 'slow',
      priority: PriorityLane.P2_NORMAL,
      timestamp: Date.now(),
      payload: `Item ${i}`,
    });
  }

  assert.equal(ring.size(), 4, 'Buffer size capped at capacity 4');

  // Dequeued items should be items 3, 4, 5, 6 (1 and 2 evicted)
  const first = ring.pop();
  assert.equal(first?.id, 'msg-3');
});

test('IsolatedBroadcaster protects fast consumers from slow socket stalling', async () => {
  const broadcaster = new IsolatedBroadcaster({ capacity: 5 });

  // Fast consumer
  const fastReceived: string[] = [];
  const fastSocket = {
    bufferedAmount: 0,
    send: (data: string) => fastReceived.push(data),
  };

  // Lagging slow consumer whose socket buffer is pinned
  const slowSocket = {
    bufferedAmount: 100000, // Exceeds drain limit
    send: () => {},
  };

  broadcaster.registerClient('fast-client', fastSocket);
  broadcaster.registerClient('slow-client', slowSocket);

  // Broadcast 3 messages
  for (let i = 1; i <= 3; i++) {
    broadcaster.broadcast({
      id: `bcast-${i}`,
      lane: 'slow',
      priority: PriorityLane.P2_NORMAL,
      timestamp: Date.now(),
      payload: `News ${i}`,
    });
  }

  // Fast client receives all 3 messages immediately despite slow client being blocked
  await new Promise((r) => setTimeout(r, 50));
  assert.equal(fastReceived.length, 3);

  broadcaster.clear();
});
