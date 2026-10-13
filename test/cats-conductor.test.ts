import test from 'node:test';
import assert from 'node:assert/strict';
import { CatsConductor } from '../src/cats-conductor.ts';
import type { UnderlyingSocket } from '../src/cats-conductor.ts';
import { PriorityLane } from '../src/types.ts';
import type { TransportMessage } from '../src/types.ts';

class MockSocket implements UnderlyingSocket {
  public bufferedAmount = 0;
  public sentFrames: string[] = [];

  public send(data: string): void {
    this.sentFrames.push(data);
    this.bufferedAmount += Buffer.byteLength(data, 'utf-8');
  }

  public drain(bytesDrained: number): void {
    this.bufferedAmount = Math.max(0, this.bufferedAmount - bytesDrained);
  }
}

test('CatsConductor prioritizes P0_CRITICAL over P4_BULK under buffer saturation', async () => {
  const socket = new MockSocket();
  // Simulate saturated socket buffer (600 > highWatermark 500)
  socket.bufferedAmount = 600;

  const conductor = new CatsConductor(socket, {
    lowWatermarkBytes: 100,
    highWatermarkBytes: 500,
    strictPriority: true,
  });

  // Enqueue 2 bulk messages while socket is saturated
  conductor.send({
    id: 'bulk-1',
    lane: 'slow',
    priority: PriorityLane.P4_BULK,
    timestamp: Date.now(),
    payload: 'BULK_DATA_1',
  }).catch(() => {});

  conductor.send({
    id: 'bulk-2',
    lane: 'slow',
    priority: PriorityLane.P4_BULK,
    timestamp: Date.now(),
    payload: 'BULK_DATA_2',
  }).catch(() => {});

  // Enqueue 1 urgent critical message while socket is saturated
  conductor.send({
    id: 'crit-urgent',
    lane: 'fast',
    priority: PriorityLane.P0_CRITICAL,
    timestamp: Date.now(),
    payload: 'CRITICAL_ALERT',
  }).catch(() => {});

  // Nothing should be sent yet due to high watermark backpressure
  assert.equal(socket.sentFrames.length, 0);

  // Now drain the socket buffer so feeder can release the next batch
  socket.drain(600);
  conductor.feedNextBatch();

  const parsed = socket.sentFrames.map((f) => JSON.parse(f) as TransportMessage);
  conductor.stop();

  // Urgent P0 frame must be dispatched FIRST
  assert.ok(parsed.length > 0, 'At least one frame must be dispatched');
  assert.equal(parsed[0].id, 'crit-urgent', 'Critical P0 frame must be sent first ahead of P4 bulk frames');
});

test('CatsConductor limits socket buffer to avoid kernel bufferbloat', async () => {
  const socket = new MockSocket();
  const conductor = new CatsConductor(socket, {
    lowWatermarkBytes: 100,
    highWatermarkBytes: 250,
    strictPriority: true,
  });

  for (let i = 0; i < 10; i++) {
    conductor.send({
      id: `msg-${i}`,
      lane: 'slow',
      priority: PriorityLane.P2_NORMAL,
      timestamp: Date.now(),
      payload: 'X'.repeat(80),
    }).catch(() => {});
  }

  assert.ok(
    socket.bufferedAmount <= 350,
    `Socket buffer (${socket.bufferedAmount}) should be constrained near high watermark (250)`
  );

  conductor.stop();
});
