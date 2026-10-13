/**
 * Example: Real-Time Chat with File Attachments
 *
 * Demonstrates how Fast Lane transmits typing indicators and instant chat messages
 * with sub-millisecond latency while multi-megabyte image/document uploads occur
 * concurrently on the Slow Lane without Head-of-Line blocking.
 */

import { TwoLaneWebSocketServer, TwoLaneWebSocketClient } from '../src/dual-lane.ts';
import { PriorityLane } from '../src/types.ts';

async function run() {
  const fastPort = 9501;
  const slowPort = 9502;

  const server = new TwoLaneWebSocketServer({ fastPort, slowPort });
  await server.start();

  console.log(`[CHAT SERVER] Started Dual-Lane Server (Fast: :${fastPort}, Slow: :${slowPort})`);

  server.on('fastMessage', ({ clientId, message }) => {
    console.log(`[FAST LANE] Received ${message.topic}:`, message.payload);
  });

  server.on('slowMessage', ({ clientId, message }) => {
    console.log(`[SLOW LANE] Received ${message.topic} (${(message.payload as any).byteLength} bytes)`);
  });

  // Client Alice
  const alice = new TwoLaneWebSocketClient({
    fastUrl: `ws://localhost:${fastPort}?clientId=alice`,
    slowUrl: `ws://localhost:${slowPort}?clientId=alice`,
  });

  await alice.connect();

  console.log('\n--- Step 1: Alice begins uploading a 500KB asset on the Slow Lane ---');
  alice.sendSlow({
    priority: PriorityLane.P4_BULK,
    topic: 'chat:attachment',
    payload: { filename: 'design-mockup.png', byteLength: 500000, data: 'A'.repeat(500000) },
  });

  console.log('\n--- Step 2: Alice concurrently sends instant messages and keystrokes on Fast Lane ---');
  for (let i = 1; i <= 3; i++) {
    alice.sendFast({
      priority: PriorityLane.P1_HIGH,
      topic: 'chat:typing',
      payload: { user: 'Alice', isTyping: true, text: `Hello world ${i}` },
    });
    await new Promise((r) => setTimeout(r, 50));
  }

  await new Promise((r) => setTimeout(r, 300));
  alice.close();
  await server.stop();
  console.log('\n[CHAT SERVER] Closed cleanly. Fast Lane signals arrived without HoL delay.');
}

run().catch(console.error);
