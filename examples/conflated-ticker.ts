/**
 * Example: High-Frequency Market Data Ticker with Message Conflation
 *
 * Demonstrates how BoundedRingBuffer collapses bursts of rapidly updating ticks
 * (e.g. BTC-USD, ETH-USD) so lagging clients receive only the freshest state,
 * preventing memory bloat and slow-consumer cascade.
 */

import { BoundedRingBuffer } from '../src/slow-consumer.ts';
import { PriorityLane } from '../src/types.ts';

async function run() {
  const buffer = new BoundedRingBuffer({ capacity: 16, dropPolicy: 'conflate-by-key' });

  console.log('[TICKER] Emitting 50 high-frequency price updates for BTC and ETH...');

  for (let i = 1; i <= 25; i++) {
    buffer.push({
      id: `btc-${i}`,
      lane: 'slow',
      priority: PriorityLane.P2_NORMAL,
      conflateKey: 'ticker:BTC-USD',
      timestamp: Date.now(),
      payload: { pair: 'BTC-USD', price: 90000 + i * 10 },
    });

    buffer.push({
      id: `eth-${i}`,
      lane: 'slow',
      priority: PriorityLane.P2_NORMAL,
      conflateKey: 'ticker:ETH-USD',
      timestamp: Date.now(),
      payload: { pair: 'ETH-USD', price: 3400 + i * 2 },
    });
  }

  const stats = buffer.getStats();
  console.log(`[TICKER STATS] Total Pushed: ${stats.totalEnqueued} | Conflated Out: ${stats.totalConflated} | Remaining In Buffer: ${stats.size}`);

  console.log('\n[CONSUMER DRAINING] Reading consolidated updates from buffer:');
  while (buffer.size() > 0) {
    const item = buffer.pop();
    if (item) {
      console.log(` -> Key: ${item.conflateKey} | Delivered Price:`, item.payload);
    }
  }
}

run().catch(console.error);
