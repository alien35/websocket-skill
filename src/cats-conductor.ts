/**
 * CATS: Conductor-driven Asymmetric Transport Scheme
 *
 * Implements transport-native semantic multi-queuing directly above the socket.
 * Prevents OS kernel TCP bufferbloat by maintaining 5 internal priority queues
 * (P0_CRITICAL to P4_BULK) and a shallow-watermark Feeder that releases frames
 * only as real socket capacity becomes available.
 */

import { PriorityLane, PriorityLaneNames } from './types.ts';
import type { TransportMessage, CatsConductorOptions } from './types.ts';

export interface UnderlyingSocket {
  readonly bufferedAmount: number;
  send(data: string | Uint8Array): void;
  readyState?: number;
}

interface QueuedItem {
  message: TransportMessage;
  serialized: string;
  byteLength: number;
  enqueuedAt: number;
  resolve: () => void;
  reject: (err: Error) => void;
}

export class CatsConductor {
  private readonly queues: Map<PriorityLane, QueuedItem[]> = new Map();
  private readonly socket: UnderlyingSocket;
  private readonly lowWatermark: number;
  private readonly highWatermark: number;
  private readonly maxQueueDepth: number;
  private readonly strictPriority: boolean;
  private readonly deficitWeights: [number, number, number, number, number];
  private deficitCounters: [number, number, number, number, number];

  private isFeeding = false;
  private feedInterval: NodeJS.Timeout | null = null;
  private totalDelivered = 0;
  private totalDropped = 0;
  private totalBytesSent = 0;

  constructor(socket: UnderlyingSocket, options: CatsConductorOptions = {}) {
    this.socket = socket;
    this.lowWatermark = options.lowWatermarkBytes ?? 4096; // 4KB
    this.highWatermark = options.highWatermarkBytes ?? 16384; // 16KB
    this.maxQueueDepth = options.maxQueueDepthPerLane ?? 1000;
    this.strictPriority = options.strictPriority ?? true;
    this.deficitWeights = options.deficitWeights ?? [16, 8, 4, 2, 1];
    this.deficitCounters = [...this.deficitWeights];

    const allLanes: PriorityLane[] = [
      PriorityLane.P0_CRITICAL,
      PriorityLane.P1_HIGH,
      PriorityLane.P2_NORMAL,
      PriorityLane.P3_LOW,
      PriorityLane.P4_BULK,
    ];

    for (const lane of allLanes) {
      this.queues.set(lane, []);
    }

    this.startFeederLoop();
  }

  private startFeederLoop(): void {
    if (this.feedInterval) return;
    this.feedInterval = setInterval(() => {
      this.feedNextBatch();
    }, 5);
  }

  public stop(): void {
    if (this.feedInterval) {
      clearInterval(this.feedInterval);
      this.feedInterval = null;
    }
  }

  /**
   * Enqueue a message into its semantic priority lane.
   * Resolves when the frame is pushed into the socket buffer.
   */
  public async send<T>(message: TransportMessage<T>, priorityOverride?: PriorityLane): Promise<void> {
    const priority = priorityOverride ?? message.priority;
    const serialized = JSON.stringify(message);
    const byteLength = Buffer.byteLength(serialized, 'utf-8');

    const queue = this.queues.get(priority)!;

    // Check TTL expiration if specified
    if (message.ttlMs && Date.now() - message.timestamp > message.ttlMs) {
      this.totalDropped++;
      throw new Error(`[CATS-CONDUCTOR] Dropped frame ${message.id} due to TTL expiry (${message.ttlMs}ms)`);
    }

    // Check queue capacity
    if (queue.length >= this.maxQueueDepth) {
      if (priority === PriorityLane.P4_BULK || priority === PriorityLane.P3_LOW) {
        this.totalDropped++;
        throw new Error(`[CATS-CONDUCTOR] Queue full for lane ${PriorityLaneNames[priority]}, frame rejected`);
      }
      this.evictLowestPriority();
    }

    return new Promise<void>((resolve, reject) => {
      queue.push({
        message,
        serialized,
        byteLength,
        enqueuedAt: Date.now(),
        resolve,
        reject,
      });

      this.feedNextBatch();
    });
  }

  /**
   * Low-Watermark Feeder:
   * Only dequeues when socket.bufferedAmount is below highWatermark.
   * Feeds the highest priority frame available.
   */
  public feedNextBatch(): void {
    if (this.isFeeding) return;
    this.isFeeding = true;

    try {
      while (this.socket.bufferedAmount < this.highWatermark) {
        const item = this.dequeueNext();
        if (!item) {
          break;
        }

        if (item.message.ttlMs && Date.now() - item.enqueuedAt > item.message.ttlMs) {
          this.totalDropped++;
          item.reject(new Error(`[CATS-CONDUCTOR] Frame ${item.message.id} expired in queue`));
          continue;
        }

        this.socket.send(item.serialized);
        this.totalDelivered++;
        this.totalBytesSent += item.byteLength;
        item.resolve();

        if (this.socket.bufferedAmount >= this.lowWatermark) {
          break;
        }
      }
    } finally {
      this.isFeeding = false;
    }
  }

  /**
   * Dequeue logic: Strict Priority (P0 > P1 > P2 > P3 > P4)
   * or Deficit Round Robin to prevent starvation.
   */
  private dequeueNext(): QueuedItem | null {
    const lanes: PriorityLane[] = [
      PriorityLane.P0_CRITICAL,
      PriorityLane.P1_HIGH,
      PriorityLane.P2_NORMAL,
      PriorityLane.P3_LOW,
      PriorityLane.P4_BULK,
    ];

    if (this.strictPriority) {
      for (const lane of lanes) {
        const queue = this.queues.get(lane)!;
        if (queue.length > 0) {
          return queue.shift()!;
        }
      }
      return null;
    }

    for (let i = 0; i < lanes.length; i++) {
      const lane = lanes[i];
      const queue = this.queues.get(lane)!;
      if (queue.length > 0 && this.deficitCounters[i] > 0) {
        this.deficitCounters[i]--;
        return queue.shift()!;
      }
    }

    this.deficitCounters = [...this.deficitWeights];
    for (let i = 0; i < lanes.length; i++) {
      const lane = lanes[i];
      const queue = this.queues.get(lane)!;
      if (queue.length > 0) {
        this.deficitCounters[i]--;
        return queue.shift()!;
      }
    }

    return null;
  }

  private evictLowestPriority(): void {
    const evictionOrder: PriorityLane[] = [
      PriorityLane.P4_BULK,
      PriorityLane.P3_LOW,
      PriorityLane.P2_NORMAL,
    ];
    for (const lane of evictionOrder) {
      const queue = this.queues.get(lane)!;
      if (queue.length > 0) {
        const evicted = queue.shift()!;
        this.totalDropped++;
        evicted.reject(new Error(`[CATS-CONDUCTOR] Evicted frame ${evicted.message.id} to yield buffer for higher priority frame`));
        return;
      }
    }
  }

  public getQueueDepths(): Record<string, number> {
    const depths: Record<string, number> = {};
    for (const [lane, queue] of this.queues.entries()) {
      depths[PriorityLaneNames[lane]] = queue.length;
    }
    return depths;
  }

  public getStats() {
    return {
      queueDepths: this.getQueueDepths(),
      socketBufferedAmount: this.socket.bufferedAmount,
      totalDelivered: this.totalDelivered,
      totalDropped: this.totalDropped,
      totalBytesSent: this.totalBytesSent,
    };
  }
}
