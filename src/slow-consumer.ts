/**
 * Slow Consumer Isolation & Bounded Ring Buffer with Message Conflation
 *
 * Implements decoupled per-client egress queues to isolate slow or lossy consumers.
 * Uses message conflation/coalescing by key (e.g. latest cursor position, orderbook tick)
 * to collapse intermediate frames under backpressure without blocking server broadcast loops.
 */

import { EventEmitter } from 'node:events';
import type { TransportMessage, RingBufferOptions } from './types.ts';

export interface EgressSocket {
  readonly bufferedAmount: number;
  send(data: string): void;
  readyState?: number;
}

export class BoundedRingBuffer<T = unknown> {
  private buffer: Array<TransportMessage<T> | null>;
  private head = 0; // Read pointer
  private tail = 0; // Write pointer
  private count = 0;
  private readonly capacity: number;
  private readonly dropPolicy: 'drop-oldest' | 'conflate-by-key' | 'reject-newest';
  private readonly highWatermarkRatio: number;
  private conflateKeyMap: Map<string, number> = new Map(); // key -> buffer index
  private totalEnqueued = 0;
  private totalDropped = 0;
  private totalConflated = 0;

  constructor(options: RingBufferOptions = {}) {
    this.capacity = Math.max(1, options.capacity ?? 256);
    this.dropPolicy = options.dropPolicy ?? 'conflate-by-key';
    this.highWatermarkRatio = options.highWatermarkRatio ?? 0.75;
    this.buffer = new Array(this.capacity).fill(null);
  }

  public push(message: TransportMessage<T>): boolean {
    this.totalEnqueued++;

    if (this.dropPolicy === 'conflate-by-key' && message.conflateKey) {
      const existingIdx = this.conflateKeyMap.get(message.conflateKey);
      if (existingIdx !== undefined && this.buffer[existingIdx] !== null) {
        this.buffer[existingIdx] = message;
        this.totalConflated++;
        return true;
      }
    }

    if (this.count >= this.capacity) {
      if (this.dropPolicy === 'reject-newest') {
        this.totalDropped++;
        return false;
      }
      this.evictOldest();
    }

    this.buffer[this.tail] = message;
    if (message.conflateKey) {
      this.conflateKeyMap.set(message.conflateKey, this.tail);
    }

    this.tail = (this.tail + 1) % this.capacity;
    this.count++;
    return true;
  }

  public pop(): TransportMessage<T> | null {
    if (this.count === 0) return null;

    let item: TransportMessage<T> | null = null;
    while (this.count > 0) {
      item = this.buffer[this.head];
      this.buffer[this.head] = null;

      if (item && item.conflateKey) {
        if (this.conflateKeyMap.get(item.conflateKey) === this.head) {
          this.conflateKeyMap.delete(item.conflateKey);
        }
      }

      this.head = (this.head + 1) % this.capacity;
      this.count--;

      if (item !== null) {
        return item;
      }
    }

    return null;
  }

  private evictOldest(): void {
    const evicted = this.buffer[this.head];
    if (evicted && evicted.conflateKey) {
      if (this.conflateKeyMap.get(evicted.conflateKey) === this.head) {
        this.conflateKeyMap.delete(evicted.conflateKey);
      }
    }
    this.buffer[this.head] = null;
    this.head = (this.head + 1) % this.capacity;
    this.count--;
    this.totalDropped++;
  }

  public size(): number {
    return this.count;
  }

  public isSaturated(): boolean {
    return this.count >= this.capacity * this.highWatermarkRatio;
  }

  public clear(): void {
    this.buffer.fill(null);
    this.head = 0;
    this.tail = 0;
    this.count = 0;
    this.conflateKeyMap.clear();
  }

  public getStats() {
    return {
      size: this.count,
      capacity: this.capacity,
      saturationRatio: Number((this.count / this.capacity).toFixed(3)),
      totalEnqueued: this.totalEnqueued,
      totalDropped: this.totalDropped,
      totalConflated: this.totalConflated,
    };
  }
}

export class IsolatedClientEgressQueue extends EventEmitter {
  public readonly clientId: string;
  private socket: EgressSocket;
  private ringBuffer: BoundedRingBuffer;
  private isDraining = false;
  private drainTimer: NodeJS.Timeout | null = null;
  private isQuarantined = false;
  private consecutiveSaturations = 0;

  constructor(clientId: string, socket: EgressSocket, options: RingBufferOptions = {}) {
    super();
    this.clientId = clientId;
    this.socket = socket;
    this.ringBuffer = new BoundedRingBuffer(options);

    this.startDrainLoop();
  }

  private startDrainLoop(): void {
    this.drainTimer = setInterval(() => {
      this.drain();
    }, 10);
  }

  public enqueue<T>(message: TransportMessage<T>): boolean {
    const success = this.ringBuffer.push(message);

    if (this.ringBuffer.isSaturated()) {
      this.consecutiveSaturations++;
      if (this.consecutiveSaturations >= 5 && !this.isQuarantined) {
        this.isQuarantined = true;
        this.emit('slowConsumerDetected', {
          clientId: this.clientId,
          bufferedAmount: this.socket.bufferedAmount,
          bufferStats: this.ringBuffer.getStats(),
        });
      }
    } else {
      if (this.consecutiveSaturations > 0) {
        this.consecutiveSaturations--;
      }
      if (this.isQuarantined && this.ringBuffer.size() === 0) {
        this.isQuarantined = false;
        this.emit('recovered', { clientId: this.clientId });
      }
    }

    this.drain();
    return success;
  }

  public drain(): void {
    if (this.isDraining) return;
    this.isDraining = true;

    try {
      while (this.socket.bufferedAmount < 32768 && this.ringBuffer.size() > 0) {
        const item = this.ringBuffer.pop();
        if (!item) break;

        const serialized = JSON.stringify(item);
        this.socket.send(serialized);
      }
    } catch (err) {
      this.emit('drainError', err);
    } finally {
      this.isDraining = false;
    }
  }

  public getStats() {
    return {
      clientId: this.clientId,
      socketBufferedAmount: this.socket.bufferedAmount,
      isQuarantined: this.isQuarantined,
      ringBuffer: this.ringBuffer.getStats(),
    };
  }

  public stop(): void {
    if (this.drainTimer) {
      clearInterval(this.drainTimer);
      this.drainTimer = null;
    }
    this.ringBuffer.clear();
  }
}

export class IsolatedBroadcaster {
  private queues: Map<string, IsolatedClientEgressQueue> = new Map();
  private options: RingBufferOptions;

  constructor(options: RingBufferOptions = {}) {
    this.options = options;
  }

  public registerClient(clientId: string, socket: EgressSocket): IsolatedClientEgressQueue {
    const queue = new IsolatedClientEgressQueue(clientId, socket, this.options);
    this.queues.set(clientId, queue);
    return queue;
  }

  public unregisterClient(clientId: string): void {
    const queue = this.queues.get(clientId);
    if (queue) {
      queue.stop();
      this.queues.delete(clientId);
    }
  }

  public broadcast<T>(message: TransportMessage<T>): void {
    for (const queue of this.queues.values()) {
      queue.enqueue(message);
    }
  }

  public getStats() {
    const clientStats: ReturnType<IsolatedClientEgressQueue['getStats']>[] = [];
    for (const queue of this.queues.values()) {
      clientStats.push(queue.getStats());
    }
    return {
      totalClients: this.queues.size,
      clients: clientStats,
    };
  }

  public clear(): void {
    for (const queue of this.queues.values()) {
      queue.stop();
    }
    this.queues.clear();
  }
}
