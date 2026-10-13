/**
 * Core type definitions for Two-Lane and Multi-Lane WebSocket Transports.
 * Grounded in RFC 9330 / RFC 9332 (DualQ / L4S), CATS Conductor scheduling,
 * and RFC 9000 / RFC 9114 / RFC 9218 (WebTransport & EPS).
 */

export type LaneType = 'fast' | 'slow';

export const PriorityLane = {
  P0_CRITICAL: 0, // Heartbeats, acks, cancellations, zero-latency RPC
  P1_HIGH: 1,     // Interactive keystrokes, cursor reticles, user events
  P2_NORMAL: 2,   // Chat messages, transaction edits, state changes
  P3_LOW: 3,      // Periodic telemetry, background sync, non-critical metrics
  P4_BULK: 4,     // Historical sync, file chunks, snapshots, image blobs
} as const;

export type PriorityLane = (typeof PriorityLane)[keyof typeof PriorityLane];

export const PriorityLaneNames: Record<PriorityLane, string> = {
  0: 'P0_CRITICAL',
  1: 'P1_HIGH',
  2: 'P2_NORMAL',
  3: 'P3_LOW',
  4: 'P4_BULK',
};

export interface TransportMessage<T = unknown> {
  id: string;
  lane: LaneType;
  priority: PriorityLane;
  topic?: string;
  conflateKey?: string;
  timestamp: number;
  ttlMs?: number;
  payload: T;
  sendOrder?: number;
  isDatagram?: boolean;
}

export type MessageHandler<T = unknown> = (message: TransportMessage<T>) => void;

export interface CatsConductorOptions {
  /** Low-watermark socket buffer threshold in bytes before feeding next chunk. Default: 4096 (4KB) */
  lowWatermarkBytes?: number;
  /** High-watermark socket buffer threshold in bytes. Default: 16384 (16KB) */
  highWatermarkBytes?: number;
  /** Maximum items queued per priority lane before drop/backpressure kicks in. Default: 1000 */
  maxQueueDepthPerLane?: number;
  /** Whether to use strict P0 > P1 > P2 > P3 > P4 or deficit round robin to prevent starvation */
  strictPriority?: boolean;
  /** Weight distribution per lane [P0, P1, P2, P3, P4] when deficit round robin is enabled */
  deficitWeights?: [number, number, number, number, number];
}

export interface RingBufferOptions {
  /** Maximum number of messages in the ring buffer before dropping or conflating */
  capacity?: number;
  /** Drop policy when buffer is saturated */
  dropPolicy?: 'drop-oldest' | 'conflate-by-key' | 'reject-newest';
  /** High-watermark ratio (0.0 to 1.0) at which conflation starts aggressively. Default: 0.75 */
  highWatermarkRatio?: number;
}

export interface DualLaneClientOptions {
  /** Fast lane endpoint (or auto-derived with ?lane=fast) */
  fastUrl: string;
  /** Slow lane endpoint (or auto-derived with ?lane=slow) */
  slowUrl: string;
  /** Byte limit for fast lane payloads. Larger payloads automatically route to slow lane. Default: 4096 */
  fastPayloadLimitBytes?: number;
  /** Fast lane heartbeat ping interval in milliseconds. Default: 2000 */
  heartbeatIntervalMs?: number;
  /** Fast lane heartbeat timeout before declaring connection degraded. Default: 1000 */
  heartbeatTimeoutMs?: number;
  /** Optional token bucket / rate limiter config for fast lane */
  maxFastMessagesPerSecond?: number;
}

export interface DualLaneServerOptions {
  /** Fast lane port */
  fastPort: number;
  /** Slow lane port */
  slowPort: number;
  /** Maximum fast-lane payload size in bytes */
  maxFastPayloadBytes?: number;
  /** Per-client ring buffer capacity for slow consumers */
  clientRingBufferCapacity?: number;
}

export interface DualQCoupledMetrics {
  /** Sub-millisecond queue delay for L4S / Fast Lane (ms) */
  l4sQueueDelayMs: number;
  /** Classic deep queue delay (ms) */
  classicQueueDelayMs: number;
  /** L4S marking probability p_L */
  markingProbabilityL4S: number;
  /** Classic marking probability p_C where p_C = k * (p_L)^2 */
  markingProbabilityClassic: number;
  /** Ratio of classic delay to baseline RTT (indicates bufferbloat severity) */
  bufferbloatIndex: number;
  /** Number of Head-of-Line blocking stalls observed on single-stream transports */
  headOfLineBlocksDetected: number;
  /** Number of slow consumers quarantined or throttled */
  slowConsumersDetected: number;
  /** Total intermediate frames dropped due to message conflation/coalescing */
  conflatedFramesDropped: number;
  /** Timestamp of sample */
  timestamp: number;
}

export interface BenchmarkResult {
  transport: string;
  totalMessagesSent: number;
  totalMessagesDelivered: number;
  p50LatencyMs: number;
  p95LatencyMs: number;
  p99LatencyMs: number;
  maxLatencyMs: number;
  heartbeatJitterMs: number;
  headOfLineStalls: number;
  bufferbloatRatio: number;
}
