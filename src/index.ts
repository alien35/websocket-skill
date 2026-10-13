/**
 * WebSocket Lanes & Multi-Queue Transport Toolkit
 *
 * Implements architectural solutions for Head-of-Line blocking, bufferbloat,
 * and slow-consumer interference across WebSocket, CATS, and WebTransport layers.
 */

export * from './cats-conductor.ts';
export * from './dual-lane.ts';
export * from './slow-consumer.ts';
export * from './webtransport-adapter.ts';
export * from './dualq-monitor.ts';
export * from './benchmark.ts';
export { PriorityLane, PriorityLaneNames } from './types.ts';
export type {
  LaneType,
  TransportMessage,
  MessageHandler,
  CatsConductorOptions,
  RingBufferOptions,
  DualLaneClientOptions,
  DualLaneServerOptions,
  DualQCoupledMetrics,
  BenchmarkResult,
} from './types.ts';
