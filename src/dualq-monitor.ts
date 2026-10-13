/**
 * DualQ Coupled AQM & Bufferbloat Monitor (RFC 9330 / RFC 9332)
 *
 * Implements mathematically coupled congestion marking and queue telemetry
 * based on DualPI2 and IETF RFC 9332.
 * Calculates p_C proportional to (p_L)^2 to ensure coexistence of scalable L4S
 * low-latency traffic with classic bulk flows while exposing bufferbloat indices.
 */

import type { DualQCoupledMetrics } from './types.ts';

export class DualQMonitor {
  private baselineRttMs = 15;
  private l4sDelays: number[] = [];
  private classicDelays: number[] = [];
  private holBlocksCount = 0;
  private slowConsumerCount = 0;
  private conflatedDropsCount = 0;
  private kCouplingFactor = 1.0;

  constructor(baselineRttMs = 15, kCouplingFactor = 1.0) {
    this.baselineRttMs = baselineRttMs;
    this.kCouplingFactor = kCouplingFactor;
  }

  public recordL4SSample(delayMs: number): void {
    this.l4sDelays.push(delayMs);
    if (this.l4sDelays.length > 200) this.l4sDelays.shift();

    if (delayMs > 0 && delayMs < this.baselineRttMs) {
      this.baselineRttMs = delayMs;
    }
  }

  public recordClassicSample(delayMs: number): void {
    this.classicDelays.push(delayMs);
    if (this.classicDelays.length > 200) this.classicDelays.shift();
  }

  public recordHoLBlock(): void {
    this.holBlocksCount++;
  }

  public recordSlowConsumer(): void {
    this.slowConsumerCount++;
  }

  public recordConflatedDrop(): void {
    this.conflatedDropsCount++;
  }

  private calculatePercentile(values: number[], percentile: number): number {
    if (values.length === 0) return 0;
    const sorted = [...values].sort((a, b) => a - b);
    const index = Math.min(sorted.length - 1, Math.floor((percentile / 100) * sorted.length));
    return Number(sorted[index].toFixed(2));
  }

  public computeMetrics(): DualQCoupledMetrics {
    const l4sP50 = this.calculatePercentile(this.l4sDelays, 50);
    const l4sP99 = this.calculatePercentile(this.l4sDelays, 99);
    const classicP50 = this.calculatePercentile(this.classicDelays, 50);
    const classicP99 = this.calculatePercentile(this.classicDelays, 99);

    const l4sTargetMs = 1.0;
    const l4sExcess = Math.max(0, l4sP50 - l4sTargetMs);
    const pL = Math.min(1.0, Number((l4sExcess / 10.0).toFixed(4)));
    const pC = Math.min(1.0, Number((this.kCouplingFactor * Math.pow(pL, 2)).toFixed(4)));

    const bufferbloatIndex = this.baselineRttMs > 0
      ? Number((classicP50 / this.baselineRttMs).toFixed(2))
      : 1.0;

    return {
      l4sQueueDelayMs: l4sP50,
      classicQueueDelayMs: classicP50,
      markingProbabilityL4S: pL,
      markingProbabilityClassic: pC,
      bufferbloatIndex,
      headOfLineBlocksDetected: this.holBlocksCount,
      slowConsumersDetected: this.slowConsumerCount,
      conflatedFramesDropped: this.conflatedDropsCount,
      timestamp: Date.now(),
    };
  }

  public getSummaryReport(): string {
    const m = this.computeMetrics();
    const l4sP99 = this.calculatePercentile(this.l4sDelays, 99);
    const classicP99 = this.calculatePercentile(this.classicDelays, 99);

    return [
      `=== DualQ Coupled AQM & Bufferbloat Report (RFC 9330/9332) ===`,
      `Baseline Network RTT:           ${this.baselineRttMs} ms`,
      `L4S (Fast Lane) Delay p50/p99:   ${m.l4sQueueDelayMs} ms / ${l4sP99} ms`,
      `Classic (Bulk Lane) Delay p50/99: ${m.classicQueueDelayMs} ms / ${classicP99} ms`,
      `L4S Marking Probability (p_L):  ${(m.markingProbabilityL4S * 100).toFixed(2)}%`,
      `Classic Coupled Marking (p_C):  ${(m.markingProbabilityClassic * 100).toFixed(2)}% (p_C ∝ p_L²)`,
      `Bufferbloat Severity Index:     ${m.bufferbloatIndex}x ${m.bufferbloatIndex > 2.5 ? '⚠️ HIGH BUFFERBLOAT' : '✅ OPTIMAL'}`,
      `HoL Blocking Incidents:         ${m.headOfLineBlocksDetected}`,
      `Slow Consumers Quarantined:     ${m.slowConsumersDetected}`,
      `Conflated Intermediate Drops:   ${m.conflatedFramesDropped}`,
      `============================================================`,
    ].join('\n');
  }
}
