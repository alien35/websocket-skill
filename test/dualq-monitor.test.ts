import test from 'node:test';
import assert from 'node:assert/strict';
import { DualQMonitor } from '../src/dualq-monitor.ts';

test('DualQMonitor computes coupled marking probabilities according to RFC 9332 (p_C = p_L^2)', () => {
  const monitor = new DualQMonitor(10, 1.0);

  // Ingest samples with slight L4S delay
  for (let i = 0; i < 20; i++) {
    monitor.recordL4SSample(5.0); // 4ms excess above 1ms target -> p_L = 0.4
    monitor.recordClassicSample(40.0);
  }

  const metrics = monitor.computeMetrics();

  assert.equal(metrics.markingProbabilityL4S, 0.4);
  assert.equal(metrics.markingProbabilityClassic, 0.16); // 0.4^2 = 0.16
  assert.ok(metrics.bufferbloatIndex >= 3.0, 'Classic delay 40ms vs baseline 10ms yields 4x bufferbloat');
});

test('DualQMonitor produces valid formatted summary report', () => {
  const monitor = new DualQMonitor(15);
  monitor.recordL4SSample(1.2);
  monitor.recordClassicSample(25.0);
  monitor.recordHoLBlock();
  monitor.recordSlowConsumer();

  const report = monitor.getSummaryReport();
  assert.ok(report.includes('DualQ Coupled AQM'));
  assert.ok(report.includes('HoL Blocking Incidents:         1'));
  assert.ok(report.includes('Slow Consumers Quarantined:     1'));
});
