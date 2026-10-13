---
name: websocket-lanes
description: Design, implement, and benchmark two-lane and multi-queue WebSocket/WebTransport architectures to eliminate head-of-line blocking, TCP bufferbloat, and slow-consumer interference.
tools:
  - run_command
  - view_file
  - client_view_file
  - client_create_file
  - client_edit_file
---

# WebSocket Multi-Lane Architecture Skill (`websocket-lanes`)

A specialized engineering skill and reference toolkit for diagnosing, mitigating, and architecturally solving **Head-of-Line (HoL) blocking**, **bufferbloat**, and **slow-consumer interference** in web-scale real-time transport systems.

Grounded in international networking standards and transport systems research:
- **IETF RFC 9330 / RFC 9332 (DualQ Coupled AQM & L4S)**: Mathematically coupled congestion marking ($p_C \propto p_L^2$) separating low-latency shallow queues from classic deep queues.
- **CATS (Conductor-driven Asymmetric Transport Scheme)**: Semantic multi-queuing ($P_0$ through $P_4$) directly above the transport socket with a low-watermark feeder to prevent kernel bufferbloat.
- **IETF RFC 9000 / RFC 9114 / RFC 9218 (WebTransport, HTTP/3, and Extensible Prioritization Scheme)**: Independent QUIC streams with `sendOrder` urgency scheduling and unreliable datagrams.
- **Enterprise WebSocket Isolation Patterns**: Two-lane dual connections (`TCP_NODELAY`), server-side bounded ring buffers, and key-based message conflation.

---

## 1. When to Use This Skill

Activate this skill when an application encounters any of the following symptoms:
1. **Heartbeat timeouts during bulk operations**: Ping/pong signals timeout or trigger reconnect storms when users upload files, sync historical state, or receive large document snapshots.
2. **Interactive UI lag over WebSockets**: Keystrokes, cursor positions, canvas drawing, or audio/video control signals experience erratic latency spikes (jitter > 50ms) during background data sync.
3. **Slow-consumer cascading failures**: A single mobile client on high packet loss causes the server's broadcast memory buffer to explode, stalling messages for fast desktop clients.
4. **Bufferbloat in WebSocket sockets**: Node.js `ws.bufferedAmount` grows to megabytes, trapping real-time frames inside OS kernel socket send buffers (`SO_SNDBUF`).
5. **Architectural migration planning**: Evaluating transition paths from single-stream WebSockets to Two-Lane WebSockets, CATS scheduling, or WebTransport (HTTP/3).

---

## 2. Theoretical Foundations & Standards

```
                      +---------------------------------------+
                      |       Application Layer (Frames)      |
                      +---------------------------------------+
                                  |                 |
          +-----------------------+                 +-----------------------+
          | Interactive / Control |                 | Bulk / State / Sync   |
          | Lane (P0, P1)         |                 | Lane (P2, P3, P4)     |
          +-----------------------+                 +-----------------------+
                     |                                         |
                     v                                         v
          +-----------------------+                 +-----------------------+
          | Shallow Egress Queue  |                 | Bounded Ring Buffer   |
          | (Target Delay < 1ms)  |                 | (Key-Conflation)      |
          +-----------------------+                 +-----------------------+
                     |                                         |
                     +--------------------+--------------------+
                                          |
                                          v
               +-----------------------------------------------------+
               |      DualQ / CATS Transport Scheduler Engine        |
               |   Strict P0-P4 / Deficit Round Robin (DRR)          |
               |   Low Watermark: 4KB  |  High Watermark: 16KB       |
               +-----------------------------------------------------+
                                          |
                     +--------------------+--------------------+
                     |                                         |
                     v                                         v
          +-----------------------+                 +-----------------------+
          | Fast Lane Socket      |                 | Slow Lane Socket      |
          | TCP_NODELAY = true    |                 | Standard TCP Egress   |
          +-----------------------+                 +-----------------------+
```

### A. DualQ Coupled Active Queue Management (RFC 9330 / 9332)
DualQ separates traffic into:
- **$L$-Queue (Low-Latency Shallow Queue)**: Scalable L4S traffic aiming for sub-millisecond queuing delay.
- **$C$-Queue (Classic Deep Queue)**: Bulk Reno/Cubic TCP flows absorbing bursts.
- **Coupled Congestion Marking**: To ensure classic flows receive their fair share of bottleneck capacity without overpowering the shallow queue, the marking probabilities are coupled:
  $$p_C = k \cdot (p_L)^2$$
  where $p_L$ is the L4S marking probability, $p_C$ is classic drop/marking probability, and $k$ is a network factor (typically 1.0).

### B. CATS (Conductor-driven Asymmetric Transport Scheme)
Rather than blindly calling `socket.send(data)` (which dumps megabytes into the kernel socket buffer where application priority is lost), CATS manages an internal multi-lane conductor:
- **$P_0$ Critical**: Heartbeats, cancellations, zero-latency RPCs.
- **$P_1$ High**: Interactive keystrokes, cursor reticles, user action responses.
- **$P_2$ Normal**: Chat messages, transaction edits, state changes.
- **$P_3$ Low**: Periodic telemetry, non-critical metrics.
- **$P_4$ Bulk**: File uploads, initial state snapshots, historical logs.
- **Low-Watermark Feeder**: The socket buffer is kept below a tight threshold (e.g. 4KB–16KB). When the OS socket drains, the Conductor feeds the highest-priority queued frame next.

### C. WebTransport & HTTP/3 Prioritization (RFC 9218)
Under HTTP/3 (QUIC), individual streams do not block each other at the transport layer:
- **Unidirectional Streams with `sendOrder`**: High-priority interactive messages are assigned `sendOrder = 1000`, while bulk chunks receive `sendOrder = 0`.
- **Unreliable Datagrams**: Telemetry and loss-tolerant ephemeral state (such as live mouse coordinates) are emitted via datagrams without retransmission overhead.

---

## 3. Toolkit Modules & Architecture

The repository provides production TypeScript modules runnable under modern Node (`--experimental-strip-types`) with zero compilation overhead:

| Module | Source File | Description |
| :--- | :--- | :--- |
| **Dual-Lane Socket** | [`src/dual-lane.ts`](file:///Users/alexanderleon/notary-project/websocket-skill/src/dual-lane.ts) | Independent Fast and Slow WebSocket connections with `TCP_NODELAY`, sub-5ms heartbeat ping-pong, and auto payload routing. |
| **CATS Conductor** | [`src/cats-conductor.ts`](file:///Users/alexanderleon/notary-project/websocket-skill/src/cats-conductor.ts) | 5-priority socket scheduler with low/high watermarks, Deficit Round Robin, and frame TTL expiration. |
| **Slow-Consumer Buffer** | [`src/slow-consumer.ts`](file:///Users/alexanderleon/notary-project/websocket-skill/src/slow-consumer.ts) | Bounded ring buffer with key-based message conflation (`conflateKey`), drop policies, and isolated broadcaster. |
| **WebTransport Adapter** | [`src/webtransport-adapter.ts`](file:///Users/alexanderleon/notary-project/websocket-skill/src/webtransport-adapter.ts) | QUIC stream adapter with RFC 9218 `sendOrder` and datagrams, failing over to Dual-Lane WebSocket. |
| **DualQ AQM Monitor** | [`src/dualq-monitor.ts`](file:///Users/alexanderleon/notary-project/websocket-skill/src/dualq-monitor.ts) | Telemetry monitor computing RFC 9332 $p_C = p_L^2$ coupled marking, latency percentiles, and bufferbloat index. |
| **Benchmark Suite** | [`src/benchmark.ts`](file:///Users/alexanderleon/notary-project/websocket-skill/src/benchmark.ts) | Comparative test harness measuring latency under 100% link saturation across transport architectures. |
| **Executable CLI** | [`bin/websocket-lanes.js`](file:///Users/alexanderleon/notary-project/websocket-skill/bin/websocket-lanes.js) | CLI supporting `benchmark`, `serve`, `demo`, and `diagnose`. |

---

## 4. Quick Implementation Recipes

### Recipe 1: Two-Lane WebSocket Client & Server
```typescript
import { TwoLaneWebSocketServer, TwoLaneWebSocketClient, PriorityLane } from 'websocket-skill';

// Server Setup
const server = new TwoLaneWebSocketServer({ fastPort: 8080, slowPort: 8081 });
server.on('fastMessage', ({ clientId, message }) => {
  console.log(`[Fast Lane] Instant event from ${clientId}:`, message.payload);
});
await server.start();

// Client Setup
const client = new TwoLaneWebSocketClient({
  fastUrl: 'ws://localhost:8080?clientId=user-1',
  slowUrl: 'ws://localhost:8081?clientId=user-1',
  heartbeatIntervalMs: 1000,
});
await client.connect();

// Send interactive keystroke (Fast Lane, zero HoL blocking)
client.sendFast({
  priority: PriorityLane.P1_HIGH,
  topic: 'editor:cursor',
  payload: { line: 42, col: 10 },
});

// Send multi-megabyte document state (Slow Lane)
client.sendSlow({
  priority: PriorityLane.P4_BULK,
  topic: 'doc:snapshot',
  payload: { documentBinary: '...' },
});
```

### Recipe 2: CATS Conductor on Existing Sockets
```typescript
import { CatsConductor, PriorityLane } from 'websocket-skill';

const conductor = new CatsConductor(existingWebSocket, {
  lowWatermarkBytes: 4096,   // 4KB
  highWatermarkBytes: 16384, // 16KB
  strictPriority: true,
});

// Enqueue critical heartbeat
conductor.send({
  id: 'ping-1',
  lane: 'fast',
  priority: PriorityLane.P0_CRITICAL,
  timestamp: Date.now(),
  payload: { ping: true },
});
```

### Recipe 3: Slow-Consumer Protection with Message Conflation
```typescript
import { BoundedRingBuffer, PriorityLane } from 'websocket-skill';

const ring = new BoundedRingBuffer({ capacity: 100, dropPolicy: 'conflate-by-key' });

// Pushing multiple updates with identical conflateKey overwrites intermediate state
ring.push({
  id: 'tick-1',
  lane: 'slow',
  priority: PriorityLane.P2_NORMAL,
  conflateKey: 'ticker:BTC-USD',
  timestamp: Date.now(),
  payload: { price: 92100 },
});

ring.push({
  id: 'tick-2',
  lane: 'slow',
  priority: PriorityLane.P2_NORMAL,
  conflateKey: 'ticker:BTC-USD',
  timestamp: Date.now(),
  payload: { price: 92150 }, // Overwrites tick-1
});
```

---

## 5. Verification & Testing

Run all unit tests:
```bash
npm test
```

Run comparative benchmark suite:
```bash
npm run bench
```

Run live interactive demo:
```bash
npm run demo
```

Run bufferbloat diagnostic report:
```bash
npm run diagnose
```
