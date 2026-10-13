/**
 * Dual-Lane WebSocket Transport (Fast Lane / Slow Lane)
 *
 * Implements architectural separation of interactive/control traffic from bulk/telemetry.
 * Uses two independent TCP/WebSocket connections to guarantee that bulk data backpressure,
 * TCP packet retransmissions, or queue bufferbloat cannot stall time-critical interactive signals.
 */

import { WebSocket, WebSocketServer } from 'ws';
import { EventEmitter } from 'node:events';
import { PriorityLane } from './types.ts';
import type {
  TransportMessage,
  DualLaneClientOptions,
  DualLaneServerOptions,
} from './types.ts';

export interface HeartbeatStats {
  rttMs: number;
  lastPingTimestamp: number;
  lastPongTimestamp: number;
  consecutiveSuccesses: number;
  consecutiveFailures: number;
  isHealthy: boolean;
}

export class TwoLaneWebSocketClient extends EventEmitter {
  private fastSocket: WebSocket | null = null;
  private slowSocket: WebSocket | null = null;
  private options: Required<DualLaneClientOptions>;
  private isConnected = false;
  private heartbeatTimer: NodeJS.Timeout | null = null;
  private pendingPings: Map<string, number> = new Map();
  private stats: HeartbeatStats = {
    rttMs: 0,
    lastPingTimestamp: 0,
    lastPongTimestamp: 0,
    consecutiveSuccesses: 0,
    consecutiveFailures: 0,
    isHealthy: false,
  };

  constructor(options: DualLaneClientOptions) {
    super();
    this.options = {
      fastUrl: options.fastUrl,
      slowUrl: options.slowUrl,
      fastPayloadLimitBytes: options.fastPayloadLimitBytes ?? 4096,
      heartbeatIntervalMs: options.heartbeatIntervalMs ?? 2000,
      heartbeatTimeoutMs: options.heartbeatTimeoutMs ?? 1000,
      maxFastMessagesPerSecond: options.maxFastMessagesPerSecond ?? 100,
    };
  }

  public async connect(): Promise<void> {
    const fastPromise = new Promise<void>((resolve, reject) => {
      const ws = new WebSocket(this.options.fastUrl);
      ws.on('open', () => {
        this.fastSocket = ws;
        this.configureSocketPerformance(ws);
        this.setupFastListeners(ws);
        resolve();
      });
      ws.on('error', reject);
    });

    const slowPromise = new Promise<void>((resolve, reject) => {
      const ws = new WebSocket(this.options.slowUrl);
      ws.on('open', () => {
        this.slowSocket = ws;
        this.configureSocketPerformance(ws);
        this.setupSlowListeners(ws);
        resolve();
      });
      ws.on('error', reject);
    });

    await Promise.all([fastPromise, slowPromise]);
    this.isConnected = true;
    this.startHeartbeatLoop();
    this.emit('connected');
  }

  private configureSocketPerformance(ws: WebSocket): void {
    const socket = (ws as any)._socket;
    if (socket && typeof socket.setNoDelay === 'function') {
      socket.setNoDelay(true);
    }
  }

  private setupFastListeners(ws: WebSocket): void {
    ws.on('message', (raw) => {
      try {
        const msg = JSON.parse(raw.toString()) as TransportMessage;
        if (msg.topic === '__HEARTBEAT_PONG__') {
          const sentTime = this.pendingPings.get(msg.id);
          if (sentTime) {
            this.stats.rttMs = Date.now() - sentTime;
            this.stats.lastPongTimestamp = Date.now();
            this.stats.consecutiveSuccesses++;
            this.stats.consecutiveFailures = 0;
            this.stats.isHealthy = true;
            this.pendingPings.delete(msg.id);
            this.emit('heartbeat', this.stats);
          }
          return;
        }
        this.emit('fastMessage', msg);
        this.emit('message', msg);
      } catch (err) {
        this.emit('error', err);
      }
    });

    ws.on('close', () => {
      this.emit('fastClosed');
    });
  }

  private setupSlowListeners(ws: WebSocket): void {
    ws.on('message', (raw) => {
      try {
        const msg = JSON.parse(raw.toString()) as TransportMessage;
        this.emit('slowMessage', msg);
        this.emit('message', msg);
      } catch (err) {
        this.emit('error', err);
      }
    });

    ws.on('close', () => {
      this.emit('slowClosed');
    });
  }

  private startHeartbeatLoop(): void {
    this.heartbeatTimer = setInterval(() => {
      if (!this.fastSocket || this.fastSocket.readyState !== WebSocket.OPEN) return;

      const pingId = `ping-${Date.now()}-${Math.random().toString(36).slice(2, 7)}`;
      const now = Date.now();
      this.pendingPings.set(pingId, now);
      this.stats.lastPingTimestamp = now;

      const pingFrame: TransportMessage<{ pingId: string }> = {
        id: pingId,
        lane: 'fast',
        priority: PriorityLane.P0_CRITICAL,
        topic: '__HEARTBEAT_PING__',
        timestamp: now,
        payload: { pingId },
      };

      this.fastSocket.send(JSON.stringify(pingFrame));

      setTimeout(() => {
        if (this.pendingPings.has(pingId)) {
          this.pendingPings.delete(pingId);
          this.stats.consecutiveFailures++;
          this.stats.consecutiveSuccesses = 0;
          if (this.stats.consecutiveFailures >= 2) {
            this.stats.isHealthy = false;
            this.emit('degraded', this.stats);
          }
        }
      }, this.options.heartbeatTimeoutMs);
    }, this.options.heartbeatIntervalMs);
  }

  public send<T>(message: Omit<TransportMessage<T>, 'id' | 'timestamp'> & { id?: string }): void {
    const fullMessage: TransportMessage<T> = {
      ...message,
      id: message.id ?? `msg-${Date.now()}-${Math.random().toString(36).slice(2, 7)}`,
      timestamp: Date.now(),
    };

    const serialized = JSON.stringify(fullMessage);
    const size = Buffer.byteLength(serialized, 'utf-8');

    const isExplicitSlow = message.lane === 'slow';
    const exceedsFastLimit = size > this.options.fastPayloadLimitBytes;

    if (!isExplicitSlow && !exceedsFastLimit && (message.lane === 'fast' || message.priority <= PriorityLane.P1_HIGH)) {
      this.sendFastRaw(serialized);
    } else {
      this.sendSlowRaw(serialized);
    }
  }

  public sendFast<T>(message: Omit<TransportMessage<T>, 'lane' | 'id' | 'timestamp'>): void {
    this.send({
      ...message,
      lane: 'fast',
      priority: message.priority ?? PriorityLane.P1_HIGH,
    });
  }

  public sendSlow<T>(message: Omit<TransportMessage<T>, 'lane' | 'id' | 'timestamp'>): void {
    this.send({
      ...message,
      lane: 'slow',
      priority: message.priority ?? PriorityLane.P3_LOW,
    });
  }

  private sendFastRaw(serialized: string): void {
    if (!this.fastSocket || this.fastSocket.readyState !== WebSocket.OPEN) {
      throw new Error('[DUAL-LANE] Fast lane socket is not open');
    }
    this.fastSocket.send(serialized);
  }

  private sendSlowRaw(serialized: string): void {
    if (!this.slowSocket || this.slowSocket.readyState !== WebSocket.OPEN) {
      throw new Error('[DUAL-LANE] Slow lane socket is not open');
    }
    this.slowSocket.send(serialized);
  }

  public getHeartbeatStats(): HeartbeatStats {
    return { ...this.stats };
  }

  public getBufferedAmounts(): { fastLane: number; slowLane: number } {
    return {
      fastLane: this.fastSocket ? (this.fastSocket as any).bufferedAmount || 0 : 0,
      slowLane: this.slowSocket ? (this.slowSocket as any).bufferedAmount || 0 : 0,
    };
  }

  public close(): void {
    if (this.heartbeatTimer) {
      clearInterval(this.heartbeatTimer);
      this.heartbeatTimer = null;
    }
    this.fastSocket?.close();
    this.slowSocket?.close();
    this.fastSocket = null;
    this.slowSocket = null;
    this.isConnected = false;
  }
}

export interface ClientSession {
  id: string;
  fastSocket?: WebSocket;
  slowSocket?: WebSocket;
  connectedAt: number;
}

export class TwoLaneWebSocketServer extends EventEmitter {
  private fastWss: WebSocketServer | null = null;
  private slowWss: WebSocketServer | null = null;
  private sessions: Map<string, ClientSession> = new Map();
  private options: Required<DualLaneServerOptions>;

  constructor(options: DualLaneServerOptions) {
    super();
    this.options = {
      fastPort: options.fastPort,
      slowPort: options.slowPort,
      maxFastPayloadBytes: options.maxFastPayloadBytes ?? 4096,
      clientRingBufferCapacity: options.clientRingBufferCapacity ?? 256,
    };
  }

  public async start(): Promise<void> {
    await Promise.all([
      new Promise<void>((resolve) => {
        this.fastWss = new WebSocketServer({ port: this.options.fastPort }, () => resolve());
      }),
      new Promise<void>((resolve) => {
        this.slowWss = new WebSocketServer({ port: this.options.slowPort }, () => resolve());
      }),
    ]);

    this.fastWss!.on('connection', (ws, req) => {
      this.configureSocket(ws);
      const clientId = this.extractClientId(req.url);
      const session = this.getOrCreateSession(clientId);
      session.fastSocket = ws;

      ws.on('message', (raw) => {
        try {
          const msg = JSON.parse(raw.toString()) as TransportMessage;
          if (msg.topic === '__HEARTBEAT_PING__') {
            const pongFrame: TransportMessage = {
              id: msg.id,
              lane: 'fast',
              priority: PriorityLane.P0_CRITICAL,
              topic: '__HEARTBEAT_PONG__',
              timestamp: Date.now(),
              payload: { ackPingId: msg.id },
            };
            ws.send(JSON.stringify(pongFrame));
            return;
          }
          this.emit('fastMessage', { clientId, message: msg });
        } catch (err) {
          this.emit('error', err);
        }
      });

      ws.on('close', () => {
        session.fastSocket = undefined;
        this.checkSessionCleanup(clientId);
      });
    });

    this.slowWss!.on('connection', (ws, req) => {
      this.configureSocket(ws);
      const clientId = this.extractClientId(req.url);
      const session = this.getOrCreateSession(clientId);
      session.slowSocket = ws;

      ws.on('message', (raw) => {
        try {
          const msg = JSON.parse(raw.toString()) as TransportMessage;
          this.emit('slowMessage', { clientId, message: msg });
        } catch (err) {
          this.emit('error', err);
        }
      });

      ws.on('close', () => {
        session.slowSocket = undefined;
        this.checkSessionCleanup(clientId);
      });
    });

    this.emit('listening', { fastPort: this.options.fastPort, slowPort: this.options.slowPort });
  }

  private configureSocket(ws: WebSocket): void {
    const socket = (ws as any)._socket;
    if (socket && typeof socket.setNoDelay === 'function') {
      socket.setNoDelay(true);
    }
  }

  private extractClientId(url?: string): string {
    if (!url) return 'default-client';
    try {
      const parsed = new URL(url, 'http://localhost');
      return parsed.searchParams.get('clientId') || 'client-' + Math.random().toString(36).slice(2, 8);
    } catch {
      return 'client-' + Math.random().toString(36).slice(2, 8);
    }
  }

  private getOrCreateSession(clientId: string): ClientSession {
    let session = this.sessions.get(clientId);
    if (!session) {
      session = { id: clientId, connectedAt: Date.now() };
      this.sessions.set(clientId, session);
      this.emit('clientConnected', session);
    }
    return session;
  }

  private checkSessionCleanup(clientId: string): void {
    const session = this.sessions.get(clientId);
    if (session && !session.fastSocket && !session.slowSocket) {
      this.sessions.delete(clientId);
      this.emit('clientDisconnected', clientId);
    }
  }

  public broadcast<T>(message: TransportMessage<T>): void {
    const serialized = JSON.stringify(message);
    for (const session of this.sessions.values()) {
      if (message.lane === 'fast' && session.fastSocket?.readyState === WebSocket.OPEN) {
        session.fastSocket.send(serialized);
      } else if (message.lane === 'slow' && session.slowSocket?.readyState === WebSocket.OPEN) {
        session.slowSocket.send(serialized);
      }
    }
  }

  public getActiveSessionsCount(): number {
    return this.sessions.size;
  }

  public async stop(): Promise<void> {
    await Promise.all([
      new Promise<void>((res) => (this.fastWss ? this.fastWss.close(() => res()) : res())),
      new Promise<void>((res) => (this.slowWss ? this.slowWss.close(() => res()) : res())),
    ]);
    this.sessions.clear();
  }
}
