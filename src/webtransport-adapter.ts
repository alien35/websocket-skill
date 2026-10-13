/**
 * WebTransport & HTTP/3 Prioritized Stream Adapter with Dual-Lane Fallback
 *
 * Implements modern multi-stream QUIC/HTTP/3 transport (RFC 9000, RFC 9114)
 * with stream urgency scheduling (RFC 9218 Extensible Prioritization Scheme / sendOrder)
 * and unreliable datagrams for telemetry.
 * Gracefully degrades to TwoLaneWebSocketClient if WebTransport is unsupported.
 */

import { EventEmitter } from 'node:events';
import { TwoLaneWebSocketClient } from './dual-lane.ts';
import { PriorityLane } from './types.ts';
import type { TransportMessage, DualLaneClientOptions } from './types.ts';

export interface WebTransportAdapterOptions {
  webTransportUrl?: string;
  webSocketFallback: DualLaneClientOptions;
  preferWebTransport?: boolean;
}

export type ActiveTransportType = 'webtransport' | 'dual-lane-websocket';

export class AdaptiveMultiLaneTransport extends EventEmitter {
  private activeTransport: ActiveTransportType = 'dual-lane-websocket';
  private wsFallbackClient: TwoLaneWebSocketClient | null = null;
  private webTransportSession: any = null;
  private isConnected = false;
  private options: WebTransportAdapterOptions;

  constructor(options: WebTransportAdapterOptions) {
    super();
    this.options = options;
  }

  public async connect(): Promise<void> {
    const hasWebTransport =
      typeof globalThis !== 'undefined' &&
      typeof (globalThis as any).WebTransport === 'function' &&
      Boolean(this.options.webTransportUrl) &&
      this.options.preferWebTransport !== false;

    if (hasWebTransport) {
      try {
        const WT = (globalThis as any).WebTransport;
        this.webTransportSession = new WT(this.options.webTransportUrl!);
        await this.webTransportSession.ready;
        this.activeTransport = 'webtransport';
        this.isConnected = true;
        this.setupWebTransportListeners();
        this.emit('connected', { transport: 'webtransport' });
        return;
      } catch (err) {
        this.emit('warning', `WebTransport negotiation failed, falling back to Dual-Lane WebSocket: ${err}`);
      }
    }

    this.wsFallbackClient = new TwoLaneWebSocketClient(this.options.webSocketFallback);
    this.wsFallbackClient.on('message', (msg) => this.emit('message', msg));
    this.wsFallbackClient.on('fastMessage', (msg) => this.emit('fastMessage', msg));
    this.wsFallbackClient.on('slowMessage', (msg) => this.emit('slowMessage', msg));
    this.wsFallbackClient.on('heartbeat', (stats) => this.emit('heartbeat', stats));
    this.wsFallbackClient.on('error', (err) => this.emit('error', err));

    await this.wsFallbackClient.connect();
    this.activeTransport = 'dual-lane-websocket';
    this.isConnected = true;
    this.emit('connected', { transport: 'dual-lane-websocket' });
  }

  private setupWebTransportListeners(): void {
    if (!this.webTransportSession) return;

    if (this.webTransportSession.datagrams?.readable) {
      const reader = this.webTransportSession.datagrams.readable.getReader();
      const readDatagrams = async () => {
        try {
          while (true) {
            const { value, done } = await reader.read();
            if (done) break;
            const text = new TextDecoder().decode(value);
            const msg = JSON.parse(text) as TransportMessage;
            this.emit('message', msg);
            this.emit('datagram', msg);
          }
        } catch (err) {
          this.emit('error', err);
        }
      };
      readDatagrams();
    }

    if (this.webTransportSession.incomingUnidirectionalStreams) {
      const reader = this.webTransportSession.incomingUnidirectionalStreams.getReader();
      const readStreams = async () => {
        try {
          while (true) {
            const { value: stream, done } = await reader.read();
            if (done) break;
            this.handleIncomingStream(stream);
          }
        } catch (err) {
          this.emit('error', err);
        }
      };
      readStreams();
    }
  }

  private async handleIncomingStream(stream: any): Promise<void> {
    const reader = stream.getReader();
    const chunks: Uint8Array[] = [];
    try {
      while (true) {
        const { value, done } = await reader.read();
        if (done) break;
        chunks.push(value);
      }
      const totalLen = chunks.reduce((acc, c) => acc + c.length, 0);
      const combined = new Uint8Array(totalLen);
      let offset = 0;
      for (const c of chunks) {
        combined.set(c, offset);
        offset += c.length;
      }
      const text = new TextDecoder().decode(combined);
      const msg = JSON.parse(text) as TransportMessage;
      this.emit('message', msg);
    } catch (err) {
      this.emit('error', err);
    }
  }

  public async sendCritical<T>(message: Omit<TransportMessage<T>, 'lane' | 'priority' | 'id' | 'timestamp'>): Promise<void> {
    const fullMessage: TransportMessage<T> = {
      ...message,
      id: `crit-${Date.now()}-${Math.random().toString(36).slice(2, 7)}`,
      lane: 'fast',
      priority: PriorityLane.P0_CRITICAL,
      sendOrder: 1000,
      timestamp: Date.now(),
    };

    if (this.activeTransport === 'webtransport' && this.webTransportSession) {
      await this.sendViaStream(fullMessage, 1000);
    } else if (this.wsFallbackClient) {
      this.wsFallbackClient.sendFast(fullMessage);
    }
  }

  public async sendInteractive<T>(message: Omit<TransportMessage<T>, 'lane' | 'priority' | 'id' | 'timestamp'>): Promise<void> {
    const fullMessage: TransportMessage<T> = {
      ...message,
      id: `int-${Date.now()}-${Math.random().toString(36).slice(2, 7)}`,
      lane: 'fast',
      priority: PriorityLane.P1_HIGH,
      sendOrder: 500,
      timestamp: Date.now(),
    };

    if (this.activeTransport === 'webtransport' && this.webTransportSession) {
      await this.sendViaStream(fullMessage, 500);
    } else if (this.wsFallbackClient) {
      this.wsFallbackClient.sendFast(fullMessage);
    }
  }

  public async sendDatagram<T>(message: Omit<TransportMessage<T>, 'lane' | 'priority' | 'id' | 'timestamp' | 'isDatagram'>): Promise<void> {
    const fullMessage: TransportMessage<T> = {
      ...message,
      id: `dgram-${Date.now()}-${Math.random().toString(36).slice(2, 7)}`,
      lane: 'fast',
      priority: PriorityLane.P1_HIGH,
      isDatagram: true,
      timestamp: Date.now(),
    };

    if (this.activeTransport === 'webtransport' && this.webTransportSession?.datagrams?.writable) {
      const writer = this.webTransportSession.datagrams.writable.getWriter();
      const encoded = new TextEncoder().encode(JSON.stringify(fullMessage));
      await writer.write(encoded);
      writer.releaseLock();
    } else if (this.wsFallbackClient) {
      const buffered = this.wsFallbackClient.getBufferedAmounts();
      if (buffered.fastLane < 8192) {
        this.wsFallbackClient.sendFast(fullMessage);
      }
    }
  }

  public async sendBulk<T>(message: Omit<TransportMessage<T>, 'lane' | 'priority' | 'id' | 'timestamp'>): Promise<void> {
    const fullMessage: TransportMessage<T> = {
      ...message,
      id: `bulk-${Date.now()}-${Math.random().toString(36).slice(2, 7)}`,
      lane: 'slow',
      priority: PriorityLane.P4_BULK,
      sendOrder: 0,
      timestamp: Date.now(),
    };

    if (this.activeTransport === 'webtransport' && this.webTransportSession) {
      await this.sendViaStream(fullMessage, 0);
    } else if (this.wsFallbackClient) {
      this.wsFallbackClient.sendSlow(fullMessage);
    }
  }

  private async sendViaStream(message: TransportMessage, sendOrder: number): Promise<void> {
    const stream = await this.webTransportSession.createUnidirectionalStream({ sendOrder });
    const writer = stream.getWriter();
    const encoded = new TextEncoder().encode(JSON.stringify(message));
    await writer.write(encoded);
    await writer.close();
  }

  public getActiveTransport(): ActiveTransportType {
    return this.activeTransport;
  }

  public close(): void {
    if (this.webTransportSession) {
      try {
        this.webTransportSession.close();
      } catch {
        // Ignore
      }
      this.webTransportSession = null;
    }
    if (this.wsFallbackClient) {
      this.wsFallbackClient.close();
      this.wsFallbackClient = null;
    }
    this.isConnected = false;
  }
}
