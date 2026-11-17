const WebSocket = require('ws');

const STREAMS_PER_CONNECTION = 200;
const DATA_IDLE_TIMEOUT_MS = 60000;
const STREAM_BASE_URL = 'wss://fstream.binance.com/stream?streams=';

class WebSocketManager {
  constructor({ logger }) {
    this.logger = logger;
    this.connections = new Map();
    this.symbols = [];
    this.timeframe = null;
    this.generation = 0;
    this.onKline = null;
  }

  configure(symbols, timeframe, onKline) {
    const normalizedSymbols = [...new Set(symbols)].sort();
    const unchanged = timeframe === this.timeframe &&
      normalizedSymbols.length === this.symbols.length &&
      normalizedSymbols.every((symbol, index) => symbol === this.symbols[index]);
    this.onKline = onKline;
    if (unchanged) return;

    this.generation += 1;
    this.closeConnections();
    this.symbols = normalizedSymbols;
    this.timeframe = timeframe;

    const streams = normalizedSymbols.map((symbol) => `${symbol}@kline_${timeframe}`);
    for (let offset = 0, index = 0; offset < streams.length; offset += STREAMS_PER_CONNECTION, index += 1) {
      const chunk = streams.slice(offset, offset + STREAMS_PER_CONNECTION);
      this.connect(index, chunk, this.generation, 0);
    }
    this.logger.info(`Configured ${normalizedSymbols.length} kline streams across ${this.connections.size} WebSocket connections.`);
  }

  connect(index, streams, generation, attempt) {
    if (generation !== this.generation) return;
    const entry = {
      index,
      socket: null,
      state: 'connecting',
      attempt,
      reconnectTimer: null,
      watchdogTimer: null,
      lastMessageAt: null,
      lastKlineAt: null,
      messagesReceived: 0,
      klineMessagesReceived: 0
    };
    const socket = new WebSocket(`${STREAM_BASE_URL}${streams.join('/')}`, {
      handshakeTimeout: 15000
    });
    entry.socket = socket;
    this.connections.set(index, entry);

    socket.on('open', () => {
      if (generation !== this.generation || this.connections.get(index) !== entry) {
        socket.close();
        return;
      }
      entry.state = 'connected';
      entry.attempt = 0;
      entry.lastMessageAt = Date.now();
      entry.watchdogTimer = setInterval(() => {
        if (generation !== this.generation || this.connections.get(index) !== entry) return;
        if (Date.now() - entry.lastMessageAt > DATA_IDLE_TIMEOUT_MS) {
          this.logger.warn(`WebSocket connection ${index + 1} has received no market data for ${DATA_IDLE_TIMEOUT_MS}ms; reconnecting.`);
          socket.terminate();
        }
      }, 10000);
      this.logger.info(`WebSocket connection ${index + 1} connected (${streams.length} streams).`);
    });

    socket.on('message', (rawMessage) => {
      if (generation !== this.generation || this.connections.get(index) !== entry) return;
      entry.lastMessageAt = Date.now();
      entry.messagesReceived += 1;
      try {
        const message = JSON.parse(rawMessage.toString());
        if (message.data && message.data.e === 'kline' && this.onKline) {
          entry.klineMessagesReceived += 1;
          entry.lastKlineAt = new Date(entry.lastMessageAt).toISOString();
          this.onKline(message.data);
        }
      } catch (error) {
        this.logger.warn(`Ignoring malformed message on WebSocket connection ${index + 1}: ${error.message}`);
      }
    });

    socket.on('error', (error) => {
      this.logger.warn(`WebSocket connection ${index + 1} error: ${error.message}`);
    });

    socket.on('close', (code) => {
      if (generation !== this.generation || this.connections.get(index) !== entry) return;
      if (entry.watchdogTimer) clearInterval(entry.watchdogTimer);
      entry.watchdogTimer = null;
      entry.state = 'disconnected';
      const nextAttempt = entry.attempt + 1;
      const backoff = Math.min(1000 * (2 ** Math.min(entry.attempt, 6)), 60000);
      const delay = backoff + Math.floor(Math.random() * 1000) + index * 100;
      this.logger.warn(`WebSocket connection ${index + 1} closed (${code}); reconnecting in ${delay}ms.`);
      entry.reconnectTimer = setTimeout(() => {
        if (generation === this.generation) {
          this.connect(index, streams, generation, nextAttempt);
        }
      }, delay);
    });
  }

  closeConnections() {
    for (const entry of this.connections.values()) {
      if (entry.reconnectTimer) clearTimeout(entry.reconnectTimer);
      if (entry.watchdogTimer) clearInterval(entry.watchdogTimer);
      if (entry.socket && entry.socket.readyState < WebSocket.CLOSING) {
        entry.socket.close();
      }
    }
    this.connections.clear();
  }

  stop() {
    this.generation += 1;
    this.closeConnections();
    this.symbols = [];
    this.timeframe = null;
    this.onKline = null;
  }

  getStatus() {
    let connected = 0;
    for (const entry of this.connections.values()) {
      if (entry.state === 'connected') connected += 1;
    }
    return {
      connected,
      total: this.connections.size,
      states: [...this.connections.values()].map(({
        index,
        state,
        attempt,
        messagesReceived,
        klineMessagesReceived,
        lastKlineAt
      }) => ({
        connection: index + 1,
        state,
        reconnectAttempt: attempt,
        messagesReceived,
        klineMessagesReceived,
        lastKlineAt
      }))
    };
  }
}

module.exports = WebSocketManager;
