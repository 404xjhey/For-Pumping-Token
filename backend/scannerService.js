const SUPPORTED_TIMEFRAMES = new Set([
  '1m', '3m', '5m', '15m', '30m',
  '1h', '2h', '4h', '6h', '8h', '12h',
  '1d', '3d', '1w', '1M'
]);
const MAX_STORED_SIGNALS = 1000;
const DEFAULT_FEED_CHECK_INTERVAL_MS = 5000;
const DEFAULT_WEBSOCKET_GRACE_PERIOD_MS = 15000;
const DEFAULT_REST_REQUESTS_PER_SECOND = 10;
const DEFAULT_REST_CONCURRENCY = 8;
const WEBSOCKET_DATA_FRESHNESS_MS = 60000;

function wait(milliseconds, signal) {
  if (signal.aborted) return Promise.resolve();
  return new Promise((resolve) => {
    const timer = setTimeout(() => {
      signal.removeEventListener('abort', onAbort);
      resolve();
    }, Math.max(0, milliseconds));
    const onAbort = () => {
      clearTimeout(timer);
      signal.removeEventListener('abort', onAbort);
      resolve();
    };
    signal.addEventListener('abort', onAbort, { once: true });
  });
}

class ScannerService {
  constructor({
    symbolManager,
    websocketManager,
    notificationService,
    binanceService,
    logger,
    feedCheckIntervalMs = DEFAULT_FEED_CHECK_INTERVAL_MS,
    websocketGracePeriodMs = DEFAULT_WEBSOCKET_GRACE_PERIOD_MS,
    restRequestsPerSecond = DEFAULT_REST_REQUESTS_PER_SECOND,
    restConcurrency = DEFAULT_REST_CONCURRENCY
  }) {
    this.symbolManager = symbolManager;
    this.websocketManager = websocketManager;
    this.notificationService = notificationService;
    this.binanceService = binanceService;
    this.logger = logger;
    this.feedCheckIntervalMs = feedCheckIntervalMs;
    this.websocketGracePeriodMs = websocketGracePeriodMs;
    this.restRequestsPerSecond = restRequestsPerSecond;
    this.restConcurrency = restConcurrency;
    this.running = false;
    this.timeframe = null;
    this.percentRange = null;
    this.symbols = [];
    this.lastNotifiedCandle = new Map();
    this.latestRanges = new Map();
    this.signals = [];
    this.refreshTimer = null;
    this.feedMonitorTimer = null;
    this.refreshInProgress = false;
    this.scannerStartedAt = 0;
    this.restFallbackController = null;
    this.nextRestRequestAt = 0;
    this.operation = Promise.resolve();
  }

  start(timeframe, percentRange) {
    if (!SUPPORTED_TIMEFRAMES.has(timeframe)) {
      const error = new Error(`Unsupported Binance Futures timeframe: ${timeframe}`);
      error.statusCode = 400;
      return Promise.reject(error);
    }
    if (typeof percentRange !== 'number' || !Number.isFinite(percentRange) || percentRange <= 0) {
      const error = new Error('percentRange must be a finite number greater than zero.');
      error.statusCode = 400;
      return Promise.reject(error);
    }

    return this.enqueue(() => this.startInternal(timeframe, percentRange));
  }

  stop() {
    return this.enqueue(() => this.stopInternal());
  }

  enqueue(operation) {
    const result = this.operation.then(operation, operation);
    this.operation = result.catch(() => {});
    return result;
  }

  async startInternal(timeframe, percentRange) {
    this.stopInternal();
    const result = await this.symbolManager.refresh();

    this.timeframe = timeframe;
    this.percentRange = percentRange;
    this.symbols = result.symbols;
    this.running = true;
    this.scannerStartedAt = Date.now();
    this.websocketManager.configure(this.symbols, timeframe, (kline) => this.handleKline(kline));
    this.feedMonitorTimer = setInterval(() => this.monitorWebSocketFeed(), this.feedCheckIntervalMs);
    this.refreshTimer = setInterval(() => {
      this.enqueue(() => this.refreshSymbols()).catch((error) => {
        this.logger.error('Symbol refresh failed; retaining the last successful symbol list:', error.message);
      });
    }, this.symbolManager.getRefreshInterval());
    this.logger.info(`Scanner started: ${timeframe}, threshold ${percentRange}%, ${this.symbols.length} symbols.`);
    return this.getStatus();
  }

  monitorWebSocketFeed() {
    if (!this.running) return;
    if (this.isWebSocketFeedHealthy()) {
      this.stopRestFallback();
      return;
    }
    if (Date.now() - this.scannerStartedAt < this.websocketGracePeriodMs ||
        this.restFallbackController) {
      return;
    }
    this.startRestFallback();
  }

  isWebSocketFeedHealthy() {
    const states = this.websocketManager.getStatus().states;
    return states.length > 0 && states.every((connection) =>
      connection.state === 'connected' &&
      connection.lastKlineAt &&
      Date.now() - Date.parse(connection.lastKlineAt) <= WEBSOCKET_DATA_FRESHNESS_MS
    );
  }

  startRestFallback() {
    const controller = new AbortController();
    this.restFallbackController = controller;
    this.logger.warn(
      `No recent kline data on every WebSocket connection; starting rate-limited REST fallback for ${this.symbols.length} symbols.`
    );
    this.runRestFallback(controller).catch((error) => {
      if (!controller.signal.aborted) {
        this.logger.error('REST fallback stopped unexpectedly:', error.message);
      }
    });
  }

  stopRestFallback() {
    if (!this.restFallbackController) return;
    const controller = this.restFallbackController;
    this.restFallbackController = null;
    controller.abort();
    this.logger.info('WebSocket kline data is healthy; REST fallback stopped.');
  }

  async runRestFallback(controller) {
    try {
      while (this.running &&
          this.restFallbackController === controller &&
          !controller.signal.aborted) {
        if (this.isWebSocketFeedHealthy()) return;
        await this.pollCurrentKlines(controller.signal);
      }
    } finally {
      if (this.restFallbackController === controller) {
        this.restFallbackController = null;
      }
    }
  }

  async pollCurrentKlines(signal) {
    const symbols = this.symbols.slice();
    if (symbols.length === 0) {
      await wait(1000, signal);
      return;
    }
    let nextIndex = 0;
    let stopScheduling = false;
    let rateLimitError = null;
    let failureCount = 0;
    let firstFailure = null;

    const worker = async () => {
      while (!signal.aborted && !stopScheduling) {
        const index = nextIndex++;
        if (index >= symbols.length) return;
        const symbol = symbols[index];
        await this.waitForRestRequestSlot(signal);
        if (signal.aborted || stopScheduling) return;

        try {
          const klines = await this.binanceService.getCurrentKlines(
            symbol.toUpperCase(),
            this.timeframe,
            signal
          );
          for (const kline of klines) {
            this.handleKline({
              k: {
                s: symbol.toUpperCase(),
                t: Number(kline[0]),
                h: kline[2],
                l: kline[3]
              }
            });
          }
        } catch (error) {
          if (signal.aborted) return;
          if (error.rateLimited) {
            rateLimitError = error;
            stopScheduling = true;
          } else {
            failureCount += 1;
            if (!firstFailure) firstFailure = `${symbol.toUpperCase()}: ${error.message}`;
          }
        }
      }
    };

    const workers = Math.min(this.restConcurrency, symbols.length);
    await Promise.all(Array.from({ length: workers }, () => worker()));

    if (signal.aborted) return;
    if (rateLimitError) {
      this.logger.error(`REST fallback was rate limited: ${rateLimitError.message}`);
      await wait(rateLimitError.retryAfterMs, signal);
    } else if (failureCount > 0) {
      this.logger.warn(`REST fallback failed for ${failureCount} symbols; first failure: ${firstFailure}`);
    }
  }

  async waitForRestRequestSlot(signal) {
    const interval = 1000 / this.restRequestsPerSecond;
    const scheduledAt = Math.max(Date.now(), this.nextRestRequestAt);
    this.nextRestRequestAt = scheduledAt + interval;
    await wait(scheduledAt - Date.now(), signal);
  }

  async refreshSymbols() {
    if (!this.running || this.refreshInProgress) return;
    this.refreshInProgress = true;
    try {
      const result = await this.symbolManager.refresh();
      if (!this.running) return;
      this.symbols = result.symbols;
      if (result.changed) {
        const activeSymbols = new Set(this.symbols);
        for (const symbol of this.lastNotifiedCandle.keys()) {
          if (!activeSymbols.has(symbol)) this.lastNotifiedCandle.delete(symbol);
        }
        for (const symbol of this.latestRanges.keys()) {
          if (!activeSymbols.has(symbol)) this.latestRanges.delete(symbol);
        }
        this.websocketManager.configure(this.symbols, this.timeframe, (kline) => this.handleKline(kline));
      }
    } finally {
      this.refreshInProgress = false;
    }
  }

  handleKline(kline) {
    if (!this.running || !kline || !kline.k) return;
    const candle = kline.k;
    if (typeof candle.s !== 'string') return;
    const low = Number(candle.l);
    const high = Number(candle.h);
    const candleStart = Number(candle.t);
    const symbol = candle.s.toLowerCase();
    if (!Number.isFinite(low) || !Number.isFinite(high) || low <= 0 ||
        high < low || !Number.isFinite(candleStart) || !this.symbols.includes(symbol)) {
      return;
    }

    const percentageRange = ((high - low) / low) * 100;
    this.latestRanges.set(symbol, {
      symbol: symbol.toUpperCase(),
      timeframe: this.timeframe,
      percentageRange,
      high,
      low,
      candleStart,
      updatedAt: new Date().toISOString()
    });
    if (percentageRange < this.percentRange ||
        this.lastNotifiedCandle.get(symbol) === candleStart) {
      return;
    }

    const signal = {
      symbol,
      timeframe: this.timeframe,
      percentageRange,
      high,
      low,
      threshold: this.percentRange,
      detectedAt: new Date().toISOString(),
      candleStart
    };
    this.lastNotifiedCandle.set(symbol, candleStart);
    this.signals.push(signal);
    if (this.signals.length > MAX_STORED_SIGNALS) {
      this.signals.splice(0, this.signals.length - MAX_STORED_SIGNALS);
    }
    this.logger.info(`Threshold reached: ${symbol.toUpperCase()} ${percentageRange.toFixed(2)}% (${this.timeframe}).`);
    this.notificationService.sendSignal(signal).catch((error) => {
      this.logger.error(`Notification failed for ${symbol.toUpperCase()} candle ${candleStart}:`, error.message);
    });
  }

  stopInternal() {
    if (this.refreshTimer) clearInterval(this.refreshTimer);
    this.refreshTimer = null;
    if (this.feedMonitorTimer) clearInterval(this.feedMonitorTimer);
    this.feedMonitorTimer = null;
    this.stopRestFallback();
    this.running = false;
    this.websocketManager.stop();
    this.symbols = [];
    this.timeframe = null;
    this.percentRange = null;
    this.lastNotifiedCandle.clear();
    this.latestRanges.clear();
    this.logger.info('Scanner stopped.');
  }

  getStatus() {
    const websocket = this.websocketManager.getStatus();
    return {
      running: this.running,
      timeframe: this.timeframe,
      percentRange: this.percentRange,
      dataSource: !this.running
        ? 'stopped'
        : this.restFallbackController
          ? 'rest-fallback'
          : this.isWebSocketFeedHealthy()
            ? 'websocket'
            : 'waiting-for-websocket-data',
      restFallback: {
        active: Boolean(this.restFallbackController),
        requestsPerSecond: this.restRequestsPerSecond,
        maximumRequestWeightPerMinute: this.restRequestsPerSecond * 60
      },
      monitoredSymbols: this.symbols.length,
      symbols: this.symbols.map((symbol) => symbol.toUpperCase()),
      latestRanges: [...this.latestRanges.values()],
      detectedSignals: this.signals.length,
      signals: this.signals.slice(-100).map((signal) => ({
        ...signal,
        symbol: signal.symbol.toUpperCase()
      })),
      websocket: {
        connected: websocket.connected,
        total: websocket.total,
        states: websocket.states
      }
    };
  }
}

module.exports = ScannerService;
