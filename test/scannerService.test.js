const assert = require('node:assert/strict');
const test = require('node:test');
const ScannerService = require('../backend/scannerService');
const { getConfiguration } = require('../scanner');

test('reads standalone scanner configuration from command-line arguments', () => {
  assert.deepEqual(getConfiguration(['5m', '7']), {
    timeframe: '5m',
    percentRange: 7
  });
});

test('rejects invalid standalone scanner arguments', () => {
  assert.throws(() => getConfiguration([]), /Usage:/);
  assert.throws(() => getConfiguration(['unknown', '5']), /Unsupported timeframe/);
  assert.throws(() => getConfiguration(['1m', '0']), /greater than zero/);
  assert.throws(() => getConfiguration(['1m', '5', 'extra']), /exactly a timeframe/);
});

test('notifies once per symbol per candle and allows later candles', async () => {
  const symbols = ['btcusdt', 'ethusdt'];
  const sentSignals = [];
  const symbolManager = {
    refresh: async () => ({ symbols, changed: true }),
    getRefreshInterval: () => 60000
  };
  const websocketManager = {
    configure(_symbols, _timeframe, onKline) {
      this.onKline = onKline;
    },
    getStatus: () => ({ connected: 1, total: 1, states: [] }),
    stop() {}
  };
  const notificationService = {
    async sendSignal(signal) {
      sentSignals.push(signal);
    }
  };
  const logger = { info() {}, warn() {}, error() {} };
  const scanner = new ScannerService({
    symbolManager,
    websocketManager,
    notificationService,
    logger
  });

  test('uses REST kline polling when every WebSocket feed is silent', async () => {
    const symbols = ['rareusdt'];
    const sentSignals = [];
    let resolveSignal;
    const signalReceived = new Promise((resolve) => {
      resolveSignal = resolve;
    });
    const symbolManager = {
      refresh: async () => ({ symbols, changed: true }),
      getRefreshInterval: () => 60000
    };
    const websocketManager = {
      configure() {},
      getStatus: () => ({
        connected: 1,
        total: 1,
        states: [{
          state: 'connected',
          lastKlineAt: null
        }]
      }),
      stop() {}
    };
    const notificationService = {
      async sendSignal(signal) {
        sentSignals.push(signal);
        resolveSignal(signal);
      }
    };
    const binanceService = {
      async getCurrentKlines(symbol, timeframe) {
        assert.equal(symbol, 'RAREUSDT');
        assert.equal(timeframe, '1m');
        return [
          [1000, '100', '100.1', '100', '100', '1', 1059],
          [2000, '100', '101', '100', '101', '1', 2059]
        ];
      }
    };
    const logger = { info() {}, warn() {}, error() {} };
    const scanner = new ScannerService({
      symbolManager,
      websocketManager,
      notificationService,
      binanceService,
      logger,
      feedCheckIntervalMs: 5,
      websocketGracePeriodMs: 0,
      restRequestsPerSecond: 100,
      restConcurrency: 1
    });

    try {
      await scanner.start('1m', 0.5);
      await Promise.race([
        signalReceived,
        new Promise((_, reject) => setTimeout(() => reject(new Error('REST fallback did not detect the signal.')), 1000))
      ]);

      assert.equal(sentSignals.length, 1);
      assert.equal(sentSignals[0].symbol, 'rareusdt');
      assert.equal(scanner.getStatus().dataSource, 'rest-fallback');
      assert.equal(
        scanner.getStatus().latestRanges.find((range) => range.symbol === 'RAREUSDT').percentageRange,
        1
      );
    } finally {
      await scanner.stop();
    }
  });

  try {
    const status = await scanner.start('15m', 10);
    assert.equal(status.monitoredSymbols, 2);

    const update = (symbol, start, high, low) => {
      websocketManager.onKline({
        k: { s: symbol, t: start, h: String(high), l: String(low) }
      });
    };

    update('BTCUSDT', 1000, 110, 100);
    update('BTCUSDT', 1000, 120, 100);
    update('BTCUSDT', 2000, 111, 100);
    update('ETHUSDT', 1000, 110, 100);
    await new Promise((resolve) => setImmediate(resolve));

    assert.equal(sentSignals.length, 3);
    assert.equal(sentSignals[0].percentageRange, 10);
    assert.equal(sentSignals[0].detectedAt.endsWith('Z'), true);
    const finalStatus = scanner.getStatus();
    assert.equal(finalStatus.detectedSignals, 3);
    assert.equal(
      finalStatus.latestRanges.find((range) => range.symbol === 'BTCUSDT').percentageRange,
      11
    );
  } finally {
    await scanner.stop();
  }
});
