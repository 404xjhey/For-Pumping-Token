const assert = require('node:assert/strict');
const test = require('node:test');
const NotificationService = require('../backend/notificationService');

test('adds a clickable Binance Futures chart link to Pushover alerts', async () => {
  const originalFetch = global.fetch;
  let request;

  global.fetch = async (url, options) => {
    request = { url, options };
    return {
      ok: true,
      status: 200,
      async json() {
        return { status: 1 };
      }
    };
  };

  try {
    await new NotificationService({
      userKey: 'user-key',
      apiToken: 'api-token'
    }).sendSignal({
      symbol: 'btcusdt',
      timeframe: '15m',
      percentageRange: 10,
      high: '110',
      low: '100',
      threshold: 8,
      detectedAt: '2026-11-17T00:00:00.000Z'
    });

    const payload = new URLSearchParams(request.options.body);
    assert.equal(request.url, 'https://api.pushover.net/1/messages.json');
    assert.equal(payload.get('url'), 'https://www.binance.com/en/futures/BTCUSDT');
    assert.equal(payload.get('url_title'), 'Open BTCUSDT Binance Futures chart');
    assert.match(payload.get('message'), /Symbol: BTCUSDT/);
  } finally {
    global.fetch = originalFetch;
  }
});
