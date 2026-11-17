const PUSHOVER_URL = 'https://api.pushover.net/1/messages.json';
const BINANCE_FUTURES_URL = 'https://www.binance.com/en/futures';

function getBinanceFuturesUrl(symbol) {
  // Use Binance's HTTPS universal link so mobile devices can open the Binance
  // app when installed, while other devices fall back to the web interface.
  return `${BINANCE_FUTURES_URL}/${encodeURIComponent(symbol.toUpperCase())}`;
}

class NotificationService {
  constructor({ userKey, apiToken }) {
    this.userKey = userKey;
    this.apiToken = apiToken;
  }

  async sendSignal(signal) {
    if (!this.userKey || !this.apiToken) {
      throw new Error('Pushover credentials are missing. Set PUSHOVER_USER_KEY and PUSHOVER_API_TOKEN in .env.');
    }

    const symbol = signal.symbol.toUpperCase();

    const body = new URLSearchParams({
      token: this.apiToken,
      user: this.userKey,
      title: '🚨 BINANCE FUTURES ALERT',
      message: [
        `Symbol: ${symbol}`,
        `Timeframe: ${signal.timeframe}`,
        `Percentage Range: ${signal.percentageRange.toFixed(2)}%`,
        `High: ${signal.high}`,
        `Low: ${signal.low}`,
        `Threshold: ${signal.threshold}%`,
        `Detected (UTC): ${signal.detectedAt}`
      ].join('\n'),
      url: getBinanceFuturesUrl(symbol),
      url_title: `Open ${symbol} Binance Futures chart`
    });

    const response = await fetch(PUSHOVER_URL, {
      method: 'POST',
      headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
      body,
      signal: AbortSignal.timeout(15000)
    });

    const result = await response.json();
    if (!response.ok || result.status !== 1) {
      const details = Array.isArray(result.errors) ? result.errors.join(', ') : `HTTP ${response.status}`;
      throw new Error(`Pushover request failed: ${details}`);
    }
  }
}

module.exports = NotificationService;
