const EXCHANGE_INFO_URL = 'https://fapi.binance.com/fapi/v1/exchangeInfo';

class BinanceService {
  async getUsdtPerpetualSymbols() {
    const response = await fetch(EXCHANGE_INFO_URL, {
      signal: AbortSignal.timeout(15000)
    });

    if (!response.ok) {
      throw new Error(`Binance exchange information request failed (${response.status}).`);
    }

    const data = await response.json();
    if (!data || !Array.isArray(data.symbols)) {
      throw new Error('Binance returned an invalid exchange information response.');
    }

    return data.symbols
      .filter((symbol) =>
        symbol.status === 'TRADING' &&
        symbol.contractType === 'PERPETUAL' &&
        symbol.quoteAsset === 'USDT' &&
        symbol.symbol
      )
      .map((symbol) => symbol.symbol.toLowerCase())
      .sort();
  }

  async getCurrentKlines(symbol, interval, signal) {
    const url = new URL('https://fapi.binance.com/fapi/v1/klines');
    url.searchParams.set('symbol', symbol);
    url.searchParams.set('interval', interval);
    url.searchParams.set('limit', '2');

    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), 15000);
    const onAbort = () => controller.abort();
    if (signal) {
      if (signal.aborted) controller.abort();
      else signal.addEventListener('abort', onAbort, { once: true });
    }

    try {
      const response = await fetch(url, { signal: controller.signal });
      if (response.status === 418 || response.status === 429) {
        const error = new Error(`Binance kline request was rate limited (${response.status}).`);
        error.rateLimited = true;
        const retryAfter = Number(response.headers.get('Retry-After'));
        error.retryAfterMs = Number.isFinite(retryAfter) && retryAfter > 0
          ? retryAfter * 1000
          : 60000;
        throw error;
      }
      if (!response.ok) {
        throw new Error(`Binance kline request failed for ${symbol} (${response.status}).`);
      }

      const klines = await response.json();
      if (!Array.isArray(klines) || klines.some((kline) => !Array.isArray(kline) || kline.length < 7)) {
        throw new Error(`Binance returned invalid kline data for ${symbol}.`);
      }
      return klines;
    } finally {
      clearTimeout(timeout);
      if (signal) signal.removeEventListener('abort', onAbort);
    }
  }
}

module.exports = BinanceService;
