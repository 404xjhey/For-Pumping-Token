const REFRESH_INTERVAL_MS = 5 * 60 * 1000;

class SymbolManager {
  constructor(binanceService, logger) {
    this.binanceService = binanceService;
    this.logger = logger;
    this.symbols = [];
  }

  async refresh() {
    const nextSymbols = await this.binanceService.getUsdtPerpetualSymbols();
    const changed = nextSymbols.length !== this.symbols.length ||
      nextSymbols.some((symbol, index) => symbol !== this.symbols[index]);

    this.symbols = nextSymbols;
    if (changed) {
      this.logger.info(`Discovered ${nextSymbols.length} eligible USDT perpetual symbols.`);
    }

    return { symbols: this.symbols.slice(), changed };
  }

  getRefreshInterval() {
    return REFRESH_INTERVAL_MS;
  }
}

module.exports = SymbolManager;
