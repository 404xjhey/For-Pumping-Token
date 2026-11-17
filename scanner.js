require('dotenv').config();

const BinanceService = require('./backend/binanceService');
const SymbolManager = require('./backend/symbolManager');
const WebSocketManager = require('./backend/websocketManager');
const NotificationService = require('./backend/notificationService');
const ScannerService = require('./backend/scannerService');

const logger = {
  info: (...args) => console.info(new Date().toISOString(), '[INFO]', ...args),
  warn: (...args) => console.warn(new Date().toISOString(), '[WARN]', ...args),
  error: (...args) => console.error(new Date().toISOString(), '[ERROR]', ...args)
};

const supportedTimeframes = new Set([
  '1m', '3m', '5m', '15m', '30m',
  '1h', '2h', '4h', '6h', '8h', '12h',
  '1d', '3d', '1w', '1M'
]);

function getConfiguration(args) {
  const timeframe = args[0] || process.env.SCANNER_TIMEFRAME;
  const thresholdValue = args[1] === undefined
    ? process.env.SCANNER_PERCENT_RANGE
    : args[1];

  if (args.length > 2) {
    throw new Error('Provide exactly a timeframe and percentage threshold.');
  }
  if (!timeframe || thresholdValue === undefined) {
    throw new Error(
      'Usage: npm start -- <timeframe> <percentRange> (example: npm start -- 1m 0.5)'
    );
  }
  if (!supportedTimeframes.has(timeframe)) {
    throw new Error(`Unsupported timeframe "${timeframe}".`);
  }

  const percentRange = Number(thresholdValue);
  if (!Number.isFinite(percentRange) || percentRange <= 0) {
    throw new Error('Percentage threshold must be a finite number greater than zero.');
  }

  return { timeframe, percentRange };
}

async function main() {
  const { timeframe, percentRange } = getConfiguration(process.argv.slice(2));
  const binanceService = new BinanceService();
  const symbolManager = new SymbolManager(binanceService, logger);
  const websocketManager = new WebSocketManager({ logger });
  const notificationService = new NotificationService({
    userKey: process.env.PUSHOVER_USER_KEY,
    apiToken: process.env.PUSHOVER_API_TOKEN
  });
  const scannerService = new ScannerService({
    symbolManager,
    websocketManager,
    notificationService,
    binanceService,
    logger
  });

  if (!process.env.PUSHOVER_USER_KEY || !process.env.PUSHOVER_API_TOKEN) {
    logger.warn('Pushover credentials are not configured; alerts will fail until .env is set.');
  }

  let stopping = false;
  const shutdown = async (signal) => {
    if (stopping) return;
    stopping = true;
    logger.info(`Received ${signal}; stopping scanner.`);
    await scannerService.stop();
  };
  process.once('SIGINT', () => shutdown('SIGINT').catch((error) => {
    logger.error('Shutdown failed:', error.message);
    process.exitCode = 1;
  }));
  process.once('SIGTERM', () => shutdown('SIGTERM').catch((error) => {
    logger.error('Shutdown failed:', error.message);
    process.exitCode = 1;
  }));

  const status = await scannerService.start(timeframe, percentRange);
  logger.info(
    `Scanning ${status.monitoredSymbols} USDT perpetual symbols; ` +
    `timeframe=${timeframe}, threshold=${percentRange}%. Press Ctrl+C to stop.`
  );
}

if (require.main === module) {
  main().catch((error) => {
    logger.error(error.message);
    process.exitCode = 1;
  });
}

module.exports = { getConfiguration };
