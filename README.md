# Binance Futures Real-Time Percentage Range Scanner

A standalone Node.js script that discovers every active USDT-margined perpetual Binance Futures contract, scans its live selected-timeframe candle, and sends one Pushover alert per symbol per candle after the configured range is reached. No browser or user interface is required.

## Requirements

- Node.js 18.17 or newer
- A Pushover account and application API token (needed for notifications)

## Install and configure

1. In this directory, install dependencies:

   ```powershell
   npm install
   ```

2. Copy `.env.example` to `.env` and set your credentials:

   ```text
   PUSHOVER_USER_KEY=your_user_key
   PUSHOVER_API_TOKEN=your_application_api_token
   ```

   `.env` is ignored by Git. Keep these values private; they are used only by the backend.

3. Start the scanner with a Binance timeframe and minimum percentage range:

   ```powershell
   npm start -- 1m 0.5
   ```

   Other examples: `npm start -- 15m 10` or `npm start -- 1h 15`. Stop it with **Ctrl+C**.

   You can also run the script directly:

   ```powershell
   node scanner.js 1m 0.5
   ```

   For a fixed configuration, set `SCANNER_TIMEFRAME` and `SCANNER_PERCENT_RANGE` in `.env`, then run `npm start` without arguments. Positional arguments override those environment variables.

## Pushover credentials

Create or sign in to an account at [pushover.net](https://pushover.net) to obtain your **User Key**. Create a Pushover application/API token on the Pushover dashboard to obtain the **API Token**. Install/sign in to the Pushover app on the device where you want to receive alerts.

## Implementation notes

- Eligible symbols are refreshed from Binance exchange information every five minutes. Only `TRADING`, `PERPETUAL`, `USDT` quote contracts are included.
- Combined kline streams are grouped into 200-stream WebSocket connections, with exponential reconnect backoff. Symbol list changes update the stream subscriptions.
- If any WebSocket connection has no kline updates for 60 seconds (or none arrive shortly after startup), the scanner falls back to Binance REST klines. It requests the current and previous candle for each symbol at a capped 10 requests/second (about 600 request weight/minute), and returns to WebSockets when all connections are receiving fresh klines. This degraded-mode fallback can take about a minute to sweep 522 symbols; WebSocket mode remains immediate.
- Range is calculated from the live kline's high and low: `((high - low) / low) * 100`.
- A symbol's candle start time is used as the duplicate-notification key. A threshold crossing triggers one alert for that symbol and candle.
- Runtime status, detected signals, and errors are written to the terminal. The process runs independently of any browser and exits cleanly on Ctrl+C.
