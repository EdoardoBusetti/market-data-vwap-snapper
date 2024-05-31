WEBSOCKET_URLS = {
    "coinbase": "wss://ws-feed.exchange.coinbase.com",
    "bitstamp": "wss://ws.bitstamp.net",
    "kraken":  "wss://ws.kraken.com/"
}

EXCEPTIONS_IN_RECONNECTION_IN_ROW_LIMIT = 5
COMMIT_EVERY_X_SECONDS = 0  # commit takes ~ 4ms. avoid doing too often to improve speed
COMMIT_EXCEPTIONS_IN_ROW_COUNT_LIMIT = 3