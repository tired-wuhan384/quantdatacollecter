"""
Orderflow Data Collector - Main Entry Point

Collects real-time data from Binance Futures:
- Trades (TRADES)
- Order Book Snapshots (L2_BOOK)
- Liquidations (LIQUIDATIONS)
- Funding Rates (FUNDING)
- Open Interest (OPEN_INTEREST)

All data is written to ClickHouse.
"""
import logging
import os
import sys

import aiohttp
from cryptofeed import connection as cf_conn

# Patch: allow cryptofeed aiohttp session to use environment variable proxy
_orig_open = cf_conn.HTTPAsyncConn._open

async def _patched_open(self):
    if self.is_open:
        cf_conn.LOG.warning('%s: HTTP session already created', self.id)
    else:
        cf_conn.LOG.debug('%s: create HTTP session', self.id)
        self.conn = aiohttp.ClientSession(trust_env=True)
        self.sent = 0
        self.received = 0
        self.last_message = None

cf_conn.HTTPAsyncConn._open = _patched_open

from cryptofeed import FeedHandler
from cryptofeed.defines import TRADES, L2_BOOK, LIQUIDATIONS, FUNDING, OPEN_INTEREST
from cryptofeed.exchanges import BinanceFutures
from backends.clickhouse import (
    TradeClickHouse,
    BookClickHouse,
    LiquidationsClickHouse,
    FundingClickHouse,
    OpenInterestClickHouse,
)

# ---- Configuration ----
# All values can be overridden via environment variables
CLICKHOUSE = {
    'host': os.environ.get('CLICKHOUSE_HOST', 'localhost'),
    'port': int(os.environ.get('CLICKHOUSE_PORT', '8123')),
    'username': os.environ.get('CLICKHOUSE_USERNAME', 'default'),
    'password': os.environ.get('CLICKHOUSE_PASSWORD', ''),
    'database': os.environ.get('CLICKHOUSE_DATABASE', 'raw'),
}

PROXY = os.environ.get('HTTP_PROXY') or os.environ.get('HTTPS_PROXY')

# Symbols to subscribe (comma-separated)
SYMBOLS_STR = os.environ.get('COLLECTOR_SYMBOLS', 'BTC-USDT-PERP,ETH-USDT-PERP')
SYMBOLS = [s.strip() for s in SYMBOLS_STR.split(',') if s.strip()]

# ---- Logging ----
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(name)s] %(levelname)s: %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
LOG = logging.getLogger('orderflow')


def main():
    LOG.info("Starting orderflow data collector...")
    LOG.info("Symbols: %s", SYMBOLS)
    LOG.info("ClickHouse: %s:%s/%s", CLICKHOUSE['host'], CLICKHOUSE['port'], CLICKHOUSE['database'])

    fh = FeedHandler()

    ch_kwargs = {**CLICKHOUSE}

    fh.add_feed(
        BinanceFutures(
            symbols=SYMBOLS,
            channels=[TRADES, L2_BOOK, LIQUIDATIONS, FUNDING, OPEN_INTEREST],
            http_proxy=PROXY,
            callbacks={
                TRADES: TradeClickHouse(**ch_kwargs),
                L2_BOOK: BookClickHouse(snapshots_only=True, snapshot_interval=100, depth=20, **ch_kwargs),
                LIQUIDATIONS: LiquidationsClickHouse(**ch_kwargs),
                FUNDING: FundingClickHouse(**ch_kwargs),
                OPEN_INTEREST: OpenInterestClickHouse(**ch_kwargs),
            },
        )
    )

    LOG.info("Feed configured, starting event loop...")
    fh.run()


if __name__ == '__main__':
    main()
