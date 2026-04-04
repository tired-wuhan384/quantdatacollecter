"""
ClickHouse Backend for cryptofeed - Orderflow Data Writer

Supports: Trades, BookSnapshots, Liquidations, Funding, OpenInterest
Uses clickhouse-connect for batch async writes.
"""
import logging
from collections import defaultdict
from datetime import datetime, timezone

import clickhouse_connect

from cryptofeed.backends.backend import BackendBookCallback, BackendCallback, BackendQueue

LOG = logging.getLogger('feedhandler')


def _to_utc_datetime(timestamp):
    return datetime.fromtimestamp(timestamp, tz=timezone.utc) if timestamp else None


def _pad_levels(levels: list[tuple[float, float]], depth: int) -> tuple[list[float], list[float]]:
    prices = [price for price, _ in levels]
    sizes = [size for _, size in levels]
    missing = depth - len(levels)
    if missing > 0:
        prices.extend([0.0] * missing)
        sizes.extend([0.0] * missing)
    return prices, sizes


def _safe_ratio(numerator: float, denominator: float) -> float:
    return numerator / denominator if denominator else 0.0


def build_trade_row(data: dict) -> dict:
    price = float(data['price'])
    quantity = float(data['amount'])
    return {
        'timestamp': _to_utc_datetime(data['timestamp']),
        'receipt_ts': _to_utc_datetime(data['receipt_timestamp']),
        'exchange': data['exchange'],
        'symbol': data['symbol'],
        'side': data['side'],
        'price': price,
        'quantity': quantity,
        'notional': price * quantity,
        'trade_id': data.get('id'),
    }


def build_book_topk_row(data: dict, depth: int) -> dict:
    book_data = data.get('book', data.get('delta', {}))
    bids_raw = book_data.get('bid', {})
    asks_raw = book_data.get('ask', {})

    bids = sorted(
        ((float(price), float(size)) for price, size in bids_raw.items()),
        key=lambda item: item[0],
        reverse=True,
    )[:depth]
    asks = sorted(
        ((float(price), float(size)) for price, size in asks_raw.items()),
        key=lambda item: item[0],
    )[:depth]

    bid_px, bid_sz = _pad_levels(bids, depth)
    ask_px, ask_sz = _pad_levels(asks, depth)

    best_bid = bid_px[0]
    best_ask = ask_px[0]
    mid_price = (best_bid + best_ask) / 2 if best_bid and best_ask else 0.0
    spread_bps = _safe_ratio(best_ask - best_bid, mid_price) * 10000 if mid_price else 0.0

    top1_total = bid_sz[0] + ask_sz[0]
    top5_bid_depth = sum(bid_sz[:5])
    top5_ask_depth = sum(ask_sz[:5])
    top10_bid_depth = sum(bid_sz[:10])
    top10_ask_depth = sum(ask_sz[:10])
    top5_total = top5_bid_depth + top5_ask_depth
    top10_total = top10_bid_depth + top10_ask_depth
    microprice = _safe_ratio((bid_sz[0] * best_ask) + (ask_sz[0] * best_bid), top1_total)

    return {
        'exchange': data['exchange'],
        'symbol': data['symbol'],
        'timestamp': _to_utc_datetime(data['timestamp']),
        'receipt_ts': _to_utc_datetime(data['receipt_timestamp']),
        'mid_price': mid_price,
        'best_bid': best_bid,
        'best_ask': best_ask,
        'spread_bps': spread_bps,
        'microprice': microprice,
        'bid_px': bid_px,
        'bid_sz': bid_sz,
        'ask_px': ask_px,
        'ask_sz': ask_sz,
        'bid_depth_5': top5_bid_depth,
        'bid_depth_10': top10_bid_depth,
        'ask_depth_5': top5_ask_depth,
        'ask_depth_10': top10_ask_depth,
        'imbalance_top1': _safe_ratio(bid_sz[0] - ask_sz[0], top1_total),
        'imbalance_top5': _safe_ratio(top5_bid_depth - top5_ask_depth, top5_total),
        'imbalance_top10': _safe_ratio(top10_bid_depth - top10_ask_depth, top10_total),
    }


class ClickHouseCallback(BackendQueue):
    def __init__(self, host='localhost', port=8123, username='default', password='',
                 database='raw', table=None, batch_size=5000, **kwargs):
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.database = database
        self.table = table if table else self.default_table
        self.batch_size = batch_size
        self.client = None
        self.running = True
        self.numeric_type = float
        self.none_to = None

    def _connect(self):
        if self.client is None:
            self.client = clickhouse_connect.get_client(
                host=self.host, port=self.port,
                username=self.username, password=self.password,
                database=self.database
            )
            LOG.info("ClickHouse connected: %s:%s/%s", self.host, self.port, self.database)

    async def writer(self):
        while self.running:
            async with self.read_queue() as updates:
                if updates:
                    self._connect()
                    self.write_batch(updates)

    def write_batch(self, updates: list):
        """Subclasses must implement: write updates list to ClickHouse"""
        raise NotImplementedError


class TradeClickHouse(ClickHouseCallback, BackendCallback):
    default_table = 'trades'

    def write_batch(self, updates: list):
        rows = []
        for data in updates:
            row = build_trade_row(data)
            rows.append([
                row['timestamp'],
                row['receipt_ts'],
                row['exchange'],
                row['symbol'],
                row['side'],
                row['price'],
                row['quantity'],
                row['notional'],
                row['trade_id'],
            ])

        self.client.insert(
            self.table, rows,
            column_names=['timestamp', 'receipt_ts', 'exchange', 'symbol',
                          'side', 'price', 'quantity', 'notional', 'trade_id']
        )
        LOG.debug("ClickHouse: inserted %d trades", len(rows))


class BookClickHouse(ClickHouseCallback, BackendBookCallback):
    default_table = 'book_topk_raw'

    def __init__(self, *args, snapshots_only=True, snapshot_interval=1000,
                 depth=10, **kwargs):
        self.snapshots_only = snapshots_only
        self.snapshot_interval = snapshot_interval
        self.snapshot_count = defaultdict(int)
        self.depth = depth
        super().__init__(*args, **kwargs)

    def write_batch(self, updates: list):
        rows = []
        for data in updates:
            row = build_book_topk_row(data, depth=self.depth)
            rows.append([
                row['exchange'],
                row['symbol'],
                row['timestamp'],
                row['receipt_ts'],
                row['mid_price'],
                row['best_bid'],
                row['best_ask'],
                row['spread_bps'],
                row['microprice'],
                row['bid_px'],
                row['bid_sz'],
                row['ask_px'],
                row['ask_sz'],
                row['bid_depth_5'],
                row['bid_depth_10'],
                row['ask_depth_5'],
                row['ask_depth_10'],
                row['imbalance_top1'],
                row['imbalance_top5'],
                row['imbalance_top10'],
            ])

        self.client.insert(
            self.table, rows,
            column_names=[
                'exchange', 'symbol', 'timestamp', 'receipt_ts',
                'mid_price', 'best_bid', 'best_ask', 'spread_bps', 'microprice',
                'bid_px', 'bid_sz', 'ask_px', 'ask_sz',
                'bid_depth_5', 'bid_depth_10', 'ask_depth_5', 'ask_depth_10',
                'imbalance_top1', 'imbalance_top5', 'imbalance_top10',
            ]
        )
        LOG.debug("ClickHouse: inserted %d book snapshots", len(rows))


class LiquidationsClickHouse(ClickHouseCallback, BackendCallback):
    default_table = 'liquidations'

    def write_batch(self, updates: list):
        rows = []
        for data in updates:
            ts = datetime.fromtimestamp(data['timestamp'], tz=timezone.utc) if data['timestamp'] else None
            rts = datetime.fromtimestamp(data['receipt_timestamp'], tz=timezone.utc)
            rows.append([
                ts,
                rts,
                data['exchange'],
                data['symbol'],
                data['side'],
                float(data['price']),
                float(data['quantity']),
                data.get('status', ''),
                data.get('id'),
            ])

        self.client.insert(
            self.table, rows,
            column_names=['timestamp', 'receipt_ts', 'exchange', 'symbol',
                          'side', 'price', 'quantity', 'status', 'order_id']
        )
        LOG.debug("ClickHouse: inserted %d liquidations", len(rows))


class FundingClickHouse(ClickHouseCallback, BackendCallback):
    default_table = 'funding'

    def write_batch(self, updates: list):
        rows = []
        for data in updates:
            ts = datetime.fromtimestamp(data['timestamp'], tz=timezone.utc) if data['timestamp'] else None
            rts = datetime.fromtimestamp(data['receipt_timestamp'], tz=timezone.utc)
            rows.append([
                ts,
                rts,
                data['exchange'],
                data['symbol'],
                float(data['mark_price']) if data.get('mark_price') else None,
                float(data['rate']),
                float(data['predicted_rate']) if data.get('predicted_rate') else None,
            ])

        self.client.insert(
            self.table, rows,
            column_names=['timestamp', 'receipt_ts', 'exchange', 'symbol',
                          'mark_price', 'rate', 'predicted_rate']
        )
        LOG.debug("ClickHouse: inserted %d funding", len(rows))


class OpenInterestClickHouse(ClickHouseCallback, BackendCallback):
    default_table = 'open_interest'

    def write_batch(self, updates: list):
        rows = []
        for data in updates:
            ts = datetime.fromtimestamp(data['timestamp'], tz=timezone.utc) if data['timestamp'] else None
            rts = datetime.fromtimestamp(data['receipt_timestamp'], tz=timezone.utc)
            rows.append([
                ts,
                rts,
                data['exchange'],
                data['symbol'],
                float(data['open_interest']),
            ])

        self.client.insert(
            self.table, rows,
            column_names=['timestamp', 'receipt_ts', 'exchange', 'symbol', 'open_interest']
        )
        LOG.debug("ClickHouse: inserted %d open_interest", len(rows))
