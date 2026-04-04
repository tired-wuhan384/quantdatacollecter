from backends.clickhouse import build_book_topk_row, build_trade_row


def test_build_trade_row_maps_quantity_and_notional():
    row = build_trade_row(
        {
            "timestamp": 1710000000.0,
            "receipt_timestamp": 1710000000.5,
            "exchange": "BINANCE_FUTURES",
            "symbol": "BTC-USDT-PERP",
            "side": "buy",
            "price": "100.0",
            "amount": "2.0",
            "id": "t-1",
        }
    )

    assert row["quantity"] == 2.0
    assert row["notional"] == 200.0
    assert row["trade_id"] == "t-1"


def test_build_book_topk_row_outputs_fixed_depth_arrays():
    row = build_book_topk_row(
        {
            "timestamp": 1710000000.0,
            "receipt_timestamp": 1710000000.5,
            "exchange": "BINANCE_FUTURES",
            "symbol": "BTC-USDT-PERP",
            "book": {
                "bid": {"100.0": "3.0", "99.5": "2.0"},
                "ask": {"100.5": "4.0", "101.0": "1.0"},
            },
        },
        depth=10,
    )

    assert len(row["bid_px"]) == 10
    assert len(row["bid_sz"]) == 10
    assert len(row["ask_px"]) == 10
    assert len(row["ask_sz"]) == 10
    assert row["best_bid"] == 100.0
    assert row["best_ask"] == 100.5
    assert row["spread_bps"] > 0
    assert row["bid_depth_5"] == 5.0
    assert row["ask_depth_5"] == 5.0
