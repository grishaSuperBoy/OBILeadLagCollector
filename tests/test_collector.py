import unittest
from models import OrderBookDepth5, LiquidationEvent, FundingScheduleEvent
from engine import OBIEngine


class TestOBILeadLagCollector(unittest.TestCase):

    def test_obi_calculation(self):
        # 40 bids, 10 asks -> (40 - 10) / (40 + 10) = 30 / 50 = +0.60
        b = OrderBookDepth5(
            exchange="binance",
            symbol="ENAUSDT",
            ts=100.0,
            exchange_ts=100000,
            bids=[(0.50, 40.0)],
            asks=[(0.51, 10.0)],
        )
        self.assertAlmostEqual(b.obi, 0.60)
        self.assertAlmostEqual(b.mid_price, 0.505)
        self.assertAlmostEqual(b.depth_bid_usd, 20.0)

    def test_multi_venue_lag_detection(self):
        engine = OBIEngine(
            lead_exchange="binance",
            lag_exchanges=["asterdex", "hyperliquid", "dydx", "mexc"],
            symbols=["ENAUSDT"],
            obi_threshold=0.50,
            min_lead_lag_bps=15.0,  # 0.15%
        )

        # 1. Provide initial quotes for lag venues (at mid = 1.0000)
        lag_venues = ["asterdex", "hyperliquid", "dydx", "mexc"]
        for ex in lag_venues:
            book = OrderBookDepth5(
                exchange=ex,
                symbol="ENAUSDT",
                ts=100.0,
                exchange_ts=100000,
                bids=[(0.9995, 100.0)],
                asks=[(1.0005, 100.0)],
            )
            engine.on_depth_update(book)

        # 2. Binance surges up to mid = 1.0050 (+50 bps) with massive OBI = +0.80
        binance_book = OrderBookDepth5(
            exchange="binance",
            symbol="ENAUSDT",
            ts=100.1,
            exchange_ts=100100,
            bids=[(1.0045, 900.0)],
            asks=[(1.0055, 100.0)],
        )
        engine.on_depth_update(binance_book)

        snap = engine.get_snapshot()
        # All 4 lag venues should have detected a dislocation because lag = +50 bps >= 15 bps
        self.assertEqual(snap["stats"]["total_dislocations"], 4)
        self.assertEqual(len(snap["active_orders"]), 4)

        # Verify limit price is at BBO (best bid for BUY)
        for order in snap["active_orders"]:
            self.assertEqual(order["side"], "BUY")
            self.assertEqual(order["limit_price"], 0.9995)

    def test_trade_fill_and_tp(self):
        engine = OBIEngine(
            lead_exchange="binance",
            lag_exchanges=["hyperliquid"],
            symbols=["SUIUSDT"],
            obi_threshold=0.50,
            min_lead_lag_bps=10.0,
            tp_bps=20.0,
            sl_bps=15.0,
        )

        # 1. Hyperliquid book
        hl_book = OrderBookDepth5(
            exchange="hyperliquid",
            symbol="SUIUSDT",
            ts=100.0,
            exchange_ts=100000,
            bids=[(2.000, 100.0)],
            asks=[(2.002, 100.0)],
        )
        engine.on_depth_update(hl_book)

        # 2. Binance surge (Buy signal triggered)
        binance_book = OrderBookDepth5(
            exchange="binance",
            symbol="SUIUSDT",
            ts=100.1,
            exchange_ts=100100,
            bids=[(2.010, 800.0)],
            asks=[(2.012, 100.0)],
        )
        engine.on_depth_update(binance_book)
        self.assertEqual(len(engine._active_trades), 1)
        self.assertEqual(engine._active_trades[0].status, "PENDING")

        # 3. Simulate Hyperliquid market moving and filling the maker bid limit
        hl_fill = OrderBookDepth5(
            exchange="hyperliquid",
            symbol="SUIUSDT",
            ts=100.3,
            exchange_ts=100300,
            bids=[(2.000, 50.0)],
            asks=[(2.000, 50.0)],  # Ask touched limit_price 2.000
        )
        engine.on_depth_update(hl_fill)
        self.assertEqual(engine._active_trades[0].status, "FILLED")

        # 4. Simulate Hyperliquid catching up to Binance (+30 bps) -> TP hit
        hl_tp = OrderBookDepth5(
            exchange="hyperliquid",
            symbol="SUIUSDT",
            ts=100.8,
            exchange_ts=100800,
            bids=[(2.007, 100.0)],
            asks=[(2.009, 100.0)],  # Mid = 2.008 (+40 bps > 20 bps TP)
        )
        engine.on_depth_update(hl_tp)

        snap = engine.get_snapshot()
        self.assertEqual(snap["stats"]["total_trades"], 1)
        self.assertEqual(snap["stats"]["winning_trades"], 1)
        self.assertGreater(snap["stats"]["total_net_pnl_usd"], 0)

    def test_liquidity_snapshot_extraction(self):
        engine = OBIEngine(symbols=["ENAUSDT"])
        book = OrderBookDepth5(
            exchange="asterdex",
            symbol="ENAUSDT",
            ts=100.0,
            exchange_ts=100000,
            bids=[(1.0, 50.0)],
            asks=[(1.01, 50.0)],
        )
        engine.on_depth_update(book)
        engine.on_depth_update(book)

        dislocations, liquidity, trades, liqs, fundings = engine.extract_and_reset_unflushed()
        self.assertEqual(len(liquidity), 1)
        self.assertEqual(liquidity[0]["symbol"], "ENAUSDT")
        self.assertEqual(liquidity[0]["exchange"], "asterdex")
        self.assertEqual(liquidity[0]["tick_count"], 2)

        # Second extraction should be empty
        _, liquidity2, _, _, _ = engine.extract_and_reset_unflushed()
        self.assertEqual(len(liquidity2), 0)

    def test_liquidation_cascade(self):
        engine = OBIEngine(symbols=["ENAUSDT"])
        liq = LiquidationEvent(
            id="12345",
            symbol="ENAUSDT",
            side="SELL",  # Long liquidation
            price=0.50,
            qty=100000.0,
            qty_usd=50000.0,
            exchange="binance",
            ts=100.0,
        )
        engine.on_liquidation(liq)
        snap = engine.get_snapshot()
        self.assertEqual(len(snap["latest_liquidations"]), 1)
        self.assertEqual(snap["latest_liquidations"][0]["qty_usd"], 50000.0)

        # Extraction check
        _, _, _, liqs, _ = engine.extract_and_reset_unflushed()
        self.assertEqual(len(liqs), 1)

    def test_cvd_toxic_flow_detection(self):
        engine = OBIEngine(symbols=["ENAUSDT"])
        # Series of aggressive buy taker trades ($30,000 within 5 seconds)
        engine.on_agg_trade("ENAUSDT", price=1.0, qty=10000.0, is_buyer_maker=False, ts=100.0)
        engine.on_agg_trade("ENAUSDT", price=1.0, qty=20000.0, is_buyer_maker=False, ts=102.0)
        # Flush the 5s window
        engine.on_agg_trade("ENAUSDT", price=1.0, qty=100.0, is_buyer_maker=False, ts=106.0)

        snap = engine.get_snapshot()
        self.assertEqual(len(snap["toxic_flow_alerts"]), 1)
        alert = snap["toxic_flow_alerts"][0]
        self.assertTrue(alert["is_toxic_flow"])
        self.assertEqual(alert["aggression_ratio"], 1.0)

    def test_funding_countdown_and_extreme_flag(self):
        engine = OBIEngine(symbols=["ENAUSDT"])
        ev = FundingScheduleEvent(
            symbol="ENAUSDT",
            exchange="binance",
            funding_rate=0.0015,  # +0.15% (extreme)
            next_funding_ts=100000.0,
            countdown_sec=240.0,  # 4 minutes left (T-5M alert)
            is_extreme=True,
        )
        engine.on_funding_update(ev)
        snap = engine.get_snapshot()
        self.assertEqual(len(snap["extreme_funding_schedules"]), 1)
        self.assertEqual(snap["extreme_funding_schedules"][0]["countdown_sec"], 240.0)

    def test_liquidity_void_detection(self):
        engine = OBIEngine(symbols=["ENAUSDT"])
        # Extremely thin book ($50 on bids, $50 on asks)
        thin_book = OrderBookDepth5(
            exchange="mexc",
            symbol="ENAUSDT",
            ts=100.0,
            exchange_ts=100000,
            bids=[(1.00, 50.0)],
            asks=[(1.01, 50.0)],
        )
        engine.on_depth_update(thin_book)
        snap = engine.get_snapshot()
        self.assertGreaterEqual(len(snap["liquidity_voids"]), 1)
        void_metric = snap["liquidity_voids"][0]
        self.assertTrue(void_metric["is_void"])
        self.assertAlmostEqual(void_metric["trough_bid_limit"], 0.96)
        self.assertAlmostEqual(void_metric["trough_ask_limit"], 1.0504)

    def test_time_anomaly_clock_profiler(self):
        engine = OBIEngine(symbols=["ENAUSDT"])
        # Simulate spikes at minute 17 of hour 1 and minute 17 of hour 2
        # ts = hour * 3600 + minute * 60
        ts_hour1_min17 = 1 * 3600 + 17 * 60 + 5.0
        ts_hour2_min17 = 2 * 3600 + 17 * 60 + 10.0

        engine._record_clock_tick("ENAUSDT", side="BUY", bps=25.0, now=ts_hour1_min17)
        engine._record_clock_tick("ENAUSDT", side="BUY", bps=30.0, now=ts_hour2_min17)

        snap = engine.get_snapshot()
        self.assertGreaterEqual(len(snap["time_anomalies"]), 1)
        anomaly = snap["time_anomalies"][0]
        self.assertEqual(anomaly["minute_of_hour"], 17)
        self.assertEqual(anomaly["occurrences"], 2)
        self.assertGreater(anomaly["confidence_score"], 0.5)

    def test_scrubber_cycle_detection(self):
        engine = OBIEngine(symbols=["ENAUSDT"])
        # Alternating BUY -> SELL -> BUY spaced by 60s
        t0 = 1000.0
        engine._record_scrubber_tick("ENAUSDT", "mexc", "BUY", 20.0, t0)
        engine._record_scrubber_tick("ENAUSDT", "mexc", "SELL", 25.0, t0 + 60.0)
        engine._record_scrubber_tick("ENAUSDT", "mexc", "BUY", 22.0, t0 + 120.0)
        engine._record_scrubber_tick("ENAUSDT", "mexc", "SELL", 24.0, t0 + 180.0)

        snap = engine.get_snapshot()
        self.assertGreaterEqual(len(snap["active_scrubbers"]), 1)
        scrubber = snap["active_scrubbers"][0]
        self.assertEqual(scrubber["symbol"], "ENAUSDT")
        self.assertEqual(scrubber["exchange"], "mexc")
        self.assertGreaterEqual(scrubber["flips_count"], 3)
        self.assertEqual(scrubber["current_phase"], "SELL_PHASE")


if __name__ == "__main__":
    unittest.main()


