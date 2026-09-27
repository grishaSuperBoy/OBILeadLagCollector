"""
High-Performance OBI Lead-Lag & Weak Market Maker Analysis Engine.
Guaranteed memory bounded with deque(maxlen=200) for Render Free Tier.
"""

from collections import deque
import logging
import time
from typing import Dict, List, Optional, Tuple, Any
import uuid

from models import (
    LiquiditySnapshot10m,
    OBIDislocationEvent,
    OrderBookDepth5,
    PaperTrade,
    LiquidationEvent,
    CVDMetric,
    FundingScheduleEvent,
    LiquidityVoidMetric,
    TimeAnomalyMetric,
    ScrubberCycleMetric,
    TradFiArbitrageMetric,
)
import config


log = logging.getLogger("engine")


class OBIEngine:
    def __init__(
        self,
        lead_exchange: str = "binance",
        lag_exchanges: Optional[List[str]] = None,
        symbols: Optional[List[str]] = None,
        obi_threshold: float = 0.50,
        min_lead_lag_bps: float = 15.0,
        tp_bps: float = 25.0,
        sl_bps: float = 18.0,
        order_timeout_sec: float = 15.0,
        max_hold_sec: float = 45.0,
        notional_usd: float = 50.0,
        cooldown_sec: float = 3.0,
    ):
        self.lead_exchange = lead_exchange.lower()
        self.lag_exchanges = [e.lower() for e in (lag_exchanges or ["asterdex", "hyperliquid", "dydx", "mexc", "bitget", "bybit", "okx"])]
        self.symbols = [s.upper() for s in (symbols or [])]

        self.obi_threshold = obi_threshold
        self.min_lead_lag_bps = min_lead_lag_bps
        self.tp_bps = tp_bps
        self.sl_bps = sl_bps
        self.order_timeout_sec = order_timeout_sec
        self.max_hold_sec = max_hold_sec
        self.notional_usd = notional_usd
        self.cooldown_sec = cooldown_sec

        # In-memory storage bounded strictly by maxlen
        self._latest_books: Dict[Tuple[str, str], OrderBookDepth5] = {}
        self._dislocations: deque = deque(maxlen=200)
        self._active_trades: deque = deque(maxlen=100)
        self._closed_trades: deque = deque(maxlen=200)
        self._cooldowns: Dict[Tuple[str, str, str], float] = {}  # (symbol, side, lag_ex) -> timestamp

        # Буферы для новых альфа-потоков: ликвидации, фандинг, CVD
        self._recent_liquidations: deque = deque(maxlen=100)
        self._unflushed_liquidations: List[Dict[str, Any]] = []

        self._funding_schedules: Dict[str, FundingScheduleEvent] = {}
        self._unflushed_funding: List[Dict[str, Any]] = []

        self._cvd_windows: Dict[str, Dict[str, Any]] = {}  # symbol -> {buy_vol, sell_vol, window_start}
        self._toxic_flow_alerts: deque = deque(maxlen=50)

        # Новые детекторы из лекции Антона (Тазики, Тайм-боты, Ёршики)
        self._liquidity_voids: Dict[Tuple[str, str], LiquidityVoidMetric] = {}
        self._clock_stats: Dict[str, Dict[int, Dict[str, Any]]] = {}  # symbol -> minute -> stats
        self._time_anomalies: Dict[str, TimeAnomalyMetric] = {}  # f"{symbol}_{min}" -> metric
        self._scrubber_state: Dict[Tuple[str, str], Dict[str, Any]] = {}  # (ex, sym) -> state
        self._active_scrubbers: Dict[Tuple[str, str], ScrubberCycleMetric] = {}

        # TradFi / CME Futures vs Crypto Arbitrage (UTex <-> Binance/Bitget)
        self._tradfi_arbitrage: Dict[str, TradFiArbitrageMetric] = {}

        # Unflushed buffers waiting for the 10-minute MongoDB batch sync
        self._unflushed_dislocations: List[Dict[str, Any]] = []
        self._unflushed_trades: List[Dict[str, Any]] = []


        # 10-minute rolling liquidity statistics per (exchange, symbol)
        self._liquidity_accumulators: Dict[Tuple[str, str], Dict[str, Any]] = {}

        # Aggregate Statistics
        self.total_dislocations = 0
        self.total_trades = 0
        self.winning_trades = 0
        self.losing_trades = 0
        self.total_net_pnl_usd = 0.0

        # Per-exchange statistics: which MM is slowest / most profitable
        self.exchange_stats: Dict[str, Dict[str, Any]] = {
            ex: {
                "dislocations_detected": 0,
                "total_lag_bps": 0.0,
                "avg_lag_bps": 0.0,
                "trades_filled": 0,
                "winning_trades": 0,
                "net_pnl_usd": 0.0,
            }
            for ex in self.lag_exchanges
        }

    def on_depth_update(self, snap: OrderBookDepth5):
        """Called immediately upon receiving parsed book from any WebSocket stream."""
        now = snap.ts if snap.ts > 0 else time.time()
        ex = snap.exchange.lower()
        sym = snap.symbol.upper()
        key = (ex, sym)

        # 1. Update in-memory book cache
        self._latest_books[key] = snap

        # 2. Accumulate liquidity profile for 10-minute rollups
        self._accumulate_liquidity(snap, key, now)

        # 3. Evaluate Liquidity Void (для ловли прострелов / 'Тазики' Long & Short)
        self._evaluate_liquidity_void(snap, key, now)

        # 4. Check for Lead-Lag Dislocation if update is from Lead (Binance)
        if ex == self.lead_exchange:
            self._evaluate_lead_update(snap, now)
        elif ex in self.lag_exchanges:
            # 5. If update is from Lag venue, update active paper trades (fill / TP / SL)
            self._evaluate_lag_trades(snap, now)

    def _evaluate_liquidity_void(self, snap: OrderBookDepth5, key: Tuple[str, str], now: float):
        """
        Оценка пустоты стакана (Liquidity Void) для стратегии 'Ловля прострелов' ('Тазики').
        Если суммарный объем топ-5 в USD < $500 или стоимость сдвига на 2% < $300,
        стакан классифицируется как экстремально дырявый (кандидат для пассивных лимиток).
        """
        mid = snap.mid_price
        if mid <= 0 or not snap.bids or not snap.asks:
            return

        depth_bid = snap.depth_bid_usd
        depth_ask = snap.depth_ask_usd

        # Расчет стоимости сдвига цены на 2% (200 bps)
        target_bid_px = mid * 0.98
        target_ask_px = mid * 1.02

        cost_bid = sum(px * qty for px, qty in snap.bids if px >= target_bid_px)
        cost_ask = sum(px * qty for px, qty in snap.asks if px <= target_ask_px)

        is_void = (depth_bid < 500.0 or depth_ask < 500.0 or cost_bid < 300.0 or cost_ask < 300.0)

        # Рекомендуемые уровни 'тазиков':
        # Long: -4% ниже лучшего Bid (ловля панического пролива)
        # Short: +4% выше лучшего Ask (ловля выноса стакана)
        metric = LiquidityVoidMetric(
            symbol=snap.symbol,
            exchange=snap.exchange,
            ts=now,
            mid_price=mid,
            depth_bid_usd=depth_bid,
            depth_ask_usd=depth_ask,
            cost_to_move_2pct_bid=cost_bid,
            cost_to_move_2pct_ask=cost_ask,
            is_void=is_void,
            trough_bid_limit=snap.best_bid * 0.96,
            trough_ask_limit=snap.best_ask * 1.04,
        )
        self._liquidity_voids[key] = metric

    def _accumulate_liquidity(self, snap: OrderBookDepth5, key: Tuple[str, str], now: float):
        if key not in self._liquidity_accumulators:
            self._liquidity_accumulators[key] = {
                "exchange": snap.exchange,
                "symbol": snap.symbol,
                "spread_sum": 0.0,
                "depth_bid_sum": 0.0,
                "depth_ask_sum": 0.0,
                "last_mid": snap.mid_price,
                "tick_count": 0,
                "start_ts": now,
            }
        acc = self._liquidity_accumulators[key]
        acc["spread_sum"] += snap.spread_bps
        acc["depth_bid_sum"] += snap.depth_bid_usd
        acc["depth_ask_sum"] += snap.depth_ask_usd
        acc["last_mid"] = snap.mid_price
        acc["tick_count"] += 1

    def _evaluate_lead_update(self, lead_snap: OrderBookDepth5, now: float):
        obi = lead_snap.obi
        lead_mid = lead_snap.mid_price
        if lead_mid <= 0:
            return

        # Determine direction based on OBI threshold
        if obi >= self.obi_threshold:
            side = "BUY"
        elif obi <= -self.obi_threshold:
            side = "SELL"
        else:
            return

        sym = lead_snap.symbol

        # Scan ALL active lag venues for this symbol
        for lag_ex in self.lag_exchanges:
            cd_key = (sym, side, lag_ex)
            last_time = self._cooldowns.get(cd_key, 0.0)
            if now - last_time < self.cooldown_sec:
                continue

            lag_snap = self._latest_books.get((lag_ex, sym))
            if not lag_snap or not lag_snap.bids or not lag_snap.asks:
                continue

            # Stale check: lag book must not be older than 2.5s
            if abs(now - lag_snap.ts) > 2.5:
                continue

            lag_mid = lag_snap.mid_price
            if lag_mid <= 0:
                continue

            # Calculate basis points displacement
            lead_lag_bps = (lead_mid - lag_mid) / lag_mid * 10000.0

            # Check if lag is in favorable direction and exceeds threshold
            is_dislocated = False
            if side == "BUY" and lead_lag_bps >= self.min_lead_lag_bps:
                is_dislocated = True
            elif side == "SELL" and lead_lag_bps <= -self.min_lead_lag_bps:
                is_dislocated = True

            if not is_dislocated:
                continue

            # --- Dislocation Event Confirmed ---
            self._cooldowns[cd_key] = now
            self.total_dislocations += 1

            # Update per-exchange stats
            st = self.exchange_stats[lag_ex]
            st["dislocations_detected"] += 1
            st["total_lag_bps"] += abs(lead_lag_bps)
            st["avg_lag_bps"] = round(st["total_lag_bps"] / st["dislocations_detected"], 2)

            # Регистрация в профайлере тайм-ботов и детектора ёршиков
            self._record_clock_tick(sym, side, lead_lag_bps, now)
            self._record_scrubber_tick(sym, lag_ex, side, lead_lag_bps, now)

            limit_price = lag_snap.best_bid if side == "BUY" else lag_snap.best_ask

            event = OBIDislocationEvent(
                id=str(uuid.uuid4())[:8],
                ts=now,
                symbol=sym,
                side=side,
                lead_exchange=self.lead_exchange,
                lead_mid=lead_mid,
                lead_obi=obi,
                lag_exchange=lag_ex,
                lag_mid=lag_mid,
                lag_best_bid=lag_snap.best_bid,
                lag_best_ask=lag_snap.best_ask,
                lag_bps=lead_lag_bps,
                maker_limit_price=limit_price,
            )
            self._dislocations.append(event)
            self._unflushed_dislocations.append(event.to_dict())

            # Spawn simulated post-only maker limit order
            trade = PaperTrade(
                id=event.id,
                symbol=sym,
                side=side,
                exchange=lag_ex,
                limit_price=limit_price,
                notional_usd=self.notional_usd,
                entry_ts=now,
                status="PENDING",
            )
            self._active_trades.append(trade)

            log.info(
                f"[DISLOCATION] {side} {sym} | Lead {self.lead_exchange.upper()} OBI={obi:+.2f} "
                f"| Lag {lag_ex.upper()} by {lead_lag_bps:+.1f} bps | Limit={limit_price}"
            )

    def _evaluate_lag_trades(self, lag_snap: OrderBookDepth5, now: float):
        ex = lag_snap.exchange.lower()
        sym = lag_snap.symbol.upper()

        for trade in list(self._active_trades):
            if trade.exchange != ex or trade.symbol != sym:
                continue

            # 1. PENDING: Check if our maker limit gets filled
            if trade.status == "PENDING":
                # Check for order timeout (canceled if not filled in 15s)
                if now - trade.entry_ts > self.order_timeout_sec:
                    trade.status = "EXPIRED"
                    trade.exit_reason = "limit_timeout"
                    self._active_trades.remove(trade)
                    self._closed_trades.append(trade)
                    self._unflushed_trades.append(trade.to_dict())
                    continue

                # Fill check: for BUY, ask must touch or cross limit_price
                if trade.side == "BUY" and lag_snap.best_ask <= trade.limit_price:
                    trade.status = "FILLED"
                    trade.fill_price = trade.limit_price
                    trade.fill_ts = now
                    self.exchange_stats[ex]["trades_filled"] += 1
                # For SELL, bid must touch or cross limit_price
                elif trade.side == "SELL" and lag_snap.best_bid >= trade.limit_price:
                    trade.status = "FILLED"
                    trade.fill_price = trade.limit_price
                    trade.fill_ts = now
                    self.exchange_stats[ex]["trades_filled"] += 1

            # 2. FILLED: Manage position (TP, SL, or TimeStop)
            elif trade.status == "FILLED":
                hold_sec = now - trade.fill_ts
                cur_price = lag_snap.mid_price
                if cur_price <= 0:
                    continue

                if trade.side == "BUY":
                    pnl_bps = (cur_price - trade.fill_price) / trade.fill_price * 10000.0
                else:
                    pnl_bps = (trade.fill_price - cur_price) / trade.fill_price * 10000.0

                reason = ""
                # Take-Profit triggered
                if pnl_bps >= self.tp_bps:
                    reason = "take_profit"
                # Stop-Loss triggered
                elif pnl_bps <= -self.sl_bps:
                    reason = "stop_loss"
                # Time stop exceeded
                elif hold_sec >= self.max_hold_sec:
                    reason = "time_stop"

                if reason:
                    trade.status = "CLOSED"
                    trade.exit_price = cur_price
                    trade.exit_ts = now
                    trade.exit_reason = reason
                    trade.gross_pnl_bps = pnl_bps
                    # Net PnL calculation: maker fee rebate/cost (~0.01% on maker)
                    fee_pct = 0.0002  # 0.02% total round-trip maker fee
                    net_pct = (pnl_bps / 10000.0) - fee_pct
                    trade.net_pnl_usd = round(net_pct * trade.notional_usd, 4)

                    self.total_trades += 1
                    self.total_net_pnl_usd += trade.net_pnl_usd
                    if trade.net_pnl_usd > 0:
                        self.winning_trades += 1
                        self.exchange_stats[ex]["winning_trades"] += 1
                    else:
                        self.losing_trades += 1

                    self.exchange_stats[ex]["net_pnl_usd"] += trade.net_pnl_usd

                    self._active_trades.remove(trade)
                    self._closed_trades.append(trade)
                    self._unflushed_trades.append(trade.to_dict())

                    log.info(
                        f"[TRADE CLOSED] {trade.side} {sym} on {ex.upper()} | "
                        f"Reason: {reason} | PnL: {pnl_bps:+.1f} bps (${trade.net_pnl_usd:+.2f})"
                    )

    # =====================================================================
    # ОБРАБОТЧИКИ НОВЫХ АЛЬФА-ПОТОКОВ (ЛИКВИДАЦИИ, ФАНДИНГ, CVD)
    # =====================================================================

    def on_liquidation(self, event: LiquidationEvent):
        """
        Обработка ликвидаций с Binance Futures.
        Крупная ликвидация лонгов ($SELL) гарантирует сброс цены вниз.
        Крупная ликвидация шортов ($BUY) гарантирует вынос цены вверх.
        """
        self._recent_liquidations.append(event)
        self._unflushed_liquidations.append(event.to_dict())

        # Если ликвидация на отслеживаемом альте и сумма >= $10,000
        sym_clean = event.symbol.replace("USDT", "")
        if (event.symbol in self.symbols or sym_clean in self.symbols) and event.qty_usd >= 10000.0:
            log.warning(
                f"🚨 [LIQUIDATION CASCADE] {event.side} on {event.symbol} (${event.qty_usd:,.0f} @ {event.price})! "
                f"Front-running lagging venues..."
            )

    def on_funding_update(self, event: FundingScheduleEvent):
        """
        Обновление ставки фандинга и таймера отсечки.
        Анализирует Стратегию 4 (сброс позиций за 5 минут до отсечки).
        """
        self._funding_schedules[event.symbol] = event

        if event.is_extreme:
            self._unflushed_funding.append(event.to_dict())
            if event.countdown_sec <= 300.0:  # За 5 минут до выплаты
                log.info(
                    f"⏰ [FUNDING T-5M ALERT] {event.symbol} rate={event.funding_rate * 100:+.4f}% "
                    f"in {event.countdown_sec:.0f}s! Anticipating crowd unwind."
                )

    def on_agg_trade(self, symbol: str, price: float, qty: float, is_buyer_maker: bool, ts: float):
        """
        Расчет 5-секундной Cumulative Volume Delta (CVD) для детекции VPIN / Toxic Flow.
        is_buyer_maker=True -> агрессивный удар продавца в Bid (Sell Taker).
        is_buyer_maker=False -> агрессивный удар покупателя в Ask (Buy Taker).
        """
        now = ts if ts > 0 else time.time()
        notional = price * qty

        if symbol not in self._cvd_windows:
            self._cvd_windows[symbol] = {
                "buy_vol": 0.0,
                "sell_vol": 0.0,
                "window_start": now,
            }

        win = self._cvd_windows[symbol]
        # Сброс окна каждые 5 секунд
        if now - win["window_start"] > 5.0:
            total_vol = win["buy_vol"] + win["sell_vol"]
            if total_vol > 15000.0:  # Значимый объем для альта ($15k за 5 секунд)
                cvd_5s = win["buy_vol"] - win["sell_vol"]
                aggression = win["buy_vol"] / total_vol

                # Институциональный токсичный поток (доминирование одной стороны > 80%)
                if aggression >= 0.80 or aggression <= 0.20:
                    dominant_side = "BUY" if aggression >= 0.80 else "SELL"
                    metric = CVDMetric(
                        symbol=symbol,
                        ts=now,
                        cvd_5s=cvd_5s,
                        buy_vol_usd=win["buy_vol"],
                        sell_vol_usd=win["sell_vol"],
                        aggression_ratio=aggression,
                        is_toxic_flow=True,
                    )
                    self._toxic_flow_alerts.append(metric)

            win["buy_vol"] = 0.0
            win["sell_vol"] = 0.0
            win["window_start"] = now

        if is_buyer_maker:
            win["sell_vol"] += notional
        else:
            win["buy_vol"] += notional

    def _record_clock_tick(self, symbol: str, side: str, bps: float, now: float):
        """
        Профайлер циклических тайм-ботов по минутам часа (Clock Profiler).
        Фиксирует всплески активности на фиксированных минутах (например :17, :34, :43, :54).
        """
        minute = int((now // 60) % 60)
        hour = int(now // 3600)

        if symbol not in self._clock_stats:
            self._clock_stats[symbol] = {}

        if minute not in self._clock_stats[symbol]:
            self._clock_stats[symbol][minute] = {
                "count": 0,
                "hours": set(),
                "total_bps": 0.0,
                "sides": set(),
                "last_seen": now,
            }

        st = self._clock_stats[symbol][minute]
        st["count"] += 1
        st["hours"].add(hour)
        st["total_bps"] += abs(bps)
        st["sides"].add(side)
        st["last_seen"] = now

        # Если всплеск на этой минуте повторился минимум в 2 разных часах или count >= 3
        distinct_hours = len(st["hours"])
        if distinct_hours >= 2 or st["count"] >= 3:
            avg_bps = st["total_bps"] / st["count"]
            dom_side = side if len(st["sides"]) == 1 else "BOTH"
            confidence = min(1.0, 0.4 * distinct_hours + 0.1 * min(st["count"], 6))

            key = f"{symbol}_{minute}"
            self._time_anomalies[key] = TimeAnomalyMetric(
                symbol=symbol,
                minute_of_hour=minute,
                occurrences=st["count"],
                avg_dislocation_bps=avg_bps,
                dominant_side=dom_side,
                confidence_score=confidence,
                last_seen_ts=now,
            )

    def _record_scrubber_tick(self, symbol: str, exchange: str, side: str, bps: float, now: float):
        """
        Детектор циклических маркет-мейкеров 'Ёршик' (Scrubber Algos).
        Отслеживает чередование направлений покупки и продажи с постоянным периодом.
        """
        key = (exchange, symbol)
        if key not in self._scrubber_state:
            self._scrubber_state[key] = {
                "last_side": side,
                "last_flip_ts": now,
                "flips_count": 0,
                "intervals": [],
                "amplitudes": [],
            }
            return

        st = self._scrubber_state[key]
        if side != st["last_side"]:
            delta_t = now - st["last_flip_ts"]
            # Цикл должен быть в разумных временных рамках (от 20 сек до 300 сек)
            if 20.0 <= delta_t <= 300.0:
                st["flips_count"] += 1
                st["intervals"].append(delta_t)
                if len(st["intervals"]) > 8:
                    st["intervals"].pop(0)

                st["amplitudes"].append(abs(bps))
                if len(st["amplitudes"]) > 8:
                    st["amplitudes"].pop(0)

                # При наличии 3+ последовательных переворотов регистрируем 'Ёршик'
                if st["flips_count"] >= 3:
                    avg_interval = sum(st["intervals"]) / len(st["intervals"])
                    avg_amp = sum(st["amplitudes"]) / len(st["amplitudes"])
                    self._active_scrubbers[key] = ScrubberCycleMetric(
                        symbol=symbol,
                        exchange=exchange,
                        cycle_period_sec=avg_interval * 2.0,  # Полный цикл (Buy + Sell)
                        amplitude_bps=avg_amp,
                        current_phase="BUY_PHASE" if side == "BUY" else "SELL_PHASE",
                        flips_count=st["flips_count"],
                        last_flip_ts=now,
                    )

            st["last_side"] = side
            st["last_flip_ts"] = now

    def extract_and_reset_unflushed(self) -> Tuple[
        List[Dict[str, Any]],
        List[Dict[str, Any]],
        List[Dict[str, Any]],
        List[Dict[str, Any]],
        List[Dict[str, Any]],
    ]:
        """Extracts unpersisted data for MongoDB batch sync and resets unflushed queues."""
        now = time.time()

        # 1. Pop unflushed dislocations & trades
        dislocations = self._unflushed_dislocations
        self._unflushed_dislocations = []

        trades = self._unflushed_trades
        self._unflushed_trades = []

        # 2. Pop liquidations & funding anomalies
        liquidations = self._unflushed_liquidations
        self._unflushed_liquidations = []

        funding_anomalies = self._unflushed_funding
        self._unflushed_funding = []

        # 3. Extract and reset 10m liquidity snapshots
        liquidity_snapshots = []
        for key, acc in list(self._liquidity_accumulators.items()):
            ticks = acc["tick_count"]
            if ticks > 0:
                snap = LiquiditySnapshot10m(
                    ts=now,
                    symbol=acc["symbol"],
                    exchange=acc["exchange"],
                    mid_price=acc["last_mid"],
                    spread_bps=round(acc["spread_sum"] / ticks, 2),
                    depth_bid_usd=round(acc["depth_bid_sum"] / ticks, 2),
                    depth_ask_usd=round(acc["depth_ask_sum"] / ticks, 2),
                    tick_count=ticks,
                )
                liquidity_snapshots.append(snap.to_dict())

        # Reset accumulators for the next 10-minute window
        self._liquidity_accumulators = {}

        return dislocations, liquidity_snapshots, trades, liquidations, funding_anomalies

    def get_snapshot(self) -> Dict[str, Any]:
        """Returns JSON snapshot of live engine state for API & Dashboard."""
        wr = (self.winning_trades / self.total_trades * 100.0) if self.total_trades > 0 else 0.0

        # Топ экстремальных фандингов
        extreme_fundings = [
            f.to_dict() for f in self._funding_schedules.values() if f.is_extreme
        ]
        extreme_fundings.sort(key=lambda x: abs(x["funding_rate"]), reverse=True)

        # Топ кандидатов на 'Тазики' (пустые стаканы)
        void_candidates = [
            v.to_dict() for v in self._liquidity_voids.values() if v.is_void
        ]
        void_candidates.sort(key=lambda x: x["depth_bid_usd"] + x["depth_ask_usd"])

        # Топ обнаруженных тайм-ботов (:17, :34, :43 и т.д.)
        time_bots = [
            t.to_dict() for t in self._time_anomalies.values()
        ]
        time_bots.sort(key=lambda x: (x["confidence_score"], x["occurrences"]), reverse=True)

        # Активные циклические 'Ёршики'
        scrubbers = [
            s.to_dict() for s in self._active_scrubbers.values()
        ]
        scrubbers.sort(key=lambda x: x["flips_count"], reverse=True)

        return {
            "stats": {
                "total_dislocations": self.total_dislocations,
                "total_trades": self.total_trades,
                "winning_trades": self.winning_trades,
                "losing_trades": self.losing_trades,
                "win_rate_pct": round(wr, 1),
                "total_net_pnl_usd": round(self.total_net_pnl_usd, 3),
            },
            "exchange_rankings": [
                {
                    "exchange": ex,
                    **data,
                    "win_rate": round(data["winning_trades"] / data["trades_filled"] * 100.0, 1) if data["trades_filled"] > 0 else 0.0,
                    "net_pnl_usd": round(data["net_pnl_usd"], 3),
                }
                for ex, data in sorted(self.exchange_stats.items(), key=lambda x: x[1]["dislocations_detected"], reverse=True)
            ],
            "active_orders": [t.to_dict() for t in self._active_trades],
            "latest_dislocations": [d.to_dict() for d in reversed(list(self._dislocations))][:20],
            "latest_closed_trades": [t.to_dict() for t in reversed(list(self._closed_trades))][:20],
            "latest_liquidations": [l.to_dict() for l in reversed(list(self._recent_liquidations))][:15],
            "extreme_funding_schedules": extreme_fundings[:10],
            "toxic_flow_alerts": [m.to_dict() for m in reversed(list(self._toxic_flow_alerts))][:10],
            "liquidity_voids": void_candidates[:15],
            "time_anomalies": time_bots[:15],
            "active_scrubbers": scrubbers[:15],
            "tracked_symbols_count": len(self.symbols),
            "lead_exchange": self.lead_exchange,
            "lag_exchanges": self.lag_exchanges,
        }

