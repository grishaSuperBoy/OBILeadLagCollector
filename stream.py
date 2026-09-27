"""
Ultra-Low Latency WebSocket Streaming Engine for Lead & Lag Venues.
Direct async WebSockets with sub-millisecond parsing and auto-reconnect.
Zero heavy SDK overhead.
"""

import asyncio
import json
import logging
import time
from typing import Callable, Dict, List, Optional, Set, Any
import websockets

from models import OrderBookDepth5, LiquidationEvent, FundingScheduleEvent

log = logging.getLogger("stream")


class BaseVenueStream:
    """Base class for venue-specific WebSocket feeds."""

    def __init__(self, name: str, symbols: List[str], callback: Callable[[OrderBookDepth5], None]):
        self.name = name.lower()
        self.symbols = [s.upper() for s in symbols]
        self.callback = callback
        self.connected = False
        self.last_msg_ts = 0.0
        self._running = False
        self._task: Optional[asyncio.Task] = None

    async def start(self):
        self._running = True
        self._task = asyncio.create_task(self._run_loop())

    async def stop(self):
        self._running = False
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        self.connected = False

    async def _run_loop(self):
        raise NotImplementedError


class BinanceStyleStream(BaseVenueStream):
    """
    Subscribes to depth5@100ms on Binance Futures or Aster DEX.
    Both venues use identical Binance-protocol formats.
    """

    def __init__(
        self,
        name: str,
        base_ws_url: str,
        symbols: List[str],
        callback: Callable[[OrderBookDepth5], None],
    ):
        super().__init__(name, symbols, callback)
        self.base_ws_url = base_ws_url.rstrip("/")

    async def _run_loop(self):
        # Format combined stream url
        # E.g. wss://fstream.binance.com/stream?streams=enausdt@depth5@100ms/wldusdt@depth5@100ms/...
        formatted_syms = [s.lower() if s.endswith("usdt") else f"{s.lower()}usdt" for s in self.symbols]
        streams_param = "/".join(f"{s}@depth5@100ms" for s in formatted_syms)
        url = f"{self.base_ws_url}/stream?streams={streams_param}"

        while self._running:
            try:
                log.info(f"[{self.name}] Connecting to {self.base_ws_url} ({len(self.symbols)} symbols)...")
                async with websockets.connect(
                    url,
                    ping_interval=20,
                    ping_timeout=10,
                    max_queue=200,
                ) as ws:
                    self.connected = True
                    log.info(f"[{self.name}] Connected successfully!")
                    while self._running:
                        msg = await ws.recv()
                        self.last_msg_ts = time.time()
                        self._parse_message(msg)
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.connected = False
                log.warning(f"[{self.name}] WS disconnected: {e}. Reconnecting in 3s...")
                await asyncio.sleep(3.0)

    def _parse_message(self, raw_msg: str):
        try:
            payload = json.loads(raw_msg)
            data = payload.get("data", payload)
            sym = data.get("s")
            if not sym:
                return

            exchange_ts = int(data.get("E", 0))
            raw_bids = data.get("b", [])
            raw_asks = data.get("a", [])

            bids = [(float(p), float(q)) for p, q in raw_bids[:5]]
            asks = [(float(p), float(q)) for p, q in raw_asks[:5]]

            snap = OrderBookDepth5(
                exchange=self.name,
                symbol=sym.upper(),
                ts=time.time(),
                exchange_ts=exchange_ts,
                bids=bids,
                asks=asks,
            )
            self.callback(snap)
        except Exception:
            pass


class HyperliquidStream(BaseVenueStream):
    """
    Direct WebSocket connection to Hyperliquid L1 orderbook.
    Subscribes to 'l2Book' for each coin.
    """

    WS_URL = "wss://api.hyperliquid.xyz/ws"

    def __init__(self, symbols: List[str], callback: Callable[[OrderBookDepth5], None]):
        super().__init__("hyperliquid", symbols, callback)

    async def _run_loop(self):
        while self._running:
            try:
                log.info(f"[hyperliquid] Connecting to {self.WS_URL}...")
                async with websockets.connect(
                    self.WS_URL,
                    ping_interval=30,
                    ping_timeout=15,
                    max_queue=200,
                ) as ws:
                    self.connected = True
                    log.info("[hyperliquid] Connected! Subscribing to l2Book...")

                    # Subscribe to each coin (filter unsupported meme tickers to prevent WS drop)
                    unsupported_hl = {"PEPE", "SHIB", "BONK", "FLOKI"}
                    for s in self.symbols:
                        coin = s.replace("USDT", "").replace("USD", "").upper()
                        if coin in unsupported_hl:
                            continue
                        sub_msg = {
                            "method": "subscribe",
                            "subscription": {"type": "l2Book", "coin": coin}
                        }
                        await ws.send(json.dumps(sub_msg))
                        await asyncio.sleep(0.025)

                    # Background ping loop
                    ping_task = asyncio.create_task(self._ping_loop(ws))
                    try:
                        while self._running:
                            msg = await ws.recv()
                            self.last_msg_ts = time.time()
                            self._parse_message(msg)
                    finally:
                        ping_task.cancel()
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.connected = False
                log.warning(f"[hyperliquid] WS error: {e}. Reconnecting in 3s...")
                await asyncio.sleep(3.0)

    async def _ping_loop(self, ws):
        try:
            while self._running:
                await asyncio.sleep(20)
                await ws.send(json.dumps({"method": "ping"}))
        except Exception:
            pass

    def _parse_message(self, raw_msg: str):
        try:
            payload = json.loads(raw_msg)
            if payload.get("channel") != "l2Book":
                return
            data = payload.get("data", {})
            coin = data.get("coin")
            if not coin:
                return

            levels = data.get("levels", [[], []])
            raw_bids = levels[0] if len(levels) > 0 else []
            raw_asks = levels[1] if len(levels) > 1 else []

            bids = [(float(x["px"]), float(x["sz"])) for x in raw_bids[:5]]
            asks = [(float(x["px"]), float(x["sz"])) for x in raw_asks[:5]]

            snap = OrderBookDepth5(
                exchange="hyperliquid",
                symbol=f"{coin.upper()}USDT",
                ts=time.time(),
                exchange_ts=int(data.get("time", 0)),
                bids=bids,
                asks=asks,
            )
            self.callback(snap)
        except Exception:
            pass


class DydxStream(BaseVenueStream):
    """
    Subscribes to dYdX v4 Indexer WebSocket for orderbook updates.
    """

    WS_URL = "wss://indexer.dydx.trade/v4/ws"

    def __init__(self, symbols: List[str], callback: Callable[[OrderBookDepth5], None]):
        super().__init__("dydx", symbols, callback)

    async def _run_loop(self):
        while self._running:
            try:
                log.info(f"[dydx] Connecting to {self.WS_URL}...")
                async with websockets.connect(
                    self.WS_URL,
                    ping_interval=25,
                    ping_timeout=15,
                    max_queue=200,
                ) as ws:
                    self.connected = True
                    log.info("[dydx] Connected! Subscribing to v4_orderbook...")

                    for s in self.symbols:
                        coin = s.replace("USDT", "").replace("USD", "").upper()
                        market_id = f"{coin}-USD"
                        sub_msg = {
                            "type": "subscribe",
                            "channel": "v4_orderbook",
                            "id": market_id
                        }
                        await ws.send(json.dumps(sub_msg))

                    while self._running:
                        msg = await ws.recv()
                        self.last_msg_ts = time.time()
                        self._parse_message(msg)
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.connected = False
                log.warning(f"[dydx] WS error: {e}. Reconnecting in 4s...")
                await asyncio.sleep(4.0)

    def _parse_message(self, raw_msg: str):
        try:
            payload = json.loads(raw_msg)
            if payload.get("channel") != "v4_orderbook":
                return
            contents = payload.get("contents", {})
            market_id = payload.get("id", "")
            coin = market_id.split("-")[0] if "-" in market_id else market_id
            if not coin:
                return

            raw_bids = contents.get("bids", [])
            raw_asks = contents.get("asks", [])
            if not raw_bids and not raw_asks:
                return

            bids = [(float(x["price"]), float(x["size"])) for x in raw_bids[:5] if "price" in x and "size" in x]
            asks = [(float(x["price"]), float(x["size"])) for x in raw_asks[:5] if "price" in x and "size" in x]

            if not bids or not asks:
                return

            snap = OrderBookDepth5(
                exchange="dydx",
                symbol=f"{coin.upper()}USDT",
                ts=time.time(),
                exchange_ts=int(time.time() * 1000),
                bids=bids,
                asks=asks,
            )
            self.callback(snap)
        except Exception:
            pass


class CEXFastStream(BaseVenueStream):
    """
    High-speed native WebSocket connection for major CEXes:
    Bybit, OKX, Bitget, MEXC, Gate.io, BingX.
    """

    URL_MAP = {
        "bybit": "wss://stream.bybit.com/v5/public/linear",
        "okx": "wss://ws.okx.com:8443/ws/v5/public",
        "bitget": "wss://ws.bitget.com/v2/ws/public",
        "mexc": "wss://contract.mexc.com/edge",
        "gateio": "wss://fx-ws.gateio.ws/v4/ws/usdt",
        "bingx": "wss://open-api-swap.bingx.com/swap-market",
    }

    def __init__(self, exchange: str, symbols: List[str], callback: Callable[[OrderBookDepth5], None]):
        super().__init__(exchange, symbols, callback)
        self.ws_url = self.URL_MAP.get(self.name, "")

    async def _run_loop(self):
        if not self.ws_url:
            log.warning(f"[{self.name}] No native WS endpoint configured.")
            return

        while self._running:
            try:
                log.info(f"[{self.name}] Connecting to {self.ws_url}...")
                async with websockets.connect(
                    self.ws_url,
                    ping_interval=20,
                    ping_timeout=10,
                    max_queue=200,
                ) as ws:
                    self.connected = True
                    log.info(f"[{self.name}] Connected! Sending subscriptions...")
                    await self._send_subscriptions(ws)

                    while self._running:
                        msg = await ws.recv()
                        self.last_msg_ts = time.time()
                        if isinstance(msg, bytes):
                            import gzip
                            try:
                                msg = gzip.decompress(msg).decode("utf-8")
                            except Exception:
                                msg = msg.decode("utf-8", errors="ignore")
                        if msg == "Ping":
                            await ws.send("Pong")
                            continue
                        self._parse_message(msg)
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.connected = False
                log.warning(f"[{self.name}] WS disconnected: {e}. Reconnecting in 3s...")
                await asyncio.sleep(3.0)

    async def _send_subscriptions(self, ws):
        try:
            if self.name == "bybit":
                args = [f"orderbook.50.{s.upper()}" for s in self.symbols]
                await ws.send(json.dumps({"op": "subscribe", "args": args}))
            elif self.name == "okx":
                args = [{"channel": "books5", "instId": f"{s.replace('USDT','')}-USDT-SWAP"} for s in self.symbols]
                await ws.send(json.dumps({"op": "subscribe", "args": args}))
            elif self.name == "bitget":
                args = [{"instType": "USDT-FUTURES", "channel": "books5", "instId": s.upper()} for s in self.symbols]
                await ws.send(json.dumps({"op": "subscribe", "args": args}))
            elif self.name == "mexc":
                for s in self.symbols:
                    pair = f"{s.replace('USDT','')}_USDT"
                    await ws.send(json.dumps({"method": "sub.depth.full", "param": {"symbol": pair, "limit": 5}}))
            elif self.name == "gateio":
                args = [f"{s.replace('USDT','')}_USDT" for s in self.symbols]
                sub = {
                    "time": int(time.time()),
                    "channel": "futures.order_book",
                    "event": "subscribe",
                    "payload": args + ["5", "0"]
                }
                await ws.send(json.dumps(sub))
            elif self.name == "bingx":
                for s in self.symbols:
                    coin = s.replace("USDT", "").upper()
                    sub = {
                        "id": f"sub_{coin}",
                        "reqType": "sub",
                        "dataType": f"{coin}-USDT@depth5",
                    }
                    await ws.send(json.dumps(sub))
                    await asyncio.sleep(0.02)
        except Exception as e:
            log.warning(f"[{self.name}] Error sending sub: {e}")

    def _parse_message(self, raw_msg: str):
        try:
            # Handles text and binary responses
            if isinstance(raw_msg, bytes):
                import gzip
                try:
                    raw_msg = gzip.decompress(raw_msg).decode("utf-8")
                except Exception:
                    raw_msg = raw_msg.decode("utf-8", errors="ignore")

            data = json.loads(raw_msg)

            # 1. Bybit
            if self.name == "bybit":
                topic = data.get("topic", "")
                if not topic.startswith("orderbook"):
                    return
                sym = data.get("data", {}).get("s", "")
                raw_b = data.get("data", {}).get("b", [])
                raw_a = data.get("data", {}).get("a", [])
                if raw_b and raw_a:
                    bids = [(float(p), float(q)) for p, q in raw_b[:5]]
                    asks = [(float(p), float(q)) for p, q in raw_a[:5]]
                    self.callback(OrderBookDepth5(self.name, sym, time.time(), int(data.get("ts", 0)), bids, asks))

            # 2. OKX
            elif self.name == "okx":
                arg = data.get("arg", {})
                if arg.get("channel") != "books5":
                    return
                book_data = data.get("data", [{}])[0]
                inst_id = arg.get("instId", "").replace("-USDT-SWAP", "USDT")
                raw_b = book_data.get("bids", [])
                raw_a = book_data.get("asks", [])
                if raw_b and raw_a:
                    bids = [(float(p), float(q)) for p, q, *_ in raw_b[:5]]
                    asks = [(float(p), float(q)) for p, q, *_ in raw_a[:5]]
                    self.callback(OrderBookDepth5(self.name, inst_id, time.time(), int(book_data.get("ts", 0)), bids, asks))

            # 3. Bitget
            elif self.name == "bitget":
                action = data.get("action")
                if action not in ("snapshot", "update"):
                    return
                book_data = data.get("data", [{}])[0]
                inst_id = data.get("arg", {}).get("instId", "")
                raw_b = book_data.get("bids", [])
                raw_a = book_data.get("asks", [])
                if raw_b and raw_a:
                    bids = [(float(p), float(q)) for p, q in raw_b[:5]]
                    asks = [(float(p), float(q)) for p, q in raw_a[:5]]
                    self.callback(OrderBookDepth5(self.name, inst_id, time.time(), int(data.get("ts", 0)), bids, asks))

            # 4. MEXC
            elif self.name == "mexc":
                channel = data.get("channel", "")
                if "depth" not in channel:
                    return
                d = data.get("data", {})
                sym = data.get("symbol", "").replace("_", "")
                raw_b = d.get("bids", [])
                raw_a = d.get("asks", [])
                if raw_b and raw_a:
                    bids = [(float(p), float(q)) for p, q in raw_b[:5]]
                    asks = [(float(p), float(q)) for p, q in raw_a[:5]]
                    self.callback(OrderBookDepth5(self.name, sym, time.time(), int(data.get("ts", 0)), bids, asks))

            # 5. Gate.io
            elif self.name == "gateio":
                event = data.get("event")
                if event != "update":
                    return
                result = data.get("result", {})
                sym = result.get("s", "").replace("_", "")
                raw_b = result.get("bids", [])
                raw_a = result.get("asks", [])
                if raw_b and raw_a:
                    bids = [(float(x.get("p", 0)), float(x.get("s", 0))) for x in raw_b[:5]]
                    asks = [(float(x.get("p", 0)), float(x.get("s", 0))) for x in raw_a[:5]]
                    self.callback(OrderBookDepth5(self.name, sym, time.time(), int(data.get("time_ms", 0)), bids, asks))

            # 6. BingX
            elif self.name == "bingx":
                datatype = data.get("dataType", "")
                if "@depth5" not in datatype:
                    return
                sym = datatype.split("@")[0].replace("-", "")
                b_data = data.get("data", {})
                raw_b = b_data.get("bids", [])
                raw_a = b_data.get("asks", [])
                if raw_b and raw_a:
                    bids = [(float(p), float(q)) for p, q in raw_b[:5]]
                    asks = [(float(p), float(q)) for p, q in raw_a[:5]]
                    self.callback(OrderBookDepth5(self.name, sym, time.time(), int(data.get("ts", 0)), bids, asks))
        except Exception as e:
            pass

# =====================================================================
# ДОПОЛНИТЕЛЬНЫЕ АЛЬФА-ПОТОКИ: ЛИКВИДАЦИИ, ФАНДИНГ, СДЕЛКИ (CVD)
# =====================================================================

class BinanceLiquidationStream:
    """
    Глобальный сокет принудительных ликвидаций Binance Futures (!forceOrder@arr).
    Всего 1 легковесный сокет на весь рынок, потребляет < 0.1% CPU.
    """
    WS_URL = "wss://fstream.binance.com/ws/!forceOrder@arr"

    def __init__(self, callback: Callable[[LiquidationEvent], None]):
        self.callback = callback
        self.connected = False
        self.last_msg_ts = 0.0
        self._running = False
        self._task: Optional[asyncio.Task] = None

    async def start(self):
        self._running = True
        self._task = asyncio.create_task(self._run_loop())

    async def stop(self):
        self._running = False
        if self._task and not self._task.done():
            self._task.cancel()
        self.connected = False

    async def _run_loop(self):
        while self._running:
            try:
                log.info("[binance_liquidations] Connecting to !forceOrder@arr...")
                async with websockets.connect(self.WS_URL, ping_interval=20, ping_timeout=10, max_queue=200) as ws:
                    self.connected = True
                    log.info("[binance_liquidations] Stream connected successfully!")
                    while self._running:
                        raw = await ws.recv()
                        self.last_msg_ts = time.time()
                        self._parse_msg(raw)
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.connected = False
                log.warning(f"[binance_liquidations] WS error: {e}. Reconnecting in 3s...")
                await asyncio.sleep(3.0)

    def _parse_msg(self, raw: str):
        try:
            data = json.loads(raw)
            order = data.get("o", {})
            sym = order.get("s", "").upper()
            if not sym:
                return

            side = order.get("S", "")       # 'BUY' или 'SELL'
            price = float(order.get("p", 0.0))
            qty = float(order.get("q", 0.0))
            qty_usd = price * qty

            event = LiquidationEvent(
                id=str(order.get("T", int(time.time() * 1000))),
                symbol=sym,
                side=side,
                price=price,
                qty=qty,
                qty_usd=qty_usd,
                exchange="binance",
                ts=time.time(),
            )
            self.callback(event)
        except Exception:
            pass


class BinanceMarkPriceFundingStream:
    """
    Поток ставок фандинга и обратного отсчета (!markPrice@arr@1s).
    Единый поток для всех пар Binance, обновляется раз в секунду.
    """
    WS_URL = "wss://fstream.binance.com/ws/!markPrice@arr@1s"

    def __init__(self, callback: Callable[[FundingScheduleEvent], None], symbols: List[str]):
        self.callback = callback
        self.tracked_symbols = set(s.upper() for s in symbols)
        self.connected = False
        self.last_msg_ts = 0.0
        self._running = False
        self._task: Optional[asyncio.Task] = None

    async def start(self):
        self._running = True
        self._task = asyncio.create_task(self._run_loop())

    async def stop(self):
        self._running = False
        if self._task and not self._task.done():
            self._task.cancel()
        self.connected = False

    async def _run_loop(self):
        while self._running:
            try:
                log.info("[binance_funding] Connecting to !markPrice@arr@1s...")
                async with websockets.connect(self.WS_URL, ping_interval=20, ping_timeout=10, max_queue=200) as ws:
                    self.connected = True
                    log.info("[binance_funding] Stream connected successfully!")
                    while self._running:
                        raw = await ws.recv()
                        self.last_msg_ts = time.time()
                        self._parse_msg(raw)
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.connected = False
                log.warning(f"[binance_funding] WS error: {e}. Reconnecting in 5s...")
                await asyncio.sleep(5.0)

    def _parse_msg(self, raw: str):
        try:
            items = json.loads(raw)
            now = time.time()
            if not isinstance(items, list):
                items = [items]

            for item in items:
                sym = item.get("s", "").upper()
                if self.tracked_symbols and sym not in self.tracked_symbols:
                    continue

                r_str = item.get("r")
                t_val = item.get("T")
                if r_str is None or t_val is None:
                    continue

                funding_rate = float(r_str)
                next_ts = float(t_val) / 1000.0
                countdown = max(0.0, next_ts - now)

                # Флаг экстремального фандинга (|rate| >= 0.10% за 8 часов = 109% годовых)
                is_extreme = abs(funding_rate) >= 0.0010

                ev = FundingScheduleEvent(
                    symbol=sym,
                    exchange="binance",
                    funding_rate=funding_rate,
                    next_funding_ts=next_ts,
                    countdown_sec=countdown,
                    is_extreme=is_extreme,
                )
                self.callback(ev)
        except Exception:
            pass


class BinanceAggTradeStream:
    """
    Поток рыночных сделок (@aggTrade) для вычисления Cumulative Volume Delta (CVD)
    и детекции скрытого токсичного напора фондов.
    """
    def __init__(self, symbols: List[str], callback: Callable[[str, float, float, bool, float], None]):
        self.symbols = [s.lower() if s.endswith("usdt") else f"{s.lower()}usdt" for s in symbols]
        self.callback = callback
        self.connected = False
        self.last_msg_ts = 0.0
        self._running = False
        self._task: Optional[asyncio.Task] = None

    async def start(self):
        self._running = True
        self._task = asyncio.create_task(self._run_loop())

    async def stop(self):
        self._running = False
        if self._task and not self._task.done():
            self._task.cancel()
        self.connected = False

    async def _run_loop(self):
        streams_param = "/".join(f"{s}@aggTrade" for s in self.symbols)
        url = f"wss://fstream.binance.com/stream?streams={streams_param}"

        while self._running:
            try:
                log.info(f"[binance_aggtrades] Connecting ({len(self.symbols)} alts)...")
                async with websockets.connect(url, ping_interval=20, ping_timeout=10, max_queue=300) as ws:
                    self.connected = True
                    log.info("[binance_aggtrades] Connected successfully!")
                    while self._running:
                        raw = await ws.recv()
                        self.last_msg_ts = time.time()
                        self._parse_msg(raw)
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.connected = False
                log.warning(f"[binance_aggtrades] WS error: {e}. Reconnecting in 4s...")
                await asyncio.sleep(4.0)

    def _parse_msg(self, raw: str):
        try:
            payload = json.loads(raw)
            data = payload.get("data", payload)
            sym = data.get("s", "").upper()
            if not sym:
                return

            price = float(data.get("p", 0.0))
            qty = float(data.get("q", 0.0))
            is_buyer_maker = bool(data.get("m", False))
            ts = float(data.get("T", time.time() * 1000)) / 1000.0

            self.callback(sym, price, qty, is_buyer_maker, ts)
        except Exception:
            pass



class StreamManager:
    """
    Управляет всеми активными WebSocket-потоками:
    1. Стаканы Depth5 для Lead и Lag площадок.
    2. Поток принудительных ликвидаций Binance (!forceOrder@arr).
    3. Поток ставок финансирования Binance (!markPrice@arr@1s).
    4. Поток сделок Binance (@aggTrade) для расчета CVD и токсичности.
    """

    def __init__(
        self,
        lead_exchange: str,
        lag_exchanges: List[str],
        symbols: List[str],
        callback: Callable[[OrderBookDepth5], None],
        on_liquidation: Optional[Callable[[LiquidationEvent], None]] = None,
        on_funding: Optional[Callable[[FundingScheduleEvent], None]] = None,
        on_agg_trade: Optional[Callable[[str, float, float, bool, float], None]] = None,
    ):
        self.lead_exchange = lead_exchange
        self.lag_exchanges = lag_exchanges
        self.symbols = symbols
        self.callback = callback
        self.on_liquidation = on_liquidation
        self.on_funding = on_funding
        self.on_agg_trade = on_agg_trade

        self.streams: Dict[str, Any] = {}

    async def start_all(self):
        log.info(f"Starting StreamManager for {len(self.symbols)} symbols across {1 + len(self.lag_exchanges)} venues...")

        # 1. Lead Venue Orderbook (Binance)
        if self.lead_exchange == "binance":
            self.streams["binance"] = BinanceStyleStream(
                name="binance",
                base_ws_url="wss://fstream.binance.com",
                symbols=self.symbols,
                callback=self.callback,
            )

        # 2. Lag Venues Orderbooks
        for ex in self.lag_exchanges:
            if ex == "asterdex":
                self.streams["asterdex"] = BinanceStyleStream(
                    name="asterdex",
                    base_ws_url="wss://fstream.asterdex.com",
                    symbols=self.symbols,
                    callback=self.callback,
                )
            elif ex == "hyperliquid":
                self.streams["hyperliquid"] = HyperliquidStream(
                    symbols=self.symbols,
                    callback=self.callback,
                )
            elif ex == "dydx":
                self.streams["dydx"] = DydxStream(
                    symbols=self.symbols,
                    callback=self.callback,
                )
            elif ex in CEXFastStream.URL_MAP:
                self.streams[ex] = CEXFastStream(
                    exchange=ex,
                    symbols=self.symbols,
                    callback=self.callback,
                )

        # 3. Дополнительные альфа-потоки Binance
        if self.on_liquidation:
            self.streams["binance_liquidations"] = BinanceLiquidationStream(callback=self.on_liquidation)

        if self.on_funding:
            self.streams["binance_funding"] = BinanceMarkPriceFundingStream(
                callback=self.on_funding, symbols=self.symbols
            )

        if self.on_agg_trade:
            self.streams["binance_aggtrades"] = BinanceAggTradeStream(
                symbols=self.symbols, callback=self.on_agg_trade
            )

        # Запуск всех потоков параллельно
        for s in self.streams.values():
            await s.start()

    async def stop_all(self):
        log.info("Stopping all streams...")
        for s in self.streams.values():
            await s.stop()

    def get_status(self) -> Dict[str, Dict[str, Any]]:
        now = time.time()
        return {
            name: {
                "connected": getattr(s, "connected", False),
                "seconds_since_last_msg": round(now - s.last_msg_ts, 1) if getattr(s, "last_msg_ts", 0) > 0 else None,
            }
            for name, s in self.streams.items()
        }

