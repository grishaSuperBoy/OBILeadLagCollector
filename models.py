"""
High-Performance Memory-Bounded Data Models.
All classes utilize __slots__ to eliminate dict overhead and guarantee RAM < 80MB on Render.
"""

from dataclasses import dataclass, field
import time
from typing import Dict, List, Optional, Tuple, Any


@dataclass(slots=True)
class OrderBookDepth5:
    """Compact Top 5 level order book snapshot."""
    exchange: str
    symbol: str
    ts: float                   # local epoch timestamp (seconds)
    exchange_ts: int            # exchange epoch timestamp (ms)
    bids: List[Tuple[float, float]]  # [(price, qty), ...] up to 5
    asks: List[Tuple[float, float]]  # [(price, qty), ...] up to 5

    @property
    def best_bid(self) -> float:
        return self.bids[0][0] if self.bids else 0.0

    @property
    def best_ask(self) -> float:
        return self.asks[0][0] if self.asks else 0.0

    @property
    def mid_price(self) -> float:
        bb = self.best_bid
        ba = self.best_ask
        return (bb + ba) / 2.0 if (bb > 0 and ba > 0) else 0.0

    @property
    def spread_bps(self) -> float:
        mid = self.mid_price
        if mid <= 0 or not self.bids or not self.asks:
            return 0.0
        return (self.best_ask - self.best_bid) / mid * 10000.0

    @property
    def bid_vol_5(self) -> float:
        return sum(qty for _, qty in self.bids)

    @property
    def ask_vol_5(self) -> float:
        return sum(qty for _, qty in self.asks)

    @property
    def depth_bid_usd(self) -> float:
        return sum(px * qty for px, qty in self.bids)

    @property
    def depth_ask_usd(self) -> float:
        return sum(px * qty for px, qty in self.asks)

    @property
    def obi(self) -> float:
        """
        Order Book Imbalance (OBI) for Top 5 levels:
        OBI = (BidVol - AskVol) / (BidVol + AskVol) in [-1.0, +1.0]
        """
        bv = self.bid_vol_5
        av = self.ask_vol_5
        total = bv + av
        if total <= 1e-9:
            return 0.0
        return (bv - av) / total


@dataclass(slots=True)
class OBIDislocationEvent:
    """Records an instantaneous divergence between Binance Lead and a Lagging Venue."""
    id: str
    ts: float
    symbol: str
    side: str                   # 'BUY' (lead surged up) or 'SELL' (lead plunged down)
    lead_exchange: str
    lead_mid: float
    lead_obi: float
    lag_exchange: str
    lag_mid: float
    lag_best_bid: float
    lag_best_ask: float
    lag_bps: float              # Basis points discrepancy
    maker_limit_price: float

    def to_dict(self) -> Dict[str, Any]:
        return {
            "event_id": self.id,
            "ts": self.ts,
            "symbol": self.symbol,
            "side": self.side,
            "lead_exchange": self.lead_exchange,
            "lead_mid": round(self.lead_mid, 6),
            "lead_obi": round(self.lead_obi, 3),
            "lag_exchange": self.lag_exchange,
            "lag_mid": round(self.lag_mid, 6),
            "lag_best_bid": round(self.lag_best_bid, 6),
            "lag_best_ask": round(self.lag_best_ask, 6),
            "lag_bps": round(self.lag_bps, 2),
            "maker_limit_price": round(self.maker_limit_price, 6),
        }


@dataclass(slots=True)
class LiquiditySnapshot10m:
    """10-minute rollup of market maker liquidity and depth profile."""
    ts: float
    symbol: str
    exchange: str
    mid_price: float
    spread_bps: float
    depth_bid_usd: float
    depth_ask_usd: float
    tick_count: int

    def to_dict(self) -> Dict[str, Any]:
        return {
            "ts": self.ts,
            "symbol": self.symbol,
            "exchange": self.exchange,
            "mid_price": round(self.mid_price, 6),
            "spread_bps": round(self.spread_bps, 2),
            "depth_bid_usd": round(self.depth_bid_usd, 2),
            "depth_ask_usd": round(self.depth_ask_usd, 2),
            "tick_count": self.tick_count,
        }


@dataclass(slots=True)
class PaperTrade:
    """Simulated post-only maker limit order tracking fill rate and PnL."""
    id: str
    symbol: str
    side: str                   # 'BUY' or 'SELL'
    exchange: str
    limit_price: float
    notional_usd: float
    entry_ts: float
    status: str = "PENDING"     # PENDING, FILLED, EXPIRED, CLOSED
    fill_price: float = 0.0
    fill_ts: float = 0.0
    exit_price: float = 0.0
    exit_ts: float = 0.0
    exit_reason: str = ""
    gross_pnl_bps: float = 0.0
    net_pnl_usd: float = 0.0

    def to_dict(self) -> Dict[str, Any]:
        return {
            "trade_id": self.id,
            "symbol": self.symbol,
            "side": self.side,
            "exchange": self.exchange,
            "limit_price": round(self.limit_price, 6),
            "notional_usd": self.notional_usd,
            "entry_ts": self.entry_ts,
            "status": self.status,
            "fill_price": round(self.fill_price, 6),
            "fill_ts": self.fill_ts,
            "exit_price": round(self.exit_price, 6),
            "exit_ts": self.exit_ts,
            "exit_reason": self.exit_reason,
            "gross_pnl_bps": round(self.gross_pnl_bps, 2),
            "net_pnl_usd": round(self.net_pnl_usd, 4),
        }


# =====================================================================
# ДОПОЛНИТЕЛЬНЫЕ АЛЬФА-ПОТОКИ (НОВЫЕ ИСТОЧНИКИ ДАННЫХ ДЛЯ КВАНТОВЫХ СТРАТЕГИЙ)
# =====================================================================

@dataclass(slots=True)
class LiquidationEvent:
    """
    Событие принудительной ликвидации (Margin Call Cascade).
    Служит 100% опережающим триггером: крупный снос на Binance за 500-1500 мс
    до того, как ведомые площадки успеют скорректировать котировки.
    """
    id: str
    symbol: str
    side: str                   # 'BUY' (ликвидация шорта -> рыночный бай) или 'SELL' (ликвидация лонга -> рыночный селл)
    price: float                # Цена ликвидации
    qty: float                  # Количество монет
    qty_usd: float              # Суммарный объем в USD
    exchange: str               # Источник (обычно 'binance')
    ts: float                   # Время события

    def to_dict(self) -> Dict[str, Any]:
        return {
            "liquidation_id": self.id,
            "symbol": self.symbol,
            "side": self.side,
            "price": round(self.price, 6),
            "qty": round(self.qty, 4),
            "qty_usd": round(self.qty_usd, 2),
            "exchange": self.exchange,
            "ts": self.ts,
        }


@dataclass(slots=True)
class CVDMetric:
    """
    Метрика кумулятивной дельты объемов (Cumulative Volume Delta) и агрессии рынка.
    Используется в VPIN / Toxic Flow для детекции скрытых рыночных покупок/продаж фондов.
    """
    symbol: str
    ts: float
    cvd_5s: float               # Дельта за 5 секунд (Buy Vol - Sell Vol)
    buy_vol_usd: float          # Объем рыночных покупок тейкером за 5с
    sell_vol_usd: float         # Объем рыночных продаж тейкером за 5с
    aggression_ratio: float     # Доля доминирующей стороны (от 0.0 до 1.0)
    is_toxic_flow: bool         # Флаг институционального сноса (напор > 80%)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "symbol": self.symbol,
            "ts": self.ts,
            "cvd_5s": round(self.cvd_5s, 2),
            "buy_vol_usd": round(self.buy_vol_usd, 2),
            "sell_vol_usd": round(self.sell_vol_usd, 2),
            "aggression_ratio": round(self.aggression_ratio, 3),
            "is_toxic_flow": self.is_toxic_flow,
        }


@dataclass(slots=True)
class FundingScheduleEvent:
    """
    Ставка финансирования и таймер отсечки (Countdown Timer).
    Используется в Стратегии 4 (Хищнический фандинг за 5 минут до часа Ч).
    """
    symbol: str
    exchange: str
    funding_rate: float         # Текущая 8-часовая ставка (например, 0.0001 = 0.01%)
    next_funding_ts: float      # Точное время следующей выплаты
    countdown_sec: float        # Секунд до часа Ч
    is_extreme: bool            # Флаг экстремального фандинга (|rate| >= 0.10% за 8ч)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "symbol": self.symbol,
            "exchange": self.exchange,
            "funding_rate": round(self.funding_rate, 6),
            "funding_rate_pct": round(self.funding_rate * 100.0, 4),
            "next_funding_ts": self.next_funding_ts,
            "countdown_sec": round(self.countdown_sec, 0),
            "is_extreme": self.is_extreme,
        }


# =====================================================================
# НОВЫЕ АНАЛИТИЧЕСКИЕ ПРОФАЙЛЕРЫ (ЛЕКЦИЯ АНТОНА: ТАЗИКИ, ТАЙМ-БОТЫ, ЁРШИКИ)
# =====================================================================

@dataclass(slots=True)
class LiquidityVoidMetric:
    """
    Метрика пустоты стакана (Liquidity Void) для стратегии 'Ловля прострелов' ('Тазики').
    Фиксирует дыры в ликвидности: если для сдвига цены на 2% требуется меньше $300-$500,
    выставляются пассивные сетки лимитных ордеров ('тазики') на отскок.
    """
    symbol: str
    exchange: str
    ts: float
    mid_price: float
    depth_bid_usd: float        # Суммарная ликвидность топ-5 бидов
    depth_ask_usd: float        # Суммарная ликвидность топ-5 асков
    cost_to_move_2pct_bid: float # USD для сноса стакана вниз на -2%
    cost_to_move_2pct_ask: float # USD для выноса стакана вверх на +2%
    is_void: bool               # True если стакан дырявый (< $500 на 2%)
    trough_bid_limit: float     # Рекомендуемый уровень 'тазика' в Long (-4%)
    trough_ask_limit: float     # Рекомендуемый уровень 'тазика' в Short (+4%)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "symbol": self.symbol,
            "exchange": self.exchange,
            "ts": self.ts,
            "mid_price": round(self.mid_price, 6),
            "depth_bid_usd": round(self.depth_bid_usd, 2),
            "depth_ask_usd": round(self.depth_ask_usd, 2),
            "cost_to_move_2pct_bid": round(self.cost_to_move_2pct_bid, 2),
            "cost_to_move_2pct_ask": round(self.cost_to_move_2pct_ask, 2),
            "is_void": self.is_void,
            "trough_bid_limit": round(self.trough_bid_limit, 6),
            "trough_ask_limit": round(self.trough_ask_limit, 6),
        }


@dataclass(slots=True)
class TimeAnomalyMetric:
    """
    Метрика циклических тайм-ботов (Minute-of-Hour Scheduled Bots).
    Детектирует роботов, исполняющих ордера в строго определенные минуты часа (:17, :34, :43, :54).
    Позволяет заранее выставить лимитные ордера за 5-10 секунд до минуты срабатывания бота.
    """
    symbol: str
    minute_of_hour: int         # 0..59 (минута часа)
    occurrences: int            # Сколько раз зафиксирован всплеск на этой минуте
    avg_dislocation_bps: float  # Средняя амплитуда импульса в базисных пунктах
    dominant_side: str          # 'BUY', 'SELL' или 'BOTH'
    confidence_score: float     # Достоверность от 0.0 до 1.0 (на основе повторяемости по часам)
    last_seen_ts: float

    def to_dict(self) -> Dict[str, Any]:
        return {
            "symbol": self.symbol,
            "minute_of_hour": self.minute_of_hour,
            "occurrences": self.occurrences,
            "avg_dislocation_bps": round(self.avg_dislocation_bps, 1),
            "dominant_side": self.dominant_side,
            "confidence_score": round(self.confidence_score, 2),
            "last_seen_ts": self.last_seen_ts,
        }


@dataclass(slots=True)
class ScrubberCycleMetric:
    """
    Метрика алгоритмических колебаний 'Ёршик' (Scrubber / Alternating MM Loop).
    Фиксирует тупого маркет-мейкера, который циклически гоняет цену:
    например, 2 минуты агрессивно продает, затем 2 минуты агрессивно покупает.
    """
    symbol: str
    exchange: str
    cycle_period_sec: float     # Примерный период полного цикла (сек)
    amplitude_bps: float        # Размах колебаний в bps
    current_phase: str          # 'BUY_PHASE' или 'SELL_PHASE'
    flips_count: int            # Количество подтвержденных смен направления
    last_flip_ts: float

    def to_dict(self) -> Dict[str, Any]:
        return {
            "symbol": self.symbol,
            "exchange": self.exchange,
            "cycle_period_sec": round(self.cycle_period_sec, 1),
            "amplitude_bps": round(self.amplitude_bps, 1),
            "current_phase": self.current_phase,
            "flips_count": self.flips_count,
            "last_flip_ts": self.last_flip_ts,
        }


@dataclass(slots=True)
class TradFiArbitrageMetric:
    """
    Метрика арбитража фандинга и базиса TradFi/CME фьючерсов и акций США (UTex <-> Binance/Bitget).
    Учитывает асимметрию овернайта:
    - Для сырьевых фьючерсов CME (GASM, CL) овернайт платится со всего ноционала (0.0194%/день).
    - Для акций США (NOK, AXTI) при торговле 1:1 без плеча овернайт = 0.00%.
    Также отслеживает раздвижку спреда (Basis Scalping между отсечками фандинга).
    """
    symbol: str
    crypto_symbol: str
    utex_symbol: str
    asset_class: str            # 'COMMODITY_FUTURES' или 'US_EQUITY'
    funding_rate: float         # Ставка за 1 интервал (например 0.0020 = +0.20%)
    funding_interval_hours: int # 4 часа (6 раз/сутки для газа) или 8 часов (3 раза)
    daily_funding_pct: float    # Суммарный фандинг за сутки (%)
    annualized_funding_pct: float
    overnight_daily_pct: float  # Суточный овернайт UTEX (%)
    net_daily_yield_pct: float  # Чистая суточная доходность: daily_funding_pct - overnight_daily_pct (%)
    basis_bps: float            # Относительная раздвижка спреда между перпом и UTex в bps
    recommended_action: str     # 'SHORT_CRYPTO_LONG_UTEX', 'LONG_CRYPTO_SHORT_UTEX', 'SCALP_BASIS', 'EXIT_COLLAPSED'
    status: str                 # 'ACTIVE_ARBITRAGE', 'SCALP_OPPORTUNITY', 'COLLAPSED_EXIT', 'NORMAL'
    ts: float

    def to_dict(self) -> Dict[str, Any]:
        return {
            "symbol": self.symbol,
            "crypto_symbol": self.crypto_symbol,
            "utex_symbol": self.utex_symbol,
            "asset_class": self.asset_class,
            "funding_rate": round(self.funding_rate, 6),
            "funding_rate_pct": round(self.funding_rate * 100.0, 4),
            "funding_interval_hours": self.funding_interval_hours,
            "daily_funding_pct": round(self.daily_funding_pct, 4),
            "annualized_funding_pct": round(self.annualized_funding_pct, 1),
            "overnight_daily_pct": round(self.overnight_daily_pct, 4),
            "net_daily_yield_pct": round(self.net_daily_yield_pct, 4),
            "basis_bps": round(self.basis_bps, 1),
            "recommended_action": self.recommended_action,
            "status": self.status,
            "ts": self.ts,
        }



