"""
Global Configuration for OBI Lead-Lag Collector.
Optimized for Render Free Tier (RAM < 80MB) and MongoDB Atlas M0 (512MB storage).
"""

import os
from typing import List
from dotenv import load_dotenv

load_dotenv()

# MongoDB Configuration
MONGO_DB_URL: str = os.getenv(
    "MONGO_DB_URL",
    ""
)
MONGO_DB_NAME: str = os.getenv("MONGO_DB_NAME", "obi_lead_lag")
FLUSH_INTERVAL_SEC: int = int(os.getenv("FLUSH_INTERVAL_SEC", "60"))  # 1 minute flush to local SQLite & Mongo

# Web Service Port (Render sets this dynamically)
PORT: int = int(os.getenv("PORT", "8080"))
ENVIRONMENT: str = os.getenv("ENVIRONMENT", "production")
LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO")

# Arbitrage & Lead-Lag Parameters
LEAD_EXCHANGE: str = os.getenv("LEAD_EXCHANGE", "binance").lower()
LAG_EXCHANGES: List[str] = [
    e.strip().lower()
    for e in os.getenv(
        "LAG_EXCHANGES",
        "asterdex,hyperliquid,dydx,bybit,okx,bitget,mexc,gateio,bingx"
    ).split(",")
    if e.strip()
]

# Detection Thresholds for Altcoins (Weak MM profile)
# Mega-caps (BTC, ETH, SOL) are excluded by design to target slow MMs
OBI_THRESHOLD: float = float(os.getenv("OBI_THRESHOLD", "0.50"))        # 75/25 book ratio
MIN_LEAD_LAG_BPS: float = float(os.getenv("MIN_LEAD_LAG_BPS", "15.0"))  # 0.15% lag threshold
ORDER_TIMEOUT_SEC: float = float(os.getenv("ORDER_TIMEOUT_SEC", "15.0"))
MAX_HOLD_SEC: float = float(os.getenv("MAX_HOLD_SEC", "45.0"))
TP_BPS: float = float(os.getenv("TP_BPS", "25.0"))                      # +0.25% TP for alts
SL_BPS: float = float(os.getenv("SL_BPS", "18.0"))                      # -0.18% SL for alts

# Target Universe of Volatile Altcoins with Cross-Listing & Weak MMs
DEFAULT_ALTS: List[str] = [
    "0G", "1000BONK", "1INCH", "2Z", "A", "AAVE", "ACE", "ACH", "ACT", "ADA",
    "AERO", "AGLD", "AKE", "AKT", "ALCH", "ALGO", "ALICE", "ALLO", "ANTHROPIC", "APE",
    "APR", "APT", "AR", "ARB", "ARK", "ARKM", "ARX", "ASTER", "ATH", "ATOM",
    "AUCTION", "AVA", "AVAX", "AVNT", "AXS", "B2", "BANK", "BB", "BCH", "BEAT",
    "BERA", "BICO", "BILL", "BIO", "BLESS", "BNB", "BOME", "BR", "BROCCOLI", "BSB",
    "BSV", "BTW", "CAKE", "CAP", "CASHCAT", "CC", "CFG", "CFX", "CHIP", "CHR",
    "CHZ", "CNPY", "COAI", "COMP", "CORE", "COTI", "CP", "CRO", "CROSS", "CRV",
    "CVC", "CYS", "DASH", "DATA", "DEEP", "DGAI", "DOGE", "DOT", "DRAM", "DYDX",
    "DYM", "EDGE", "EGLD", "EIGEN", "ENA", "ENS", "ENSO", "ETC", "ETHFI", "EVAA",
    "EWY", "F", "FARTCOIN", "FET", "FF", "FIL", "FLOCK", "FLOKI", "FLOW", "FOLKS",
    "FORM", "G", "GALA", "GAS", "GIGGLE", "GMT", "GMX", "GRAM", "GRASS", "GRT",
    "GRVT", "H", "HBAR", "HUMA", "ICP", "ID", "IMX", "INJ", "INTW", "IO",
    "IOST", "IOTA", "JASMY", "JST", "JTO", "JUP", "KAITO", "KAS", "KERNEL", "KITE",
    "KMNO", "KORU", "KSM", "LAB", "LDO", "LINEA", "LIT", "LPT", "LSK", "LUNA",
    "LUNC", "MAGIC", "MANA", "MANTRA", "MARSCOIN", "ME", "MELANIA", "MERL", "MET", "METIS",
    "MINA", "MOODENG", "MORPHO", "MOVR", "MSTU", "MUBARAK", "MUU", "MVLL", "MYX", "NEO",
    "NIGHT", "NIL", "NMR", "OKB", "ON", "ONE", "ONG", "OP", "OPENAI", "ORCA",
    "ORDI", "PAXG", "PENDLE", "PENGU", "PHA", "PI", "PIEVERSE", "PIPPIN", "PIXEL", "PLUME",
    "POL", "POLYX", "PONS", "PROM", "PROVE", "PUMP", "PYTH", "QNT", "RAVE", "RAY",
    "RE", "RENDER", "REZ", "RIVER", "ROSE", "RUNE", "S", "SAND", "SEI", "SENT",
    "SHIB", "SIREN", "SKR", "SKY", "SKYAI", "SNX", "SNXX", "SOXS", "SPK", "SPX",
    "SPY", "SQQQ", "SSV", "STEEM", "STONK", "STRK", "STX", "SUSHI", "SYN", "SYRUP",
    "TIA", "TQQQ", "TRB", "TRIA", "TRUMP", "TRX", "TUT", "UAI", "UB", "UMA",
    "UNI", "US", "USELESS", "VANA", "VELVET", "VET", "VIRTUAL", "VTHO", "VVV", "W",
    "WAXP", "WIF", "WLD", "WLFI", "XAI", "XAUT", "XLM", "XMR", "XPD", "XPL",
    "XPT", "XTZ", "YFI", "YGG", "ZAMA", "ZBT", "ZEN", "ZETA", "ZK", "ZRO"
]

RAW_SYMBOLS_ENV = os.getenv("SYMBOLS", "")
if RAW_SYMBOLS_ENV.strip():
    SYMBOLS = [s.strip().upper() for s in RAW_SYMBOLS_ENV.split(",") if s.strip()]
else:
    SYMBOLS = DEFAULT_ALTS

# =====================================================================
# TRADFI & CME FUTURES VS CRYPTO ARBITRAGE (UTEX <-> BINANCE / BITGET)
# =====================================================================
TRADFI_COMMODITIES: List[str] = ["NATGAS", "CL", "BZ", "XAU", "XAG"]
TRADFI_EQUITIES: List[str] = ["NOK", "AXTI", "MSTR", "COIN", "TSLA"]
TRADFI_SYMBOLS: List[str] = TRADFI_COMMODITIES + TRADFI_EQUITIES

# Metadata per instrument: (UTex ticker, asset class, funding interval hours, overnight fee daily pct, cap pct)
TRADFI_SPECS = {
    "NATGAS": {
        "utex_ticker": "GASM",
        "crypto_ticker": "NATGASUSDT",
        "asset_class": "COMMODITY_FUTURES",
        "interval_hours": 4,      # 6 payouts per day on Binance
        "overnight_daily_pct": 0.0194, # ~7% annualized on FULL notional
        "funding_cap_pct": 0.50,
        "lot_size": 1000,         # 1000 MMBtu for micro
        "min_profitable_funding_pct": 0.010, # 0.01% per 4h interval
    },
    "CL": {
        "utex_ticker": "CL",
        "crypto_ticker": "CLUSDT",
        "asset_class": "COMMODITY_FUTURES",
        "interval_hours": 8,
        "overnight_daily_pct": 0.0194,
        "funding_cap_pct": 0.50,
        "lot_size": 1000,
        "min_profitable_funding_pct": 0.015,
    },
    "BZ": {
        "utex_ticker": "BZ",
        "crypto_ticker": "BZUSDT",
        "asset_class": "COMMODITY_FUTURES",
        "interval_hours": 8,
        "overnight_daily_pct": 0.0194,
        "funding_cap_pct": 0.50,
        "lot_size": 1000,
        "min_profitable_funding_pct": 0.015,
    },
    "XAU": {
        "utex_ticker": "XAU",
        "crypto_ticker": "XAUUSDT",
        "asset_class": "COMMODITY_FUTURES",
        "interval_hours": 8,
        "overnight_daily_pct": 0.0194,
        "funding_cap_pct": 0.50,
        "lot_size": 100,
        "min_profitable_funding_pct": 0.010,
    },
    "XAG": {
        "utex_ticker": "XAG",
        "crypto_ticker": "XAGUSDT",
        "asset_class": "COMMODITY_FUTURES",
        "interval_hours": 8,
        "overnight_daily_pct": 0.0194,
        "funding_cap_pct": 0.50,
        "lot_size": 5000,
        "min_profitable_funding_pct": 0.010,
    },
    "NOK": {
        "utex_ticker": "NOK",
        "crypto_ticker": "NOKUSDT",
        "asset_class": "US_EQUITY",
        "interval_hours": 8,
        "overnight_daily_pct": 0.0,    # 0.0% at 1:1 unleveraged!
        "funding_cap_pct": 2.00,
        "min_profitable_funding_pct": 0.020,
    },
    "AXTI": {
        "utex_ticker": "AXTI",
        "crypto_ticker": "AXTIUSDT",
        "asset_class": "US_EQUITY",
        "interval_hours": 8,
        "overnight_daily_pct": 0.0,
        "funding_cap_pct": 2.00,
        "min_profitable_funding_pct": 0.020,
    },
    "MSTR": {
        "utex_ticker": "MSTR",
        "crypto_ticker": "MSTRUSDT",
        "asset_class": "US_EQUITY",
        "interval_hours": 8,
        "overnight_daily_pct": 0.0,
        "funding_cap_pct": 2.00,
        "min_profitable_funding_pct": 0.020,
    },
    "COIN": {
        "utex_ticker": "COIN",
        "crypto_ticker": "COINUSDT",
        "asset_class": "US_EQUITY",
        "interval_hours": 8,
        "overnight_daily_pct": 0.0,
        "funding_cap_pct": 2.00,
        "min_profitable_funding_pct": 0.020,
    },
    "TSLA": {
        "utex_ticker": "TSLA",
        "crypto_ticker": "TSLAUSDT",
        "asset_class": "US_EQUITY",
        "interval_hours": 8,
        "overnight_daily_pct": 0.0,
        "funding_cap_pct": 2.00,
        "min_profitable_funding_pct": 0.020,
    },
}

# Symbols tracked by BinanceMarkPriceFundingStream (alts + TradFi perps)
FUNDING_TRACKED_SYMBOLS: List[str] = list(
    set(SYMBOLS + [f"{s}USDT" for s in SYMBOLS] + [s["crypto_ticker"] for s in TRADFI_SPECS.values()])
)

