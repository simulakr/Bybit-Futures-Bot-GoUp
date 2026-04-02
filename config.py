import os
from dotenv import load_dotenv

load_dotenv()

# ─── Binance API ──────────────────────────────────────────────────────────────
BINANCE_API_KEY    = os.getenv("BINANCE_API_KEY")
BINANCE_API_SECRET = os.getenv("BINANCE_API_SECRET")
TESTNET            = os.getenv("TESTNET", "false").lower() == "true"

# ─── Semboller & Zaman Aralığı ────────────────────────────────────────────────
SYMBOLS  = [ "ETHUSDT", "SOLUSDT", "XRPUSDT", "DOGEUSDT"] #"BTCUSDT","BNBUSDT"
INTERVAL = "15m"

# ─── Percent ATR Filtreleri ───────────────────────────────────────────────────
ATR_RANGES = {
    "BTCUSDT":  (0.173, 0.645),
    "ETHUSDT":  (0.363, 0.990),
    "SOLUSDT":  (0.423, 1.176),
    "XRPUSDT":  (0.363, 1.378),
    "DOGEUSDT": (0.465, 1.306),
}

# ─── Z Göstergesi: atr.quantile(0.25 - 0.55) ────────────────────────────────────────────────────────────
Z_RANGES = {
    'BTCUSDT': (0.181, 0.272),
    'ETHUSDT': (0.325, 0.466),
    'SOLUSDT': (0.399, 0.555),
    'DOGEUSDT': (0.432, 0.621),
    'XRPUSDT': (0.341, 0.525),
}

Z_INDICATOR_PARAMS = {
    "atr_period":     14,
    "atr_multiplier": 1,
}

# ─── Pozisyon Büyüklüğü ───────────────────────────────────────────────────────
QUANTITY_PRECISION = {
    "BTCUSDT":  3,
    "ETHUSDT":  2,
    "SOLUSDT":  1,
    "XRPUSDT":  0,
    "DOGEUSDT": 0,
}

PRICE_PRECISION = {
    "BTCUSDT":  2,
    "ETHUSDT":  2,
    "SOLUSDT":  3,
    "XRPUSDT":  4,
    "DOGEUSDT": 5,
}


# ─── Risk & Kaldıraç ─────────────────────────────────────────────────────────

DEFAULT_LEVERAGE  = 25
DEFAULT_RISK_USDT = 20

SYMBOL_SETTINGS = {
    "BTCUSDT":  {"risk": 20, "leverage": 25},
    "ETHUSDT":  {"risk": 20, "leverage": 25},
    "SOLUSDT":  {"risk": 20, "leverage": 25},
    "XRPUSDT":  {"risk": 20, "leverage": 25},
    "DOGEUSDT": {"risk": 20, "leverage": 25},
}

# ─── TP / SL Çarpanları ───────────────────────────────────────────────────────
TP_ATR_MULTIPLIER = 2.5
SL_ATR_MULTIPLIER = 2.5
