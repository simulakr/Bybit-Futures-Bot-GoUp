import numpy as np
import pandas as pd
import warnings

from config import ATR_RANGES, Z_RANGES, Z_INDICATOR_PARAMS

warnings.filterwarnings("ignore", category=FutureWarning)


# ─── Temel Göstergeler ────────────────────────────────────────────────────────

def calculate_atr(df: pd.DataFrame, window: int = 14) -> pd.Series:
    high  = df["high"]
    low   = df["low"]
    close = df["close"]
    prev  = close.shift(1)
    tr    = pd.concat([high - low, (high - prev).abs(), (low - prev).abs()], axis=1).max(axis=1)
    return tr.ewm(alpha=1 / window, adjust=False).mean()

# ─── Z Göstergesi ─────────────────────────────────────────────────────────────

def calculate_z(df: pd.DataFrame, symbol: str) -> pd.Series:
    if symbol not in Z_RANGES:
        raise ValueError(f"Z_RANGES içinde {symbol} tanımlı değil.")
    pct_min, pct_max = Z_RANGES[symbol]
    mult = Z_INDICATOR_PARAMS["atr_multiplier"]
    return np.minimum(
        np.maximum(df["close"] * pct_min / 100, mult * df["atr"]),
        df["close"] * pct_max / 100,
    )


# ─── ATR ZigZag ───────────────────────────────────────────────────────────────

def calculate_atr_zigzag(df: pd.DataFrame, atr_col: str = "atr", atr_mult: float = 1.0, suffix: str = "") -> pd.DataFrame:
    """
    ATR tabanlı zigzag pivot noktaları hesaplar.
    suffix ile aynı DataFrame üzerinde birden fazla kez çalıştırılabilir.
    """
    closes = df["close"].values
    atrs   = df[atr_col].values
    n      = len(df)

    high_pivot     = [None] * n
    low_pivot      = [None] * n
    high_pivot_atr = [None] * n
    low_pivot_atr  = [None] * n
    high_confirmed = [0] * n
    low_confirmed  = [0] * n
    pivot_bars_ago = [None] * n

    last_price     = closes[0]
    last_pivot_idx = 0
    direction      = None

    for i in range(1, n):
        price = closes[i]
        atr   = atrs[i] * atr_mult

        if direction is None:
            if price >= last_price + atr:
                direction = "up"
                high_pivot[last_pivot_idx]     = closes[last_pivot_idx]
                high_pivot_atr[last_pivot_idx] = atrs[last_pivot_idx]
            elif price <= last_price - atr:
                direction = "down"
                low_pivot[last_pivot_idx]     = closes[last_pivot_idx]
                low_pivot_atr[last_pivot_idx] = atrs[last_pivot_idx]

        elif direction == "up":
            if price <= last_price - atr:
                high_pivot[last_pivot_idx]     = last_price
                high_pivot_atr[last_pivot_idx] = atrs[last_pivot_idx]
                high_confirmed[i] = 1
                pivot_bars_ago[i] = i - last_pivot_idx
                direction      = "down"
                last_price     = price
                last_pivot_idx = i
            elif price > last_price:
                last_price     = price
                last_pivot_idx = i

        elif direction == "down":
            if price >= last_price + atr:
                low_pivot[last_pivot_idx]     = last_price
                low_pivot_atr[last_pivot_idx] = atrs[last_pivot_idx]
                low_confirmed[i] = 1
                pivot_bars_ago[i] = i - last_pivot_idx
                direction      = "up"
                last_price     = price
                last_pivot_idx = i
            elif price < last_price:
                last_price     = price
                last_pivot_idx = i

    s = suffix
    df[f"high_pivot{s}"]     = high_pivot
    df[f"low_pivot{s}"]      = low_pivot
    df[f"high_pivot_atr{s}"] = high_pivot_atr
    df[f"low_pivot_atr{s}"]  = low_pivot_atr
    df[f"high_confirmed{s}"] = high_confirmed
    df[f"low_confirmed{s}"]  = low_confirmed
    df[f"pivot_bars_ago{s}"] = pivot_bars_ago

    # Forward fill
    df[f"high_pivot_ff{s}"]     = df[f"high_pivot{s}"].ffill()
    df[f"low_pivot_ff{s}"]      = df[f"low_pivot{s}"].ffill()
    df[f"high_pivot_atr_ff{s}"] = df[f"high_pivot_atr{s}"].ffill()
    df[f"low_pivot_atr_ff{s}"]  = df[f"low_pivot_atr{s}"].ffill()

    for base, col in [("high_confirmed", f"high_confirmed{s}"), ("low_confirmed", f"low_confirmed{s}")]:
        tmp = df[col].replace(0, np.nan).ffill()
        df[f"{col}_ff"] = tmp.fillna(0).astype(int)

    # Pivot bars ago forward fill (sürekli artan)
    filled = []
    last_val = last_idx = None
    for i, v in enumerate(pivot_bars_ago):
        if v is not None:
            last_val, last_idx = v, i
            filled.append(v)
        elif last_val is not None:
            filled.append(last_val + (i - last_idx))
        else:
            filled.append(None)
    df[f"pivot_bars_ago_ff{s}"] = filled

    return df


# ─── Market Yapısı (HH/HL/LH/LL) ─────────────────────────────────────────────

def add_market_structure(df: pd.DataFrame, suffix: str) -> pd.DataFrame:
    s = suffix
    high_ff = f"high_pivot_ff{s}"
    low_ff  = f"low_pivot_ff{s}"

    df[f"high_structure{s}"] = np.nan
    df.loc[df[high_ff] < df[high_ff].shift(1), f"high_structure{s}"] = "LH"
    df.loc[df[high_ff] > df[high_ff].shift(1), f"high_structure{s}"] = "HH"
    df[f"high_structure{s}"] = df[f"high_structure{s}"].ffill().fillna("HH")

    df[f"low_structure{s}"] = np.nan
    df.loc[df[low_ff] < df[low_ff].shift(1), f"low_structure{s}"] = "LL"
    df.loc[df[low_ff] > df[low_ff].shift(1), f"low_structure{s}"] = "HL"
    df[f"low_structure{s}"] = df[f"low_structure{s}"].ffill().fillna("LL")

    return df


# ─── Ana Hesaplama ────────────────────────────────────────────────────────────

def calculate_indicators(df: pd.DataFrame, symbol: str) -> pd.DataFrame:
    atr_low, atr_high = ATR_RANGES[symbol]

    # Temel göstergeler
    df["atr"]     = calculate_atr(df)
    df["pct_atr"] = (df["atr"] / df["close"]) * 100

    df["z"]     = calculate_z(df, symbol)
    df["pct_z"] = (df["z"] / df["close"]) * 100


  
    # ATR ZigZag (2x ve 3x)
    df = calculate_atr_zigzag(df, atr_col="z", atr_mult=1.25, suffix="_2x")
    # df = calculate_atr_zigzag(df, atr_col="z", atr_mult=3.0, suffix="_3x")

    # Market Yapısı
    df = add_market_structure(df, "_2x")
    # df = add_market_structure(df, "_3x")

    # ATR filtre maskesi
    atr_ok = (atr_low < df["pct_atr"]) & (df["pct_atr"] < atr_high)

    # Breakout Condition
    long_break_condition = (df['high_pivot_filled_2x'] + 0.1*df['z'])
    short_break_condition = (df['low_pivot_filled_2x'] - 0.1*df['z']) 

    # ─── Breakout / Breakdown Sinyalleri ──────────────────────────────────────

    df["pivot_go_breakout_2x"]  = False
    df["pivot_go_breakdown_2x"] = False

    df.loc[
        df["low_confirmed_2x"].astype(bool) &
        (df["low_structure_2x"]  == "HL") &
        (df["high_structure_2x"] != "HH") &
        df["high_pivot_ff_2x"].notna() &
        (df["close"] > long_break_condition) & atr_ok,
        "pivot_go_breakout_2x",
    ] = True

    df.loc[
        df["high_confirmed_2x"].astype(bool) &
        (df["high_structure_2x"] == "LH") &
        (df["low_structure_2x"]  != "LL") &
        df["low_pivot_ff_2x"].notna() &
        (df["close"] < short_break_condition) & atr_ok,
        "pivot_go_breakdown_2x",
    ] = True

    # ─── Shift koşulu (ortak) ────────────────────────────────────────────────

    long_conditions = [
        (df["high_pivot_ff_2x"].notna() & (df["close"].shift(i) < long_break_condition)).fillna(False)
        for i in range(1, 6)
    ]
    long_shift_ok = pd.concat(long_conditions, axis=1).all(axis=1)

    short_conditions = [
        (df["low_pivot_ff_2x"].notna() & (df["close"].shift(i) > short_break_condition)).fillna(False)
        for i in range(1, 6)
    ]
    short_shift_ok = pd.concat(short_conditions, axis=1).all(axis=1)

    # ─── pivot_go_breakout_2x: high_structure != HH (erken geçiş) ────────────

    secondary_long = (
        (df["low_structure_2x"]  == "HL") &
        long_shift_ok &
        (df["high_structure_2x"] != "HH") &
        df["high_pivot_ff_2x"].notna() &
        (df["close"] > long_break_condition) &
        atr_ok &
        (~df["pivot_go_breakout_2x"])
    )

    secondary_short = (
        (df["low_structure_2x"]  != "LL") &
        short_shift_ok &
        (df["high_structure_2x"] == "LH") &
        df["low_pivot_ff_2x"].notna() &
        (df["close"] < short_break_condition) &
        atr_ok &
        (~df["pivot_go_breakdown_2x"])
    )

    df.loc[secondary_long,  "pivot_go_breakout_2x"]  = True
    df.loc[secondary_short, "pivot_go_breakdown_2x"] = True

    # ─── pivot_goup_breakout_2x: high_structure == HH (yapı teyitli) ─────────
    # pivot_go_breakout_2x'ten BAĞIMSIZ ayrı bir sinyal sütunu

    df["pivot_goup_breakout_2x"]  = False
    df["pivot_goup_breakdown_2x"] = False

    df.loc[
        df["low_confirmed_2x"].astype(bool) &
        (df["low_structure_2x"]  == "HL") &
        (df["high_structure_2x"] == "HH") &
        df["high_pivot_ff_2x"].notna() &
        (df["close"] > long_break_condition) & atr_ok,
        "pivot_goup_breakout_2x",
    ] = True

    df.loc[
        df["high_confirmed_2x"].astype(bool) &
        (df["high_structure_2x"] == "LH") &
        (df["low_structure_2x"]  == "LL") &
        df["low_pivot_ff_2x"].notna() &
        (df["close"] < short_break_condition) & atr_ok,
        "pivot_goup_breakdown_2x",
    ] = True

    goup_long = (
        (df["low_structure_2x"]  == "HL") &
        long_shift_ok &
        (df["high_structure_2x"] == "HH") &
        df["high_pivot_ff_2x"].notna() &
        (df["close"] > long_break_condition) &
        atr_ok &
        (~df["pivot_goup_breakout_2x"])
    )

    goup_short = (
        (df["low_structure_2x"]  == "LL") &
        short_shift_ok &
        (df["high_structure_2x"] == "LH") &
        df["low_pivot_ff_2x"].notna() &
        (df["close"] < short_break_condition) &
        atr_ok &
        (~df["pivot_goup_breakdown_2x"])
    )

    df.loc[goup_long,  "pivot_goup_breakout_2x"]  = True
    df.loc[goup_short, "pivot_goup_breakdown_2x"] = True

    return df
