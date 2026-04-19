import numpy as np
import pandas as pd
from config import ATR_RANGES, Z_RANGES, Z_INDICATOR_PARAMS

# --- Temel Hesaplamalar ---

def calculate_atr(df: pd.DataFrame, window: int = 14) -> pd.Series:
    tr = pd.concat([
        df["high"] - df["low"], 
        (df["high"] - df["close"].shift(1)).abs(), 
        (df["low"] - df["close"].shift(1)).abs()
    ], axis=1).max(axis=1)
    return tr.ewm(alpha=1 / window, adjust=False).mean()

def calculate_z(df: pd.DataFrame, symbol: str) -> pd.Series:
    pct_min, pct_max = Z_RANGES[symbol]
    mult = Z_INDICATOR_PARAMS["atr_multiplier"]
    return np.minimum(
        np.maximum(df["close"] * pct_min / 100, mult * df["atr"]),
        df["close"] * pct_max / 100,
    )

# --- Sade ZigZag ve Yapı ---

def calculate_structure_2x(df: pd.DataFrame, atr_mult: float = 1.25):
    closes, atrs = df["close"].values, df["z"].values
    n = len(df)
    
    high_pivot, low_pivot = [None] * n, [None] * n
    high_confirmed, low_confirmed = [0] * n, [0] * n
    last_price, last_pivot_idx, direction = closes[0], 0, None

    # ZigZag Döngüsü
    for i in range(1, n):
        price, atr = closes[i], atrs[i] * atr_mult
        if direction is None:
            if price >= last_price + atr: direction = "up"
            elif price <= last_price - atr: direction = "down"
        elif direction == "up":
            if price <= last_price - atr:
                high_pivot[last_pivot_idx], high_confirmed[i] = last_price, 1
                direction, last_price, last_pivot_idx = "down", price, i
            elif price > last_price: last_price, last_pivot_idx = price, i
        elif direction == "down":
            if price >= last_price + atr:
                low_pivot[last_pivot_idx], low_confirmed[i] = last_price, 1
                direction, last_price, last_pivot_idx = "up", price, i
            elif price < last_price: last_price, last_pivot_idx = price, i

    # Sütunları DataFrame'e ekle
    hff = pd.Series(high_pivot).ffill()
    lff = pd.Series(low_pivot).ffill()
    
    # HH/HL ve LH/LL tespiti
    df["hs"] = np.where(hff > hff.shift(1), "HH", "LH")
    df["ls"] = np.where(lff > lff.shift(1), "HL", "LL")
    
    return hff, lff, high_confirmed, low_confirmed

# --- Ana Fonksiyon ---

def calculate_indicators(df: pd.DataFrame, symbol: str) -> pd.DataFrame:
    atr_low, atr_high = ATR_RANGES[symbol]
    
    # 1. Göstergeler
    df["atr"] = calculate_atr(df)
    df["z"] = calculate_z(df, symbol)
    
    # 2. ZigZag ve Market Yapısı
    hff, lff, hcon, lcon = calculate_structure_2x(df)
    
    # 3. Filtreler
    pct_atr = (df["atr"] / df["close"]) * 100
    atr_ok = (pct_atr > atr_low) & (pct_atr < atr_high)

    # 4. pivot_goup_breakout_2x Hesaplama
    # Koşul: Low Teyit + HH + HL + Fiyat > High Pivot
    df["pivot_goup_breakout_2x"] = (
        (lcon == 1) & 
        (df["ls"] == "HL") & 
        (df["hs"] == "HH") & 
        (df["close"] > hff) & 
        atr_ok
    )

    # 5. pivot_goup_breakdown_2x Hesaplama
    # Koşul: High Teyit + LH + LL + Fiyat < Low Pivot
    df["pivot_goup_breakdown_2x"] = (
        (hcon == 1) & 
        (df["hs"] == "LH") & 
        (df["ls"] == "LL") & 
        (df["close"] < lff) & 
        atr_ok
    )

    return df
