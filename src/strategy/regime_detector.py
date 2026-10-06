"""
src/strategy/regime_detector.py

Market Regime Detector (multi-indikator).
SENGAJA bukan src/strategy/regime.py -- file itu sudah ada dan dipakai
donchian_breakout.py (MIN_ENTRY_EFFICIENCY_RATIO, efficiency_ratio).

Output per candle: TREND_UP / TREND_DOWN / SIDEWAYS / TRANSITION

Tiga indikator "voting":
  - ADX              : kekuatan trend (tinggi = trending)
  - Choppiness Index : seberapa acak/choppy (tinggi = sideways)
  - Efficiency Ratio : |net move| / total jarak tempuh (tinggi = gerak searah)
Arah trend: +DI vs -DI dan posisi close terhadap EMA.
Hysteresis (confirm_bars) mencegah regime gonta-ganti tiap candle.
"""
from dataclasses import dataclass

import numpy as np
import pandas as pd


@dataclass
class RegimeConfig:
    adx_len: int = 14
    chop_len: int = 14
    er_len: int = 20
    ema_len: int = 50
    # Ambang batas -- kalibrasi ulang untuk instrumen & timeframe kamu
    adx_trend: float = 25.0
    adx_range: float = 20.0
    chop_trend: float = 38.2
    chop_range: float = 61.8
    er_trend: float = 0.30
    er_range: float = 0.15
    min_votes: int = 2      # minimal indikator yang setuju (dari 3)
    confirm_bars: int = 3   # regime baru harus bertahan N candle dulu


def _rma(s: pd.Series, n: int) -> pd.Series:
    """Wilder's smoothing."""
    return s.ewm(alpha=1 / n, adjust=False).mean()


def true_range(df: pd.DataFrame) -> pd.Series:
    prev_close = df["close"].shift(1)
    return pd.concat(
        [df["high"] - df["low"],
         (df["high"] - prev_close).abs(),
         (df["low"] - prev_close).abs()],
        axis=1,
    ).max(axis=1)


def adx(df: pd.DataFrame, n: int):
    up = df["high"].diff()
    down = -df["low"].diff()
    plus_dm = pd.Series(np.where((up > down) & (up > 0), up, 0.0), index=df.index)
    minus_dm = pd.Series(np.where((down > up) & (down > 0), down, 0.0), index=df.index)
    atr = _rma(true_range(df), n).replace(0, np.nan)
    plus_di = 100 * _rma(plus_dm, n) / atr
    minus_di = 100 * _rma(minus_dm, n) / atr
    dx = 100 * (plus_di - minus_di).abs() / (plus_di + minus_di).replace(0, np.nan)
    return _rma(dx, n), plus_di, minus_di


def choppiness(df: pd.DataFrame, n: int) -> pd.Series:
    tr_sum = true_range(df).rolling(n).sum()
    rng = (df["high"].rolling(n).max() - df["low"].rolling(n).min()).replace(0, np.nan)
    return 100 * np.log10(tr_sum / rng) / np.log10(n)


def efficiency_ratio(close: pd.Series, n: int) -> pd.Series:
    change = (close - close.shift(n)).abs()
    path = close.diff().abs().rolling(n).sum().replace(0, np.nan)
    return change / path


def _confirm(raw: pd.Series, k: int) -> pd.Series:
    """Ganti regime hanya jika regime baru muncul k candle berturut-turut."""
    out, current, candidate, count = [], "TRANSITION", None, 0
    for r in raw:
        if r == current:
            candidate, count = None, 0
        else:
            count = count + 1 if r == candidate else 1
            candidate = r
            if count >= k:
                current, candidate, count = r, None, 0
        out.append(current)
    return pd.Series(out, index=raw.index)


def detect_regime(df: pd.DataFrame, cfg: RegimeConfig = RegimeConfig()) -> pd.DataFrame:
    """df wajib punya kolom: open, high, low, close (index = waktu)."""
    out = df.copy()
    out["adx"], out["plus_di"], out["minus_di"] = adx(df, cfg.adx_len)
    out["chop"] = choppiness(df, cfg.chop_len)
    out["er"] = efficiency_ratio(df["close"], cfg.er_len)
    out["ema"] = df["close"].ewm(span=cfg.ema_len, adjust=False).mean()

    out["trend_votes"] = (
        (out["adx"] > cfg.adx_trend).astype(int)
        + (out["chop"] < cfg.chop_trend).astype(int)
        + (out["er"] > cfg.er_trend).astype(int)
    )
    out["range_votes"] = (
        (out["adx"] < cfg.adx_range).astype(int)
        + (out["chop"] > cfg.chop_range).astype(int)
        + (out["er"] < cfg.er_range).astype(int)
    )

    is_trend = out["trend_votes"] >= cfg.min_votes
    is_range = out["range_votes"] >= cfg.min_votes
    up = (out["plus_di"] > out["minus_di"]) & (out["close"] > out["ema"])
    down = (out["plus_di"] < out["minus_di"]) & (out["close"] < out["ema"])

    out["regime_raw"] = np.select(
        [is_trend & up, is_trend & down, is_range & ~is_trend],
        ["TREND_UP", "TREND_DOWN", "SIDEWAYS"],
        default="TRANSITION",
    )
    out["regime"] = _confirm(out["regime_raw"], cfg.confirm_bars)
    return out


def resample_ohlcv(df: pd.DataFrame, rule: str = "15min") -> pd.DataFrame:
    """Index = open time candle (format ccxt/Binance)."""
    agg = {"open": "first", "high": "max", "low": "min", "close": "last"}
    if "volume" in df:
        agg["volume"] = "sum"
    # Origin Senin 1970-01-05 00:00 UTC: candle harian sejajar 00:00 UTC dan
    # candle mingguan mulai Senin -- sama dengan pembagian candle Binance.
    origin = pd.Timestamp("1970-01-05", tz=df.index.tz) if df.index.tz is not None else pd.Timestamp("1970-01-05")
    return df.resample(rule, origin=origin).agg(agg).dropna()


def regime_from_higher_tf(df_ltf: pd.DataFrame, rule: str = "15min",
                          cfg: RegimeConfig = RegimeConfig()) -> pd.Series:
    """
    Regime dari timeframe lebih tinggi, dipetakan ke candle timeframe kecil
    TANPA lookahead: hanya pakai candle HTF yang sudah close.
    """
    htf = detect_regime(resample_ohlcv(df_ltf, rule), cfg)
    return htf["regime"].shift(1).reindex(df_ltf.index, method="ffill").fillna("TRANSITION")


if __name__ == "__main__":
    import ccxt

    ex = ccxt.binanceusdm()
    raw = ex.fetch_ohlcv("BTC/USDT:USDT", "1m", limit=1500)
    df = pd.DataFrame(raw, columns=["ts", "open", "high", "low", "close", "volume"])
    df.index = pd.to_datetime(df.pop("ts"), unit="ms", utc=True)
    df = df.iloc[:-1]  # buang candle yang belum close

    res = detect_regime(df)
    print(res[["close", "adx", "chop", "er", "regime"]].tail(10).round(2))
    print("\nDistribusi regime 1m:")
    print(res["regime"].value_counts(normalize=True).round(3))

    df["regime_15m"] = regime_from_higher_tf(df, "15min")
    print("\nRegime 1m saat ini :", res["regime"].iloc[-1])
    print("Regime 15m saat ini:", df["regime_15m"].iloc[-1])