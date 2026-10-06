"""
src/strategy/regime_filtered.py

Pembungkus (wrapper) strategi: strategi apa pun (mis. DonchianCloseFuturesStrategy)
tetap menghasilkan sinyal seperti biasa, lalu ENTRY-nya disaring pakai
market regime dari src/strategy/regime_detector.py.

ATURAN FILTER
-------------
Satu "run" = rentetan bar dengan sinyal mentah yang sama (mis. LONG terus).
  - Posisi dalam run itu BARU diizinkan mulai bar pertama di mana regime
    TREND searah sinyal (LONG butuh TREND_UP, SHORT butuh TREND_DOWN).
    Sebelum itu sinyal ditahan jadi FLAT.
  - Sekali diizinkan, posisi DIPEGANG sampai sinyal mentah berubah --
    regime yang berubah jadi sideways TIDAK menutup posisi paksa.
    (Kalau sinyal cuma di-set 0 tiap kali sideways, PaperRunner akan
    langsung menutup posisi terbuka -- itu yang dihindari di sini.)
  - Exit dan pembalikan arah tetap mengikuti strategi asli. Untuk strategi
    selalu-di-pasar (long <-> short), pembalikan di kondisi tidak trending
    berarti: posisi lama ditutup, lalu FLAT sampai regime mengonfirmasi.

Tetap FUNGSI MURNI dan tanpa look-ahead (kontrak strategy/base.py):
sinyal mentah dan regime di bar t sama-sama hanya memakai data <= t.

BATASAN (disampaikan terbuka)
-----------------------------
Strategi dievaluasi ulang dari buffer PaperRunner tiap bar. Kalau satu run
lebih panjang dari buffer dan SEMUA bar "trend searah" di run itu sudah
keluar dari buffer, izin hilang dan posisi ditutup. Karena itu launcher
memperbesar buffer_size saat filter aktif.
"""

from dataclasses import asdict

import pandas as pd

from src.strategy.base import Position, Strategy
from src.strategy.regime_detector import RegimeConfig, detect_regime, regime_from_higher_tf

import re

_TF_RE = re.compile(r"^(\d+)([mhdwM])$")
_UNIT_MIN = {"m": 1, "h": 60, "d": 1440, "w": 10080}


def normalize_tf(tf: str) -> str:
    """'1H' -> '1h', '1D' -> '1d', '1W' -> '1w'. '1M' tetap = 1 bulan (beda dari '1m' = 1 menit)."""
    tf = str(tf).strip()
    if tf and tf[-1] in "HDW":
        tf = tf[:-1] + tf[-1].lower()
    return tf

#: jumlah candle timeframe-regime minimal supaya EMA50/ADX14 + konfirmasi stabil
REGIME_WARMUP_HTF_BARS = 70


def tf_minutes(tf: str) -> int:
    """'5m' -> 5, '2h' -> 120, '1d' -> 1440, '1w' -> 10080 (format timeframe Binance)."""
    m = _TF_RE.match(normalize_tf(tf))
    if not m or int(m.group(1)) <= 0:
        raise ValueError(f"timeframe tidak valid: {tf!r} -- pakai format seperti 5m, 15m, 1h, 4h, 1d, 1w")
    if m.group(2) == "M":
        raise ValueError("timeframe bulanan (1M) tidak didukung untuk regime -- panjang bulan tidak tetap")
    return int(m.group(1)) * _UNIT_MIN[m.group(2)]


def tf_rule(tf: str) -> str:
    """Aturan resample pandas: '15m' -> '15min'."""
    return f"{tf_minutes(tf)}min"


def apply_regime_filter(raw: pd.Series, regime: pd.Series) -> pd.Series:
    """Fungsi murni -- bisa diuji tanpa strategi sungguhan."""
    run_id = (raw != raw.shift()).cumsum()
    match = ((raw == Position.LONG) & (regime == "TREND_UP")) | (
        (raw == Position.SHORT) & (regime == "TREND_DOWN")
    )
    allowed = match.astype(int).groupby(run_id).cummax().astype(bool)
    return raw.where(allowed, Position.FLAT).astype(int)


class RegimeFilteredStrategy(Strategy):
    def __init__(self, inner: Strategy, cfg: RegimeConfig | None = None, regime_timeframe: str | None = None):
        """
        regime_timeframe: None = regime dihitung di timeframe bot sendiri.
            "5m"/"15m"/... = candle bot di-resample ke timeframe ini, regime
            dihitung di sana, lalu dipetakan balik ke tiap candle bot --
            hanya memakai candle regime yang SUDAH close (tanpa look-ahead).
        """
        super().__init__(inner.params)
        self.inner = inner
        self.cfg = cfg or RegimeConfig()
        self.ALLOWS_SHORT = inner.ALLOWS_SHORT
        if regime_timeframe is not None:
            regime_timeframe = normalize_tf(regime_timeframe)
            tf_minutes(regime_timeframe)  # validasi format
        self.regime_timeframe = regime_timeframe

    def required_bars(self, bot_timeframe: str) -> int:
        """Jumlah candle bot yang dibutuhkan supaya regime sudah 'panas'."""
        if self.regime_timeframe is None:
            return REGIME_WARMUP_HTF_BARS
        ratio = max(1, tf_minutes(self.regime_timeframe) // tf_minutes(bot_timeframe))
        return REGIME_WARMUP_HTF_BARS * ratio

    def regime_series(self, df: pd.DataFrame) -> pd.Series:
        if self.regime_timeframe is None:
            return detect_regime(df, self.cfg)["regime"]
        return regime_from_higher_tf(df, tf_rule(self.regime_timeframe), self.cfg)

    def regime_snapshot(self, df: pd.DataFrame) -> dict:
        """Regime + indikator dari candle regime TERAKHIR yang sudah close (untuk log)."""
        if self.regime_timeframe is None:
            r = detect_regime(df, self.cfg).iloc[-1]
            tf = "tf bot"
        else:
            from src.strategy.regime_detector import resample_ohlcv
            htf = detect_regime(resample_ohlcv(df, tf_rule(self.regime_timeframe)), self.cfg)
            r = htf.iloc[-2] if len(htf) >= 2 else htf.iloc[-1]   # -2 = candle terakhir yg sudah close
            tf = self.regime_timeframe
        return {"tf": tf, "regime": r["regime"], "adx": r["adx"], "chop": r["chop"], "er": r["er"]}

    @property
    def name(self) -> str:
        return f"RegimeFiltered[{self.inner.name}]"

    def describe(self) -> str:
        tf = self.regime_timeframe or "tf bot"
        return f"{self.inner.describe()} + filter regime[{tf}]({asdict(self.cfg)})"

    def regime_frame(self, df: pd.DataFrame) -> pd.DataFrame:
        return detect_regime(df, self.cfg)

    def generate_signals(self, df: pd.DataFrame) -> pd.Series:
        raw = self.inner.generate_signals(df).astype(int)
        regime = self.regime_series(df)
        return apply_regime_filter(raw, regime)