"""
runner/paper.py

Runner Hari 3: menyatukan semua bagian -- price stream (data/stream.py),
strategi (strategy/*.py), order manager (execution/order_manager.py),
slippage tracker (execution/slippage_tracker.py) -- jadi satu program
yang bisa dijalankan terus-menerus di testnet ("paper trading": uang
virtual, tapi jalur kode IDENTIK dengan live nanti).

Alur tiap bar baru:
  1. Bar baru masuk dari data/stream.py -> tambahkan ke buffer harga
  2. Panggil strategy.generate_signals(buffer) -> ambil sinyal TERBARU
  3. Kalau sinyal beda dari posisi saat ini -> kirim order lewat
     execution/order_manager.py (TIDAK PERNAH langsung ke broker)
  4. Kalau order terisi -> catat lewat execution/slippage_tracker.py

WAJIB panggil order_manager.reconcile_pending() SEBELUM loop utama
mulai -- lihat execution/order_manager.py soal kenapa ini penting
(kalau program ini pernah crash sebelumnya, order yang "menggantung"
diperiksa dulu ke bursa, bukan diabaikan atau dikirim ulang membabi
buta).

PERBAIKAN (ditambahkan setelah insiden reduceOnly -2022 rejected):
_sync_position_from_exchange() -- dipanggil di AWAL _handle_signal_change().
Order limit pasif yang tidak langsung fill saat dikirim bisa terisi
BELAKANGAN (kadang jauh belakangan, sebagai maker) tanpa program pernah
tahu -- posisi internal (self._current_position) jadi "basi" (stale)
dibanding posisi SUNGGUHAN di bursa. Bar berikutnya, program salah
kira masih perlu menutup posisi yang sebenarnya sudah closed, kirim
reduceOnly lagi -- DITOLAK bursa karena memang tidak ada apa-apa lagi
untuk dikurangi. Perbaikannya: cek fetch_position() ke bursa dulu
SEBELUM memutuskan aksi, bukan cuma percaya status yang dilihat sesaat
saat order dulu dikirim.

FITUR SL + TP RASIO (risk_reward, mis. 2.0) -- MATI secara default
-------------------------------------------------------------------------
Kalau risk_reward diisi, begitu posisi terbuka bot menitipkan DUA order
bersyarat ke BURSA (bukan dipantau bot):
  - STOP LOSS  : jarak = maks(sl_atr_mult x ATR, sl_min_fee_mult x fee
                 bolak-balik). ATR = rata-rata true range `sl_atr_period`
                 bar dari timeframe bot (aturan 2N sistem Turtle).
  - TAKE PROFIT: jarak = R x SL + (R+1) x fee bolak-balik, supaya UANG
                 BERSIH setelah fee benar-benar R:1 (bukan cuma jarak harga).
Keduanya tetap melindungi posisi walau bot/VPS mati. SL diverifikasi ke
bursa setelah dipasang; kalau gagal dipasang atau tidak lolos verifikasi,
posisi DITUTUP SEGERA dan bot berhenti membuka posisi baru -- posisi
tidak pernah dibiarkan terbuka tanpa SL.
Saat posisi ditutup lewat jalur apa pun, sisa order bersyarat DIBATALKAN
eksplisit (tidak mengandalkan pembatalan otomatis bursa, yang belum
terverifikasi). Setelah SL kena, arah itu DITAHAN sampai sinyal berbalik.
Setelah TP kena, berlaku aturan stop_after_take_profit yang sudah ada.
Tanpa risk_reward, perilaku bot SAMA PERSIS dengan sebelumnya.
"""

import asyncio
import json
import os
import time
from collections.abc import Callable
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd

from src.data.stream import PriceStream
from src.execution.broker import OrderStatus
from src.execution.order_manager import OrderManager
from src.execution.slippage_tracker import SignalContext, SlippageTracker
from src.strategy.base import Position, Strategy


def compute_atr(bars: list[dict], period: int) -> float | None:
    """
    ATR = rata-rata true range `period` bar terakhir (tanpa smoothing).
    True range = maks(high-low, |high-close_sebelumnya|, |low-close_sebelumnya|).
    None kalau bar belum cukup. FUNGSI MURNI.
    """
    if period <= 0 or len(bars) < period + 1:
        return None
    recent = bars[-(period + 1):]
    trs = []
    for prev, cur in zip(recent[:-1], recent[1:]):
        h, lo, pc = float(cur["high"]), float(cur["low"]), float(prev["close"])
        trs.append(max(h - lo, abs(h - pc), abs(lo - pc)))
    return sum(trs) / len(trs)


def compute_bracket(entry: float, direction: int, atr: float | None, fee_side: float,
                    rr: float, atr_mult: float, min_fee_mult: float) -> dict:
    """
    Hitung harga SL & TP. FUNGSI MURNI. Semua jarak dalam FRAKSI harga.

      f  = fee bolak-balik = 2 x fee satu sisi
      s  = maks(atr_mult x ATR / entry, min_fee_mult x f)   (jarak SL)
      t  = rr x s + (rr + 1) x f                             (jarak TP)

    Kenapa rumus TP begitu: rugi bersih saat SL = s + f, untung bersih saat
    TP = t - f. Syarat untung bersih = rr x rugi bersih memberi
    t - f = rr(s + f)  ->  t = rr x s + (rr + 1) x f.
    Leverage tidak muncul: untung, rugi, dan fee sama-sama dikali leverage,
    jadi rasionya tidak berubah.
    """
    fee_rt = 2 * fee_side
    floor = min_fee_mult * fee_rt
    atr_dist = (atr_mult * atr / entry) if (atr and entry) else None
    if atr_dist is not None and atr_dist >= floor:
        s, basis = atr_dist, f"{atr_mult:g}xATR"
    else:
        s = floor
        basis = f"batas bawah {min_fee_mult:g}x fee" + (" (ATR lebih kecil)" if atr_dist is not None
                                                       else " (ATR belum tersedia)")
    t = rr * s + (rr + 1) * fee_rt
    return {
        "entry": entry, "direction": direction, "atr": atr, "fee_rt": fee_rt, "rr": rr,
        "sl_dist": s, "tp_dist": t, "sl_basis": basis,
        "sl_price": entry * (1 - direction * s),
        "tp_price": entry * (1 + direction * t),
    }


def compute_bracket_roi(entry: float, direction: int, leverage: float | None, tp_roi: float,
                        sl_roi: float, fee_side: float) -> dict:
    """
    SL & TP dari ROI KOTOR terhadap MARGIN (definisi yang sama dengan
    ROI di aplikasi Binance). FUNGSI MURNI.

      gerak harga = ROI / leverage      (mis. ROI 5% di 20x -> harga 0,25%)
      untung bersih TP = t - f,  rugi total SL = s + f   (f = fee bolak-balik)

    Fee dibayar di KEDUA hasil, jadi rasio bersih (rr) biasanya jauh lebih
    kecil dari tp_roi / sl_roi. rr dikembalikan supaya bisa ditampilkan.
    """
    lev = leverage if leverage and leverage > 0 else 1.0
    fee_rt = 2 * fee_side
    s, t = sl_roi / lev, tp_roi / lev
    rugi, untung = s + fee_rt, t - fee_rt
    return {
        "entry": entry, "direction": direction, "atr": None, "fee_rt": fee_rt, "leverage": lev,
        "tp_roi": tp_roi, "sl_roi": sl_roi, "sl_dist": s, "tp_dist": t,
        "rr": (untung / rugi) if rugi > 0 else None,
        "sl_basis": f"ROI kotor {sl_roi:.2%} / {lev:g}x",
        "sl_price": entry * (1 - direction * s),
        "tp_price": entry * (1 + direction * t),
    }


def classify_bracket_exit(exit_price: float | None, bracket: dict) -> str:
    """'TP', 'SL', atau 'UNKNOWN' -- pemicu mana yang paling dekat dengan harga keluar."""
    if exit_price is None:
        return "UNKNOWN"
    kandidat = [(k, bracket.get(f"{k.lower()}_price")) for k in ("TP", "SL")]
    kandidat = [(k, v) for k, v in kandidat if v]
    if not kandidat:
        return "UNKNOWN"
    return min(kandidat, key=lambda kv: abs(exit_price - kv[1]))[0]


class PaperRunner:
    # ------------------------------------------------------------------
    # Dua status ini disimpan ke file tiap kali berubah (lihat state_path).
    # ------------------------------------------------------------------
    @property
    def _blocked_direction(self):
        return getattr(self, "_blocked_direction_v", None)

    @_blocked_direction.setter
    def _blocked_direction(self, v):
        lama = getattr(self, "_blocked_direction_v", None)
        self._blocked_direction_v = v
        if v != lama:
            self._persist_state()

    @property
    def _reentry_armed(self):
        return getattr(self, "_reentry_armed_v", False)

    @_reentry_armed.setter
    def _reentry_armed(self, v):
        lama = getattr(self, "_reentry_armed_v", False)
        self._reentry_armed_v = bool(v)
        if bool(v) != lama:
            self._persist_state()

    def __init__(
        self,
        strategy: Strategy,
        broker,  # Broker ATAU MockBroker -- lihat execution/broker.py
        symbol: str = "BTC/USDT:USDT",
        timeframe: str = "2h",
        order_amount: float = 0.01,
        buffer_size: int = 500,
        aggression_bps: float = 5.0,
        use_reduce_only: bool = True,
        session_hours: float | None = None,
        min_entry_buffer_hours: float | None = None,
        take_profit_pct: float | None = None,
        stop_after_take_profit: bool = False,
        backfill_bars: int | None = None,
        live_take_profit_poll_seconds: float | None = None,
        debug_info_fn: Callable[[list[dict]], str] | None = None,
        risk_reward: float | None = None,
        sl_atr_mult: float = 2.0,
        sl_atr_period: int = 20,
        sl_min_fee_mult: float = 2.0,
        bracket_poll_seconds: float = 5.0,
        reentry_mode: str = "reversal",
        tp_roi: float | None = None,
        sl_roi: float | None = None,
        state_path: str | None = None,
        max_consecutive_sl: int | None = None,
        max_daily_loss_pct: float | None = None,
        day_utc_offset_hours: float = 7.0,
        reentry_channel_fn: Callable[[list[dict]], tuple[float, float] | None] | None = None,
    ):
        # Status yang DISIMPAN ke file (state_path) supaya bertahan lintas
        # restart: arah yang ditahan setelah TP/SL, status "siap" masuk-lagi,
        # hitungan SL beruntun, dan batas harian. Tanpa ini, restart/crash
        # menghapus status itu dan bot langsung masuk lagi ke arah yang baru
        # kena SL. File HANYA ditulis setelah dibaca (_load_persistent_state),
        # supaya nilai awal di __init__ tidak menimpa status tersimpan.
        self.state_path = state_path
        self._state_loaded = False
        self._state_saved_at_ms: int | None = None
        # Kill switch: berhenti membuka posisi sampai hari berganti (zona
        # UTC+day_utc_offset_hours, default WIB) setelah max_consecutive_sl SL
        # beruntun di hari yang sama, atau rugi bersih hari itu >=
        # max_daily_loss_pct % saldo awal hari. None/0 = mati.
        self.max_consecutive_sl = max_consecutive_sl or None
        self.max_daily_loss_pct = max_daily_loss_pct or None
        self.day_utc_offset_hours = day_utc_offset_hours
        self._consecutive_sl = 0
        self._sl_day_ms: int | None = None
        self._halt_until_ms: int | None = None
        self._halt_reason: str = ""
        self.strategy = strategy
        self.broker = broker
        self.symbol = symbol
        self.timeframe = timeframe
        self.order_amount = order_amount
        self.buffer_size = buffer_size
        self.aggression_bps = aggression_bps
        self.use_reduce_only = use_reduce_only
        self.session_hours = session_hours
        self.min_entry_buffer_hours = min_entry_buffer_hours
        self.take_profit_pct = take_profit_pct
        self.stop_after_take_profit = stop_after_take_profit
        self.backfill_bars = backfill_bars
        self.live_take_profit_poll_seconds = live_take_profit_poll_seconds
        self.debug_info_fn = debug_info_fn
        # SL + TP rasio di bursa -- lihat docstring modul. None = mati.
        self.risk_reward = risk_reward
        self.sl_atr_mult = sl_atr_mult
        self.sl_atr_period = sl_atr_period
        self.sl_min_fee_mult = sl_min_fee_mult
        self.bracket_poll_seconds = bracket_poll_seconds
        # SL/TP berdasarkan ROI KOTOR terhadap margin (fraksi: 0.05 = 5%).
        # Alternatif dari risk_reward; keduanya tidak boleh aktif bersamaan.
        if (tp_roi is None) != (sl_roi is None):
            raise ValueError("tp_roi dan sl_roi harus diisi berdua")
        if tp_roi is not None:
            if risk_reward is not None:
                raise ValueError("pilih salah satu: risk_reward ATAU tp_roi/sl_roi")
            if tp_roi <= 0 or sl_roi <= 0:
                raise ValueError("tp_roi dan sl_roi harus > 0")
        self.tp_roi = tp_roi
        self.sl_roi = sl_roi
        self._bracket_enabled = risk_reward is not None or tp_roi is not None
        # reentry_mode: kapan boleh masuk lagi ke arah YANG SAMA setelah
        # posisi ditutup TP/SL (mode berulang):
        #   "reversal" (default, perilaku lama): tunggu sinyal berbalik.
        #   "midline": "siap" begitu close kembali ke tengah channel, lalu
        #              masuk lagi saat close menembus batas channel searah
        #              (breakout BARU) -- tidak perlu menunggu sisi seberang jebol.
        # reentry_channel_fn(bars) -> (atas, bawah): WAJIB untuk "midline".
        # Disuplai launcher dengan rumus channel yang SAMA dengan sinyal,
        # supaya PaperRunner tetap tidak tahu-menahu soal Donchian.
        if reentry_mode not in ("reversal", "midline"):
            raise ValueError(f"reentry_mode tidak dikenal: {reentry_mode!r}")
        if reentry_mode == "midline" and reentry_channel_fn is None:
            raise ValueError("reentry_mode='midline' butuh reentry_channel_fn")
        self.reentry_mode = reentry_mode
        self.reentry_channel_fn = reentry_channel_fn
        self._reentry_armed = False
        # Catatan keadaan menunggu/berhenti -- dicetak di tiap heartbeat dan
        # ditulis ke session_state.json untuk dashboard. Tanpa ini bot yang
        # sengaja menunggu tampak MATI: log-nya cuma heartbeat.
        self._status_note: str = ""
        # debug_info_fn: opsional -- fungsi (bars: list[dict]) -> str,
        # dipanggil tiap heartbeat DAN sekali setelah backfill, hasilnya
        # ditempel di output. PaperRunner TIDAK tahu apa isinya (tetap
        # generik, tidak terikat ke strategi Donchian tertentu) -- yang
        # tahu isinya adalah launcher (mis. run_paper_donchian_futures.py),
        # yang bisa hitung channel/level apa pun sesuai strategi yang
        # sedang dipakai, dari `bars` yang SAMA PERSIS dipakai sinyal
        # sungguhan (bukan hitungan terpisah yang bisa diam-diam beda).
        # live_take_profit_poll_seconds: kalau diisi (mis. 5.0), take-
        # profit dipantau lewat harga LIVE tiap sekian detik -- TERPISAH
        # dari evaluasi sinyal yang tetap sekali per candle. Tanpa ini
        # (default None), take-profit TETAP jalan seperti sebelumnya --
        # cuma dievaluasi sekali per candle tutup lewat process_bar().
        # Lihat _price_watch_loop() dan _action_lock di bawah.
        self._action_lock = asyncio.Lock()  # cegah loop candle & loop pemantau harga saling menyela
        # backfill_bars: kalau diisi (mis. 200), buffer harga diisi dari
        # histori SEBELUM live trading mulai -- lewat Broker.fetch_recent_bars()
        # yang SUDAH ADA (dibangun persis untuk ini). Supaya sinyal pertama
        # bisa langsung dihitung begitu bar live pertama datang, TIDAK perlu
        # menunggu `lookback` bar baru satu-satu dari stream (bisa berjam-jam
        # untuk lookback besar di timeframe pendek). Lihat _backfill().
        # stop_after_take_profit: False (default) = pakai _blocked_direction
        # (tahan arah yang sama sampai sinyal BENAR-BENAR berbalik, lalu
        # boleh trading lagi -- lihat penjelasan _blocked_direction).
        # True = begitu take-profit kena SEKALI, program BERHENTI TOTAL
        # trading -- tidak akan buka posisi apa pun lagi sampai program
        # di-restart manual, terlepas sinyal berbalik berkali-kali pun.
        # Heartbeat/dashboard TETAP jalan (cuma tidak ada order baru).
        self._entry_price: float | None = None
        self._entry_leverage: float = 1.0
        self._entry_taker_fee_pct: float = 0.0005  # fallback KONSERVATIF (0,05%) -- ditimpa dari bursa saat posisi dibuka
        self._tp_order_id: str | None = None  # id order TAKE_PROFIT_MARKET yang dititipkan ke bursa
        self._sl_order_id: str | None = None  # id order STOP_MARKET (SL) yang dititipkan ke bursa
        self._bracket: dict | None = None  # SL/TP yang sedang aktif -- lihat compute_bracket()
        self._emergency_close_pending: bool = False  # SL gagal terpasang, posisi sedang dipaksa tutup
        self._blocked_direction: int | None = None
        self._trading_halted: bool = False
        self._last_order_info: dict | None = None  # {"client_order_id", "target_position"} -- lihat _sync_position_from_exchange
        # min_entry_buffer_hours: kalau diisi, posisi BARU tidak akan
        # dibuka kalau sisa waktu sesi lebih pendek dari ini -- supaya
        # tidak buka posisi yang kemungkinan besar belum sempat keluar
        # SECARA ALAMI (lewat sinyal exit strategi) sebelum sesi habis.
        # Rekomendasi: samakan dengan durasi max_hold_bars strategi
        # (dalam jam) -- itu jaring pengaman TERAKHIR strategi, jadi
        # posisi yang dibuka kurang dari durasi itu sebelum sesi habis
        # nyaris pasti masih terbuka saat sesi berakhir. TIDAK
        # mempengaruhi penutupan posisi yang SUDAH terbuka -- itu tetap
        # selalu diizinkan kapan pun.
        # Ambang peringatan (dalam JAM tersisa) -- dicetak SEKALI tiap
        # ambang terlewati (bukan tiap bar, biar gak spam), MAKIN SERING
        # mendekati akhir sesi. Daftar ini tetap, bukan parameter yang
        # perlu disetel.
        self._warning_thresholds_hours = [1.0, 0.5, 0.25, 0.0833]  # 60/30/15/5 menit
        self._warnings_fired: set[float] = set()
        self._session_start: datetime | None = None
        # use_reduce_only=False WAJIB untuk SPOT -- reduceOnly itu konsep
        # khusus futures/margin (menahan order supaya cuma boleh MENGURANGI
        # posisi, tidak pernah membuka baru). Spot tidak punya "posisi"
        # bergaya margin sama sekali -- jual itu ya jual aset yang dimiliki,
        # tidak ada konsep reduceOnly untuk ditegakkan. Mengirim parameter
        # ini ke API spot berisiko ditolak/diabaikan API secara tidak
        # terduga. Default True (perilaku FUTURES lama, tidak berubah).
        # aggression_bps: seberapa jauh harga limit "menyerbu" dari harga
        # sinyal (close bar terakhir), supaya order MENYEBERANG spread
        # dan langsung ke-fill (seperti taker), bukan cuma nangkring pasif
        # menunggu pasar datang ke harga itu. Tanpa ini, limit order bisa
        # tidak pernah ke-fill kalau harga tidak kebetulan mampir --
        # ditemukan di lapangan: order penutup posisi terjebak loop
        # batal-kirim-ulang tanpa henti karena harga limitnya terlalu
        # pasif. Slippage tambahan dari buffer ini TETAP tercatat jujur
        # lewat execution/slippage_tracker.py -- bukan disembunyikan.

        self.order_manager = OrderManager(broker)
        self.slippage_tracker = SlippageTracker()
        self.stream = PriceStream(symbol=symbol, timeframe=timeframe)

        self._bars: list[dict] = []
        self._current_position = Position.FLAT

    def _aggressive_price(self, side: str, reference_price: float) -> float:
        """
        Geser harga sinyal sedikit ke arah yang membuat order MENYEBERANG
        pasar (buy sedikit lebih mahal, sell sedikit lebih murah) --
        supaya limit order berperilaku seperti order yang langsung
        ke-fill (taker), bukan menunggu pasif di orderbook.
        """
        offset = reference_price * (self.aggression_bps / 10_000)
        return reference_price + offset if side == "buy" else reference_price - offset

    def _bars_to_df(self) -> pd.DataFrame:
        df = pd.DataFrame(self._bars)
        df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms", utc=True)
        return df.set_index("timestamp")

    def _backfill(self) -> None:
        """
        Isi buffer harga dari histori SEBELUM live trading mulai --
        REUSE Broker.fetch_recent_bars() yang SUDAH ADA (dibangun
        persis untuk ini, lihat docstring aslinya), bukan kode baru
        dari nol.

        PENTING: bar histori ini TIDAK diproses lewat process_bar() --
        cuma mengisi buffer, TIDAK memicu evaluasi sinyal/order untuk
        harga yang sudah lewat (itu akan berarti "trading" di masa
        lalu, tidak masuk akal). Evaluasi sinyal sungguhan baru mulai
        di bar LIVE PERTAMA yang datang dari stream, setelah buffer
        ini sudah terisi -- jadi sinyal itu bisa langsung dihitung,
        tidak perlu menunggu `lookback` bar baru satu-satu.
        """
        try:
            bars = self.broker.fetch_recent_bars(self.symbol, self.timeframe, limit=self.backfill_bars)
        except Exception as e:
            print(f"  [backfill] gagal ambil histori ({e}) -- mulai dari buffer kosong seperti biasa")
            return

        if not bars:
            print("  [backfill] bursa tidak mengembalikan bar histori apa pun -- mulai dari buffer kosong")
            return

        self._bars = bars[-self.buffer_size:]  # tetap hormati batas buffer_size
        print(f"  [backfill] {len(self._bars)} bar histori dimuat ({self._bars[0]['timestamp']} "
              f"s.d. {self._bars[-1]['timestamp']}, unix ms) -- sinyal bisa langsung dihitung "
              f"begitu bar live pertama datang, tidak perlu menunggu dari nol.")

    async def run(self) -> None:
        """Loop utama -- panggil ini untuk menjalankan runner sungguhan (butuh jaringan)."""
        print(f"=== PaperRunner mulai: {self.symbol} @ {self.timeframe} ===")
        self._load_persistent_state()
        if self.max_consecutive_sl or self.max_daily_loss_pct:
            print(f"  [batas] kill switch AKTIF: berhenti membuka posisi sampai hari berganti "
                  f"(UTC{self.day_utc_offset_hours:+g}) setelah "
                  + " atau ".join(x for x in [
                      f"{self.max_consecutive_sl} SL beruntun" if self.max_consecutive_sl else "",
                      f"rugi bersih harian {self.max_daily_loss_pct:g}% saldo" if self.max_daily_loss_pct else ""] if x)
                  + ".")
        if self.session_hours is not None:
            self._session_start = datetime.now(timezone.utc)
            end_time = self._session_start.timestamp() + self.session_hours * 3600
            end_dt = datetime.fromtimestamp(end_time, tz=timezone.utc)
            print(f"  Sesi dibatasi {self.session_hours} jam -- berakhir sekitar {end_dt:%Y-%m-%d %H:%M} UTC")
            print(f"  Peringatan otomatis akan muncul kalau mendekati batas ini dan posisi masih terbuka.")

        if self.backfill_bars:
            self._backfill()
            if self.debug_info_fn is not None and self._bars:
                try:
                    print(f"  [backfill] {self.debug_info_fn(self._bars)}")
                except Exception as e:
                    print(f"  [backfill] (debug_info_fn error: {e})")

        self._reconcile_restored_block()
        self.order_manager.reconcile_pending(self.symbol)  # WAJIB, lihat docstring modul
        self._recover_position_from_exchange()  # WAJIB juga -- lihat docstring: cegah order tutup tak perlu saat restart
        if self._bracket_enabled:
            await self._ensure_bracket_after_recovery()
        if self.max_consecutive_sl or self.max_daily_loss_pct:
            self._evaluate_kill_switch()

        async def _bar_loop() -> None:
            async for bar in self.stream.watch_bars():
                await self.process_bar(bar)

        loops = [_bar_loop()]
        if self.live_take_profit_poll_seconds is not None:
            print(f"  [price-watch] take-profit real-time AKTIF -- cek harga live tiap "
                  f"{self.live_take_profit_poll_seconds:.0f} detik, TERPISAH dari evaluasi candle.")
            loops.append(self._price_watch_loop(self.live_take_profit_poll_seconds))
        if self._bracket_enabled:
            aturan = (f"TP ROI kotor +{self.tp_roi:.2%} / SL ROI kotor -{self.sl_roi:.2%}"
                      if self.tp_roi is not None else f"SL + TP rasio {self.risk_reward:g}:1 (bersih setelah fee)")
            print(f"  [bracket] {aturan} AKTIF -- "
                  f"dieksekusi BURSA; bot mengecek posisi tiap {self.bracket_poll_seconds:g} detik "
                  f"untuk membereskan sisa order setelah salah satunya kena.")
            loops.append(self._bracket_watch_loop(self.bracket_poll_seconds))
        if len(loops) > 1:
            await asyncio.gather(*loops)
        else:
            await loops[0]

    async def process_bar(self, bar: dict) -> None:
        """
        Proses SATU bar baru. Dipisah dari run() supaya bisa dites
        langsung (dipanggil manual dengan bar tiruan) tanpa perlu
        koneksi WebSocket sungguhan -- lihat blok __main__ di bawah.
        """
        self._bars.append(bar)
        if len(self._bars) > self.buffer_size:
            self._bars = self._bars[-self.buffer_size :]

        bar_time = datetime.fromtimestamp(bar["timestamp"] / 1000, tz=timezone.utc)

        if len(self._bars) < 2:
            print(f"  [heartbeat] {bar_time:%Y-%m-%d %H:%M} UTC  close={bar['close']:.2f}  "
                  f"(bar {len(self._bars)}, belum cukup data untuk strategi)")
            # WAJIB tetap tulis session state di sini juga -- BUG yang
            # ditemukan di lapangan: kalau di-skip lewat return sebelum
            # sempat menulis, dashboard membaca file yang TIDAK PERNAH
            # ter-update sama sekali selama fase pengisian buffer,
            # menampilkan "?" dan "tanpa batas waktu sesi" padahal sesi
            # sedang berjalan normal.
            self._write_session_state(bar_time, bar["close"], self._current_position)
            return

        df = self._bars_to_df()
        signals = self.strategy.generate_signals(df)
        latest_signal = int(signals.iloc[-1])

        debug_suffix = ""
        if self.debug_info_fn is not None:
            try:
                debug_suffix = "  " + self.debug_info_fn(self._bars)
            except Exception as e:
                debug_suffix = f"  (debug_info_fn error: {e})"

        self._status_note = self._compute_status_note(latest_signal)
        status_suffix = f"  [status] {self._status_note}" if self._status_note else ""
        print(f"  [heartbeat] {bar_time:%Y-%m-%d %H:%M} UTC  close={bar['close']:.2f}  "
              f"posisi={self._current_position}  sinyal={latest_signal}{debug_suffix}{status_suffix}")

        if self._trading_halted:
            # stop_after_take_profit aktif dan sudah pernah kena -- tidak
            # ada order baru lagi, TAPI heartbeat/dashboard tetap jalan
            # supaya Anda tetap bisa memantau harga & tahu program masih hidup.
            self._write_session_state(bar_time, bar["close"], latest_signal)
            return

        current_price = float(bar["close"])
        async with self._action_lock:
            if self._emergency_close_pending:
                await self._retry_emergency_close(df)
            elif await self._check_bracket_exit():
                pass  # SL/TP bursa baru saja menutup posisi -- sinyal dievaluasi lagi bar berikutnya
            elif self._check_take_profit(bar):
                direction = 1 if self._current_position == Position.LONG else -1
                extreme_price = bar["high"] if direction == 1 else bar["low"]
                raw_pct = (extreme_price - self._entry_price) / self._entry_price * direction
                gross_roi_pct = raw_pct * self._entry_leverage
                fee_roi_pct = 2 * self._entry_taker_fee_pct * self._entry_leverage
                net_roi_pct = gross_roi_pct - fee_roi_pct
                print(f"  [take-profit] posisi {self._current_position} ROI BERSIH >= "
                      f"{self.take_profit_pct:.1%} (ekstrem bar {extreme_price:.2f}, ROI kotor "
                      f"{gross_roi_pct:.2%} - estimasi fee bolak-balik {fee_roi_pct:.2%} = "
                      f"BERSIH {net_roi_pct:.2%}) -- TUTUP PAKSA.")
                triggered_direction = self._current_position
                await self._handle_signal_change(Position.FLAT, df)

                # PENTING: cuma anggap take-profit "berhasil" kalau posisi
                # BENAR-BENAR sudah flat setelah percobaan tutup di atas --
                # BUKAN cuma karena order-nya sudah DIKIRIM. Order limit
                # pasif bisa saja belum terisi (status "open", bukan
                # "filled") -- kalau kita anggap berhasil padahal belum,
                # _trading_halted akan mengunci program PADAHAL POSISI
                # MASIH TERBUKA SUNGGUHAN di bursa, dan tidak akan pernah
                # dicoba tutup lagi (BUG SERIUS yang baru ketahuan).
                if self._current_position == Position.FLAT:
                    if self.stop_after_take_profit:
                        self._trading_halted = True
                        print("  [take-profit] Penutupan terkonfirmasi FLAT -- stop_after_take_profit "
                              "AKTIF, program BERHENTI trading total (restart manual untuk lanjut).")
                    else:
                        self._blocked_direction = triggered_direction
                        self._reentry_armed = False
                else:
                    print(f"  [take-profit] Order penutup TERKIRIM tapi BELUM terkonfirmasi flat "
                          f"(posisi masih {self._current_position}) -- AKAN DICOBA LAGI bar berikutnya, "
                          f"BELUM berhenti trading.")
            elif (self._current_position == Position.FLAT and latest_signal != Position.FLAT
                  and self._limit_halt_active()):
                pass  # batas harian tercapai -- tidak membuka posisi baru (lihat catatan status)
            elif latest_signal != self._current_position:
                if self._blocked_direction is not None and latest_signal == self._blocked_direction:
                    if self.reentry_mode == "midline":
                        await self._maybe_reenter(df, latest_signal)
                    else:
                        print(f"  [take-profit] sinyal masih arah {latest_signal}, arah yang BARU SAJA "
                              f"di-take-profit -- DITAHAN, tunggu sinyal benar-benar berbalik dulu.")
                else:
                    self._blocked_direction = None  # sinyal sudah beda arah -- blokir dicabut
                    self._reentry_armed = False
                    await self._handle_signal_change(latest_signal, df)

        self._check_session_warning(bar_time)
        self._write_session_state(bar_time, bar["close"], latest_signal)

    def _write_session_state(self, bar_time: datetime, price: float, signal: int) -> None:
        """
        Tulis snapshot status sesi ke data/session_state.json tiap bar --
        DIBACA oleh scripts/dashboard.py (proses TERPISAH) untuk
        menampilkan status live di browser. Ditulis ulang PENUH tiap
        kali (bukan di-append) -- ini snapshot TERKINI, bukan log.

        Kegagalan menulis file ini TIDAK BOLEH menghentikan trading --
        dibungkus try/except, cuma dicetak peringatan sekali.
        """
        import json
        from pathlib import Path

        state = {
            "symbol": self.symbol,
            "timeframe": self.timeframe,
            "session_hours": self.session_hours,
            "min_entry_buffer_hours": self.min_entry_buffer_hours,
            "session_start": self._session_start.isoformat() if self._session_start else None,
            "last_update": datetime.now(timezone.utc).isoformat(),
            "current_position": self._current_position,
            "latest_signal": signal,
            "latest_price": price,
            "latest_bar_time": bar_time.isoformat(),
            "status_note": self._status_note,
        }
        try:
            Path("data").mkdir(exist_ok=True)
            with open("data/session_state.json", "w") as f:
                json.dump(state, f, indent=2)
        except Exception as e:
            print(f"  (Peringatan: gagal tulis session_state.json untuk dashboard: {e})")

    def _check_session_warning(self, bar_time: datetime) -> None:
        """
        Cek sisa waktu sesi (kalau session_hours diset) dan cetak
        PERINGATAN kalau mendekati batas DAN posisi masih terbuka.
        Dicetak SEKALI per ambang (bukan tiap bar) supaya tidak spam --
        tapi ambang-nya sengaja makin sering mendekati akhir (60/30/15/5
        menit) supaya user makin sulit terlewat, bukan cuma satu
        peringatan di awal yang gampang kelewat baca.
        """
        if self.session_hours is None or self._session_start is None:
            return

        elapsed_hours = (bar_time - self._session_start).total_seconds() / 3600
        remaining_hours = self.session_hours - elapsed_hours

        if remaining_hours <= 0:
            if 0.0 not in self._warnings_fired:
                self._warnings_fired.add(0.0)
                if self._current_position != Position.FLAT:
                    print("\n" + "!" * 70)
                    print("  BATAS WAKTU SESI TERCAPAI -- POSISI MASIH TERBUKA!")
                    print(f"  Posisi saat ini: {self._current_position}. Program TIDAK otomatis")
                    print("  menutup posisi ini -- segera putuskan manual (biarkan program")
                    print("  jalan sampai exit alami, atau tutup manual lewat dashboard).")
                    print("!" * 70 + "\n")
                else:
                    print(f"\n  (Batas waktu sesi {self.session_hours} jam tercapai, posisi FLAT -- aman.)\n")
            return

        for threshold in self._warning_thresholds_hours:
            if remaining_hours <= threshold and threshold not in self._warnings_fired:
                self._warnings_fired.add(threshold)
                if self._current_position != Position.FLAT:
                    minutes_left = remaining_hours * 60
                    print("\n" + "!" * 70)
                    print(f"  PERINGATAN: sesi berakhir dalam ~{minutes_left:.0f} menit, "
                          f"posisi MASIH TERBUKA ({self._current_position}).")
                    print("  Pantau terus -- kalau tetap terbuka sampai batas waktu,")
                    print("  Anda perlu putuskan manual apa yang dilakukan.")
                    print("!" * 70 + "\n")
                break  # cuma cetak ambang TERDEKAT yang baru terlewati, bukan semua sekaligus

    def _entry_blocked_by_session(self, df: pd.DataFrame) -> bool:
        """
        True kalau posisi BARU sebaiknya TIDAK dibuka karena sisa waktu
        sesi lebih pendek dari min_entry_buffer_hours -- lihat penjelasan
        parameter ini di __init__.
        """
        if self.session_hours is None or self.min_entry_buffer_hours is None:
            return False
        if self._session_start is None:
            return False

        bar_time = df.index[-1]
        if bar_time.tzinfo is None:
            bar_time = bar_time.tz_localize(timezone.utc)
        elapsed_hours = (bar_time - self._session_start).total_seconds() / 3600
        remaining_hours = self.session_hours - elapsed_hours

        if remaining_hours < self.min_entry_buffer_hours:
            print(f"  [signal] sinyal entry MUNCUL tapi DITAHAN -- sisa waktu sesi "
                  f"({remaining_hours:.1f} jam) kurang dari buffer minimal "
                  f"({self.min_entry_buffer_hours:.1f} jam). Tetap FLAT.")
            return True
        return False

    def _net_roi_pct(self, price: float) -> float:
        """
        ROI BERSIH (dikurangi estimasi fee bolak-balik) untuk posisi
        SAAT INI, dihitung di harga `price` -- fungsi bersama dipakai
        baik oleh pengecekan berbasis candle (_check_take_profit,
        pakai high/low) MAUPUN pengecekan real-time berbasis harga
        live (_check_take_profit_price) -- supaya rumusnya SATU
        tempat, tidak ada risiko dua rumus diam-diam beda.
        """
        direction = 1 if self._current_position == Position.LONG else -1
        raw_price_pct = (price - self._entry_price) / self._entry_price * direction
        gross_roi_pct = raw_price_pct * self._entry_leverage
        fee_roi_pct = 2 * self._entry_taker_fee_pct * self._entry_leverage
        return gross_roi_pct - fee_roi_pct

    def _check_take_profit(self, bar: dict) -> bool:
        """
        True kalau take-profit terpicu di CANDLE ini -- dievaluasi
        SEKALI per candle tutup, lewat process_bar(). Untuk pemantauan
        REAL-TIME (tidak menunggu candle tutup), lihat
        _check_take_profit_price() + _price_watch_loop().

        Pakai HIGH (long) / LOW (short) candle ini, BUKAN cuma close --
        supaya lonjakan yang terjadi DI DALAM satu candle ikut
        terdeteksi.
        """
        if self.take_profit_pct is None:
            return False
        if self._current_position == Position.FLAT or self._entry_price is None:
            return False
        direction = 1 if self._current_position == Position.LONG else -1
        extreme_price = bar["high"] if direction == 1 else bar["low"]
        return self._net_roi_pct(extreme_price) >= self.take_profit_pct

    def _check_take_profit_price(self, price: float) -> bool:
        """Padanan _check_take_profit(), TAPI dari satu harga live -- dipakai _price_watch_loop()."""
        if self.take_profit_pct is None:
            return False
        if self._current_position == Position.FLAT or self._entry_price is None:
            return False
        return self._net_roi_pct(price) >= self.take_profit_pct

    async def _price_watch_tick(self) -> None:
        """
        SATU siklus cek-dan-tindak untuk take-profit REAL-TIME --
        dipisah dari _price_watch_loop() supaya bisa diuji langsung
        tanpa perlu menunggu asyncio.sleep sungguhan.

        PENTING soal `self._action_lock`: loop candle (process_bar)
        dan loop pemantau harga ini jalan BERSAMAAN (asyncio, bukan
        thread, tapi tetap bisa saling menyela di titik `await`) --
        tanpa kunci ini, keduanya bisa sama-sama membaca
        self._current_position SEBELUM salah satu sempat memperbarui
        setelah aksi, lalu SAMA-SAMA mencoba menutup posisi yang sama
        (order dobel). Kunci memastikan cuma SATU dari keduanya yang
        boleh memutuskan & bertindak pada satu waktu.
        """
        if self._current_position == Position.FLAT or self._entry_price is None:
            return  # tidak ada posisi terbuka -- cek murah ini SEBELUM ambil kunci, hemat panggilan API

        loop = asyncio.get_event_loop()
        try:
            current_price = await loop.run_in_executor(None, self.broker.fetch_current_price, self.symbol)
        except Exception as e:
            print(f"  [price-watch] gagal ambil harga live ({e}) -- coba lagi poll berikutnya")
            return

        async with self._action_lock:
            # Cek ULANG di dalam kunci -- state mungkin sudah berubah
            # (mis. ditutup lewat candle) selagi menunggu fetch_current_price
            # tadi (itu panggilan jaringan, makan waktu, poin rawan interleaving).
            if self._current_position == Position.FLAT or self._entry_price is None:
                return
            if not self._check_take_profit_price(current_price):
                return

            net_roi = self._net_roi_pct(current_price)
            triggered_direction = self._current_position
            print(f"  [price-watch][take-profit] posisi {self._current_position} ROI BERSIH >= "
                  f"{self.take_profit_pct:.1%} (harga LIVE {current_price:.2f}, BERSIH {net_roi:.2%}) "
                  f"-- TUTUP PAKSA REAL-TIME, tidak menunggu candle tutup.")
            await self._handle_signal_change(Position.FLAT, override_price=current_price)

            if self._current_position == Position.FLAT:
                if self.stop_after_take_profit:
                    self._trading_halted = True
                    print("  [price-watch][take-profit] Penutupan terkonfirmasi FLAT -- "
                          "stop_after_take_profit AKTIF, BERHENTI trading total.")
                else:
                    self._blocked_direction = triggered_direction
                    self._reentry_armed = False
            else:
                print("  [price-watch][take-profit] Order penutup TERKIRIM tapi BELUM terkonfirmasi "
                      "flat -- dicoba lagi poll berikutnya.")

    async def _price_watch_loop(self, poll_interval_seconds: float) -> None:
        """
        Loop TERPISAH, jalan BERSAMAAN dengan loop candle utama (lihat
        run()) -- memantau harga tiap `poll_interval_seconds` detik,
        BUKAN menunggu candle tutup. Berhenti sendiri begitu
        _trading_halted True (tidak ada gunanya terus polling kalau
        program sudah berhenti trading total).
        """
        while not self._trading_halted:
            await self._price_watch_tick()
            await asyncio.sleep(poll_interval_seconds)

    # ------------------------------------------------------------------
    # SL + TP rasio di bursa
    # ------------------------------------------------------------------

    def _cancel_conditional_orders_safely(self, alasan: str) -> int:
        """Batalkan semua order bersyarat simbol ini. Gagal = dicetak, tidak menghentikan bot."""
        try:
            n = self.broker.cancel_conditional_orders(self.symbol)
            if n:
                print(f"  [bracket] {n} order bersyarat (SL/TP) dibatalkan -- {alasan}.")
            return n
        except Exception as e:
            print(f"  [bracket] GAGAL membatalkan order bersyarat ({alasan}): {e}")
            return -1

    def _reset_position_state(self) -> None:
        self._current_position = Position.FLAT
        self._entry_price = None
        self._entry_leverage = 1.0
        self._entry_taker_fee_pct = 0.0005
        self._tp_order_id = None
        self._sl_order_id = None
        self._bracket = None

    async def _place_bracket(self, direction: int, ref_price: float) -> None:
        """
        Pasang SL lalu TP di bursa untuk posisi yang BARU dibuka. SL dipasang
        lebih dulu karena itu pelindung utama. Kalau SL gagal dipasang atau
        tidak lolos verifikasi, posisi DITUTUP -- tidak pernah dibiarkan
        terbuka tanpa SL.
        """
        if self.tp_roi is not None:
            b = compute_bracket_roi(self._entry_price, direction, self._entry_leverage,
                                    self.tp_roi, self.sl_roi, self._entry_taker_fee_pct)
        else:
            atr = compute_atr(self._bars, self.sl_atr_period)
            b = compute_bracket(self._entry_price, direction, atr, self._entry_taker_fee_pct,
                                self.risk_reward, self.sl_atr_mult, self.sl_min_fee_mult)
        pos_side = "long" if direction == Position.LONG else "short"

        # Harga SUDAH melewati target? (terjadi saat posisi lama dipasangi
        # SL/TP baru setelah restart, atau SL yang sangat dekat.) Bursa akan
        # menolak order bersyarat yang langsung terpicu, jadi tutup sekarang.
        try:
            kini = self.broker.fetch_current_price(self.symbol)
        except Exception:
            kini = None
        if kini is not None:
            long = direction == Position.LONG
            lewat_tp = (kini >= b["tp_price"]) if long else (kini <= b["tp_price"])
            lewat_sl = (kini <= b["sl_price"]) if long else (kini >= b["sl_price"])
            if lewat_tp or lewat_sl:
                await self._close_target_already_reached("TP" if lewat_tp else "SL", kini, b, direction)
                return

        self._cancel_conditional_orders_safely("bersihkan sisa sebelum pasang SL/TP baru")

        try:
            sl_id = self.broker.place_stop_market(self.symbol, pos_side, b["sl_price"])
        except Exception as e:
            await self._emergency_close(ref_price, f"SL gagal dipasang di bursa ({e})")
            return
        self._sl_order_id = sl_id
        self._bracket = b

        try:
            self._tp_order_id = self.broker.place_take_profit_market(self.symbol, pos_side, b["tp_price"])
        except Exception as e:
            self._tp_order_id = None
            print(f"  [take-profit] GAGAL titip TP ke bursa ({e}) -- posisi TETAP dilindungi SL, "
                  f"keluar lewat SL atau reversal sinyal.")

        ok, detail = self._verify_stop_loss(b["sl_price"], sl_id)
        if not ok:
            self._cancel_conditional_orders_safely("SL tidak lolos verifikasi")
            await self._emergency_close(ref_price, detail)
            return

        notional = self.order_amount * self._entry_price
        rugi = notional * (b["sl_dist"] + b["fee_rt"])
        untung = notional * (b["tp_dist"] - b["fee_rt"])
        rasio = f"{b['rr']:.3g}" if b.get("rr") is not None else "?"
        if b.get("tp_roi") is not None:
            print(f"  [bracket] SL/TP dititipkan ke BURSA -- {pos_side.upper()} entry {self._entry_price:.2f}, "
                  f"leverage {b['leverage']:g}x, fee {self._entry_taker_fee_pct:.3%}/sisi:")
            print(f"     SL {b['sl_price']:.2f} (harga {b['sl_dist']:.3%} = ROI kotor -{b['sl_roi']:.2%}) "
                  f"-> rugi total ~{rugi:.2f} USDT termasuk fee")
            print(f"     TP {b['tp_price']:.2f} (harga {b['tp_dist']:.3%} = ROI kotor +{b['tp_roi']:.2%}) "
                  f"-> untung bersih ~{untung:.2f} USDT (rasio bersih {rasio}:1)")
            if untung <= 0:
                print("     PERINGATAN: TP ini TIDAK menutup fee -- kalau TP kena, hasilnya tetap RUGI.")
        else:
            atr_txt = f"{b['atr']:.2f}" if b["atr"] is not None else "belum tersedia"
            print(f"  [bracket] SL/TP dititipkan ke BURSA -- {pos_side.upper()} entry {self._entry_price:.2f}, "
                  f"ATR{self.sl_atr_period} {atr_txt}, fee {self._entry_taker_fee_pct:.3%}/sisi:")
            print(f"     SL {b['sl_price']:.2f} (jarak {b['sl_dist']:.3%}, dasar: {b['sl_basis']}) "
                  f"-> rugi bersih ~{rugi:.2f} USDT")
            print(f"     TP {b['tp_price']:.2f} (jarak {b['tp_dist']:.3%}) -> untung bersih ~{untung:.2f} USDT "
                  f"(rasio bersih {rasio}:1)")
        print(f"     {detail}")

    async def _close_target_already_reached(self, kind: str, kini: float, b: dict, direction: int) -> None:
        """Harga sudah di/lewat TP atau SL saat akan dipasang -> tutup sekarang, terapkan aturan TP/SL."""
        if kind == "TP":
            print(f"  [take-profit] harga {kini:.2f} SUDAH mencapai target TP {b['tp_price']:.2f} -- TUTUP PAKSA sekarang.")
        else:
            print(f"  [stop-loss] harga {kini:.2f} SUDAH melewati SL {b['sl_price']:.2f} -- posisi ditutup sekarang.")
        self._bracket = None
        self._cancel_conditional_orders_safely("harga sudah melewati target")
        await self._handle_signal_change(Position.FLAT, override_price=kini)
        if self._current_position != Position.FLAT:
            # Jangan biarkan posisi terbuka tanpa SL: jalur darurat mencoba
            # menutup tiap bar, lalu menghentikan bot setelah flat.
            print("  [stop-loss] order penutup belum terisi -- dialihkan ke penutupan DARURAT.")
            self._emergency_close_pending = True
            return
        if kind == "TP":
            if self.stop_after_take_profit:
                self._trading_halted = True
                print("  [take-profit] Penutupan terkonfirmasi FLAT -- stop_after_take_profit "
                      "AKTIF, program BERHENTI trading total (restart manual untuk lanjut).")
            else:
                self._blocked_direction = direction
                self._reentry_armed = False
        else:
            print("  [stop-loss] posisi TERTUTUP di sekitar harga SL.")
            self._blocked_direction = direction
            self._reentry_armed = False
        self._register_exit(kind)

    def _verify_stop_loss(self, expected: float, sl_id: str) -> tuple[bool, str]:
        """
        Pastikan SL BENAR-BENAR ada di bursa dengan harga pemicu yang benar.
        Menangkap jebakan migrasi Binance: order bersyarat yang dikirim dengan
        nama parameter lama DITERIMA tapi tanpa harga pemicu, sehingga tidak
        pernah terpicu.
        """
        try:
            orders = self.broker.fetch_open_conditional_orders(self.symbol)
        except Exception as e:
            return True, (f"verifikasi SL dilewati (gagal membaca order bursa: {e}) -- "
                          f"SL diterima bursa saat dikirim")
        cocok = [o for o in orders if str(o["id"]) == str(sl_id)]
        if not cocok:
            return False, f"SL id={sl_id} TIDAK ditemukan di daftar order bersyarat bursa"
        trig = cocok[0]["trigger_price"]
        if trig is None:
            return False, ("SL tercatat di bursa TANPA harga pemicu -- order seperti ini tidak akan "
                           "pernah terpicu (kemungkinan versi ccxt tidak cocok dengan endpoint Algo)")
        if abs(trig - expected) / expected > 0.0005:
            return False, f"harga pemicu SL di bursa {trig:.2f} beda dari yang diminta {expected:.2f}"
        return True, f"SL terverifikasi di bursa (pemicu {trig:.2f})"

    async def _emergency_close(self, ref_price: float, alasan: str) -> None:
        """Tutup posisi SEKARANG karena tidak terlindungi SL, lalu berhenti membuka posisi baru."""
        print("\n" + "!" * 70)
        print(f"  [stop-loss] DARURAT: {alasan}.")
        print("  Posisi TIDAK boleh dibiarkan terbuka tanpa SL -- ditutup SEKARANG. Bot berhenti")
        print("  membuka posisi baru sampai Anda memeriksa penyebabnya dan me-restart manual.")
        print("!" * 70 + "\n")
        self._bracket = None  # SL yang gagal tidak dipasang ulang
        self._emergency_close_pending = True
        await self._handle_signal_change(Position.FLAT, override_price=ref_price)
        self._finish_emergency_if_flat()

    async def _retry_emergency_close(self, df: pd.DataFrame) -> None:
        print("  [stop-loss] DARURAT: posisi belum tertutup -- mencoba menutup lagi.")
        await self._handle_signal_change(Position.FLAT, df)
        self._finish_emergency_if_flat()

    def _finish_emergency_if_flat(self) -> None:
        if self._current_position == Position.FLAT:
            self._emergency_close_pending = False
            self._trading_halted = True
            print("  [stop-loss] DARURAT selesai: posisi FLAT, bot BERHENTI membuka posisi. "
                  "Periksa log di atas, lalu restart manual.")

    def _restore_stop_loss(self) -> None:
        pos_side = "long" if self._current_position == Position.LONG else "short"
        try:
            self._sl_order_id = self.broker.place_stop_market(self.symbol, pos_side, self._bracket["sl_price"])
            self._tp_order_id = None
            print(f"  [stop-loss] order penutup belum terisi -- SL {self._bracket['sl_price']:.2f} DIPASANG "
                  f"ULANG supaya posisi tidak terbuka tanpa pelindung.")
        except Exception as e:
            print(f"  [stop-loss] GAGAL memasang ulang SL ({e}) -- posisi SEMENTARA TANPA SL, "
                  f"penutupan dicoba lagi bar berikutnya.")

    async def _check_bracket_exit(self) -> bool:
        """
        Kalau SL/TP sedang aktif tapi posisi di bursa sudah FLAT, berarti
        salah satunya kena (atau ditutup di luar bot). Bereskan sisa order,
        catat SL atau TP, terapkan aturan setelahnya. True kalau ini terjadi.
        Dipanggil di bawah _action_lock.
        """
        if self._bracket is None or self._current_position == Position.FLAT:
            return False
        try:
            actual = self.broker.fetch_position(self.symbol, quiet=True)
        except Exception as e:
            print(f"  [bracket] gagal cek posisi ({e}) -- dicoba lagi berikutnya")
            return False
        if actual is not None:
            return False

        b = self._bracket
        direction = self._current_position
        try:
            exit_price = self.broker.fetch_last_fill_price(self.symbol)
        except Exception:
            exit_price = None
        kind = classify_bracket_exit(exit_price, b)
        self._cancel_conditional_orders_safely("pasangan SL/TP yang tidak kena")
        self._reset_position_state()
        keluar = f"{exit_price:.2f}" if exit_price is not None else "?"

        if kind == "TP":
            print(f"  [take-profit] TP di BURSA KENA (pemicu {b['tp_price']:.2f}, keluar ~{keluar}) "
                  f"-- posisi TERTUTUP.")
            if self.stop_after_take_profit:
                self._trading_halted = True
                print("  [take-profit] Penutupan terkonfirmasi FLAT -- stop_after_take_profit "
                      "AKTIF, program BERHENTI trading total (restart manual untuk lanjut).")
            else:
                self._blocked_direction = direction
                self._reentry_armed = False
        else:
            if kind == "SL":
                print(f"  [stop-loss] SL di BURSA KENA (pemicu {b['sl_price']:.2f}, keluar ~{keluar}) "
                      f"-- posisi TERTUTUP.")
            else:
                print("  [stop-loss] posisi TERTUTUP tapi harga keluar tidak terbaca -- "
                      "diperlakukan sebagai STOP LOSS demi keamanan.")
            self._blocked_direction = direction
            self._reentry_armed = False
            print(f"  [stop-loss] arah {direction} DITAHAN sampai sinyal benar-benar berbalik "
                  f"(tidak langsung masuk lagi ke arah yang baru rugi).")
        self._register_exit(kind)
        return True

    # ------------------------------------------------------------------
    # Status tersimpan & kill switch
    # ------------------------------------------------------------------

    def _clock_ms(self) -> int:
        return int(time.time() * 1000)

    def _day_start_ms(self, ms: int) -> int:
        off = int(self.day_utc_offset_hours * 3_600_000)
        return ((ms + off) // 86_400_000) * 86_400_000 - off

    def _fmt_local(self, ms: int) -> str:
        tz = timezone(timedelta(hours=self.day_utc_offset_hours))
        return f"{datetime.fromtimestamp(ms / 1000, tz=tz):%Y-%m-%d %H:%M} (UTC{self.day_utc_offset_hours:+g})"

    def _persist_state(self) -> None:
        if not self.state_path or not getattr(self, "_state_loaded", False):
            return
        try:
            path = Path(self.state_path)
            semua = {}
            if path.exists():
                try:
                    semua = json.loads(path.read_text() or "{}")
                except Exception:
                    semua = {}
            semua[self.symbol] = {
                "blocked_direction": self._blocked_direction,
                "reentry_armed": bool(self._reentry_armed),
                "consecutive_sl": self._consecutive_sl,
                "sl_day_ms": self._sl_day_ms,
                "halt_until_ms": self._halt_until_ms,
                "halt_reason": self._halt_reason,
                "saved_at_ms": self._clock_ms(),
            }
            path.parent.mkdir(parents=True, exist_ok=True)
            tmp = path.with_name(path.name + ".tmp")
            tmp.write_text(json.dumps(semua, indent=2))
            os.replace(tmp, path)  # atomik: file tidak pernah setengah tertulis
        except Exception as e:
            print(f"  (Peringatan: gagal menyimpan status bot ke {self.state_path}: {e})")

    def _load_persistent_state(self) -> None:
        """Pulihkan status dari sesi sebelumnya. Setelah ini, perubahan status disimpan."""
        self._state_loaded = True
        if not self.state_path:
            return
        path = Path(self.state_path)
        if not path.exists():
            print(f"  [state] belum ada status tersimpan ({self.state_path}) -- mulai bersih.")
            return
        try:
            data = (json.loads(path.read_text() or "{}") or {}).get(self.symbol)
        except Exception as e:
            print(f"  [state] status tersimpan tidak bisa dibaca ({e}) -- mulai bersih.")
            return
        if not data:
            return
        self._state_saved_at_ms = data.get("saved_at_ms")
        self._blocked_direction_v = data.get("blocked_direction")   # langsung: jangan tulis ulang saat memuat
        self._reentry_armed_v = bool(data.get("reentry_armed"))
        self._consecutive_sl = int(data.get("consecutive_sl") or 0)
        self._sl_day_ms = data.get("sl_day_ms")
        if self._sl_day_ms is not None and self._sl_day_ms != self._day_start_ms(self._clock_ms()):
            self._consecutive_sl = 0          # hitungan SL beruntun berlaku per hari
        h = data.get("halt_until_ms")
        if h and h > self._clock_ms():
            self._halt_until_ms, self._halt_reason = h, data.get("halt_reason") or ""
        bagian = []
        if self._blocked_direction_v is not None:
            bagian.append(f"arah {self._blocked_direction_v} DITAHAN"
                          f"{' (sudah siap masuk lagi)' if self._reentry_armed_v else ''}")
        if self._consecutive_sl:
            bagian.append(f"{self._consecutive_sl} SL beruntun hari ini")
        if self._halt_until_ms:
            bagian.append(f"batas harian aktif sampai {self._fmt_local(self._halt_until_ms)}")
        if bagian:
            print(f"  [state] status sesi sebelumnya dipulihkan: {'; '.join(bagian)}.")

    def _reconcile_restored_block(self) -> None:
        """
        Arah yang ditahan dari sesi lalu dicabut kalau sinyal SEMPAT berbalik
        selama bot mati (dicek dari candle backfill sesudah status terakhir
        disimpan). Kalau backfill tidak menjangkau waktu itu, blokir
        dipertahankan -- pilihan yang aman.
        """
        d = self._blocked_direction
        if d is None or not self._state_saved_at_ms or len(self._bars) < 2:
            return
        if self._bars[0]["timestamp"] > self._state_saved_at_ms:
            print("  [state] histori backfill tidak menjangkau saat bot terakhir jalan -- "
                  f"arah {d} tetap DITAHAN (aman).")
            return
        try:
            sinyal = list(self.strategy.generate_signals(self._bars_to_df()).astype(int))
        except Exception as e:
            print(f"  [state] gagal mengecek sinyal selama bot mati ({e}) -- arah {d} tetap DITAHAN.")
            return
        sesudah = [v for b, v in zip(self._bars, sinyal) if b["timestamp"] > self._state_saved_at_ms]
        if any(v == -d for v in sesudah):
            print(f"  [state] sinyal sempat berbalik selama bot mati -- arah {d} tidak lagi ditahan.")
            self._blocked_direction = None
            self._reentry_armed = False
        else:
            print(f"  [state] arah {d} tetap DITAHAN dari sesi sebelumnya sampai sinyal berbalik.")

    def _register_exit(self, kind: str) -> None:
        """Catat hasil keluar lewat TP/SL untuk kill switch, lalu evaluasi."""
        if kind == "TP":
            self._consecutive_sl = 0
        else:  # "SL" atau "UNKNOWN" (diperlakukan sebagai SL)
            hari = self._day_start_ms(self._clock_ms())
            if self._sl_day_ms != hari:
                self._consecutive_sl, self._sl_day_ms = 0, hari
            self._consecutive_sl += 1
        self._persist_state()
        if self.max_consecutive_sl or self.max_daily_loss_pct:
            self._evaluate_kill_switch()

    def _evaluate_kill_switch(self) -> None:
        if self._halt_until_ms is not None:
            return
        now = self._clock_ms()
        hari = self._day_start_ms(now)
        alasan = None
        if (self.max_consecutive_sl and self._sl_day_ms == hari
                and self._consecutive_sl >= self.max_consecutive_sl):
            alasan = f"{self._consecutive_sl} SL beruntun hari ini (batas {self.max_consecutive_sl})"
        if alasan is None and self.max_daily_loss_pct:
            try:
                r = self.broker.fetch_realized_pnl_since(self.symbol, hari)
                bersih = float(r["realized"]) - float(r["fee"])
                saldo = float(((self.broker.fetch_balance() or {}).get("USDT") or {}).get("total") or 0)
                awal = saldo - bersih
                if bersih < 0 and awal > 0 and -bersih / awal * 100 >= self.max_daily_loss_pct:
                    alasan = (f"rugi bersih hari ini {bersih:.2f} USDT = {-bersih / awal * 100:.2f}% "
                              f"saldo awal hari (batas {self.max_daily_loss_pct:g}%)")
            except Exception as e:
                print(f"  [batas] gagal menghitung rugi harian ({e}) -- dicek lagi di penutupan berikutnya.")
        if alasan:
            self._halt_until_ms, self._halt_reason = hari + 86_400_000, alasan
            print("\n" + "!" * 70)
            print(f"  [batas] BATAS HARIAN TERCAPAI: {alasan}.")
            print(f"  Entry baru ditahan sampai {self._fmt_local(self._halt_until_ms)}. "
                  f"Posisi yang sudah tertutup tidak dibuka lagi sampai hari berganti.")
            print("!" * 70 + "\n")
            self._persist_state()

    def _limit_halt_active(self) -> bool:
        if self._halt_until_ms is None:
            return False
        if self._clock_ms() >= self._halt_until_ms:
            print("  [batas] hari berganti -- batas harian dicabut, bot boleh membuka posisi lagi.")
            self._halt_until_ms, self._halt_reason, self._consecutive_sl = None, "", 0
            self._persist_state()
            return False
        return True

    def _compute_status_note(self, latest_signal: int) -> str:
        """
        Kalimat singkat: kenapa bot TIDAK membuka posisi padahal sinyal ada.
        Kosong = tidak ada yang perlu dijelaskan.

        HATI-HATI dengan teksnya: dashboard mematikan bot kalau ada SATU baris
        log yang memuat dua penanda take-profit-selesai sekaligus, dan
        notifier membaca "[take-profit]"/"[stop-loss]" untuk alasan
        penutupan. Teks di sini sengaja menghindari semua penanda itu
        (diuji di smoke_reentry).
        """
        if self._emergency_close_pending:
            return "DARURAT: SL gagal terpasang, bot sedang menutup posisi"
        if self._trading_halted:
            return "DIHENTIKAN: tidak membuka posisi baru (jalankan ulang bot untuk lanjut)"
        if self._halt_until_ms is not None and self._clock_ms() < self._halt_until_ms:
            return (f"BATAS HARIAN: {self._halt_reason}. Entry baru ditahan sampai "
                    f"{self._fmt_local(self._halt_until_ms)}")
        if (self.reentry_mode == "midline" and self._blocked_direction is not None
                and latest_signal == self._blocked_direction and self._current_position != latest_signal):
            long = latest_signal == Position.LONG
            arah = "LONG" if long else "SHORT"
            try:
                ch = self.reentry_channel_fn(self._bars)
            except Exception:
                ch = None
            if not ch:
                return f"DITAHAN {arah}: menunggu data channel"
            atas, bawah = ch
            tengah = (atas + bawah) / 2
            batas = atas if long else bawah
            if self._reentry_armed:
                return (f"DITAHAN {arah} (sudah siap): masuk begitu close "
                        f"{'di atas' if long else 'di bawah'} {batas:.2f}")
            return (f"DITAHAN {arah}: belum siap. Tunggu close "
                    f"{'turun ke' if long else 'naik ke'} {tengah:.2f} (tengah channel) "
                    f"{'atau lebih rendah' if long else 'atau lebih tinggi'}, "
                    f"baru masuk saat close {'di atas' if long else 'di bawah'} {batas:.2f}")
        return ""

    async def _maybe_reenter(self, df: pd.DataFrame, direction: int) -> None:
        """
        Mode "midline": arah `direction` sedang diblokir setelah TP/SL.
          1. Tunggu close KEMBALI ke tengah channel -> status "siap".
          2. Setelah siap, close menembus batas channel searah (breakout
             baru) -> blokir dicabut, masuk lagi ke arah itu.
        Channel dihitung dari `lookback` bar SEBELUM bar ini (rumus yang
        sama dengan sinyal), jadi "menembus" di sini sama artinya dengan
        breakout yang memicu sinyal.
        """
        try:
            ch = self.reentry_channel_fn(self._bars)
        except Exception as e:
            print(f"  [re-entry] gagal menghitung channel ({e}) -- dicoba lagi bar berikutnya")
            return
        if not ch:
            return
        atas, bawah = ch
        tengah = (atas + bawah) / 2
        close = float(df["close"].iloc[-1])
        long = direction == Position.LONG
        arah = "LONG" if long else "SHORT"

        if not self._reentry_armed:
            if (close <= tengah) if long else (close >= tengah):
                self._reentry_armed = True
                batas = atas if long else bawah
                print(f"  [re-entry] harga {close:.2f} kembali ke tengah channel ({tengah:.2f}) -- SIAP masuk "
                      f"{arah} lagi begitu close menembus {'atas' if long else 'bawah'} {batas:.2f}.")
            return

        if (close > atas) if long else (close < bawah):
            batas = atas if long else bawah
            print(f"  [re-entry] BREAKOUT BARU searah: close {close:.2f} menembus "
                  f"{'atas' if long else 'bawah'} {batas:.2f} -- masuk {arah} lagi.")
            self._blocked_direction = None
            self._reentry_armed = False
            await self._handle_signal_change(direction, df)

    async def _bracket_watch_loop(self, poll_interval_seconds: float) -> None:
        """
        Cek ringan tiap beberapa detik: apakah SL/TP sudah menutup posisi.
        Eksekusinya tetap oleh BURSA; loop ini cuma membereskan sisa order
        dan mencatat hasilnya cepat (tidak menunggu candle tutup), supaya
        notifier Gmail sempat membaca alasannya dari log.
        """
        loop = asyncio.get_event_loop()
        while not self._trading_halted:
            await asyncio.sleep(poll_interval_seconds)
            if self._bracket is None or self._current_position == Position.FLAT:
                continue
            try:
                actual = await loop.run_in_executor(
                    None, lambda: self.broker.fetch_position(self.symbol, quiet=True))
            except Exception as e:
                print(f"  [bracket] gagal cek posisi ({e}) -- dicoba lagi berikutnya")
                continue
            if actual is not None:
                continue
            async with self._action_lock:
                await self._check_bracket_exit()

    async def _ensure_bracket_after_recovery(self) -> None:
        """
        Setelah restart dengan posisi terbuka: pakai SL/TP yang sudah ada di
        bursa (dari sesi sebelumnya) kalau ada; kalau tidak ada SL, pasang
        sekarang. Posisi tidak boleh terbuka tanpa SL.
        """
        if self._current_position == Position.FLAT:
            return
        try:
            orders = self.broker.fetch_open_conditional_orders(self.symbol)
        except Exception as e:
            print(f"  [startup] gagal membaca SL/TP di bursa ({e}) -- SL/TP dipasang ulang.")
            orders = []
        sls = [o for o in orders if o["type"].startswith("STOP") and o["trigger_price"]]
        tps = [o for o in orders if o["type"].startswith("TAKE_PROFIT")]
        if self.tp_roi is not None and self._entry_price:
            # Mode ROI hasilnya PASTI dari harga masuk + leverage, jadi SL/TP
            # yang tidak cocok dengan pengaturan sekarang (mis. sisa mode
            # rasio/ATR) diganti. Mode ATR tidak begini: SL-nya sengaja tetap.
            b = compute_bracket_roi(self._entry_price, self._current_position, self._entry_leverage,
                                    self.tp_roi, self.sl_roi, self._entry_taker_fee_pct)
            cocok = lambda o, harga: o["trigger_price"] and abs(o["trigger_price"] - harga) / harga <= 0.0005
            if not (sls and tps and cocok(sls[0], b["sl_price"]) and cocok(tps[0], b["tp_price"])):
                lama = (f"SL {sls[0]['trigger_price']:.2f}" if sls else "tanpa SL") + \
                       (f" / TP {tps[0]['trigger_price']:.2f}" if tps and tps[0]["trigger_price"] else " / tanpa TP")
                print(f"  [startup] SL/TP di bursa ({lama}) TIDAK sesuai pengaturan sekarang (TP ROI "
                      f"+{self.tp_roi:.2%}, SL ROI -{self.sl_roi:.2%}) -- DIGANTI dari harga masuk "
                      f"{self._entry_price:.2f}.")
                await self._place_bracket(self._current_position, self._entry_price)
                return
        if sls:
            self._sl_order_id = sls[0]["id"]
            self._tp_order_id = tps[0]["id"] if tps else None
            self._bracket = {"sl_price": sls[0]["trigger_price"],
                             "tp_price": tps[0]["trigger_price"] if tps else None, "adopted": True}
            tp_txt = f"{tps[0]['trigger_price']:.2f}" if tps and tps[0]["trigger_price"] else "tidak ada"
            print(f"  [startup] SL {sls[0]['trigger_price']:.2f} dan TP {tp_txt} di bursa DIPAKAI dari "
                  f"sesi sebelumnya -- tidak dipasang ulang.")
            return
        ref = self._entry_price
        if ref is None:
            try:
                ref = self.broker.fetch_current_price(self.symbol)
            except Exception:
                ref = None
        if ref is None:
            print("  [startup] posisi terbuka TANPA SL dan harga acuan tidak terbaca -- ditutup demi keamanan.")
            self._emergency_close_pending = True
            return
        self._entry_price = ref
        print(f"  [startup] posisi TERBUKA TANPA SL di bursa -- SL/TP dipasang sekarang dari harga {ref:.2f}.")
        await self._place_bracket(self._current_position, ref)

    def _recover_position_from_exchange(self) -> None:
        """
        Dipanggil SEKALI di awal run() -- kalau TERNYATA sudah ada
        posisi terbuka di bursa (mis. sisa sesi SEBELUMNYA yang
        di-Ctrl+C lalu program di-restart), pulihkan self._current_position
        DAN self._entry_price/_entry_leverage/_entry_taker_fee_pct dari
        bursa -- supaya:

        1. Bar pertama TIDAK salah kira ini skenario "reversal" dan
           mengirim order PENUTUP yang tidak perlu (posisi yang sudah
           benar sesuai sinyal tidak akan diutak-atik).
        2. Take-profit TETAP bisa melindungi posisi lama ini juga --
           bukan cuma posisi baru yang dibuka instance ini sendiri.

        TANPA INI: __init__ selalu mulai dari asumsi FLAT, dan
        _sync_position_from_exchange() yang ADA di dalam
        _handle_signal_change() BARU jalan setelah sinyal dianggap
        "berubah" (karena dibandingkan ke asumsi FLAT yang salah) --
        titik itu SUDAH TERLAMBAT, cabang kode di situ tidak mengecek
        apakah posisi yang baru terkoreksi itu sebenarnya SUDAH SESUAI
        dengan yang diminta sinyal, jadi tetap kirim order penutup.
        """
        try:
            actual = self.broker.fetch_position(self.symbol)
        except Exception as e:
            print(f"  [startup] gagal cek posisi awal ({e}) -- mulai dari asumsi FLAT seperti biasa")
            return

        if actual is None:
            return  # memang flat, tidak ada yang perlu dipulihkan

        self._current_position = Position.LONG if actual["side"] == "long" else Position.SHORT
        self._entry_leverage = actual.get("leverage") or 1.0
        entry_price = actual.get("entry_price")

        print(f"  [startup] Posisi SUNGGUHAN terdeteksi saat program mulai: {self._current_position} "
              f"(kemungkinan sisa sesi sebelumnya) -- dipulihkan ke state internal, BUKAN dianggap flat.")

        if entry_price is not None:
            self._entry_price = float(entry_price)
            try:
                fee = self.broker.fetch_trading_fee_pct(self.symbol)
                if fee is not None:
                    self._entry_taker_fee_pct = fee
            except Exception:
                pass  # biarkan fallback konservatif yang sudah ada di __init__
            print(f"  [startup] entry_price dipulihkan: {self._entry_price:.2f}, leverage: "
                  f"{self._entry_leverage:.0f}x -- take-profit AKTIF untuk posisi lama ini juga.")
        else:
            print(f"  [startup] entry_price TIDAK tersedia dari bursa untuk posisi ini -- "
                  f"take-profit TIDAK AKTIF untuk posisi lama ini (cuma untuk posisi BARU yang "
                  f"dibuka instance ini sendiri), sampai posisi ini ditutup dan dibuka ulang.")

    def _sync_position_from_exchange(self) -> None:
        """
        Cek posisi SUNGGUHAN ke bursa dan perbaiki self._current_position
        kalau ternyata berbeda dari yang tercatat internal -- INI YANG
        MEMPERBAIKI bug reduceOnly rejected: order limit pasif yang
        tidak langsung fill saat dikirim bisa terisi BELAKANGAN tanpa
        program pernah tahu, membuat posisi internal "basi" (stale)
        dibanding posisi sungguhan di bursa. Dipanggil di AWAL
        _handle_signal_change(), SEBELUM memutuskan order apa yang
        perlu dikirim -- supaya keputusan selalu berbasis kenyataan
        di bursa, bukan asumsi status sesaat saat order dulu dikirim.

        JUGA mencoba MENJELASKAN penyebab drift -- BUKAN asal bilang
        "kemungkinan order sebelumnya terisi belakangan" tanpa
        verifikasi (klaim itu pernah terbukti salah: posisi bisa
        berubah karena INTERVENSI MANUAL atau proses LAIN, bukan cuma
        order bot yang telat). Lihat _explain_position_drift().
        """
        try:
            actual = self.broker.fetch_position(self.symbol)
        except Exception as e:
            print(f"  [sync] gagal cek posisi sungguhan ({e}) -- pakai posisi tercatat apa adanya")
            return

        actual_position = Position.FLAT
        if actual is not None:
            actual_position = Position.LONG if actual["side"] == "long" else Position.SHORT

        if actual_position != self._current_position:
            explanation = self._explain_position_drift(actual_position)
            print(f"  [sync] POSISI TERCATAT ({self._current_position}) BEDA dari posisi "
                  f"SUNGGUHAN di bursa ({actual_position}). {explanation}")
            self._current_position = actual_position

    def _explain_position_drift(self, actual_position: int) -> str:
        """
        Coba jelaskan APAKAH drift ini bisa dijelaskan oleh order yang
        BENAR-BENAR kita kirim sendiri -- dengan VERIFIKASI ke bursa,
        bukan tebakan. Return satu kalimat siap cetak.

        Syarat "dijelaskan oleh order kita": order TERAKHIR yang kita
        kirim (self._last_order_info) punya target_position yang SAMA
        dengan actual_position yang baru terdeteksi ini, DAN order itu
        SUNGGUHAN berstatus filled kalau dicek ulang ke bursa.
        """
        if self._last_order_info is None:
            return ("TIDAK bisa dijelaskan oleh order bot manapun (belum pernah kirim order sama "
                    "sekali di sesi ini) -- kemungkinan INTERVENSI MANUAL atau proses LAIN.")

        if self._last_order_info["target_position"] != actual_position:
            return (f"TIDAK bisa dijelaskan oleh order bot -- order terakhir yang kita kirim "
                    f"(id={self._last_order_info['client_order_id']}) menuju posisi "
                    f"{self._last_order_info['target_position']}, BUKAN {actual_position} yang "
                    f"terdeteksi sekarang. Kemungkinan INTERVENSI MANUAL atau proses LAIN.")

        try:
            order = self.broker.fetch_order_by_client_id(self._last_order_info["client_order_id"], self.symbol)
        except Exception as e:
            return f"Tidak bisa diverifikasi ke bursa ({e}) -- diperbaiki ke posisi sungguhan apa adanya."

        if order is not None and order.status == OrderStatus.FILLED:
            return (f"Dikonfirmasi: order KITA SENDIRI (id={self._last_order_info['client_order_id']}) "
                    f"yang terisi belakangan -- bukan intervensi luar.")

        status_str = order.status.value if order is not None else "tidak ditemukan"
        return (f"TIDAK bisa dijelaskan oleh order bot -- order terakhir kita (id="
                f"{self._last_order_info['client_order_id']}) statusnya '{status_str}', bukan filled. "
                f"Kemungkinan INTERVENSI MANUAL atau proses LAIN.")

    async def _handle_signal_change(
        self, new_position: int, df: pd.DataFrame | None = None, override_price: float | None = None
    ) -> None:
        """
        df: WAJIB diisi kalau new_position bisa berupa "buka posisi
            baru" (dipakai _entry_blocked_by_session(df)) -- yaitu
            SEMUA pemanggilan normal dari process_bar(). Boleh None
            HANYA kalau dipanggil dengan new_position=Position.FLAT
            DAN override_price diisi (dari _price_watch_tick() --
            take-profit real-time TIDAK PERNAH membuka posisi baru,
            cuma menutup, jadi df tidak pernah benar-benar dibutuhkan
            di jalur itu).
        override_price: kalau diisi, dipakai sebagai signal_price
            LANGSUNG -- BUKAN diambil dari df["close"]. Dipakai
            _price_watch_tick() supaya order penutup take-profit
            real-time memakai harga LIVE saat itu, bukan close candle
            terakhir yang mungkin sudah beberapa detik/menit basi.
        """
        self._sync_position_from_exchange()  # cek posisi sungguhan dulu -- lihat docstring di atas

        if self._current_position == new_position:
            # PENTING: sync di atas mungkin BARU SAJA mengoreksi posisi
            # jadi PERSIS yang diminta new_position -- kalau begitu,
            # TIDAK ADA yang perlu dikirim sama sekali. TANPA cek ini,
            # kode di bawah akan salah kira ini skenario "reversal"
            # (karena cuma mengecek "posisi FLAT atau tidak", tanpa
            # bandingkan ke new_position) dan mengirim order PENUTUP
            # yang tidak perlu untuk posisi yang sudah benar. Ini yang
            # terjadi kalau program di-restart sambil masih ada posisi
            # terbuka -- lihat _recover_position_from_exchange().
            return

        signal_price = override_price if override_price is not None else float(df["close"].iloc[-1])
        signal_time = datetime.now(timezone.utc)

        side = None
        is_closing = False
        # target_after_this_order: posisi internal yang akan di-set
        # KALAU order ini berhasil. TIDAK SELALU sama dengan new_position
        # -- lihat penjelasan di bawah soal kenapa flip harus dua langkah.
        target_after_this_order = new_position

        if self._current_position != Position.FLAT:
            side = "sell" if self._current_position == Position.LONG else "buy"  # tutup posisi lama
            is_closing = True
            # PENTING: reduceOnly SECARA DEFINISI cuma bisa membawa posisi
            # ke FLAT (nol) -- TIDAK BISA sekaligus membalik ke arah
            # berlawanan dalam satu order yang sama. Kalau new_position
            # ternyata bukan FLAT (mis. langsung SHORT -> LONG, "flip"),
            # order INI cuma bertugas menutup ke FLAT dulu -- bursa akan
            # MENOLAK (error -2022) kalau kita berharap satu order
            # reduceOnly ini juga sekalian membuka posisi baru.
            # Pembukaan posisi baru diserahkan ke evaluasi bar
            # BERIKUTNYA (saat posisi internal sudah benar FLAT), bukan
            # dipaksakan di order yang sama.
            target_after_this_order = Position.FLAT
        elif new_position != Position.FLAT:
            if self._entry_blocked_by_session(df):
                # Sisa waktu sesi lebih pendek dari min_entry_buffer_hours
                # -- JANGAN buka posisi baru, biarkan FLAT. Ini TIDAK
                # mempengaruhi penutupan posisi yang sudah terbuka (itu
                # tidak lewat cabang ini sama sekali).
                self._current_position = Position.FLAT
                return
            side = "buy" if new_position == Position.LONG else "sell"  # buka posisi baru
            target_after_this_order = new_position

        if side is None:
            self._current_position = new_position
            return

        send_reduce_only = is_closing and self.use_reduce_only

        print(f"  [signal] posisi {self._current_position} -> {target_after_this_order}"
              f"{f' (tujuan akhir {new_position}, butuh 1 langkah lagi)' if target_after_this_order != new_position else ''}"
              f", kirim order {side}{' (reduceOnly)' if send_reduce_only else ''}")

        # WAJIB: batalkan order lama yang masih nangkring (belum ke-fill)
        # untuk simbol ini SEBELUM kirim yang baru -- lihat docstring
        # cancel_open_orders() soal kenapa ini krusial (mencegah order
        # menumpuk yang bikin reduceOnly ditolak bursa).
        try:
            self.order_manager.cancel_open_orders(self.symbol)
        except Exception as e:
            print(f"  [signal] gagal cek/batalkan order lama: {e} -- tetap coba kirim order baru")

        # Order BERSYARAT (SL/TP di endpoint Algo) TIDAK terbaca oleh
        # cancel_open_orders() di atas -- endpoint-nya berbeda sejak migrasi
        # Binance 2025-12-09. Tanpa ini, TP/SL posisi lama tertinggal di
        # bursa setelah posisi ditutup lewat reversal sinyal. Hanya dipanggil
        # kalau memang ada yang mungkin tertinggal, jadi mode lama tanpa
        # TP bursa tidak terpengaruh.
        if self._bracket is not None or self._tp_order_id is not None or self._bracket_enabled:
            self._cancel_conditional_orders_safely("sebelum kirim order baru")

        order_price = self._aggressive_price(side, signal_price)

        order_amount = self.order_amount
        if is_closing and side == "sell" and not self.use_reduce_only:
            # SPOT: jual sebesar saldo BENERAN yang dimiliki, BUKAN
            # order_amount nominal yang dipakai saat beli -- fee spot
            # biasanya dipotong dari aset yang DITERIMA saat beli, jadi
            # saldo asli SEDIKIT LEBIH KECIL dari order_amount. Ditemukan
            # di lapangan: order jual dengan amount persis sama seperti
            # order beli ditolak bursa ("insufficient balance").
            try:
                base_asset = self.symbol.split("/")[0]
                actual_balance = self.broker.fetch_free_balance(base_asset)
                if actual_balance > 0:
                    order_amount = min(self.order_amount, actual_balance)
                    if order_amount < self.order_amount:
                        print(f"  [signal] saldo {base_asset} aktual ({actual_balance}) < order_amount "
                              f"({self.order_amount}) -- jual saldo aktual saja")
            except Exception as e:
                print(f"  [signal] gagal cek saldo aktual ({e}) -- tetap coba order_amount apa adanya")

        try:
            result = self.order_manager.submit_limit_order(
                symbol=self.symbol, side=side, amount=order_amount, price=order_price,
                reduce_only=send_reduce_only,
            )
        except Exception as e:
            print(f"  [signal] gagal kirim order: {e} -- posisi TIDAK diubah, dievaluasi ulang bar berikutnya")
            return

        # Catat SEGERA setelah order berhasil terkirim (terlepas statusnya
        # sudah "filled" atau belum) -- dipakai _sync_position_from_exchange()
        # untuk membedakan "posisi berubah karena order KITA yang telat
        # terisi" vs "posisi berubah tanpa order bot yang bisa menjelaskan
        # (kemungkinan intervensi manual atau proses lain)".
        self._last_order_info = {
            "client_order_id": result.client_order_id,
            "target_position": target_after_this_order,
        }

        status = OrderManager.classify_result(result)
        if status in ("filled", "partial"):
            signal = SignalContext(signal_time=signal_time, signal_price=signal_price)
            self.slippage_tracker.record_fill(signal, result)

        if is_closing and status != "filled" and self._bracket is not None:
            # SL sudah dibatalkan di atas, tapi posisi BELUM tertutup --
            # jangan biarkan terbuka tanpa pelindung sampai bar berikutnya.
            self._restore_stop_loss()

        if status == "filled":
            self._current_position = target_after_this_order
            if target_after_this_order == Position.FLAT:
                self._entry_price = None  # posisi ditutup -- tidak ada entry aktif lagi
                self._entry_leverage = 1.0
                self._entry_taker_fee_pct = 0.0005  # kembali ke fallback konservatif
                if self.max_daily_loss_pct:
                    self._evaluate_kill_switch()
                self._tp_order_id = None  # posisi tutup -- id TP lama tidak relevan lagi
                self._sl_order_id = None
                self._bracket = None
            else:
                # posisi BARU dibuka -- catat harga eksekusi SUNGGUHAN
                # (average_price), bukan harga limit yang diminta,
                # untuk basis perhitungan take-profit yang akurat.
                self._entry_price = result.average_price if result.average_price is not None else order_price
                # Ambil leverage SUNGGUHAN dari bursa (BUKAN diasumsikan/
                # dihardcode) -- supaya kalau Anda ganti leverage di UI
                # Binance, take-profit ikut menyesuaikan otomatis, bukan
                # diam-diam pakai angka lama.
                try:
                    pos_info = self.broker.fetch_position(self.symbol)
                    self._entry_leverage = pos_info["leverage"] if pos_info else 1.0
                except Exception as e:
                    print(f"  [signal] gagal ambil leverage dari bursa ({e}) -- take-profit "
                          f"pakai fallback leverage=1.0 (lebih sulit tercapai, bukan lebih mudah)")
                    self._entry_leverage = 1.0

                # Ambil taker fee SUNGGUHAN dari akun -- dipakai
                # mengurangi ROI kotor sebelum dibandingkan ke
                # take_profit_pct, supaya ambang itu benar-benar berarti
                # UNTUNG BERSIH (setelah fee), bukan kotor. None -> pakai
                # fallback konservatif (0,05%) yang SUDAH di-set di __init__,
                # BUKAN diam-diam dianggap 0 (itu akan membuat take-profit
                # terlalu gampang terpicu, berpotensi rugi bersih tanpa Anda
                # sadari -- lihat insiden lookback=10 sebelumnya).
                try:
                    fee = self.broker.fetch_trading_fee_pct(self.symbol)
                    if fee is not None:
                        self._entry_taker_fee_pct = fee
                    else:
                        print(f"  [signal] bursa tidak beri tahu fee taker -- pakai fallback "
                              f"{self._entry_taker_fee_pct:.3%} (konservatif, BELUM diverifikasi ke akun Anda)")
                except Exception as e:
                    print(f"  [signal] gagal ambil fee dari bursa ({e}) -- pakai fallback "
                          f"{self._entry_taker_fee_pct:.3%}")

                # Titipkan TAKE-PROFIT ke BURSA -- dipasang DI SINI,
                # setelah _entry_price/_entry_leverage/_entry_taker_fee_pct
                # ketiganya sudah terisi, karena harga triggernya
                # diturunkan dari ketiganya. Dipasang lebih awal =
                # dihitung dari angka yang belum final.
                #
                # Rumusnya adalah KEBALIKAN _net_roi_pct(): cari harga
                # yang membuat net_roi PERSIS sama dengan take_profit_pct.
                #   net = raw*L - 2*f*L >= TP   ->   raw >= TP/L + 2*f
                # Perhatikan suku fee TIDAK ikut terbagi leverage.
                if self._bracket_enabled:
                    await self._place_bracket(target_after_this_order, signal_price)
                elif self.take_profit_pct is not None:
                    direction = 1 if target_after_this_order == Position.LONG else -1
                    move = self.take_profit_pct / self._entry_leverage + 2 * self._entry_taker_fee_pct
                    tp_price = self._entry_price * (1 + direction * move)
                    try:
                        self._tp_order_id = self.broker.place_take_profit_market(
                            self.symbol, "long" if direction == 1 else "short", tp_price)
                        print(f"  [take-profit] dititipkan ke BURSA di {tp_price:.2f} "
                              f"({move:+.4%} dari entry {self._entry_price:.2f}) -- "
                              f"Binance yang eksekusi, bukan bot ini. id={self._tp_order_id}")
                    except Exception as e:
                        # JANGAN diam -- kalau titip gagal, satu-satunya
                        # pengaman yang tersisa adalah pemantauan internal,
                        # dan itu cuma jalan kalau --live-take-profit-poll-seconds
                        # diisi. Tanpa keduanya, posisi ini TIDAK punya TP sama sekali.
                        self._tp_order_id = None
                        print(f"  [take-profit] GAGAL titip ke bursa ({e}) -- posisi ini "
                              f"bergantung sepenuhnya pada pemantauan internal. Pastikan "
                              f"--live-take-profit-poll-seconds aktif, kalau tidak posisi ini TANPA TP.")
        # kalau partial/open/rejected: posisi belum berubah penuh,
        # dibiarkan -- bar berikutnya mengevaluasi ulang otomatis.


if __name__ == "__main__":
    import sys

    if "--live" in sys.argv:
        # INI JALUR SUNGGUHAN -- butuh kredensial testnet DAN jaringan.
        # TIDAK dijalankan otomatis; user harus eksplisit tambahkan --live.
        import argparse

        from scripts.run_backtest import build_params  # reuse logika override
        # parameter yang sama seperti run_backtest.py -- supaya cuma ADA
        # SATU cara di seluruh proyek buat override parameter strategi,
        # bukan dua cara berbeda yang bisa beda perilaku diam-diam.
        from src.execution.broker import Broker
        from src.strategy.donchian_breakout import DonchianBreakoutParams, DonchianBreakoutStrategy

        parser = argparse.ArgumentParser(description="Jalankan paper trading Hari 3 di testnet")
        parser.add_argument("--live", action="store_true")
        parser.add_argument("--symbol", default="BTC/USDT:USDT")
        parser.add_argument("--timeframe", default="2h")
        parser.add_argument("--amount", type=float, default=0.01)
        parser.add_argument(
            "--exchange",
            default="binanceusdm",
            help="'binanceusdm' (default, futures) atau 'binance' (spot). "
            "Kredensial diambil dari env var <EXCHANGE>_TESTNET_API_KEY/SECRET "
            "-- untuk spot berarti BINANCE_TESTNET_API_KEY/SECRET (env var BEDA "
            "dari futures, meski mungkin nilai key-nya sama kalau satu akun Demo "
            "Trading mencakup keduanya).",
        )
        parser.add_argument(
            "--param",
            action="append",
            default=[],
            help="override parameter strategi, format key=value, bisa diulang "
            "(mis. --param entry_window_bars=5) -- HANYA untuk validasi pipa, "
            "BUKAN nilai yang sudah tervalidasi Hari 2",
        )
        parser.add_argument(
            "--session-hours",
            type=float,
            default=None,
            help="kalau diisi (mis. 8), program akan mencetak PERINGATAN kalau "
            "mendekati batas waktu ini DAN posisi masih terbuka -- TIDAK otomatis "
            "menutup posisi, cuma mengingatkan. Default: tanpa batas waktu.",
        )
        parser.add_argument(
            "--min-entry-buffer-hours",
            type=float,
            default=None,
            help="kalau diisi (mis. 40, samakan dengan durasi max_hold_bars strategi "
            "dalam jam), posisi BARU TIDAK akan dibuka kalau sisa waktu sesi lebih "
            "pendek dari ini -- mencegah buka posisi yang kemungkinan besar belum "
            "sempat keluar alami sebelum sesi habis. TIDAK mempengaruhi penutupan "
            "posisi yang sudah terbuka.",
        )
        args = parser.parse_args()

        params = build_params(DonchianBreakoutParams, args.param)
        strat = DonchianBreakoutStrategy(params)
        print(f"  (Parameter dipakai: {params})")
        print(f"  (Bursa: {args.exchange}, reduceOnly {'AKTIF' if args.exchange == 'binanceusdm' else 'NONAKTIF (spot)'})")

        broker = Broker(exchange_id=args.exchange, testnet=True)
        use_reduce_only = args.exchange == "binanceusdm"  # cuma futures yang kenal reduceOnly
        runner = PaperRunner(
            strat, broker, symbol=args.symbol, timeframe=args.timeframe, order_amount=args.amount,
            use_reduce_only=use_reduce_only, session_hours=args.session_hours,
            min_entry_buffer_hours=args.min_entry_buffer_hours,
        )
        asyncio.run(runner.run())

    else:
        # Uji asap TANPA jaringan -- pakai MockBroker + bar TIRUAN yang
        # sengaja dibuat menembus channel breakout, supaya proses
        # sinyal -> order -> catat fill teruji tuntas tanpa koneksi
        # sungguhan. INI BUKAN TES REALISME STRATEGI, cuma tes pipa.
        import numpy as np

        from src.execution.broker import MockBroker
        from src.strategy.donchian_breakout import DonchianBreakoutStrategy

        print("=== Uji asap PaperRunner (tanpa jaringan, pakai bar tiruan) ===\n")

        async def run_smoke_test():
            strat = DonchianBreakoutStrategy()  # default: entry 20 bar, exit 10 bar
            broker = MockBroker(fill_immediately=True)
            runner = PaperRunner(strat, broker, symbol="BTC/USDT:USDT", order_amount=0.01)

            rng = np.random.default_rng(7)
            base_ts = 1_700_000_000_000  # ms, sembarang titik awal
            price = 100_000.0

            # 25 bar flat/acak dulu -- supaya channel breakout kebentuk
            # tanpa memicu sinyal (mengisi window, bukan menembusnya).
            for i in range(25):
                price += rng.normal(0, 5)
                bar = {
                    "timestamp": base_ts + i * 2 * 60 * 60 * 1000,
                    "open": price, "high": price + 10, "low": price - 10,
                    "close": price, "volume": 100.0,
                }
                await runner.process_bar(bar)

            # Sekarang dorong harga jauh ke atas -- sengaja menembus
            # channel entry, supaya kita lihat order_manager beneran
            # dipanggil dan slippage_tracker beneran mencatat fill.
            price += 500
            bar = {
                "timestamp": base_ts + 25 * 2 * 60 * 60 * 1000,
                "open": price, "high": price + 10, "low": price - 10,
                "close": price, "volume": 100.0,
            }
            await runner.process_bar(bar)

            print(f"\nPosisi akhir runner: {runner._current_position}")
            print(f"Ringkasan slippage tercatat: {runner.slippage_tracker.summarize()}")

        asyncio.run(run_smoke_test())
        print("\nUntuk jalur SUNGGUHAN (butuh kredensial testnet + jaringan):")
        print("  python -m runner.paper --live --symbol \"BTC/USDT:USDT\" --timeframe 2h")