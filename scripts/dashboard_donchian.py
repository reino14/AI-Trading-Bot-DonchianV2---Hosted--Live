"""
scripts/dashboard_donchian.py

Dashboard pemantau + pengendali bot Donchian -- PROSES TERPISAH.

=========================================================================
TIDAK ADA file bot yang diubah. Dashboard ini:
  - MEMBACA bursa (read-only: saldo, posisi, riwayat trade, kline)
  - MENJALANKAN bot yang SUDAH ADA sebagai subprocess, lewat CLI yang
    SAMA PERSIS yang biasa Anda ketik manual -- bukan memanggil internal
    bot, bukan menyalin logikanya
Dashboard sendiri TIDAK PERNAH mengirim order. Yang mengirim order tetap
bot (scripts/run_paper_donchian_futures.py), persis seperti sebelumnya.
=========================================================================

PERBAIKAN BUG "cuma tanggal 16 yang muncul"
-------------------------------------------------------------------------
Versi sebelumnya panggil fetch_my_trades(limit=200) TANPA rentang waktu.
Binance mengembalikan trade URUT NAIK dari yang TERLAMA dalam 7 hari
terakhir -- jadi 200 slot itu HABIS dimakan fill tanggal 16 (yang
jumlahnya ratusan gara-gara split-fill), tanggal 17-18 tidak kebagian
sama sekali. Perbaikannya di fetch_all_trades(): paginasi sungguhan
(maju terus sampai habis) DAN dipotong per 7 hari, karena Binance
membatasi rentang startTime/endTime maksimal 7 hari per permintaan.

CATATAN ARSITEKTUR (disampaikan terbuka)
-------------------------------------------------------------------------
File ini memanggil broker.exchange.* langsung (fetch_my_trades,
fetch_ohlcv) -- secara teknis melanggar aturan proyek "cuma broker.py
yang menyentuh ccxt". Alasannya sama seperti sebelumnya: Broker tidak
punya method untuk itu dan saya tidak boleh menambahkannya. Trade-off
ini saya pilih karena risikonya jauh lebih kecil daripada mengubah
broker.py yang dipakai bot yang sedang jalan.

Cara pakai:
    python -m scripts.dashboard_donchian
lalu buka http://localhost:8100
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
import threading
import time
from collections import deque
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse

from src.execution.account import AccountConfigError, resolve_account

SESSION_STATE_PATH = Path("data/session_state.json")
IS_WINDOWS = sys.platform == "win32"

#: Binance membatasi rentang startTime/endTime userTrades maksimal 7 hari
#: per permintaan -- rentang lebih panjang WAJIB dipotong jadi beberapa
#: permintaan, bukan dikirim sekaligus (akan ditolak/dipotong diam-diam).
CHUNK_MS = 7 * 24 * 60 * 60 * 1000

#: Periode ATR untuk pratinjau SL di form -- HARUS sama dengan default
#: --sl-atr-period di run_paper_donchian_futures.py (20), karena bot yang
#: dijalankan dari form tidak mengirim flag itu.
ATR_PERIOD = 20
#: Batas bawah jarak SL (kelipatan fee bolak-balik) -- sama dengan default
#: --sl-min-fee-mult di launcher.
SL_MIN_FEE_MULT = 2.0


def fetch_all_trades(exchange, symbol: str, since_ms: int, until_ms: int,
                     page_limit: int = 1000, max_pages: int = 200) -> list[dict]:
    """
    Ambil SEMUA trade dalam rentang waktu -- dengan paginasi sungguhan,
    bukan sekadar limit besar (lihat penjelasan bug di kepala file).

    Dipisah jadi fungsi MURNI terhadap `exchange` (apa pun objek yang
    punya .fetch_my_trades) supaya bisa diuji tuntas dengan bursa palsu,
    tanpa koneksi sungguhan.

    max_pages: jaring pengaman -- kalau bursa terus mengembalikan data
    (mis. karena bug/timestamp aneh), berhenti daripada loop selamanya.
    """
    collected: list[dict] = []
    seen_ids: set = set()
    pages = 0

    chunk_start = since_ms
    while chunk_start < until_ms and pages < max_pages:
        chunk_end = min(chunk_start + CHUNK_MS, until_ms)
        cursor = chunk_start

        while cursor < chunk_end and pages < max_pages:
            pages += 1
            batch = exchange.fetch_my_trades(
                symbol, since=cursor, limit=page_limit,
                params={"endTime": chunk_end},
            )
            if not batch:
                break

            new_in_batch = 0
            last_ts = cursor
            for t in batch:
                ts = t.get("timestamp") or 0
                last_ts = max(last_ts, ts)
                tid = t.get("id") or f"{ts}-{t.get('order')}-{t.get('amount')}"
                if tid in seen_ids:
                    continue  # Binance bisa kirim ulang trade di tepi halaman
                seen_ids.add(tid)
                collected.append(t)
                new_in_batch += 1

            if len(batch) < page_limit and new_in_batch == 0:
                break
            if last_ts <= cursor:
                # Tidak maju sama sekali -- cegah loop tak berujung.
                break
            cursor = last_ts + 1

        chunk_start = chunk_end

    collected.sort(key=lambda t: t.get("timestamp") or 0)
    return collected


def filter_trades_by_time(trades: list[dict], from_ms: int | None, to_ms: int | None) -> list[dict]:
    """Saring trade berdasarkan rentang waktu (ms epoch). None = tanpa batas di sisi itu."""
    out = []
    for t in trades:
        ts = t.get("timestamp") or 0
        if from_ms is not None and ts < from_ms:
            continue
        if to_ms is not None and ts > to_ms:
            continue
        out.append(t)
    return out


def aggregate_trades(raw_trades: list[dict]) -> dict:
    """
    Ringkas riwayat trade jadi angka siap tampil. FUNGSI MURNI.

    'bersih' = total realized P&L DIKURANGI total fee. Binance melaporkan
    realizedPnl TERPISAH dari commission (fee TIDAK ikut dipotong di
    realizedPnl), jadi pengurangan ini memang perlu -- bukan dobel-potong.

    Fee dibaca dari fee.cost (unified ccxt) dulu, fallback ke
    info.commission (mentah). Kalau DUA-DUANYA kosong, dihitung 0 TAPI
    dicatat di 'n_fee_unknown' -- supaya ketahuan kalau 'bersih' tidak
    bisa dipercaya, bukan diam-diam dianggap nol.
    """
    total_realized = 0.0
    total_fee = 0.0
    n_fee_unknown = 0
    wins: list[float] = []
    losses: list[float] = []

    for t in raw_trades:
        info = t.get("info") or {}

        raw_pnl = info.get("realizedPnl")
        pnl = float(raw_pnl) if raw_pnl not in (None, "") else 0.0
        total_realized += pnl

        fee_obj = t.get("fee") or {}
        fee_cost = fee_obj.get("cost")
        if fee_cost in (None, ""):
            fee_cost = info.get("commission")
        if fee_cost in (None, ""):
            n_fee_unknown += 1
            fee_cost = 0.0
        total_fee += float(fee_cost)

        # Trade PEMBUKA posisi realizedPnl-nya 0 -- itu bukan "seri",
        # memang belum ada yang direalisasikan. Tidak dihitung menang/kalah.
        if pnl > 0:
            wins.append(pnl)
        elif pnl < 0:
            losses.append(pnl)

    n_closing = len(wins) + len(losses)
    return {
        "n_fills": len(raw_trades),
        "n_closing_trades": n_closing,
        "total_realized": total_realized,
        "total_fee": total_fee,
        "net": total_realized - total_fee,
        "total_profit": sum(wins),
        "total_loss": sum(losses),
        "n_wins": len(wins),
        "n_losses": len(losses),
        "win_rate": (len(wins) / n_closing) if n_closing else 0.0,
        "avg_win": (sum(wins) / len(wins)) if wins else 0.0,
        "avg_loss": (sum(losses) / len(losses)) if losses else 0.0,
        "best_win": max(wins) if wins else 0.0,
        "worst_loss": min(losses) if losses else 0.0,
        "profit_factor": (sum(wins) / abs(sum(losses))) if losses and sum(losses) != 0 else None,
        "n_fee_unknown": n_fee_unknown,
    }


def hitung_pertumbuhan(wallet: float | None, net: float) -> dict | None:
    """
    Pertumbuhan portofolio: laba BERSIH dibagi MODAL AWAL. FUNGSI MURNI.

    Pembaginya SENGAJA bukan wallet sekarang. Wallet sekarang sudah
    mengandung labanya, jadi membaginya dengan itu membuat persentase
    terlihat lebih kecil dari kenyataan (dan untuk rugi, lebih kecil dari
    kerugian sebenarnya). Modal awal = wallet sekarang - laba bersih.

    BATASNYA, dan ini penting: angka ini hanya benar kalau
    (a) TIDAK ada setor/tarik dana selama rentang yang dipilih, dan
    (b) rentangnya mencakup semua trading yang membentuk saldo itu.
    Kalau Anda menyaring "hari ini" saja padahal bot sudah jalan seminggu,
    modal awal yang dihitung di sini adalah saldo awal HARI INI, bukan
    saldo awal Anda seminggu lalu. Ditandai lewat 'asumsi' di bawah.
    """
    if wallet is None or wallet <= 0:
        return None
    modal_awal = wallet - net
    if modal_awal <= 0:
        return {"wallet": wallet, "net": net, "modal_awal": None, "pct": None,
                "asumsi": "modal awal tidak masuk akal (<=0) -- kemungkinan ada setor/tarik dana"}
    return {
        "wallet": wallet, "net": net, "modal_awal": modal_awal,
        "pct": net / modal_awal,
        "asumsi": "dihitung dari saldo sekarang dikurangi laba rentang ini; "
                  "tidak memperhitungkan setor/tarik dana",
    }


def _angka(v) -> float | None:
    """float() yang tidak meledak untuk None / "" / teks aneh dari bursa."""
    if v in (None, ""):
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def hitung_pnl_berjalan(raw_positions: list[dict], position: dict | None,
                        last_price: float | None, fee_pct: float | None) -> dict | None:
    """
    Untung/rugi posisi yang MASIH TERBUKA (belum direalisasi). FUNGSI MURNI.

    Angka utama diambil dari BURSA (unrealizedPnl + markPrice di objek
    posisi ccxt), bukan dihitung sendiri -- supaya persis sama dengan
    yang tampil di aplikasi Binance. Binance menghitungnya dari MARK
    price, bukan harga transaksi terakhir, jadi hitungan sendiri dari
    "last" bisa beda beberapa sen/dolar.

    Kalau bursa tidak mengirim angka itu, baru dihitung sendiri dari
    mark price (atau harga terakhir kalau mark juga kosong), dan
    'sumber' ditandai supaya tampilan bisa memberi tahu.

    roi_pct memakai definisi yang SAMA dengan Binance: PnL dibagi margin
    awal, dan margin awal = entry x jumlah / leverage.

    bersih_setelah_fee = PERKIRAAN kalau posisi ditutup SEKARANG:
    PnL dikurangi fee buka (sudah terbayar) dan fee tutup (akan dibayar).
    Binance sendiri TIDAK memotong fee di angka PnL-nya, jadi angka ini
    selalu lebih kecil dari yang tampil di Binance.
    """
    if not position:
        return None
    qty = abs(_angka(position.get("contracts")) or 0.0)
    entry = _angka(position.get("entry_price"))
    lev = _angka(position.get("leverage"))
    side = position.get("side")
    if not qty or not entry or side not in ("long", "short"):
        return None
    arah = 1 if side == "long" else -1

    mentah = None
    for p in raw_positions or []:
        if abs(_angka(p.get("contracts")) or 0.0) > 0:
            mentah = p
            break
    info = (mentah or {}).get("info") or {}

    mark = _angka((mentah or {}).get("markPrice"))
    if mark is None:
        mark = _angka(info.get("markPrice"))
    harga_acuan = mark if mark is not None else last_price
    acuan = "mark price" if mark is not None else "harga terakhir"

    pnl = _angka((mentah or {}).get("unrealizedPnl"))
    if pnl is None:
        pnl = _angka(info.get("unRealizedProfit"))
    sumber = "bursa"
    if pnl is None:
        if harga_acuan is None:
            return None
        pnl = arah * (harga_acuan - entry) * qty
        sumber = "dihitung"

    margin_awal = entry * qty / lev if lev else None
    fee_total = None
    if fee_pct is not None:
        fee_buka = entry * qty * fee_pct
        fee_tutup = (harga_acuan if harga_acuan is not None else entry) * qty * fee_pct
        fee_total = fee_buka + fee_tutup
    return {
        "side": side, "qty": qty, "entry": entry, "leverage": lev,
        "harga_acuan": harga_acuan, "acuan": acuan,
        "gerak_harga_pct": (arah * (harga_acuan - entry) / entry) if harga_acuan else None,
        "pnl": pnl, "sumber": sumber,
        "margin_awal": margin_awal,
        "roi_pct": (pnl / margin_awal) if margin_awal else None,
        "fee_pct": fee_pct, "fee_total": fee_total,
        "bersih_setelah_fee": pnl - fee_total if fee_total is not None else None,
    }


def build_equity_curve(raw_trades: list[dict]) -> list[dict]:
    """
    Kurva P&L KUMULATIF BERSIH dari daftar fill. FUNGSI MURNI.

    Memakai definisi "bersih" YANG SAMA dengan aggregate_trades():
    realizedPnl DIKURANGI commission. Kalau dua fungsi ini memakai
    definisi berbeda, angka terakhir di kurva tidak akan cocok dengan
    kartu "BERSIH" di atasnya -- dan itu bug yang sulit dilacak.

    Titik pertama sengaja 0 di waktu fill pertama, supaya kurvanya
    mulai dari garis nol, bukan langsung melompat ke nilai fill pertama.

    Fill pembuka posisi realizedPnl-nya 0, jadi kurva mendatar di situ
    lalu turun sebesar fee -- itu BENAR, bukan glitch: biaya sudah
    keluar sebelum ada yang direalisasikan.
    """
    titik: list[dict] = []
    kumulatif = 0.0
    urut = sorted(raw_trades, key=lambda t: t.get("timestamp") or 0)
    if urut:
        titik.append({"t": int(urut[0].get("timestamp") or 0), "v": 0.0})
    for t in urut:
        info = t.get("info") or {}
        raw_pnl = info.get("realizedPnl")
        pnl = float(raw_pnl) if raw_pnl not in (None, "") else 0.0
        fee_obj = t.get("fee") or {}
        fee_cost = fee_obj.get("cost")
        if fee_cost in (None, ""):
            fee_cost = info.get("commission")
        fee = float(fee_cost) if fee_cost not in (None, "") else 0.0
        kumulatif += pnl - fee
        titik.append({"t": int(t.get("timestamp") or 0), "v": kumulatif})
    return titik


def compute_channel(closes: list[float], lookback: int) -> dict | None:
    """
    Channel Donchian close-only -- RUMUS SAMA PERSIS dengan
    compute_donchian_signal(): max/min close dari `lookback` bar SEBELUM
    bar terakhir (TIDAK termasuk bar terakhir itu sendiri).

    Return None kalau data belum cukup. FUNGSI MURNI, bisa diuji tanpa jaringan.
    """
    if len(closes) < lookback + 1:
        return None
    window = closes[-(lookback + 1):-1]
    upper = max(window)
    lower = min(window)
    current = closes[-1]
    return {
        "lookback": lookback,
        "upper": upper,
        "lower": lower,
        "current": current,
        "dist_upper": (upper - current) / current,
        "dist_lower": (current - lower) / current,
    }


def build_bot_command(cfg: dict) -> list[str]:
    """
    Susun perintah CLI bot dari konfigurasi dashboard -- memanggil
    scripts/run_paper_donchian_futures.py yang SUDAH ADA, persis seperti
    Anda mengetiknya manual di terminal. FUNGSI MURNI supaya bisa diuji.
    """
    cmd = [
        sys.executable, "-u", "-m", "scripts.run_paper_donchian_futures",
        "--live",
        "--symbol", str(cfg["symbol"]),
        "--timeframe", str(cfg["timeframe"]),
        "--lookback", str(int(cfg["lookback"])),
        "--amount", str(float(cfg["amount"])),
    ]
    if cfg.get("session_hours"):
        cmd += ["--session-hours", str(float(cfg["session_hours"]))]
    if cfg.get("take_profit_pct"):
        cmd += ["--take-profit-pct", str(float(cfg["take_profit_pct"]))]
    if cfg.get("stop_after_take_profit"):
        cmd += ["--stop-after-take-profit"]
    if cfg.get("backfill_bars"):
        cmd += ["--backfill-bars", str(int(cfg["backfill_bars"]))]
    if cfg.get("live_take_profit_poll_seconds"):
        cmd += ["--live-take-profit-poll-seconds", str(float(cfg["live_take_profit_poll_seconds"]))]
    # SL + TP rasio di bursa. take_profit_pct lama sengaja TIDAK ikut dikirim
    # bersamaan: launcher menolak kombinasi keduanya.
    if cfg.get("risk_reward"):
        cmd += ["--risk-reward", str(float(cfg["risk_reward"]))]
        if cfg.get("sl_atr_mult"):
            cmd += ["--sl-atr-mult", str(float(cfg["sl_atr_mult"]))]
    # SL/TP dari ROI kotor terhadap margin (persen).
    if cfg.get("tp_roi_pct") and cfg.get("sl_roi_pct"):
        cmd += ["--tp-roi-pct", str(float(cfg["tp_roi_pct"])), "--sl-roi-pct", str(float(cfg["sl_roi_pct"]))]
        # Angka dari dashboard = % dari nilai posisi; bot mengonversinya
        # sendiri pakai leverage posisi SEBENARNYA saat entry.
        cmd += ["--tp-sl-price-pct"]
    if cfg.get("reentry_mode") == "midline":
        cmd += ["--reentry-mode", "midline"]
    if cfg.get("regime_filter"):
        cmd += ["--regime-filter"]
        if cfg.get("regime_timeframe"):
            cmd += ["--regime-timeframe", str(cfg["regime_timeframe"])]
    if cfg.get("leverage"):
        cmd += ["--leverage", str(int(cfg["leverage"]))]
    if cfg.get("close_on_stop"):
        cmd += ["--close-on-stop"]
    # Kill switch selalu dikirim eksplisit (0 = mati), supaya yang tertulis
    # di form sama persis dengan yang dipakai bot.
    if cfg.get("max_consecutive_sl") is not None:
        cmd += ["--max-consecutive-sl", str(int(cfg["max_consecutive_sl"]))]
    if cfg.get("max_daily_loss_pct") is not None:
        cmd += ["--max-daily-loss-pct", str(float(cfg["max_daily_loss_pct"]))]
    return cmd


class BotProcess:
    """
    Mengelola SATU proses bot. Start = jalankan CLI yang sudah ada.
    Stop = kirim sinyal ala Ctrl+C (BUKAN kill paksa) -- supaya bot
    sempat membereskan order yang menggantung, persis alasan kenapa
    Anda selalu diminta Ctrl+C, bukan tutup jendela.
    """

    def __init__(self, max_log_lines: int = 500):
        self.proc: subprocess.Popen | None = None
        self.cmd: list[str] | None = None
        self.started_at: float | None = None
        self.log: deque[str] = deque(maxlen=max_log_lines)
        self.lock = threading.Lock()

    def is_running(self) -> bool:
        return self.proc is not None and self.proc.poll() is None

    def start(self, cfg: dict) -> tuple[bool, str]:
        with self.lock:
            if self.is_running():
                return False, "Bot sudah jalan -- hentikan dulu sebelum menjalankan yang baru."
            cmd = build_bot_command(cfg)
            kwargs = {
                "stdout": subprocess.PIPE, "stderr": subprocess.STDOUT,
                "text": True, "bufsize": 1,
            }
            if IS_WINDOWS:
                # Perlu grup proses sendiri supaya bisa dikirim CTRL_BREAK
                # (padanan Ctrl+C) nanti saat stop.
                kwargs["creationflags"] = subprocess.CREATE_NEW_PROCESS_GROUP
            try:
                self.proc = subprocess.Popen(cmd, **kwargs)
            except Exception as e:
                return False, f"Gagal menjalankan bot: {type(e).__name__}: {e}"

            self.cmd = cmd
            self.started_at = time.time()
            self.log.clear()
            self.log.append(f"$ {' '.join(cmd)}")
            threading.Thread(target=self._pump_output, daemon=True).start()
            return True, "Bot dijalankan."

    # Penanda di stdout bot yang berarti take-profit KENA dan penutupan
    # sudah TERKONFIRMASI flat. Dicocokkan DUA potong sekaligus supaya
    # baris lain yang menyebut take-profit tidak ikut memicu -- termasuk
    # baris "Order penutup TERKIRIM tapi BELUM terkonfirmasi flat", yang
    # justru berarti posisi MASIH TERBUKA dan bot TIDAK boleh dimatikan.
    #
    # KALAU TEKS LOG DI paper.py DIUBAH, mekanisme ini diam-diam berhenti
    # bekerja tanpa error. Itu konsekuensi pendekatan "dashboard saja".
    TP_HALT_MARKERS = ("stop_after_take_profit", "BERHENTI trading total")

    def _pump_output(self) -> None:
        proc = self.proc
        if proc is None or proc.stdout is None:
            return
        sudah_minta_stop = False
        for line in proc.stdout:
            self.log.append(line.rstrip("\n"))
            # Sekali saja -- kalau tidak, tiap baris berikutnya memanggil
            # stop() lagi dan membanjiri log dengan "Tidak ada bot yang jalan."
            if not sudah_minta_stop and all(m in line for m in self.TP_HALT_MARKERS):
                sudah_minta_stop = True
                self.log.append("--- take profit terkonfirmasi -- dashboard mengirim sinyal "
                                "berhenti (ala Ctrl+C), sama seperti tombol Hentikan ---")
                ok, msg = self.stop()
                self.log.append(f"--- {msg} ---")
        self.log.append("--- proses bot berhenti ---")

    def stop(self) -> tuple[bool, str]:
        with self.lock:
            if not self.is_running():
                return False, "Tidak ada bot yang jalan."
            try:
                if IS_WINDOWS:
                    import signal
                    self.proc.send_signal(signal.CTRL_BREAK_EVENT)
                else:
                    self.proc.terminate()
            except Exception as e:
                return False, f"Gagal mengirim sinyal berhenti: {e}"
            return True, "Sinyal berhenti (ala Ctrl+C) dikirim -- bot membereskan diri, tunggu sebentar."

    def snapshot(self) -> dict:
        return {
            "running": self.is_running(),
            "cmd": " ".join(self.cmd) if self.cmd else None,
            "uptime_seconds": (time.time() - self.started_at) if self.started_at and self.is_running() else None,
            "log": list(self.log)[-200:],
        }


def read_session_state() -> dict | None:
    try:
        if not SESSION_STATE_PATH.exists():
            return None
        with open(SESSION_STATE_PATH) as f:
            return json.load(f)
    except Exception:
        return None


class DashboardState:
    """
    Snapshot bursa, di-refresh thread terpisah supaya permintaan HTTP
    tidak pernah menunggu jaringan.
    """

    def __init__(self, symbol: str, refresh_seconds: float, history_days: int, lookback: int, timeframe: str,
                 account: dict | None = None):
        # Akun dari TRADING_ACCOUNT (.env.bot) -- SAMA dengan yang dibaca bot,
        # karena bot dijalankan sebagai subprocess dengan environment ini.
        self.account = account
        self.symbol = symbol
        self.refresh_seconds = refresh_seconds
        self.history_days = history_days
        self.lookback = lookback
        self.timeframe = timeframe
        self.lock = threading.Lock()
        self.snapshot: dict = {"status": "memuat", "last_error": None, "last_update": None}
        self.trades: list[dict] = []
        self._broker = None
        # Fee tidak di-cache: setiap refresh meminta tarif akun kembali.
        # Leverage posisi terakhir yang pernah terlihat -- dipakai pratinjau
        # SL/TP di form saat sedang tidak ada posisi.
        self._last_leverage: float | None = None

    def set_params(self, symbol: str, lookback: int, timeframe: str) -> None:
        with self.lock:
            self.symbol = symbol
            self.lookback = lookback
            self.timeframe = timeframe

    def _ensure_broker(self):
        if self._broker is None:
            from src.execution.broker import Broker
            self._broker = Broker(exchange_id="binanceusdm", testnet=self._akun()["testnet"])
        return self._broker

    def _akun(self) -> dict:
        if self.account is None:
            self.account = resolve_account()
        return self.account

    def _fee_taker(self, broker, symbol: str) -> float | None:
        """Tarif akun Binance; gagal = tidak diketahui, tanpa fallback/cache lama."""
        try:
            raw = broker.fetch_trading_fee_pct(symbol)
            if isinstance(raw, bool):
                return None
            fee = _angka(raw)
            return fee if fee is not None and 0 <= fee < 1 else None
        except Exception:
            return None

    def refresh_once(self) -> None:
        try:
            broker = self._ensure_broker()
            with self.lock:
                symbol, lookback, timeframe = self.symbol, self.lookback, self.timeframe

            # Tepat satu permintaan tarif per refresh, dipakai seluruh panel.
            fee_taker = self._fee_taker(broker, symbol)
            balance = broker.fetch_balance()
            usdt = balance.get("USDT", {})
            wallet = float(usdt.get("total") or 0.0)
            free = float(usdt.get("free") or 0.0)
            # "used" DILAPORKAN BURSA -- ini margin sungguhan yang terkunci,
            # bukan hasil hitungan kita. Dipakai sebagai angka utama.
            used_raw = usdt.get("used")
            used = float(used_raw) if used_raw not in (None, "") else None

            position = broker.fetch_position(symbol)
            price = broker.fetch_current_price(symbol)

            # Margin TURUNAN dari posisi -- notional / leverage. Ini ESTIMASI,
            # disandingkan dengan "used" di atas supaya ketahuan kalau beda
            # jauh (mis. leverage salah baca, atau ada posisi simbol lain
            # yang ikut memakai margin di akun yang sama).
            notional = margin_est = margin_pct = None
            if position and price:
                lev = float(position.get("leverage") or 0) or None
                notional = abs(float(position.get("contracts") or 0)) * price
                if lev:
                    margin_est = notional / lev
            # Persentase memakai "used" dari bursa kalau ada; kalau tidak,
            # baru pakai estimasi. Pembaginya wallet (total ekuitas), jadi
            # angka ini = berapa persen modal yang sedang terkunci sebagai margin.
            margin_dipakai = used if used is not None else margin_est
            if margin_dipakai is not None and wallet > 0:
                margin_pct = margin_dipakai / wallet

            # Untung/rugi posisi berjalan. Pakai objek posisi MENTAH dari ccxt
            # (bukan broker.fetch_position) karena hanya objek mentah yang
            # membawa unrealizedPnl dan markPrice. Gagal di sini TIDAK boleh
            # menggagalkan seluruh refresh -- angka lain tetap ditampilkan.
            pnl_berjalan = None
            if position:
                try:
                    raw_positions = broker.exchange.fetch_positions([symbol])
                except Exception:
                    raw_positions = []
                pnl_berjalan = hitung_pnl_berjalan(
                    raw_positions, position, price, fee_taker)

            bars = broker.fetch_recent_bars(symbol, timeframe, limit=lookback + 1)
            channel = compute_channel([b["close"] for b in bars], lookback)

            # Bahan pratinjau SL/TP di form: ATR dengan RUMUS YANG SAMA
            # dipakai bot (diimpor dari paper.py, bukan disalin), dan fee
            # taker akun. Gagal di sini tidak boleh menggagalkan refresh.
            atr = None
            try:
                from src.runner.paper import compute_atr
                atr = compute_atr(bars, ATR_PERIOD)
            except Exception:
                atr = None

            # SL/TP yang BENAR-BENAR ada di bursa untuk posisi ini -- supaya
            # tidak perlu membuka Binance untuk memastikan posisi terlindungi.
            # None = tidak bisa dibaca (dibedakan dari [] = memang tidak ada).
            if position and position.get("leverage"):
                self._last_leverage = float(position["leverage"])
            bracket_orders = None
            if position and hasattr(broker, "fetch_open_conditional_orders"):
                try:
                    bracket_orders = broker.fetch_open_conditional_orders(symbol)
                except Exception:
                    bracket_orders = None

            now_ms = int(time.time() * 1000)
            since_ms = now_ms - self.history_days * 24 * 60 * 60 * 1000
            trades = fetch_all_trades(broker.exchange, symbol, since_ms, now_ms)

            with self.lock:
                self.trades = trades
                self.snapshot = {
                    "status": "ok", "last_error": None,
                    "last_update": datetime.now(timezone.utc).isoformat(),
                    "symbol": symbol, "wallet": wallet, "free": free, "price": price,
                    "used": used, "notional": notional, "margin_est": margin_est,
                    "margin_dipakai": margin_dipakai, "margin_pct": margin_pct,
                    "position": position, "channel": channel,
                    "pnl_berjalan": pnl_berjalan,
                    "atr": atr, "atr_period": ATR_PERIOD, "fee_taker": fee_taker,
                    "fee_status": "ok" if fee_taker is not None else "gagal",
                    "fee_source": "binance" if fee_taker is not None else None,
                    "fee_error": None if fee_taker is not None else
                        "Gagal mengambil tarif fee dari Binance. Tidak memakai asumsi; mencoba lagi pada refresh berikutnya.",
                    "bracket_orders": bracket_orders,
                    "leverage_terakhir": round(self._last_leverage) if self._last_leverage else None,
                    "history_days": self.history_days,
                    "n_trades_loaded": len(trades),
                    "oldest_trade": trades[0].get("datetime") if trades else None,
                    "newest_trade": trades[-1].get("datetime") if trades else None,
                }
        except Exception as e:
            with self.lock:
                old_pnl = self.snapshot.get("pnl_berjalan")
                self.snapshot = {
                    **self.snapshot, "status": "galat",
                    "last_error": f"{type(e).__name__}: {e}",
                    "last_update": datetime.now(timezone.utc).isoformat(),
                    "fee_taker": None, "fee_status": "gagal", "fee_source": None,
                    "fee_error": "Gagal memperbarui data Binance; tarif fee tidak terverifikasi.",
                    "pnl_berjalan": {**old_pnl, "fee_pct": None, "fee_total": None,
                                     "bersih_setelah_fee": None} if old_pnl else None,
                }

    def loop(self) -> None:
        while True:
            self.refresh_once()
            time.sleep(self.refresh_seconds)

    def payload(self, from_ms: int | None, to_ms: int | None) -> dict:
        with self.lock:
            snap = dict(self.snapshot)
            trades = list(self.trades)

        filtered = filter_trades_by_time(trades, from_ms, to_ms)
        snap["agg"] = aggregate_trades(filtered)
        snap["filter"] = {"from_ms": from_ms, "to_ms": to_ms, "n_after_filter": len(filtered)}

        recent = []
        for t in reversed(filtered[-60:]):
            info = t.get("info") or {}
            fee_obj = t.get("fee") or {}
            fee_cost = fee_obj.get("cost")
            if fee_cost in (None, ""):
                fee_cost = info.get("commission", 0)
            recent.append({
                "time": t.get("datetime", ""), "side": t.get("side", ""),
                "price": float(t.get("price") or 0), "amount": float(t.get("amount") or 0),
                "fee": float(fee_cost or 0), "pnl": float(info.get("realizedPnl") or 0),
                "role": "maker" if info.get("maker") in (True, "true") else "taker",
            })
        snap["recent"] = recent
        snap["equity_curve"] = build_equity_curve(filtered)
        snap["porto"] = hitung_pertumbuhan(snap.get("wallet"), snap["agg"]["net"])
        snap["session_state"] = read_session_state()
        try:
            a = self._akun()
            snap["account"] = {"mode": a["mode"], "label": a["label"], "is_live": a["is_live"]}
        except AccountConfigError as e:
            snap["account"] = {"mode": None, "label": None, "is_live": None, "error": str(e)}
        return snap


HTML_PAGE = r"""<!DOCTYPE html>
<html lang="id">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Pemantau Bot Donchian</title>
<style>
  :root { --bg:#fff; --fg:#1a1a1a; --muted:#6b7280; --line:#e5e7eb; --hijau:#15803d;
          --merah:#b91c1c; --kuning:#a16207; --panel:#fafafa; --biru:#1d4ed8; }
  @media (prefers-color-scheme: dark) { :root:not([data-theme="light"]) {
      --bg:#0f1115; --fg:#e8e8e8; --muted:#9ca3af; --line:#2a2f3a; --hijau:#4ade80;
      --merah:#f87171; --kuning:#fbbf24; --panel:#161a21; --biru:#60a5fa; } }
  * { box-sizing:border-box; }
  body { margin:0; padding:24px; background:var(--bg); color:var(--fg);
         font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,sans-serif; }
  .wrap { max-width:1280px; margin:0 auto; }
  h1 { font-size:26px; margin:0 0 4px; }
  .sub { color:var(--muted); font-size:13px; }
  h2 { font-size:13px; text-transform:uppercase; letter-spacing:.06em; color:var(--muted);
       font-weight:600; margin:30px 0 12px; }
  .panel { border:1px solid var(--line); border-radius:12px; padding:18px; background:var(--panel); }
  .cards { display:grid; grid-template-columns:repeat(auto-fit,minmax(215px,1fr)); gap:14px; }
  .card { border:1px solid var(--line); border-radius:10px; padding:16px; background:var(--bg); }
  .card .label { font-size:11px; text-transform:uppercase; letter-spacing:.05em; color:var(--muted); }
  .card .big { font-size:25px; font-weight:600; margin-top:6px; font-variant-numeric:tabular-nums; }
  .card .note { font-size:12px; color:var(--muted); margin-top:4px; }
  .card.hijau{border-color:var(--hijau)} .card.merah{border-color:var(--merah)} .card.tebal{border-width:2px}
  .grid { display:grid; grid-template-columns:repeat(auto-fit,minmax(145px,1fr)); gap:18px 14px; }
  .item .label { font-size:11px; text-transform:uppercase; letter-spacing:.05em; color:var(--muted); }
  .item .val { font-size:18px; font-weight:600; margin-top:4px; font-variant-numeric:tabular-nums; }
  .item .note { font-size:12px; color:var(--muted); margin-top:4px; line-height:1.4; }
  .hijau-t{color:var(--hijau)} .merah-t{color:var(--merah)} .kuning-t{color:var(--kuning)} .biru-t{color:var(--biru)}
  table { width:100%; border-collapse:collapse; font-size:13px; }
  th { text-align:left; font-size:11px; text-transform:uppercase; letter-spacing:.05em; color:var(--muted);
       font-weight:600; padding:8px 10px; border-bottom:1px solid var(--line); }
  td { padding:9px 10px; border-bottom:1px solid var(--line); font-variant-numeric:tabular-nums; }
  tr:last-child td{border-bottom:none}
  .scroll { max-height:340px; overflow-y:auto; }
  .mono { font-family:ui-monospace,"SF Mono",Menlo,Consolas,monospace; font-size:12px; }
  .warn { border-left:3px solid var(--kuning); padding:10px 14px; background:var(--panel);
          border-radius:6px; font-size:13px; margin-bottom:14px; }
  .err { border-left:3px solid var(--merah); padding:10px 14px; background:var(--panel);
         border-radius:6px; font-size:13px; margin-bottom:14px; }
  .form { display:grid; grid-template-columns:repeat(auto-fit,minmax(140px,1fr)); gap:12px; }
  .f label { display:block; font-size:10px; text-transform:uppercase; letter-spacing:.05em;
             color:var(--muted); margin-bottom:4px; }
  .f input, .f select { width:100%; padding:8px 10px; border:1px solid var(--line); border-radius:7px;
                        background:var(--bg); color:var(--fg); font-size:13px; font-family:inherit; }
  button { padding:9px 18px; border-radius:8px; border:1px solid var(--line); background:var(--bg);
           color:var(--fg); font-size:13px; font-weight:600; cursor:pointer; font-family:inherit; }
  button.pri { background:var(--biru); border-color:var(--biru); color:#fff; }
  button.bahaya { border-color:var(--merah); color:var(--merah); }
  button:disabled { opacity:.45; cursor:not-allowed; }
  .baris { display:flex; gap:10px; align-items:center; flex-wrap:wrap; margin-top:14px; }
  .cek { display:flex; align-items:center; gap:7px; font-size:13px; }
  .grup-wrap { display:grid; grid-template-columns:repeat(auto-fit,minmax(min(100%,420px),1fr)); gap:14px; }
  .grup { border:1px solid var(--line); border-radius:10px; padding:14px 16px; background:var(--bg); }
  .grup-judul { font-size:11px; font-weight:600; text-transform:uppercase; letter-spacing:.06em;
                color:var(--muted); margin-bottom:12px; }
  .grup .form { grid-template-columns:repeat(auto-fit,minmax(150px,1fr)); }
  .f.lebar { grid-column:1 / -1; }
  .grup .form.dua { grid-template-columns:repeat(2,minmax(0,1fr)); }
  .ket { font-size:11px; color:var(--muted); margin-top:5px; line-height:1.5; }
  .ket.merah-t { color:var(--merah); }
  .ket-blok { font-size:12px; color:var(--muted); line-height:1.6; margin-top:12px;
              padding-top:12px; border-top:1px dashed var(--line); }
  .aksi { display:flex; gap:10px; align-items:center; flex-wrap:wrap; margin-top:16px;
          padding-top:16px; border-top:1px solid var(--line); }
  .cmd { margin-top:10px; word-break:break-all; }
  .cek input { width:16px; height:16px; margin:0; }
  pre.log { margin:0; padding:14px; background:#0b0d11; color:#d4d4d4; border-radius:8px;
            font-family:ui-monospace,Menlo,Consolas,monospace; font-size:11.5px; line-height:1.55;
            max-height:340px; overflow:auto; white-space:pre-wrap; word-break:break-word; }
  .chip { display:inline-block; padding:2px 9px; border-radius:20px; font-size:11px; font-weight:600; }
  .chip.on { background:rgba(21,128,61,.15); color:var(--hijau); }
  .chip.off { background:rgba(185,28,28,.15); color:var(--merah); }
  .kurva { width:100%; height:auto; display:block; margin:0; }
  .kurva .garis { fill:none; stroke:var(--biru); stroke-width:2; }
  .kurva .nol { stroke:var(--muted); stroke-width:1; stroke-dasharray:3 3; opacity:.55; }
  .kurva .sumbu { stroke:var(--line); stroke-width:1; }
  .kurva text { fill:var(--muted); font-size:10px; font-variant-numeric:tabular-nums; }
</style>
</head>
<body>
<div class="wrap">
  <h1>Pemantau Bot Donchian</h1>
  <div class="sub">Donchian close-only &middot; <b id="akun_label">memuat akun&hellip;</b> &middot; dashboard tidak pernah kirim order sendiri &mdash; yang kirim order tetap bot</div>
  <div id="banner_akun"></div>
  <div class="sub mono" id="jam" style="margin-top:6px">&mdash;</div>

  <h2>Jalankan bot</h2>
  <div class="panel">
    <div class="grup-wrap">
      <section class="grup">
        <div class="grup-judul">Pasar &amp; strategi</div>
        <div class="form dua">
          <div class="f lebar"><label for="c_symbol">Simbol</label><input id="c_symbol" value="BTC/USDT:USDT"></div>
          <div class="f"><label for="c_timeframe">Timeframe bot</label>
            <select id="c_timeframe" onchange="isiOpsiRegime()">
              <option value="1m" selected>1m</option><option value="3m">3m</option><option value="5m">5m</option>
              <option value="15m">15m</option><option value="30m">30m</option><option value="1h">1H</option>
              <option value="2h">2H</option><option value="4h">4H</option><option value="6h">6H</option>
              <option value="8h">8H</option><option value="12h">12H</option><option value="1d">1D</option>
            </select></div>
          <div class="f"><label for="c_lookback">Lookback (bar)</label><input id="c_lookback" type="number" value="200"></div>
          <div class="f"><label for="c_backfill">Backfill (bar)</label><input id="c_backfill" type="number" value="200"></div>
          <div class="f"><label for="c_session">Sesi (jam)</label><input id="c_session" type="number" step="0.5" value="24"></div>
        </div>
      </section>

      <section class="grup">
        <div class="grup-judul">Posisi &amp; risiko</div>
        <div class="form dua">
          <div class="f"><label for="c_amount">Amount (BTC)</label><input id="c_amount" type="number" step="0.0001" value="0.0012" oninput="hitungNotional()">
            <div class="ket" id="ket_amount">&mdash;</div></div>
          <div class="f"><label for="c_leverage">Leverage (x)</label><input id="c_leverage" type="number" min="1" max="125" step="1" value="1" oninput="hitungNotional()"></div>
          <div class="f"><label for="c_tp_roi">Take profit (% posisi)</label><input id="c_tp_roi" type="number" step="0.05" min="0" value="0.5" oninput="hitungNotional()"></div>
          <div class="f"><label for="c_sl_roi">Stop loss (% posisi)</label><input id="c_sl_roi" type="number" step="0.05" min="0" value="0.25" oninput="hitungNotional()"></div>
          <div class="f"><label for="c_max_sl">Maks SL beruntun / hari</label><input id="c_max_sl" type="number" min="0" step="1" value="3"></div>
          <div class="f"><label for="c_max_loss">Maks rugi harian (% saldo)</label><input id="c_max_loss" type="number" min="0" step="0.5" value="2"></div>
          <div class="f lebar"><div class="ket">Kill switch: kalau salah satu batas tercapai, bot berhenti membuka posisi sampai 00:00 WIB. 0 = mati. Bertahan walau bot di-restart.</div></div>
        </div>
        <div class="ket-blok" id="ket_rr">&mdash;</div>
      </section>

      <section class="grup">
        <div class="grup-judul">Filter regime</div>
        <label class="cek"><input type="checkbox" id="c_regime" onchange="isiOpsiRegime()"> Entry hanya saat trend searah</label>
        <div class="form" style="margin-top:10px">
          <div class="f lebar"><label for="c_regime_tf">Regime dibaca di timeframe</label>
            <select id="c_regime_tf" onchange="cekRegimeTf()"></select>
            <div class="ket" id="ket_regime_tf">&mdash;</div></div>
        </div>
      </section>

      <section class="grup">
        <div class="grup-judul">Perilaku</div>
        <div class="form">
          <div class="f lebar"><label for="c_mode_tp">Setelah take profit</label>
            <select id="c_mode_tp" onchange="ketModeTp()">
              <option value="sekali" selected>Sekali saja &mdash; bot berhenti</option>
              <option value="tengah">Berulang &mdash; masuk lagi dari tengah channel</option>
              <option value="berulang">Berulang &mdash; tunggu sinyal berbalik</option>
            </select>
            <div class="ket" id="ket_mode_tp"></div></div>
        </div>
        <label class="cek" style="margin-top:12px"><input type="checkbox" id="c_close_stop" checked> Tutup posisi saat bot dihentikan</label>
      </section>
    </div>

    <div class="aksi">
      <button class="pri" id="btn_start" onclick="mulai()">Mulai bot</button>
      <button class="bahaya" id="btn_stop" onclick="hentikan()">Hentikan</button>
      <span id="pesan" class="sub"></span>
    </div>
    <div class="sub mono cmd" id="cmd_preview"></div>
  </div>

  <div id="isi"></div>
</div>
<script>
const uang = (n,d=2) => (n>=0?"+":"") + n.toFixed(d);
const warna = n => n>0?"hijau-t":(n<0?"merah-t":"");
const pilih = id => document.getElementById(id);

function konfig() {
  return {
    symbol: pilih("c_symbol").value, timeframe: pilih("c_timeframe").value,
    lookback: +pilih("c_lookback").value, amount: +pilih("c_amount").value,
    session_hours: +pilih("c_session").value || null,
    // Input = % gerak harga (= % dari nilai posisi), dikirim apa adanya +
    // flag --tp-sl-price-pct. Bot yang mengonversi pakai leverage SEBENARNYA.
    tp_roi_pct: roiKeduanya() ? +pilih("c_tp_roi").value : null,
    sl_roi_pct: roiKeduanya() ? +pilih("c_sl_roi").value : null,
    stop_after_take_profit: pilih("c_mode_tp").value === "sekali",
    reentry_mode: pilih("c_mode_tp").value === "tengah" ? "midline" : null,
    backfill_bars: +pilih("c_backfill").value || null,
    leverage: Math.max(1, Math.round(+pilih("c_leverage").value || 1)),
    regime_filter: pilih("c_regime").checked,
    regime_timeframe: pilih("c_regime_tf").value || null,
    close_on_stop: pilih("c_close_stop").checked,
    max_consecutive_sl: Math.max(0, Math.round(+pilih("c_max_sl").value || 0)),
    max_daily_loss_pct: Math.max(0, +pilih("c_max_loss").value || 0),
  };
}

// null = data akun belum termuat; tombol Mulai menunggu sampai jelas akunnya.
let akunLive = null;

// Teks konfirmasi sebelum menjalankan bot di AKUN ASLI. Fungsi murni (diuji).
function pesanKonfirmasiLive(cfg, harga) {
  const nilai = harga ? " (~" + (cfg.amount * harga).toFixed(2) + " USDT)" : "";
  const tpsl = (cfg.tp_roi_pct && cfg.sl_roi_pct)
    ? "TP +" + cfg.tp_roi_pct + "% / SL -" + cfg.sl_roi_pct + "% dari posisi" : "TANPA SL/TP";
  const batas = [cfg.max_consecutive_sl ? cfg.max_consecutive_sl + " SL beruntun" : "",
                 cfg.max_daily_loss_pct ? "rugi " + cfg.max_daily_loss_pct + "% saldo" : ""].filter(x => x);
  return "AKUN UANG ASLI\n\n"
    + "Simbol   : " + cfg.symbol + " " + cfg.timeframe + "\n"
    + "Amount   : " + cfg.amount + " BTC" + nilai + "\n"
    + "Leverage : " + cfg.leverage + "x\n"
    + "SL/TP    : " + tpsl + "\n"
    + "Batas    : " + (batas.length ? batas.join(" atau ") + " per hari" : "TANPA kill switch") + "\n\n"
    + "Jalankan bot dengan uang sungguhan?";
}

// Timeframe regime -- daftar sama dengan pilihan interval Binance.
// Opsi yang tidak bisa dipakai untuk timeframe bot saat ini DIMATIKAN
// dengan alasannya, bukan disembunyikan.
const TF_REGIME = [["1m","1m"],["3m","3m"],["5m","5m"],["15m","15m"],["30m","30m"],["1h","1H"],
  ["2h","2H"],["4h","4H"],["6h","6H"],["8h","8H"],["12h","12H"],["1d","1D"],["1w","1W"],["1M","1M"]];
const MAKS_PEMANASAN = 12000, PEMANASAN_HTF = 70;
function menitTf(tf) {
  const m = /^(\d+)([mhdwM])$/.exec(String(tf).trim());
  if (!m || +m[1] <= 0 || m[2] === "M") return null;
  return +m[1] * {m:1, h:60, d:1440, w:10080}[m[2]];
}
function alasanTf(tf) {
  if (tf === "1M") return "bulanan tidak didukung";
  const r = menitTf(tf), b = menitTf(pilih("c_timeframe").value);
  if (r <= b) return "tidak lebih besar dari timeframe bot";
  if (r % b) return "bukan kelipatan timeframe bot";
  const warm = PEMANASAN_HTF * r / b;
  if (warm > MAKS_PEMANASAN) return `butuh ${warm.toLocaleString("id-ID")} candle, maks ${MAKS_PEMANASAN.toLocaleString("id-ID")}`;
  return null;
}
function isiOpsiRegime() {
  const el = pilih("c_regime_tf"), lama = el.value || "15m";
  el.innerHTML = `<option value="">Sama dengan timeframe bot</option>` + TF_REGIME.map(([v, l]) => {
    const a = alasanTf(v);
    return `<option value="${v}"${a ? " disabled" : ""}>${l}${a ? " \u2014 " + a : ""}</option>`;
  }).join("");
  const opsiLama = [...el.options].find(o => o.value === lama && !o.disabled);
  el.value = opsiLama ? lama : "";
  el.disabled = !pilih("c_regime").checked;
  cekRegimeTf();
}
function cekRegimeTf() {
  const ket = pilih("ket_regime_tf"), v = pilih("c_regime_tf").value, bot = pilih("c_timeframe").value;
  ket.className = "ket";
  if (!pilih("c_regime").checked) { ket.textContent = "Filter mati \u2014 entry tidak disaring regime."; return true; }
  if (!v) { ket.textContent = `Regime dibaca di ${bot}, sama dengan bot.`; return true; }
  const a = alasanTf(v);
  if (a) { ket.textContent = a; ket.className = "ket merah-t"; return false; }
  const warm = PEMANASAN_HTF * menitTf(v) / menitTf(bot);
  ket.textContent = `Pemanasan ${warm.toLocaleString("id-ID")} candle ${bot}` +
    (warm > 1500 ? " \u2014 diambil bertahap dari Binance." : ".");
  return true;
}

async function mulai() {
  if (pilih("c_regime").checked && !cekRegimeTf()) {
    pilih("pesan").textContent = "Timeframe regime belum valid.";
    return;
  }
  if (akunLive === null) {
    pilih("pesan").textContent = "Data akun belum termuat -- tunggu sebentar.";
    return;
  }
  if (akunLive && !confirm(pesanKonfirmasiLive(konfig(), hargaTerakhir))) {
    pilih("pesan").textContent = "Dibatalkan.";
    return;
  }
  pilih("pesan").textContent = "mengirim...";
  const r = await fetch("/api/start", {method:"POST", body: JSON.stringify(konfig())});
  const d = await r.json();
  pilih("pesan").textContent = d.message;
  muat();
}
async function hentikan() {
  pilih("pesan").textContent = "mengirim...";
  const r = await fetch("/api/stop", {method:"POST"});
  const d = await r.json();
  pilih("pesan").textContent = d.message;
  muat();
}

// Rentang tanggal -> ms epoch, pakai zona waktu LOKAL browser (WIB),
// supaya "hari ini" berarti hari ini menurut jam Anda, bukan UTC.
function rentang() {
  const f = pilih("f_dari")?.value, t = pilih("f_sampai")?.value;
  let from_ms = null, to_ms = null;
  if (f) from_ms = new Date(f + "T00:00:00").getTime();
  if (t) to_ms = new Date(t + "T23:59:59.999").getTime();
  return {from_ms, to_ms};
}
function setHariIni() {
  const h = new Date(); const s = h.getFullYear()+"-"+String(h.getMonth()+1).padStart(2,"0")+"-"+String(h.getDate()).padStart(2,"0");
  pilih("f_dari").value = s; pilih("f_sampai").value = s; muat();
}
function setSemua() { pilih("f_dari").value=""; pilih("f_sampai").value=""; muat(); }
function set7Hari() {
  const h=new Date(), l=new Date(Date.now()-6*864e5);
  const fmt=d=>d.getFullYear()+"-"+String(d.getMonth()+1).padStart(2,"0")+"-"+String(d.getDate()).padStart(2,"0");
  pilih("f_dari").value=fmt(l); pilih("f_sampai").value=fmt(h); muat();
}

// Harga terakhir dari server -- dipakai hitungNotional(). Disimpan di
// variabel supaya keterangan di bawah field Amount ikut ter-update tiap
// refresh, bukan cuma saat diketik.
let hargaTerakhir = null;
let leverageTerakhir = null;
let atrTerakhir = null, feeTerakhir = null, periodeAtr = 20;
let feeStatus = "memuat", snapshotTerakhir = {};

// Pratinjau SL/TP berbasis ROI KOTOR terhadap margin -- RUMUS SAMA dengan
// compute_bracket_roi() di paper.py (diuji angka per angka di smoke test).
//   gerak harga = ROI / leverage;  f = fee bolak-balik (fraksi harga)
//   untung bersih TP = nilai x (t - f);  rugi total SL = nilai x (s + f)
function pratinjauROI(harga, jumlah, lev, fee, tpPct, slPct) {
  const f = 2 * fee, s = slPct / 100 / lev, t = tpPct / 100 / lev;
  const nilai = jumlah * harga, biaya = nilai * f;
  const untungBersih = nilai * (t - f), rugiTotal = nilai * (s + f);
  const roiFee = f * lev * 100;
  return {
    s, t, margin: nilai / lev, biaya, roiFee,
    untungKotor: nilai * t, rugiKotor: nilai * s, untungBersih, rugiTotal,
    roiBersihTp: tpPct - roiFee, roiTotalSl: slPct + roiFee,
    rasio: rugiTotal > 0 ? untungBersih / rugiTotal : null,
    impas: untungBersih > 0 ? rugiTotal / (untungBersih + rugiTotal) : null,
    slDiBawahFee: s < f,
  };
}

// % dari posisi -> ROI terhadap margin, HANYA untuk teks pratinjau.
function keRoi(persenPosisi) {
  const lev = Math.max(1, Math.round(+pilih("c_leverage").value || 1));
  return Math.round(persenPosisi * lev * 100) / 100;
}

function roiKeduanya() {
  return +pilih("c_tp_roi").value > 0 && +pilih("c_sl_roi").value > 0;
}

// Leverage untuk pratinjau: posisi yang sedang terbuka, atau leverage
// posisi terakhir yang pernah terlihat. Bot sendiri selalu memakai
// leverage posisi SEBENARNYA saat masuk.
let leverageAcuan = null;

function hitungSlTp() {
  const el = pilih("ket_rr");
  if (!el) return;
  if (!roiKeduanya()) {
    el.innerHTML = "<b>Tanpa SL/TP</b> &mdash; isi keduanya (TP dan SL) untuk memasang SL/TP. "
      + "Tanpa itu, posisi hanya ditutup saat sinyal berbalik.";
    return;
  }
  const tpH = +pilih("c_tp_roi").value, slH = +pilih("c_sl_roi").value;
  const amt = parseFloat(pilih("c_amount").value);
  if (feeTerakhir === null) {
    el.innerHTML = feeStatus === "memuat"
      ? "Memuat tarif fee dari Binance..."
      : '<b class="merah-t">Gagal mengambil tarif fee dari Binance.</b> '
        + 'Estimasi fee, profit bersih, dan rasio impas tidak tersedia. Tidak memakai asumsi; mencoba lagi saat refresh.';
    return;
  }
  if (!amt || !hargaTerakhir) { el.innerHTML = "&mdash;"; return; }
  // Pratinjau memakai leverage yang AKAN diset bot (field Leverage).
  const asumsi = false, lev = Math.max(1, Math.round(+pilih("c_leverage").value || 1));
  const tp = keRoi(tpH), sl = keRoi(slH);
  const p = pratinjauROI(hargaTerakhir, amt, lev, feeTerakhir, tp, sl);
  const u = v => v.toFixed(2), pc = (v, d=2) => v.toFixed(d);
  let h = `Leverage <b>${lev}x</b>${asumsi ? " (asumsi, belum terbaca dari bursa)" : ""} &middot; `
    + `margin ${u(p.margin)} USDT &middot; tarif taker dari Binance ${pc(feeTerakhir*100,3)}%/sisi `
    + `&rarr; estimasi fee ${u(p.biaya)} USDT bolak-balik `
    + `(setara ROI ${pc(p.roiFee)}%).<br>`;
  if (p.untungBersih > 0) {
    h += `<b class="hijau-t">TP +${tpH}% dari posisi</b> (ROI +${pc(tp,1)}% dari margin) &rarr; untung kotor +${u(p.untungKotor)}, `
      + `dikurangi fee ${u(p.biaya)} &rarr; <b class="hijau-t">BERSIH +${u(p.untungBersih)} USDT</b> `
      + `(ROI bersih +${pc(p.roiBersihTp)}%). Masih untung setelah fee.<br>`;
  } else {
    h += `<b class="merah-t">TP +${tpH}% TIDAK menutup fee</b>: untung kotor +${u(p.untungKotor)} `
      + `dikurangi fee ${u(p.biaya)} &rarr; bersih ${u(p.untungBersih)} USDT. TP kena pun tetap RUGI &mdash; `
      + `naikkan di atas ${pc(2*feeTerakhir*100,3)}% dari posisi.<br>`;
  }
  h += `<b class="merah-t">SL &minus;${slH}% dari posisi</b> (ROI &minus;${pc(sl,1)}% dari margin) &rarr; rugi &minus;${u(p.rugiKotor)} `
    + `ditambah fee ${u(p.biaya)} &rarr; <b class="merah-t">TOTAL &minus;${u(p.rugiTotal)} USDT</b> `
    + `(ROI &minus;${pc(p.roiTotalSl)}%).<br>`;
  if (p.impas !== null) {
    h += `Rasio bersih &asymp; ${pc(p.rasio)} : 1 &rarr; perlu menang minimal <b>${pc(p.impas*100,1)}%</b> trade untuk impas.`;
  }
  if (sl >= 90) {
    h += `<br><span class="merah-t"><b>SL terlalu jauh:</b> &minus;${slH}% di leverage ${lev}x = &minus;${pc(sl,1)}% margin &mdash; `
      + `posisi TERLIKUIDASI sebelum SL kena. Maksimal sekitar ${pc(90/lev,2)}% dari posisi.</span>`;
  }
  if (p.slDiBawahFee) {
    h += `<br><span class="kuning-t">Perhatian: gerak harga SL (${pc(p.s*100,3)}%) lebih kecil dari fee bolak-balik `
      + `(${pc(2*feeTerakhir*100,3)}%) &mdash; fee lebih besar dari rugi harganya sendiri, dan gerakan sekecil ini `
      + `bisa terjadi dalam hitungan detik.</span>`;
  }
  el.innerHTML = h;
}

// Notional = amount x HARGA TERAKHIR dari bursa (ticker "last"), BUKAN
// angka karangan. Kalau harga belum termuat, JANGAN tampilkan tebakan --
// lebih baik bilang belum tahu.
//
// Dua angka ditampilkan karena keduanya beda dan sering tertukar:
//   notional = nilai kontrak yang masuk ke pasar
//   margin   = uang Anda yang benar-benar terkunci = notional / leverage
function hitungNotional() {
  const el = pilih("ket_amount");
  if (!el) return;
  const amt = parseFloat(pilih("c_amount").value);
  if (!amt || !hargaTerakhir) { el.innerHTML = "&mdash;"; return; }
  const notional = amt * hargaTerakhir;
  const levInput = Math.max(1, Math.round(+pilih("c_leverage").value || 1));
  el.innerHTML = `<b>${notional.toFixed(2)} USDT</b> &middot; margin ${(notional/levInput).toFixed(2)} (${levInput}x)`;
  hitungSlTp();
}

// Kurva ekuitas, digambar manual tanpa library.
//
// Sumbu X memakai URUTAN FILL, bukan waktu. Alasannya: bot ini bisa
// menghasilkan ratusan fill yang menggerombol di beberapa jam saja,
// lalu diam berhari-hari. Dengan sumbu waktu, hasilnya gurun datar
// panjang diselingi tebing vertikal -- yang justru menyembunyikan
// pergerakannya. Dengan urutan fill, tiap transaksi dapat lebar yang
// sama dan bentuk kurvanya terbaca. Konteks waktu tetap ada lewat
// label tanggal di bawah.
function gambarKurva(titik, porto) {
  if (!titik || titik.length < 2) {
    return `<div class="sub">Belum ada transaksi di rentang ini.</div>`;
  }
  const n = titik.length;
  const W = 1200, H = 340, padL = 78, padR = 26, padT = 26, padB = 56;
  const ys = titik.map(p => p.v);
  let lo = Math.min(...ys, 0), hi = Math.max(...ys, 0);
  if (hi === lo) hi = lo + 1;
  const ruang = (hi - lo) * 0.1; lo -= ruang; hi += ruang;
  const px = i => padL + (n === 1 ? 0 : i / (n - 1)) * (W - padL - padR);
  const py = v => padT + (1 - (v - lo) / (hi - lo)) * (H - padT - padB);

  const akhir = ys[n - 1];
  const iPuncak = ys.indexOf(Math.max(...ys)), iLembah = ys.indexOf(Math.min(...ys));
  const puncak = ys[iPuncak], lembah = ys[iLembah];
  const warna = akhir >= 0 ? "var(--hijau)" : "var(--merah)";
  const y0 = py(0);

  let bantu = "";
  for (let i = 0; i <= 4; i++) {
    const v = lo + (hi - lo) * i / 4, y = py(v);
    bantu += `<line class="sumbu" x1="${padL}" y1="${y.toFixed(1)}" x2="${W-padR}" y2="${y.toFixed(1)}"/>`
           + `<text x="${padL-9}" y="${(y+3.5).toFixed(1)}" text-anchor="end">${v.toFixed(1)}</text>`;
  }
  bantu += `<line class="nol" x1="${padL}" y1="${y0.toFixed(1)}" x2="${W-padR}" y2="${y0.toFixed(1)}"/>`
         + `<text x="${padL+5}" y="${(y0-6).toFixed(1)}" style="opacity:.85">impas</text>`;
  const ty = padT + (H - padT - padB) / 2;
  bantu += `<text x="16" y="${ty.toFixed(1)}" transform="rotate(-90 16 ${ty.toFixed(1)})" text-anchor="middle" style="font-size:11px">P&amp;L kumulatif (USDT)</text>`;

  // Label waktu di 4 titik -- konteks kapan, tanpa mengorbankan bentuk kurva
  const fmt = ms => new Date(ms).toLocaleString("id-ID", {day:"2-digit", month:"short", hour:"2-digit", minute:"2-digit"});
  for (let k = 0; k <= 3; k++) {
    const i = Math.round((n - 1) * k / 3), x = px(i);
    const anchor = k === 0 ? "start" : (k === 3 ? "end" : "middle");
    bantu += `<line class="sumbu" x1="${x.toFixed(1)}" y1="${padT}" x2="${x.toFixed(1)}" y2="${H-padB}" opacity=".45"/>`
           + `<text x="${x.toFixed(1)}" y="${H-32}" text-anchor="${anchor}">${fmt(titik[i].t)}</text>`;
  }
  bantu += `<text x="${((padL+W-padR)/2).toFixed(1)}" y="${H-10}" text-anchor="middle" style="font-size:11px">urutan fill (1 &#8211; ${n-1})</text>`;

  const garis = titik.map((p,i) => (i?"L":"M") + px(i).toFixed(1) + " " + py(p.v).toFixed(1)).join(" ");
  const area = `M ${px(0).toFixed(1)} ${y0.toFixed(1)} `
             + titik.map((p,i) => "L " + px(i).toFixed(1) + " " + py(p.v).toFixed(1)).join(" ")
             + ` L ${px(n-1).toFixed(1)} ${y0.toFixed(1)} Z`;

  // Titik per fill HANYA kalau jumlahnya sedikit. Di atas 80 fill,
  // titik-titik itu saling menempel dan justru menutupi garisnya.
  const noktah = n <= 80
    ? titik.map((p,i) => `<circle cx="${px(i).toFixed(1)}" cy="${py(p.v).toFixed(1)}" r="2.4"
        fill="var(--bg)" stroke="${warna}" stroke-width="1.4"/>`).join("")
    : "";

  // Penanda puncak / lembah / akhir -- tiga angka yang paling dicari
  function tanda(i, v, label, warnaT, atas) {
    const x = px(i), y = py(v);
    return `<circle cx="${x.toFixed(1)}" cy="${y.toFixed(1)}" r="4" fill="${warnaT}"/>`
         + `<text x="${x.toFixed(1)}" y="${(atas ? y-10 : y+16).toFixed(1)}" text-anchor="middle"
              style="fill:${warnaT};font-weight:600">${label}</text>`;
  }
  let penanda = "";
  if (n > 2) {
    penanda += tanda(iPuncak, puncak, (puncak>=0?"+":"")+puncak.toFixed(1), "var(--hijau)", true);
    penanda += tanda(iLembah, lembah, (lembah>=0?"+":"")+lembah.toFixed(1), "var(--merah)", false);
  }
  penanda += tanda(n-1, akhir, (akhir>=0?"+":"")+akhir.toFixed(1), warna, akhir >= puncak*0.98);

  // Titik hover DIBULATKAN ke rentang, tidak satu per satu fill.
  //
  // Alasannya: di 385 fill, jarak antar titik cuma ~2 piksel. Menyorot
  // satu fill berarti angkanya melompat tiap gerakan kecil mouse, dan
  // "fill ke-237" juga tidak berarti apa-apa bagi pembaca non-teknis.
  // Dengan dikelompokkan jadi maksimal 40 rentang, tiap hover memberi
  // satu periode yang jelas: kapan, berapa hasilnya, berubah berapa.
  //
  // Garis kurvanya TETAP digambar dari seluruh titik -- yang dibulatkan
  // hanya titik berhentinya hover, bukan bentuk grafiknya.
  const MAKS_RENTANG = 40;
  const lebarKel = Math.max(1, Math.ceil((n - 1) / MAKS_RENTANG));
  const hover = [];
  for (let mulai = 0; mulai < n; mulai += lebarKel) {
    const habis = Math.min(mulai + lebarKel - 1, n - 1);
    const sebelum = mulai === 0 ? titik[0].v : titik[mulai - 1].v;
    hover.push([
      +px(habis).toFixed(1),            // 0 x
      +py(titik[habis].v).toFixed(1),   // 1 y
      titik[habis].v,                   // 2 nilai di akhir rentang
      titik[mulai].t,                   // 3 waktu mulai
      titik[habis].t,                   // 4 waktu selesai
      titik[habis].v - sebelum,         // 5 perubahan dalam rentang
      habis - mulai + 1,                // 6 jumlah transaksi
    ]);
    if (habis === n - 1) break;
  }
  const dataTitik = JSON.stringify(hover);

  return `<svg id="svg_kurva" class="kurva" viewBox="0 0 ${W} ${H}"
      data-titik='${dataTitik}' data-plot="${padL},${W-padR},${padT},${H-padB}"
      data-warna="${akhir>=0?'hijau':'merah'}"
      role="img" aria-label="Kurva P&amp;L kumulatif bersih">
      ${bantu}
      <path d="${area}" fill="${warna}" opacity="0.10"/>
      <path class="garis" style="stroke:${warna};stroke-width:1.8" d="${garis}"/>
      ${noktah}${penanda}
      <g id="tip_g" style="display:none;pointer-events:none">
        <line id="tip_garis" y1="${padT}" y2="${H-padB}"
          stroke="var(--fg)" stroke-width="1" opacity=".45" stroke-dasharray="3 3"/>
        <circle id="tip_bulat" r="4.5" fill="${warna}" stroke="var(--bg)" stroke-width="2"/>
        <rect id="tip_kotak" rx="5" fill="var(--bg)" stroke="var(--line)" stroke-width="1"/>
        <text id="tip_baris1" style="font-size:12px;font-weight:600;fill:var(--fg)"></text>
        <text id="tip_baris2" style="font-size:10.5px;fill:var(--muted)"></text>
        <text id="tip_baris3" style="font-size:10.5px;font-weight:600"></text>
      </g>
      <rect id="tip_area" x="${padL}" y="${padT}" width="${W-padL-padR}" height="${H-padT-padB}"
        fill="transparent" style="cursor:crosshair"/>
    </svg>
    <div class="grid" style="margin-top:16px">
      <div class="item"><div class="label">Posisi akhir</div>
        <div class="val" style="color:${warna}">${akhir>=0?"+":""}${akhir.toFixed(2)} USDT</div></div>
      <div class="item"><div class="label">Puncak tertinggi</div>
        <div class="val">${puncak>=0?"+":""}${puncak.toFixed(2)}</div>
        <div class="note">fill ke-${iPuncak}</div></div>
      <div class="item"><div class="label">Titik terendah</div>
        <div class="val">${lembah>=0?"+":""}${lembah.toFixed(2)}</div>
        <div class="note">fill ke-${iLembah}</div></div>
      <div class="item"><div class="label">Turun dari puncak</div>
        <div class="val ${(puncak-akhir)>0.005?"merah-t":""}">${(puncak-akhir).toFixed(2)}</div></div>
      <div class="item"><div class="label">Jumlah fill</div><div class="val">${n-1}</div></div>
      ${porto && porto.pct!==null && porto.pct!==undefined ? `<div class="item"><div class="label">Terhadap modal</div>
        <div class="val ${porto.pct>=0?"hijau-t":"merah-t"}">${porto.pct>=0?"+":""}${(porto.pct*100).toFixed(3)}%</div></div>` : ""}
    </div>
    <div class="sub" style="margin-top:10px">Sumbu mendatar = urutan fill, bukan waktu &mdash;
      supaya jeda panjang antar sesi tidak meratakan kurvanya. Nilai = realized P&amp;L dikurangi fee.
      ${porto && porto.asumsi ? "<br>Persentase: " + porto.asumsi + "." : ""}</div>`;
}

// Hover pada kurva. Dipasang ULANG tiap render karena innerHTML
// mengganti seluruh elemen SVG -- listener lama ikut terbuang bersamanya.
function pasangHover() {
  const svg = pilih("svg_kurva");
  if (!svg) return;
  let titik;
  try { titik = JSON.parse(svg.dataset.titik || "[]"); } catch (e) { return; }
  if (!titik.length) return;

  const [plotKiri, plotKanan, plotAtas, plotBawah] = svg.dataset.plot.split(",").map(Number);
  const W = svg.viewBox.baseVal.width;
  const g = pilih("tip_g"), garis = pilih("tip_garis"), bulat = pilih("tip_bulat");
  const kotak = pilih("tip_kotak"), b1 = pilih("tip_baris1"), b2 = pilih("tip_baris2"),
        b3 = pilih("tip_baris3");

  function posisikan(ev) {
    // Lebar tampil SVG berubah mengikuti layar, sementara koordinat di
    // dalamnya memakai viewBox. Jadi posisi mouse harus diskalakan dulu,
    // kalau tidak titik yang disorot akan meleset makin jauh ke kanan.
    const kotakSvg = svg.getBoundingClientRect();
    const x = (ev.clientX - kotakSvg.left) / kotakSvg.width * W;

    let terdekat = 0, jarak = Infinity;
    for (let i = 0; i < titik.length; i++) {
      const d = Math.abs(titik[i][0] - x);
      if (d < jarak) { jarak = d; terdekat = i; }
    }
    const [tx, ty, nilai, tMulai, tSelesai, delta, jumlahTx] = titik[terdekat];

    garis.setAttribute("x1", tx); garis.setAttribute("x2", tx);
    bulat.setAttribute("cx", tx); bulat.setAttribute("cy", ty);

    // Angka dibulatkan 2 desimal dengan pemisah ribuan gaya Indonesia --
    // pembacanya manajemen, bukan orang yang terbiasa baca angka mentah.
    const rupiahkan = v => (v >= 0 ? "+" : "\u2212") +
      Math.abs(v).toLocaleString("id-ID", {minimumFractionDigits: 2, maximumFractionDigits: 2});
    const jam = ms => new Date(ms).toLocaleString("id-ID",
      {day: "2-digit", month: "short", hour: "2-digit", minute: "2-digit"});

    const teks1 = rupiahkan(nilai) + " USDT";
    const periode = (tMulai === tSelesai) ? jam(tSelesai) : jam(tMulai) + " \u2013 " + jam(tSelesai);
    const teks2 = periode;
    const teks3 = rupiahkan(delta) + " pada periode ini \u00b7 " + jumlahTx +
      (jumlahTx > 1 ? " transaksi" : " transaksi");
    b1.textContent = teks1; b2.textContent = teks2; b3.textContent = teks3;

    const lebar = Math.max(teks1.length * 7.8, teks2.length * 5.8, teks3.length * 5.8) + 22;
    const tinggi = 56;
    // Balik ke kiri kalau mepet tepi kanan, supaya kotaknya tidak terpotong.
    let kx = tx + 14;
    if (kx + lebar > plotKanan) kx = tx - 14 - lebar;
    let ky = ty - tinggi - 10;
    if (ky < plotAtas) ky = ty + 12;

    kotak.setAttribute("x", kx); kotak.setAttribute("y", ky);
    kotak.setAttribute("width", lebar); kotak.setAttribute("height", tinggi);
    b1.setAttribute("x", kx + 11); b1.setAttribute("y", ky + 19);
    b2.setAttribute("x", kx + 11); b2.setAttribute("y", ky + 34);
    b3.setAttribute("x", kx + 11); b3.setAttribute("y", ky + 48);
    b3.style.fill = delta >= 0 ? "var(--hijau)" : "var(--merah)";
    g.style.display = "";
  }

  const area = pilih("tip_area");
  area.addEventListener("mousemove", posisikan);
  area.addEventListener("mouseleave", () => { g.style.display = "none"; });
  // Sentuhan di layar sentuh diperlakukan sama seperti gerakan mouse.
  area.addEventListener("touchmove", ev => {
    if (ev.touches[0]) posisikan(ev.touches[0]);
  }, {passive: true});
}

// SL/TP yang BENAR-BENAR ada di bursa untuk posisi terbuka. Peringatan
// merah kalau bot jalan dengan mode SL/TP tapi SL tidak ditemukan --
// artinya posisi sedang tidak terlindungi.
function panelSlTp(d, modeSlTp) {
  const ords = d.bracket_orders;
  const entry = d.position && d.position.entry_price;
  if (ords === null || ords === undefined) {
    return `<div class="sub" style="margin-top:14px">SL/TP di bursa: tidak bisa dibaca saat ini.</div>`;
  }
  const sl = ords.find(o => (o.type || "").startsWith("STOP"));
  const tp = ords.find(o => (o.type || "").startsWith("TAKE_PROFIT"));
  const jarak = v => (entry && v) ? ((v - entry) / entry * 100) : null;
  const lev = d.position && d.position.leverage;
  const sel = (label, o, kelas) => {
    if (!o) return `<div class="item"><div class="label">${label}</div><div class="val">&mdash;</div>
      <div class="note">tidak ada di bursa</div></div>`;
    const j = jarak(o.trigger_price);
    return `<div class="item"><div class="label">${label}</div>
      <div class="val ${kelas}">${o.trigger_price ? o.trigger_price.toFixed(2) : "?"}</div>
      <div class="note">${j !== null ? "harga " + (j >= 0 ? "+" : "\u2212") + Math.abs(j).toFixed(3) + "%"
        + (lev ? " &middot; <b>ROI " + (j >= 0 ? "+" : "\u2212") + Math.abs(j * lev).toFixed(2) + "%</b> (" + lev.toFixed(0) + "x)" : "")
        : ""}</div></div>`;
  };
  let h = "";
  if (!sl && modeSlTp) {
    h += `<div class="err" style="margin-top:14px"><b>Posisi TIDAK dilindungi stop loss di bursa.</b>
      Bot berjalan dengan mode SL/TP, tapi order SL tidak ditemukan. Cek log bot di bawah.</div>`;
  }
  h += `<div class="grid" style="margin-top:14px">${sel("Stop loss di bursa", sl, "merah-t")}${sel("Take profit di bursa", tp, "hijau-t")}</div>`;
  return h;
}

// Kenapa bot TIDAK membuka posisi padahal sinyal ada. Diambil dari
// session_state.json yang ditulis bot tiap bar. Hanya tampil kalau bot
// sedang jalan -- file itu tidak dibersihkan saat bot mati, jadi
// catatan lamanya bisa menyesatkan.
function panelStatusBot(d, b) {
  const ss = d.session_state;
  const catatan = (b && b.running && ss && ss.status_note) ? ss.status_note : "";
  if (!catatan) return "";
  const aman = catatan.replace(/&/g, "&amp;").replace(/</g, "&lt;");
  const gawat = /^(DARURAT|DIHENTIKAN|BATAS HARIAN)/.test(catatan);
  return `<div class="${gawat ? "err" : "warn"}" style="margin-top:14px">`
    + `<b>Status bot:</b> ${aman}</div>`;
}

// Banner akun. Fungsi murni (diuji): merah untuk akun asli, juga merah
// kalau TRADING_ACCOUNT belum diisi (tombol Mulai tidak akan jalan).
function panelAkun(ak) {
  if (ak && ak.is_live === true) {
    return `<div class="err" style="margin:12px 0 0"><b>AKUN UANG ASLI.</b> Setiap order yang dikirim bot `
      + `dari halaman ini memakai dana sungguhan.</div>`;
  }
  if (ak && ak.error) {
    return `<div class="err" style="margin:12px 0 0"><b>Akun belum diatur:</b> `
      + String(ak.error).replace(/&/g, "&amp;").replace(/</g, "&lt;") + `</div>`;
  }
  return "";
}

// Keterangan satu baris di samping pilihan mode, supaya jelas akibatnya
// sebelum bot dijalankan.
function ketModeTp() {
  const el = pilih("ket_mode_tp"); if (!el) return;
  const m = pilih("c_mode_tp").value;
  el.textContent = m === "sekali"
    ? "TP kena \u2192 posisi ditutup \u2192 bot mati."
    : m === "tengah"
    ? "TP/SL kena \u2192 tunggu harga kembali ke tengah channel \u2192 masuk lagi saat breakout baru searah."
    : "TP/SL kena \u2192 tunggu sinyal berbalik arah, baru masuk lagi.";
}

function render(d) {
  snapshotTerakhir = d;
  pilih("jam").textContent = "diperbarui " + (d.last_update ? new Date(d.last_update).toLocaleTimeString("id-ID") : "?")
    + (d.n_trades_loaded !== undefined ? ` · ${d.n_trades_loaded} fill dimuat (${d.history_days} hari terakhir)` : "");

  // Simpan harga & leverage supaya keterangan di bawah field Amount ikut
  // hidup mengikuti pasar, bukan hanya saat pengguna mengetik.
  if (typeof d.price === "number") hargaTerakhir = d.price;
  leverageTerakhir = (d.position && d.position.leverage) ? d.position.leverage : null;
  if (typeof d.atr === "number") atrTerakhir = d.atr;
  // Selalu timpa, termasuk saat null/galat: jangan pertahankan tarif lama.
  feeTerakhir = d.status !== "galat" && d.fee_status === "ok" && d.fee_source === "binance"
    && Number.isFinite(d.fee_taker) && d.fee_taker >= 0 && d.fee_taker < 1 ? d.fee_taker : null;
  feeStatus = feeTerakhir !== null ? "ok" : (d.status === "memuat" ? "memuat" : "gagal");
  if (d.atr_period) periodeAtr = d.atr_period;
  leverageAcuan = leverageTerakhir || d.leverage_terakhir || null;
  hitungNotional();
  hitungSlTp();

  const ak = d.account || {};
  akunLive = ak.is_live === true ? true : (ak.is_live === false ? false : null);
  pilih("akun_label").textContent = ak.label || "akun belum terbaca";
  pilih("banner_akun").innerHTML = panelAkun(ak);

  const b = d.bot || {};
  pilih("btn_start").disabled = !!b.running;
  pilih("btn_stop").disabled = !b.running;
  if (b.cmd) pilih("cmd_preview").textContent = "$ " + b.cmd;

  let h = "";
  if (d.status === "galat") {
    h += `<div class="err"><b>Gagal ambil data bursa:</b> <span class="mono">${d.last_error||"?"}</span><br>
          Angka di bawah snapshot TERAKHIR yang berhasil, bukan kondisi sekarang.</div>`;
  }
  if (feeStatus === "gagal") {
    h += '<div class="warn"><b>Gagal mengambil tarif fee dari Binance.</b> '
      + 'Estimasi biaya dan profit bersih posisi tidak tersedia; tidak memakai asumsi fee.</div>';
  }
  if (d.agg && d.agg.n_fee_unknown > 0) {
    h += `<div class="warn"><b>Perhatian:</b> ${d.agg.n_fee_unknown} dari ${d.agg.n_fills} fill tidak punya
          data fee &mdash; angka BERSIH kemungkinan lebih optimis dari kenyataan.</div>`;
  }

  // ---- Status bot + channel breakout ----
  const c = d.channel;
  const modeSlTp = !!(b.running && b.cmd && (b.cmd.includes("--risk-reward") || b.cmd.includes("--tp-roi-pct")));
  const angkaFlag = nama => ((b.cmd || "").match(new RegExp("--" + nama + " (\\S+)")) || [,"?"])[1].replace(/\.0$/, "");
  h += `<h2>Status</h2><div class="panel"><div class="grid">
    <div class="item"><div class="label">Proses bot</div><div class="val">
      <span class="chip ${b.running?"on":"off"}">${b.running?"JALAN":"MATI"}</span></div>
      <div class="note">${b.running && b.cmd ? (b.cmd.includes("--stop-after-take-profit")
        ? "mode: sekali saja" : (b.cmd.includes("--reentry-mode midline")
          ? "mode: berulang dari tengah" : "mode: berulang, tunggu berbalik")) + (modeSlTp
        ? (b.cmd.includes("--tp-roi-pct")
          ? (b.cmd.includes("--tp-sl-price-pct")
            ? " &middot; TP +" + angkaFlag("tp-roi-pct") + "% / SL \u2212" + angkaFlag("sl-roi-pct") + "% dari posisi"
            : " &middot; TP ROI +" + angkaFlag("tp-roi-pct") + "% / SL ROI \u2212" + angkaFlag("sl-roi-pct") + "%")
          : " &middot; SL/TP " + angkaFlag("risk-reward") + ":1")
        : " &middot; tanpa SL/TP") : ""}</div></div>
    <div class="item"><div class="label">Posisi di bursa</div><div class="val ${
      d.position?(d.position.side==="long"?"hijau-t":"merah-t"):""}">${
      d.position ? d.position.side.toUpperCase()+" "+d.position.contracts : "KOSONG"}</div></div>
    <div class="item"><div class="label">Harga kini</div><div class="val">${d.price?d.price.toFixed(2):"?"}</div></div>
    <div class="item"><div class="label">Harga masuk</div><div class="val">${
      d.position&&d.position.entry_price?d.position.entry_price.toFixed(2):"&mdash;"}</div></div>
    <div class="item"><div class="label">Leverage</div><div class="val">${
      d.position&&d.position.leverage?d.position.leverage.toFixed(0)+"x":"&mdash;"}</div></div>
    <div class="item"><div class="label">Saldo dompet</div><div class="val">${d.wallet!==undefined?d.wallet.toFixed(2):"?"}</div></div>
    <div class="item"><div class="label">Pertumbuhan portofolio</div><div class="val ${
      !d.porto||d.porto.pct===null ? "" : (d.porto.pct>=0?"hijau-t":"merah-t")}">${
      !d.porto||d.porto.pct===null ? "&mdash;" : (d.porto.pct>=0?"+":"")+(d.porto.pct*100).toFixed(3)+"%"}</div>
      <div class="note">${d.porto&&d.porto.modal_awal
        ? (d.porto.net>=0?"+":"")+d.porto.net.toFixed(2)+" USDT dari modal "+d.porto.modal_awal.toFixed(2)
        : "belum ada data saldo"}</div></div>
  </div>${panelStatusBot(d, b)}</div>`;

  // ---- Posisi berjalan: untung/rugi yang BELUM direalisasi ----
  // Angka utama disamakan dengan yang tampil di aplikasi Binance, supaya
  // tidak perlu membuka Binance hanya untuk mengecek posisi sedang untung
  // atau rugi.
  const pb = d.pnl_berjalan;
  const feePosisiTersedia = feeTerakhir !== null && Number.isFinite(pb?.bersih_setelah_fee)
    && Number.isFinite(pb?.fee_total);
  const ang = v => { const dp = Math.abs(v) < 1 ? 3 : 2;
    return (v >= 0 ? "+" : "\u2212") + Math.abs(v).toFixed(dp); };
  const wr = v => v >= 0 ? "hijau-t" : "merah-t";
  h += `<h2>Posisi berjalan &mdash; untung/rugi saat ini</h2><div class="panel">`;
  if (!d.position) {
    h += `<div class="sub">Tidak ada posisi terbuka.</div>`;
  } else if (!pb) {
    h += `<div class="sub">Ada posisi terbuka, tapi data untung/rugi belum bisa diambil dari bursa.</div>`;
  } else {
    h += `<div class="grid">
      <div class="item"><div class="label">Untung / rugi</div>
        <div class="val ${wr(pb.pnl)}" style="font-size:24px">${ang(pb.pnl)} USDT</div>
        <div class="note">${pb.sumber === "bursa" ? "seperti di Binance" : "dihitung dashboard"} &middot; sebelum fee</div></div>
      <div class="item"><div class="label">ROI terhadap margin</div>
        <div class="val ${wr(pb.pnl)}">${pb.roi_pct !== null ? (pb.roi_pct >= 0 ? "+" : "\u2212") + Math.abs(pb.roi_pct * 100).toFixed(2) + "%" : "&mdash;"}</div>
        <div class="note">margin ${pb.margin_awal ? pb.margin_awal.toFixed(2) : "?"} USDT${pb.leverage ? " &middot; " + pb.leverage.toFixed(0) + "x" : ""}</div></div>
      <div class="item"><div class="label">Kalau ditutup sekarang</div>
        <div class="val ${feePosisiTersedia ? wr(pb.bersih_setelah_fee) : ""}">${
          feePosisiTersedia ? ang(pb.bersih_setelah_fee) + " USDT" : "&mdash;"}</div>
        <div class="note">${feePosisiTersedia
          ? "perkiraan setelah fee " + pb.fee_total.toFixed(3) + " USDT (tarif Binance)"
          : "Estimasi bersih tidak tersedia: tarif fee Binance gagal dibaca."}</div></div>
      <div class="item"><div class="label">Pergerakan harga</div>
        <div class="val">${pb.gerak_harga_pct !== null ? ang(pb.gerak_harga_pct * 100) + "%" : "&mdash;"}</div>
        <div class="note">${pb.side.toUpperCase()} ${pb.entry.toFixed(2)} &rarr; ${pb.harga_acuan ? pb.harga_acuan.toFixed(2) : "?"} (${pb.acuan})</div></div>
    </div>`;
  }
  if (d.position) h += panelSlTp(d, modeSlTp);
  h += `</div>`;

  h += `<h2>Batas breakout &mdash; channel Donchian</h2><div class="panel">`;
  if (!c) {
    h += `<div class="sub">Belum cukup data kline untuk menghitung channel.</div>`;
  } else {
    h += `<div class="grid">
      <div class="item"><div class="label">Lookback</div><div class="val">${c.lookback} bar</div></div>
      <div class="item"><div class="label">Batas atas (breakout long)</div>
        <div class="val hijau-t">${c.upper.toFixed(2)}</div></div>
      <div class="item"><div class="label">Jarak ke atas</div>
        <div class="val ${c.dist_upper<0.002?"kuning-t":""}">${uang(c.dist_upper*100,3)}%</div></div>
      <div class="item"><div class="label">Batas bawah (breakout short)</div>
        <div class="val merah-t">${c.lower.toFixed(2)}</div></div>
      <div class="item"><div class="label">Jarak ke bawah</div>
        <div class="val ${c.dist_lower<0.002?"kuning-t":""}">${uang(c.dist_lower*100,3)}%</div></div>
      <div class="item"><div class="label">Harga acuan</div><div class="val">${c.current.toFixed(2)}</div></div>
    </div>
    <div class="sub" style="margin-top:12px">Rumus sama persis dengan yang dipakai sinyal bot: max/min
      <b>close</b> dari ${c.lookback} bar sebelum bar terakhir. Jarak kecil = breakout makin dekat.</div>`;
  }
  h += `</div>`;

  // ---- Filter tanggal ----
  const fl = d.filter || {};
  h += `<h2>Perkembangan portofolio &mdash; P&amp;L kumulatif bersih</h2><div class="panel">
    ${gambarKurva(d.equity_curve, d.porto)}</div>`;

  h += `<h2>Untung dan rugi &mdash; per rentang tanggal</h2>
    <div class="panel" style="margin-bottom:14px"><div class="baris" style="margin-top:0">
      <div class="f"><label>Dari</label><input type="date" id="f_dari" onchange="muat()"></div>
      <div class="f"><label>Sampai</label><input type="date" id="f_sampai" onchange="muat()"></div>
      <button onclick="setHariIni()">Hari ini</button>
      <button onclick="set7Hari()">7 hari</button>
      <button onclick="setSemua()">Semua</button>
      <span class="sub">${fl.n_after_filter !== undefined ? fl.n_after_filter + " fill dalam rentang" : ""}</span>
    </div></div>`;

  if (d.agg) {
    const a = d.agg;
    const persen = d.wallet ? (a.net / d.wallet * 100) : null;
    h += `<div class="cards">
      <div class="card hijau"><div class="label">Total untung &middot; ${a.n_wins} transaksi</div>
        <div class="big hijau-t">${uang(a.total_profit)} USDT</div><div class="note">belum dikurangi fee</div></div>
      <div class="card merah"><div class="label">Total rugi &middot; ${a.n_losses} transaksi</div>
        <div class="big merah-t">${uang(a.total_loss)} USDT</div><div class="note">belum dikurangi fee</div></div>
      <div class="card tebal ${a.net>=0?"hijau":"merah"}"><div class="label">Bersih &mdash; sudah dikurangi fee</div>
        <div class="big ${warna(a.net)}">${uang(a.net)} USDT</div>
        <div class="note">realisasi ${uang(a.total_realized)} &minus; fee ${a.total_fee.toFixed(4)}${
          persen!==null?` &middot; ${uang(persen,3)}% dari saldo`:""}</div></div>
    </div>`;

    h += `<h2>Rincian rentang ini</h2><div class="panel"><div class="grid">
      <div class="item"><div class="label">Transaksi penutup</div><div class="val">${a.n_closing_trades}</div></div>
      <div class="item"><div class="label">Menang / kalah</div><div class="val">${a.n_wins} / ${a.n_losses}</div></div>
      <div class="item"><div class="label">Tingkat menang</div><div class="val">${(a.win_rate*100).toFixed(1)}%</div></div>
      <div class="item"><div class="label">Rata menang</div><div class="val hijau-t">${uang(a.avg_win,4)}</div></div>
      <div class="item"><div class="label">Rata kalah</div><div class="val merah-t">${uang(a.avg_loss,4)}</div></div>
      <div class="item"><div class="label">Menang terbaik</div><div class="val hijau-t">${uang(a.best_win,4)}</div></div>
      <div class="item"><div class="label">Kalah terburuk</div><div class="val merah-t">${uang(a.worst_loss,4)}</div></div>
      <div class="item"><div class="label">Rasio untung/rugi</div><div class="val">${
        a.profit_factor===null?"&mdash;":a.profit_factor.toFixed(2)}</div></div>
      <div class="item"><div class="label">Total fee dibayar</div><div class="val merah-t">${a.total_fee.toFixed(4)}</div></div>
      <div class="item"><div class="label">Jumlah fill</div><div class="val">${a.n_fills}</div></div>
    </div></div>`;
  }

  if (d.recent && d.recent.length) {
    h += `<h2>Riwayat transaksi &mdash; ${d.recent.length} fill terbaru dalam rentang</h2>
      <div class="panel scroll"><table>
      <tr><th>Waktu</th><th>Sisi</th><th>Harga</th><th>Jumlah</th><th>Fee</th><th>Realisasi</th><th>Peran</th></tr>`;
    for (const t of d.recent) {
      h += `<tr><td class="mono">${t.time?new Date(t.time).toLocaleString("id-ID"):"?"}</td>
        <td class="${t.side==="buy"?"hijau-t":"merah-t"}">${t.side.toUpperCase()}</td>
        <td>${t.price.toFixed(2)}</td><td>${t.amount}</td>
        <td class="merah-t">${t.fee.toFixed(5)}</td>
        <td class="${warna(t.pnl)}">${t.pnl===0?"&mdash;":uang(t.pnl,5)}</td>
        <td class="mono">${t.role}</td></tr>`;
    }
    h += `</table></div>`;
  } else {
    h += `<h2>Riwayat transaksi</h2><div class="panel"><div class="sub">
      Tidak ada transaksi dalam rentang tanggal ini.</div></div>`;
  }

  h += `<h2>Keluaran bot &mdash; apa pun yang dicetak, termasuk galat</h2>
    <pre class="log" id="logbox">${(b.log||["(bot belum pernah dijalankan dari dashboard ini)"]).join("\n")
      .replace(/&/g,"&amp;").replace(/</g,"&lt;")}</pre>`;

  const dari = pilih("f_dari")?.value, sampai = pilih("f_sampai")?.value;
  pilih("isi").innerHTML = h;
  pasangHover();  // listener ikut terbuang saat innerHTML diganti -- pasang ulang
  if (dari) pilih("f_dari").value = dari;
  if (sampai) pilih("f_sampai").value = sampai;

  const lb = pilih("logbox");
  if (lb) lb.scrollTop = lb.scrollHeight;
}

ketModeTp();

async function muat() {
  try {
    const {from_ms, to_ms} = rentang();
    let url = "/api?";
    if (from_ms) url += "from_ms=" + from_ms + "&";
    if (to_ms) url += "to_ms=" + to_ms;
    const response = await fetch(url);
    if (!response.ok) throw new Error("HTTP " + response.status);
    const data = await response.json();
    if (!data || typeof data !== "object" || Array.isArray(data)) throw new Error("Respons API tidak valid");
    render(data);
  } catch (e) {
    render({...snapshotTerakhir, status: "galat",
      last_error: "Gagal menghubungi dashboard atau respons API tidak valid.",
      fee_taker: null, fee_status: "gagal", fee_source: null});
  }
}
muat();
setInterval(muat, 5000);
isiOpsiRegime();
</script>
</body>
</html>
"""
# Batas bawah fee di pratinjau JS diisi dari konstanta Python yang SAMA,
# supaya tidak bisa diam-diam berbeda dari angka yang dipakai bot.
HTML_PAGE = HTML_PAGE.replace("__SL_MIN_FEE_MULT__", repr(float(SL_MIN_FEE_MULT)))


def make_handler(state: DashboardState, bot: BotProcess):
    class Handler(BaseHTTPRequestHandler):
        def _json(self, obj, code=200):
            body = json.dumps(obj).encode()
            self.send_response(code)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def do_GET(self):  # noqa: N802
            parsed = urlparse(self.path)
            if parsed.path.startswith("/api"):
                q = parse_qs(parsed.query)
                from_ms = int(q["from_ms"][0]) if "from_ms" in q else None
                to_ms = int(q["to_ms"][0]) if "to_ms" in q else None
                payload = state.payload(from_ms, to_ms)
                payload["bot"] = bot.snapshot()
                self._json(payload)
            else:
                body = HTML_PAGE.encode()
                self.send_response(200)
                self.send_header("Content-Type", "text/html; charset=utf-8")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

        def do_POST(self):  # noqa: N802
            parsed = urlparse(self.path)
            if parsed.path == "/api/start":
                length = int(self.headers.get("Content-Length") or 0)
                try:
                    cfg = json.loads(self.rfile.read(length) or "{}")
                except Exception as e:
                    return self._json({"ok": False, "message": f"Konfigurasi tidak valid: {e}"}, 400)
                ok, msg = bot.start(cfg)
                if ok:
                    # Samakan simbol/lookback/timeframe dashboard dengan bot
                    # yang baru dijalankan -- supaya channel yang ditampilkan
                    # SESUAI dengan yang dipakai bot, bukan parameter lama.
                    state.set_params(cfg["symbol"], int(cfg["lookback"]), cfg["timeframe"])
                return self._json({"ok": ok, "message": msg})
            if parsed.path == "/api/stop":
                ok, msg = bot.stop()
                return self._json({"ok": ok, "message": msg})
            self._json({"ok": False, "message": "endpoint tidak dikenal"}, 404)

        def log_message(self, *args):
            pass

    return Handler


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--symbol", default="BTC/USDT:USDT")
    p.add_argument("--timeframe", default="1m")
    p.add_argument("--lookback", type=int, default=200)
    p.add_argument("--port", type=int, default=8100)
    p.add_argument("--host", default="127.0.0.1",
                    help="alamat yang didengarkan. Default 127.0.0.1: hanya bisa dibuka lewat Caddy (berpassword) "
                         "atau dari server itu sendiri. 0.0.0.0 membuka tombol Mulai/Hentikan ke internet.")
    p.add_argument("--refresh-seconds", type=float, default=5.0)
    p.add_argument("--history-days", type=int, default=30,
                    help="berapa hari riwayat trade ditarik dari bursa untuk difilter di dashboard")
    args = p.parse_args()

    try:
        akun = resolve_account()
    except AccountConfigError as e:
        raise SystemExit(str(e))
    state = DashboardState(args.symbol, args.refresh_seconds, args.history_days, args.lookback, args.timeframe,
                           account=akun)
    bot = BotProcess()
    threading.Thread(target=state.loop, daemon=True).start()

    print("=== Dashboard Donchian ===")
    print(f"  Simbol awal : {args.symbol} {args.timeframe} lookback={args.lookback}")
    print(f"  Riwayat     : {args.history_days} hari terakhir (difilter per tanggal di UI)")
    print(f"  Akun        : {akun['label']}")
    print(f"  Buka        : http://{args.host}:{args.port}")
    print("  Dashboard TIDAK kirim order sendiri -- yang kirim order tetap bot.\n")

    HTTPServer((args.host, args.port), make_handler(state, bot)).serve_forever()


if __name__ == "__main__":
    main()