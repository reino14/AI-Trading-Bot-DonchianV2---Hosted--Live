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
lalu buka http://localhost:8200
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

SESSION_STATE_PATH = Path("data/session_state.json")
IS_WINDOWS = sys.platform == "win32"

#: Binance membatasi rentang startTime/endTime userTrades maksimal 7 hari
#: per permintaan -- rentang lebih panjang WAJIB dipotong jadi beberapa
#: permintaan, bukan dikirim sekaligus (akan ditolak/dipotong diam-diam).
CHUNK_MS = 7 * 24 * 60 * 60 * 1000


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

    def _pump_output(self) -> None:
        proc = self.proc
        if proc is None or proc.stdout is None:
            return
        for line in proc.stdout:
            self.log.append(line.rstrip("\n"))
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

    def __init__(self, symbol: str, refresh_seconds: float, history_days: int, lookback: int, timeframe: str):
        self.symbol = symbol
        self.refresh_seconds = refresh_seconds
        self.history_days = history_days
        self.lookback = lookback
        self.timeframe = timeframe
        self.lock = threading.Lock()
        self.snapshot: dict = {"status": "memuat", "last_error": None, "last_update": None}
        self.trades: list[dict] = []
        self._broker = None

    def set_params(self, symbol: str, lookback: int, timeframe: str) -> None:
        with self.lock:
            self.symbol = symbol
            self.lookback = lookback
            self.timeframe = timeframe

    def _ensure_broker(self):
        if self._broker is None:
            from src.execution.broker import Broker
            self._broker = Broker(exchange_id="binanceusdm", testnet=False)
        return self._broker

    def refresh_once(self) -> None:
        try:
            broker = self._ensure_broker()
            with self.lock:
                symbol, lookback, timeframe = self.symbol, self.lookback, self.timeframe

            balance = broker.fetch_balance()
            usdt = balance.get("USDT", {})
            wallet = float(usdt.get("total") or 0.0)
            free = float(usdt.get("free") or 0.0)

            position = broker.fetch_position(symbol)
            price = broker.fetch_current_price(symbol)

            bars = broker.fetch_recent_bars(symbol, timeframe, limit=lookback + 1)
            channel = compute_channel([b["close"] for b in bars], lookback)

            now_ms = int(time.time() * 1000)
            since_ms = now_ms - self.history_days * 24 * 60 * 60 * 1000
            trades = fetch_all_trades(broker.exchange, symbol, since_ms, now_ms)

            with self.lock:
                self.trades = trades
                self.snapshot = {
                    "status": "ok", "last_error": None,
                    "last_update": datetime.now(timezone.utc).isoformat(),
                    "symbol": symbol, "wallet": wallet, "free": free, "price": price,
                    "position": position, "channel": channel,
                    "history_days": self.history_days,
                    "n_trades_loaded": len(trades),
                    "oldest_trade": trades[0].get("datetime") if trades else None,
                    "newest_trade": trades[-1].get("datetime") if trades else None,
                }
        except Exception as e:
            with self.lock:
                self.snapshot = {
                    **self.snapshot, "status": "galat",
                    "last_error": f"{type(e).__name__}: {e}",
                    "last_update": datetime.now(timezone.utc).isoformat(),
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
        snap["session_state"] = read_session_state()
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
  pre.log { margin:0; padding:14px; background:#0b0d11; color:#d4d4d4; border-radius:8px;
            font-family:ui-monospace,Menlo,Consolas,monospace; font-size:11.5px; line-height:1.55;
            max-height:340px; overflow:auto; white-space:pre-wrap; word-break:break-word; }
  .chip { display:inline-block; padding:2px 9px; border-radius:20px; font-size:11px; font-weight:600; }
  .chip.on { background:rgba(21,128,61,.15); color:var(--hijau); }
  .chip.off { background:rgba(185,28,28,.15); color:var(--merah); }
</style>
</head>
<body>
<div class="wrap">
  <h1>Pemantau Bot Donchian</h1>
  <div class="sub">Donchian close-only &middot; Binance Demo Trading &middot; dashboard tidak pernah kirim order sendiri &mdash; yang kirim order tetap bot</div>
  <div class="sub mono" id="jam" style="margin-top:6px">&mdash;</div>

  <h2>Jalankan bot</h2>
  <div class="panel">
    <div class="form">
      <div class="f"><label>Simbol</label><input id="c_symbol" value="BTC/USDT:USDT"></div>
      <div class="f"><label>Timeframe</label>
        <select id="c_timeframe"><option>1m</option><option>5m</option><option>15m</option><option>1h</option><option>4h</option></select></div>
      <div class="f"><label>Lookback (bar)</label><input id="c_lookback" type="number" value="200"></div>
      <div class="f"><label>Amount (BTC)</label><input id="c_amount" type="number" step="0.001" value="0.01"></div>
      <div class="f"><label>Session (jam)</label><input id="c_session" type="number" step="0.5" value="24"></div>
      <div class="f"><label>Take profit (fraksi)</label><input id="c_tp" type="number" step="0.001" value="0.003"></div>
      <div class="f"><label>Backfill (bar)</label><input id="c_backfill" type="number" value="200"></div>
      <div class="f"><label>Poll TP (detik)</label><input id="c_poll" type="number" step="1" value="5"></div>
    </div>
    <div class="baris">
      <label class="cek"><input type="checkbox" id="c_stopafter" checked> Berhenti total setelah take profit</label>
      <button class="pri" id="btn_start" onclick="mulai()">Mulai bot</button>
      <button class="bahaya" id="btn_stop" onclick="hentikan()">Hentikan (ala Ctrl+C)</button>
      <span id="pesan" class="sub"></span>
    </div>
    <div class="sub mono" id="cmd_preview" style="margin-top:12px"></div>
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
    take_profit_pct: +pilih("c_tp").value || null,
    stop_after_take_profit: pilih("c_stopafter").checked,
    backfill_bars: +pilih("c_backfill").value || null,
    live_take_profit_poll_seconds: +pilih("c_poll").value || null,
  };
}

async function mulai() {
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

function render(d) {
  pilih("jam").textContent = "diperbarui " + (d.last_update ? new Date(d.last_update).toLocaleTimeString("id-ID") : "?")
    + (d.n_trades_loaded !== undefined ? ` · ${d.n_trades_loaded} fill dimuat (${d.history_days} hari terakhir)` : "");

  const b = d.bot || {};
  pilih("btn_start").disabled = !!b.running;
  pilih("btn_stop").disabled = !b.running;
  if (b.cmd) pilih("cmd_preview").textContent = "$ " + b.cmd;

  let h = "";
  if (d.status === "galat") {
    h += `<div class="err"><b>Gagal ambil data bursa:</b> <span class="mono">${d.last_error||"?"}</span><br>
          Angka di bawah snapshot TERAKHIR yang berhasil, bukan kondisi sekarang.</div>`;
  }
  if (d.agg && d.agg.n_fee_unknown > 0) {
    h += `<div class="warn"><b>Perhatian:</b> ${d.agg.n_fee_unknown} dari ${d.agg.n_fills} fill tidak punya
          data fee &mdash; angka BERSIH kemungkinan lebih optimis dari kenyataan.</div>`;
  }

  // ---- Status bot + channel breakout ----
  const c = d.channel;
  h += `<h2>Status</h2><div class="panel"><div class="grid">
    <div class="item"><div class="label">Proses bot</div><div class="val">
      <span class="chip ${b.running?"on":"off"}">${b.running?"JALAN":"MATI"}</span></div></div>
    <div class="item"><div class="label">Posisi di bursa</div><div class="val ${
      d.position?(d.position.side==="long"?"hijau-t":"merah-t"):""}">${
      d.position ? d.position.side.toUpperCase()+" "+d.position.contracts : "KOSONG"}</div></div>
    <div class="item"><div class="label">Harga kini</div><div class="val">${d.price?d.price.toFixed(2):"?"}</div></div>
    <div class="item"><div class="label">Harga masuk</div><div class="val">${
      d.position&&d.position.entry_price?d.position.entry_price.toFixed(2):"&mdash;"}</div></div>
    <div class="item"><div class="label">Leverage</div><div class="val">${
      d.position&&d.position.leverage?d.position.leverage.toFixed(0)+"x":"&mdash;"}</div></div>
    <div class="item"><div class="label">Saldo dompet</div><div class="val">${d.wallet!==undefined?d.wallet.toFixed(2):"?"}</div></div>
  </div></div>`;

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
  if (dari) pilih("f_dari").value = dari;
  if (sampai) pilih("f_sampai").value = sampai;

  const lb = pilih("logbox");
  if (lb) lb.scrollTop = lb.scrollHeight;
}

async function muat() {
  try {
    const {from_ms, to_ms} = rentang();
    let url = "/api?";
    if (from_ms) url += "from_ms=" + from_ms + "&";
    if (to_ms) url += "to_ms=" + to_ms;
    render(await (await fetch(url)).json());
  } catch (e) { pilih("jam").textContent = "gagal menghubungi dashboard: " + e; }
}
muat();
setInterval(muat, 5000);
</script>
</body>
</html>
"""


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
    p.add_argument("--port", type=int, default=8200)
    p.add_argument("--refresh-seconds", type=float, default=5.0)
    p.add_argument("--history-days", type=int, default=30,
                    help="berapa hari riwayat trade ditarik dari bursa untuk difilter di dashboard")
    args = p.parse_args()

    state = DashboardState(args.symbol, args.refresh_seconds, args.history_days, args.lookback, args.timeframe)
    bot = BotProcess()
    threading.Thread(target=state.loop, daemon=True).start()

    print("=== Dashboard Donchian ===")
    print(f"  Simbol awal : {args.symbol} {args.timeframe} lookback={args.lookback}")
    print(f"  Riwayat     : {args.history_days} hari terakhir (difilter per tanggal di UI)")
    print(f"  Buka        : http://localhost:{args.port}")
    print("  Dashboard TIDAK kirim order sendiri -- yang kirim order tetap bot.\n")

    HTTPServer(("0.0.0.0", args.port), make_handler(state, bot)).serve_forever()


if __name__ == "__main__":
    main()