"""
scripts/smoke_live_ready.py

Uji asap paket siap-akun-asli:
  1. Saklar akun TRADING_ACCOUNT (tanpa default)
  2. Pemeriksaan akun sebelum jalan (one-way, isolated, minimum order)
  3. Status bertahan lintas restart (arah ditahan setelah SL)
  4. Kill switch harian (SL beruntun & rugi harian), juga lintas restart
  5. Perbaikan: SL tidak dobel saat bot dihentikan
  6. Notifier & dashboard

    python -m scripts.smoke_live_ready
"""

from __future__ import annotations

import argparse
import asyncio
import io
import json
import os
import shutil
import subprocess
import sys
import tempfile
from contextlib import redirect_stdout
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from scripts import dashboard_donchian as D  # noqa: E402
from scripts import notifier_email as N  # noqa: E402
from scripts import run_paper_donchian_futures as L  # noqa: E402
import src.execution.broker as broker_mod  # noqa: E402
from src.execution.account import AccountConfigError, resolve_account  # noqa: E402
from src.execution.broker import MockBroker  # noqa: E402
from src.execution.preflight import run_preflight, saran_amount  # noqa: E402
from src.runner.paper import PaperRunner  # noqa: E402
from src.strategy.base import Position, Strategy  # noqa: E402

FAILURES: list[str] = []
SYM = "BTC/USDT:USDT"
NODE = shutil.which("node")
HARI = 86_400_000


def check(name, cond, detail=""):
    print(f"  [{'LULUS' if cond else 'GAGAL'}] {name}" + (f" -- {detail}" if detail else ""))
    if not cond:
        FAILURES.append(name)


def diam(fn, *a, **k):
    out = io.StringIO()
    with redirect_stdout(out):
        r = fn(*a, **k)
    return r, out.getvalue()


async def adiam(coro):
    out = io.StringIO()
    with redirect_stdout(out):
        r = await coro
    return r, out.getvalue()


# ---------------------------------------------------------------------------
# Bursa & broker palsu untuk jalur non-mock launcher
# ---------------------------------------------------------------------------

class FakeExchange:
    def __init__(self, hedged=False, min_cost=100.0, margin_error=None):
        self.hedged, self.min_cost, self.margin_error = hedged, min_cost, margin_error
        self.leverage_set = None
        self.margin_set = None

    def fetch_position_mode(self, symbol):
        return {"hedged": self.hedged}

    def set_margin_mode(self, mode, symbol):
        if self.margin_error:
            raise RuntimeError(self.margin_error)
        self.margin_set = mode

    def set_leverage(self, lev, symbol):
        self.leverage_set = lev

    def load_markets(self):
        return {}

    def market(self, symbol):
        return {"limits": {"amount": {"min": 0.001}, "cost": {"min": self.min_cost}},
                "precision": {"amount": 0.001}}


class FakeBroker:
    dibuat: list = []
    exchange_cfg: dict = {}

    def __init__(self, exchange_id, testnet):
        self.testnet = testnet
        self.exchange = FakeExchange(**FakeBroker.exchange_cfg)
        FakeBroker.dibuat.append(self)

    def fetch_balance(self):
        return {"USDT": {"total": 1000.0, "free": 1000.0}}

    def fetch_current_price(self, symbol):
        return 83000.0


def ns(**ubah):
    base = dict(lookback=200, mock=False, symbol=SYM, timeframe="1m", amount=0.002, leverage=3,
                regime_filter=False, regime_timeframe=None, take_profit_pct=None, tp_sl_price_pct=True,
                tp_roi_pct=0.5, sl_roi_pct=0.25, stop_after_take_profit=False, risk_reward=None,
                session_hours=None, min_entry_buffer_hours=None, backfill_bars=None,
                live_take_profit_poll_seconds=None, sl_atr_mult=2.0, sl_atr_period=20, sl_min_fee_mult=2.0,
                bracket_poll_seconds=5.0, reentry_mode="reversal", state_path=None,
                max_consecutive_sl=3, max_daily_loss_pct=2.0, account=None)
    base.update(ubah)
    return argparse.Namespace(**base)


def bangun(env_akun, **cfg):
    """build_runner jalur NON-mock dengan Broker palsu. Return (runner|SystemExit, log, broker)."""
    amount = cfg.pop("amount", 0.002)
    asli_broker, asli_env = L.Broker, os.environ.get("TRADING_ACCOUNT")
    FakeBroker.dibuat, FakeBroker.exchange_cfg = [], cfg
    L.Broker = FakeBroker
    if env_akun is None:
        os.environ.pop("TRADING_ACCOUNT", None)
    else:
        os.environ["TRADING_ACCOUNT"] = env_akun
    out = io.StringIO()
    try:
        with redirect_stdout(out):
            hasil = L.build_runner(ns(amount=amount))
    except SystemExit as e:
        hasil = e
    finally:
        L.Broker = asli_broker
        if asli_env is None:
            os.environ.pop("TRADING_ACCOUNT", None)
        else:
            os.environ["TRADING_ACCOUNT"] = asli_env
    return hasil, out.getvalue(), (FakeBroker.dibuat[-1] if FakeBroker.dibuat else None)


def test_akun():
    print("== 1. Saklar akun TRADING_ACCOUNT ==")
    check("demo -> testnet", resolve_account({"TRADING_ACCOUNT": "demo"})["testnet"] is True)
    a = resolve_account({"TRADING_ACCOUNT": " LIVE "})
    check("live (huruf besar/spasi diterima) -> akun asli", a["is_live"] and a["testnet"] is False)
    for env, nama in [({}, "kosong"), ({"TRADING_ACCOUNT": "prod"}, "salah ketik 'prod'")]:
        try:
            resolve_account(env)
            check(f"{nama} -> DITOLAK (tanpa default)", False)
        except AccountConfigError:
            check(f"{nama} -> DITOLAK (tanpa default)", True)

    hasil, log, br = bangun("live")
    check("launcher: TRADING_ACCOUNT=live -> Broker(testnet=False)", br is not None and br.testnet is False)
    check("launcher: label & banner AKUN ASLI tercetak", "AKUN ASLI" in log and "UANG SUNGGUHAN" in log)
    check("launcher: leverage & isolated dipasang di bursa", br.exchange.leverage_set == 3 and br.exchange.margin_set == "isolated")
    hasil, log, br = bangun("demo")
    check("launcher: TRADING_ACCOUNT=demo -> Broker(testnet=True)", br is not None and br.testnet is True)
    check("launcher: tidak ada lagi label Demo untuk akun asli", "Demo Trading" in log and "AKUN ASLI" not in log)
    hasil, log, br = bangun(None)
    check("launcher: TRADING_ACCOUNT kosong -> bot TIDAK jalan", isinstance(hasil, SystemExit) and br is None,
          str(hasil)[:80])


def test_preflight():
    print("\n== 2. Pemeriksaan akun sebelum jalan ==")
    hasil, log, _ = bangun("live", hedged=True)
    check("akun ASLI dalam hedge mode -> bot TIDAK jalan", isinstance(hasil, SystemExit) and "HEDGE MODE" in log)
    hasil, log, _ = bangun("live", min_cost=100.0, amount=0.001)
    check("akun ASLI, 0,001 BTC (83 USDT) < minimum 100 -> bot TIDAK jalan",
          isinstance(hasil, SystemExit) and "di bawah minimum notional" in log)
    check("... dan menyarankan amount yang lolos (0,002)", ">= 0.002" in log, log[log.find("Pakai amount"):][:40])
    hasil, log, _ = bangun("demo", hedged=True, amount=0.001)
    check("akun DEMO dengan masalah sama -> hanya PERINGATAN, tetap jalan",
          not isinstance(hasil, SystemExit) and "PERINGATAN" in log and "FATAL" not in log)
    hasil, log, _ = bangun("live", margin_error="ada posisi terbuka")
    check("isolated gagal dipasang -> peringatan saja (bukan berhenti)",
          not isinstance(hasil, SystemExit) and "tidak bisa diset ke isolated" in log)
    br = FakeBroker("x", False)
    br.exchange.min_cost = 100.0
    hasil = dict((p.split(":")[0], t) for t, p in run_preflight(br, SYM, 0.00122))
    check("nilai mepet minimum (101 USDT) -> WARN", any(t == "WARN" for t, _ in run_preflight(br, SYM, 0.00122)))
    check("saran_amount dibulatkan ke atas sesuai langkah 0,001",
          saran_amount(100.0, 83000.0, 0.001) == 0.002 and saran_amount(100.0, 30000.0, 0.001) == 0.004)


class Arah(Strategy):
    """Sinyal sama untuk semua bar; diubah lewat .v."""
    def __init__(self, v):
        super().__init__(None)
        self.v = v

    def generate_signals(self, df):
        return pd.Series([self.v] * len(df), index=df.index)


class Jam:
    def __init__(self, ms):
        self.ms = ms

    def __call__(self):
        return self.ms


T0 = 1_790_000_000_000  # waktu uji


def runner(strat, br, state, jam, **kw):
    r = PaperRunner(strat, br, symbol=SYM, order_amount=0.002, tp_roi=0.005, sl_roi=0.0025,
                    state_path=state, **kw)
    r._clock_ms = jam
    return r


async def bar(r, i, p=83000.0):
    _, log = await adiam(r.process_bar({"timestamp": T0 + i * 60_000, "open": p, "high": p + 5,
                                        "low": p - 5, "close": p, "volume": 1}))
    return log


def picu_sl(br):
    sl = [o for o in br.fetch_open_conditional_orders(SYM) if o["type"] == "STOP_MARKET"][0]
    br.simulate_conditional_trigger(sl["id"], sl["trigger_price"])


async def test_restart():
    print("\n== 3. Status bertahan lintas restart ==")
    d = tempfile.mkdtemp()
    state = os.path.join(d, "bot_state.json")
    jam = Jam(T0 + 5 * 60_000)
    br = MockBroker(fill_immediately=True, leverage=1, taker_fee_pct=0.0004)
    s = Arah(-1)
    r = runner(s, br, state, jam)
    diam(r._load_persistent_state)
    for i in range(3):
        await bar(r, i)
    check("(prasyarat) short terbuka", r._current_position == Position.SHORT)
    picu_sl(br)
    await bar(r, 3)
    simpan = json.loads(Path(state).read_text()).get(SYM, {}) if Path(state).exists() else {}
    check("setelah SL: arah -1 DITAHAN tersimpan ke file", simpan.get("blocked_direction") == -1, str(simpan))
    if not Path(state).exists():
        return   # tanpa file status, skenario restart di bawah tidak bermakna
    salinan = Path(state).read_text()     # tiap skenario restart di bawah mulai dari status yang SAMA

    # RESTART: proses baru, sinyal masih short
    r2 = runner(Arah(-1), br, state, jam)
    _, log = diam(r2._load_persistent_state)
    check("restart: status dipulihkan dari file", "arah -1 DITAHAN" in log and r2._blocked_direction == -1)
    r2._bars = [{"timestamp": T0 + i * 60_000, "open": 1, "high": 1, "low": 1, "close": 1, "volume": 1}
                for i in range(10)]
    _, log = diam(r2._reconcile_restored_block)
    check("sinyal tidak berbalik selama mati -> tetap DITAHAN", r2._blocked_direction == -1, log.strip())
    for i in range(10, 13):
        await bar(r2, i)
    check("restart TIDAK lagi langsung masuk short (dulu: masuk seketika)",
          r2._current_position == Position.FLAT and br.fetch_position(SYM) is None)

    # Sinyal sempat long selama bot mati -> blokir dicabut
    class Pernah(Strategy):
        def __init__(self):
            super().__init__(None)

        def generate_signals(self, df):
            v = [-1] * len(df)
            v[-3] = 1   # satu bar long setelah status terakhir disimpan
            return pd.Series(v, index=df.index)
    Path(state).write_text(salinan)
    r3 = runner(Pernah(), br, state, jam)
    diam(r3._load_persistent_state)
    r3._bars = [{"timestamp": T0 + 5 * 60_000 + i * 60_000, "open": 1, "high": 1, "low": 1, "close": 1,
                 "volume": 1} for i in range(-2, 6)]
    _, log = diam(r3._reconcile_restored_block)
    check("sinyal SEMPAT berbalik selama mati -> blokir dicabut", r3._blocked_direction is None, log.strip())

    check("pencabutan blokir ikut tersimpan ke file",
          json.loads(Path(state).read_text())[SYM]["blocked_direction"] is None)
    Path(state).write_text(salinan)
    r4 = runner(Arah(-1), br, state, jam)
    diam(r4._load_persistent_state)
    r4._bars = [{"timestamp": T0 + 10 * 60_000 + i * 60_000, "open": 1, "high": 1, "low": 1, "close": 1,
                 "volume": 1} for i in range(5)]
    _, log = diam(r4._reconcile_restored_block)
    check("backfill tidak menjangkau waktu terakhir -> tetap DITAHAN (aman)",
          r4._blocked_direction == -1 and "tidak menjangkau" in log)


async def test_kill_switch():
    print("\n== 4. Kill switch harian ==")
    d = tempfile.mkdtemp()
    state = os.path.join(d, "bot_state.json")
    jam = Jam(T0 + 5 * 60_000)
    br = MockBroker(fill_immediately=True, leverage=1, taker_fee_pct=0.0004)
    s = Arah(-1)
    r = runner(s, br, state, jam, max_consecutive_sl=2)
    diam(r._load_persistent_state)
    for i in range(3):
        await bar(r, i)
    picu_sl(br)
    await bar(r, 3)
    check("SL ke-1: belum berhenti", r._halt_until_ms is None and r._consecutive_sl == 1)
    s.v = 1
    await bar(r, 4)
    check("(prasyarat) arah lain dibuka", r._current_position == Position.LONG)
    picu_sl(br)
    log = await bar(r, 5)
    check("SL ke-2 hari yang sama -> BATAS HARIAN", r._halt_until_ms is not None and "BATAS HARIAN TERCAPAI" in log)
    s.v = -1
    log = await bar(r, 6) + await bar(r, 7)
    check("sinyal baru -> TIDAK masuk selama batas aktif", r._current_position == Position.FLAT)
    check("heartbeat menjelaskan: [status] BATAS HARIAN", "[status] BATAS HARIAN" in log)
    hb = [x for x in log.splitlines() if "[heartbeat]" in x]
    check("teks status tidak memicu auto-stop dashboard",
          not any("stop_after_take_profit" in x and "BERHENTI trading total" in x for x in hb))

    r2 = runner(Arah(-1), br, state, jam, max_consecutive_sl=2)
    _, log = diam(r2._load_persistent_state)
    check("RESTART tidak menghapus batas harian", r2._halt_until_ms == r._halt_until_ms and "batas harian aktif" in log)
    for i in range(8, 10):
        await bar(r2, i)
    check("... dan tetap tidak masuk setelah restart", r2._current_position == Position.FLAT)

    jam.ms = r._halt_until_ms + 60_000          # hari berganti
    log = await bar(r2, 10)
    check("hari berganti -> batas dicabut & boleh masuk lagi",
          r2._halt_until_ms is None and r2._current_position == Position.SHORT and "batas harian dicabut" in log)
    check("hitungan SL beruntun di-reset", r2._consecutive_sl == 0)
    batas_wib = r._fmt_local(r._halt_until_ms)
    check("batas berlaku sampai 00:00 WIB", batas_wib.endswith("00:00 (UTC+7)"), batas_wib)

    print("   rugi harian:")
    br2 = MockBroker(fill_immediately=True, leverage=1, taker_fee_pct=0.0004)
    r3 = runner(Arah(-1), br2, None, Jam(T0), max_daily_loss_pct=2.0)
    br2.realized_override = {"realized": -150.0, "fee": 10.0}     # saldo mock ~10.000 -> ~1,6%
    diam(r3._register_exit, "SL")
    check("rugi 1,6% < batas 2% -> tetap boleh trading", r3._halt_until_ms is None)
    br2.realized_override = {"realized": -250.0, "fee": 10.0}     # ~2,5%
    _, log = diam(r3._register_exit, "SL")
    check("rugi 2,5% >= batas 2% -> BATAS HARIAN", r3._halt_until_ms is not None and "rugi bersih hari ini" in log)

    print("   tanpa kill switch (perilaku lama):")
    br3 = MockBroker(fill_immediately=True, leverage=1, taker_fee_pct=0.0004)
    hit = {"n": 0}
    asli = br3.fetch_realized_pnl_since
    br3.fetch_realized_pnl_since = lambda *a: (hit.__setitem__("n", hit["n"] + 1), asli(*a))[1]
    r5 = PaperRunner(Arah(-1), br3, symbol=SYM, order_amount=0.002, tp_roi=0.005, sl_roi=0.0025)
    for i in range(3):
        await bar(r5, i)
    picu_sl(br3)
    await bar(r5, 3)
    check("tanpa parameter baru: tidak ada file status & tidak cek rugi harian",
          r5.state_path is None and hit["n"] == 0 and r5._halt_until_ms is None)


async def test_stop_tanpa_sl_dobel():
    print("\n== 5. Bot dihentikan saat order penutup tidak terisi -> SL tidak dobel ==")
    br = MockBroker(fill_immediately=True, leverage=1, taker_fee_pct=0.0004)
    r = PaperRunner(Arah(1), br, symbol=SYM, order_amount=0.002, tp_roi=0.005, sl_roi=0.0025)
    for i in range(3):
        await bar(r, i)
    br.set_current_price(83000.0)
    br.fill_immediately = False
    _, log = await adiam(L.close_position_on_stop(r, wait_seconds=0.0))
    stop = [o for o in br.fetch_open_conditional_orders(SYM) if o["type"] == "STOP_MARKET"]
    check("tepat SATU SL tersisa (dulu dua)", len(stop) == 1, str(len(stop)))
    check("log: SL masih terpasang -- posisi tetap terlindungi", "SL masih terpasang" in log)

    br2 = MockBroker(fill_immediately=True, leverage=1, taker_fee_pct=0.0004)
    r2 = PaperRunner(Arah(1), br2, symbol=SYM, order_amount=0.002, tp_roi=0.005, sl_roi=0.0025)
    for i in range(3):
        await bar(r2, i)
    br2.set_current_price(83000.0)
    _, log_stop = await adiam(L.close_position_on_stop(r2, wait_seconds=0.0))
    check("(prasyarat) order penutup terisi -> flat", br2.fetch_position(SYM) is None)

    isi = {"lines": []}
    dl = N.DashboardLog("http://x/api", fetch=lambda u: {"bot": {"log": isi["lines"]}})
    dl.poll(1000)
    isi["lines"] = log_stop.splitlines()
    dl.poll(2000)
    alasan = dl.reason(3000)
    check("email menulis 'BOT DIHENTIKAN', bukan 'REVERSAL SINYAL'",
          (alasan or "").startswith("BOT DIHENTIKAN"), str(alasan))


def test_notifier_akun():
    print("\n== 6. Notifier mengikuti TRADING_ACCOUNT ==")
    f = N.resolve_notifier_testnet
    check("TRADING_ACCOUNT=live -> pantau akun asli", f({"TRADING_ACCOUNT": "live"}) is False)
    check("TRADING_ACCOUNT=demo -> demo", f({"TRADING_ACCOUNT": "demo"}) is True)
    check("hanya NOTIF_TESTNET=0 (cara lama) -> akun asli", f({"NOTIF_TESTNET": "0"}) is False)
    check("keduanya kosong -> demo (perilaku lama)", f({}) is True)
    try:
        f({"TRADING_ACCOUNT": "live", "NOTIF_TESTNET": "1"})
        check("bertentangan (live vs NOTIF_TESTNET=1) -> DITOLAK", False)
    except ValueError:
        check("bertentangan (live vs NOTIF_TESTNET=1) -> DITOLAK", True)


def js_fn(name):
    src = D.HTML_PAGE.split("<script>")[1].split("</script>")[0]
    start = src.index(f"function {name}(")
    depth, i = 0, src.index("{", start)
    while True:
        depth += {"{": 1, "}": -1}.get(src[i], 0)
        if depth == 0 and src[i] == "}":
            return src[start:i + 1]
        i += 1


def node(code):
    with tempfile.NamedTemporaryFile("w", suffix=".js", delete=False) as fh:
        fh.write(code)
    out = subprocess.run([NODE, fh.name], capture_output=True, text=True, timeout=20)
    if out.returncode:
        raise RuntimeError(out.stderr)
    return json.loads(out.stdout)


def test_dashboard():
    print("\n== 7. Dashboard ==")
    cfg = {"symbol": SYM, "timeframe": "1m", "lookback": 200, "amount": 0.002, "tp_roi_pct": 0.5,
           "sl_roi_pct": 0.25, "leverage": 3, "close_on_stop": True, "max_consecutive_sl": 3,
           "max_daily_loss_pct": 2}
    cmd = D.build_bot_command(cfg)
    s = " ".join(cmd)
    check("perintah memuat kill switch dari form", "--max-consecutive-sl 3" in s and "--max-daily-loss-pct 2.0" in s, s)
    check("nilai 0 tetap dikirim (= mati, bukan default)",
          "--max-consecutive-sl 0" in " ".join(D.build_bot_command({**cfg, "max_consecutive_sl": 0})))
    argv = ["--mock" if a == "--live" else a for a in cmd[cmd.index("scripts.run_paper_donchian_futures") + 1:]]
    asli = sys.argv
    try:
        sys.argv = ["x"] + argv
        _, log = diam(L.main)
        check("launcher menerima perintah dashboard (diuji mode --mock)", "MOCK" in log)
        check("label TP/SL tidak lagi menyebut ROI untuk mode % posisi",
              "% PERGERAKAN HARGA" in log and "ROI KOTOR" not in log)
    except SystemExit as e:
        check("launcher menerima perintah dashboard (diuji mode --mock)", False, str(e))
    finally:
        sys.argv = asli

    dibuat = []
    asli_b = broker_mod.Broker
    broker_mod.Broker = lambda exchange_id, testnet: dibuat.append(testnet) or object()
    try:
        st = D.DashboardState(SYM, 5, 1, 200, "1m", account=resolve_account({"TRADING_ACCOUNT": "live"}))
        st._ensure_broker()
    finally:
        broker_mod.Broker = asli_b
    check("dashboard akun live -> Broker(testnet=False)", dibuat == [False])
    pl = st.payload(None, None)
    check("payload membawa info akun ke halaman", pl["account"] == {"mode": "live", "label": "AKUN ASLI (UANG SUNGGUHAN)",
                                                                    "is_live": True})
    asli_env = os.environ.pop("TRADING_ACCOUNT", None)
    asli = sys.argv
    try:
        sys.argv = ["dashboard"]
        diam(D.main)
        check("dashboard tanpa TRADING_ACCOUNT -> menolak jalan", False)
    except SystemExit as e:
        check("dashboard tanpa TRADING_ACCOUNT -> menolak jalan", "TRADING_ACCOUNT" in str(e))
    finally:
        sys.argv = asli
        if asli_env is not None:
            os.environ["TRADING_ACCOUNT"] = asli_env
    check("default hanya mendengarkan 127.0.0.1", 'p.add_argument("--host", default="127.0.0.1"' in Path(D.__file__).read_text())
    h = D.HTML_PAGE
    check("teks 'Binance Demo Trading' tidak lagi ditulis mati", "Binance Demo Trading" not in h)
    if not NODE:
        check("node tersedia untuk uji JavaScript", False, "pasang nodejs untuk uji halaman")
        return
    r = node(js_fn("panelAkun") + "\nconsole.log(JSON.stringify([panelAkun({is_live:true}), panelAkun({is_live:false}),"
             "panelAkun({error:'TRADING_ACCOUNT kosong <x>'})]));")
    check("banner MERAH 'AKUN UANG ASLI' untuk akun asli", "AKUN UANG ASLI" in r[0] and 'class="err"' in r[0])
    check("tanpa banner untuk demo", r[1] == "")
    check("akun belum diatur -> peringatan (teks disanitasi)", "Akun belum diatur" in r[2] and "<x>" not in r[2])
    k = node(js_fn("pesanKonfirmasiLive") + "\nconsole.log(JSON.stringify(pesanKonfirmasiLive(" + json.dumps(cfg) + ", 83000)));")
    check("konfirmasi akun asli merangkum amount, leverage, SL/TP, batas",
          all(x in k for x in ["AKUN UANG ASLI", "0.002 BTC (~166.00 USDT)", "3x", "TP +0.5% / SL -0.25%", "3 SL beruntun"]), k)
    src = h.split("<script>")[1].split("</script>")[0]
    check("tombol Mulai: tunggu data akun & minta konfirmasi di akun asli",
          "akunLive === null" in src and "confirm(pesanKonfirmasiLive(" in src)
    stub = "const pilih = () => null;\n" + js_fn("panelStatusBot")
    r = node(stub + "\nconsole.log(JSON.stringify(panelStatusBot({session_state:{status_note:'BATAS HARIAN: 3 SL'}},{running:true})));")
    check("status BATAS HARIAN tampil merah", 'class="err"' in r)


async def main_async():
    await test_restart()
    await test_kill_switch()
    await test_stop_tanpa_sl_dobel()


def main() -> int:
    asal = os.getcwd()
    with tempfile.TemporaryDirectory() as d:
        os.chdir(d)
        try:
            test_akun()
            test_preflight()
            asyncio.run(main_async())
            test_notifier_akun()
            test_dashboard()
        finally:
            os.chdir(asal)
    print("\n" + "=" * 62)
    if FAILURES:
        print(f"GAGAL: {len(FAILURES)} tes -> {FAILURES}")
        return 1
    print("SEMUA TES LULUS.")
    return 0


if __name__ == "__main__":
    sys.exit(main())