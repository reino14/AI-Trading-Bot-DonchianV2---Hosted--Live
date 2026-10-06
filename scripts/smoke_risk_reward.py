"""
scripts/smoke_risk_reward.py

Uji asap fitur SL + TP rasio (--risk-reward). Tanpa jaringan: memakai
MockBroker, plus ccxt ASLI dengan endpoint yang dicegat untuk memastikan
order SL benar-benar diarahkan ke endpoint Algo Binance.

    python -m scripts.smoke_risk_reward
"""

from __future__ import annotations

import asyncio
import io
import os
import sys
import tempfile
from contextlib import redirect_stdout
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from src.execution.broker import Broker, MockBroker  # noqa: E402
from src.runner.paper import (PaperRunner, classify_bracket_exit, compute_atr,  # noqa: E402
                              compute_bracket)
from src.strategy.base import Position, Strategy  # noqa: E402

FAILURES: list[str] = []
SYM = "BTC/USDT:USDT"
T0 = 1_790_000_000_000
MIN = 60_000


def check(name, cond, detail=""):
    print(f"  [{'LULUS' if cond else 'GAGAL'}] {name}" + (f" -- {detail}" if detail else ""))
    if not cond:
        FAILURES.append(name)


def close_to(a, b, tol=1e-9):
    return a is not None and b is not None and abs(a - b) <= tol


class Scripted(Strategy):
    """Sinyal diatur manual per bar: sig[i] untuk bar ke-i di buffer."""

    def __init__(self, sig):
        self.sig = sig

    def generate_signals(self, df):
        n = len(df)
        vals = (self.sig + [self.sig[-1]] * n)[:n]
        return pd.Series(vals, index=df.index)


def bar(i, price, rng=40.0):
    return {"timestamp": T0 + i * MIN, "open": price, "high": price + rng / 2,
            "low": price - rng / 2, "close": price, "volume": 1.0}


def runner(sig, *, rr=2.0, stop_after=False, broker=None, **kw):
    br = broker or MockBroker(fill_immediately=True, leverage=20, taker_fee_pct=0.0004)
    r = PaperRunner(Scripted(sig), br, symbol=SYM, order_amount=0.01, risk_reward=rr,
                    stop_after_take_profit=stop_after, **kw)
    return r, br


async def feed(r, prices, start=0, rng=40.0):
    out = io.StringIO()
    with redirect_stdout(out):
        for k, p in enumerate(prices):
            await r.process_bar(bar(start + k, p, rng))
    return out.getvalue()


def cond_orders(br):
    return br.fetch_open_conditional_orders(SYM)


# ---------------------------------------------------------------------------

def test_math():
    print("== 1. Rumus ATR, SL, TP -- dihitung tangan ==")
    bars = [{"high": 101, "low": 99, "close": 100}, {"high": 103, "low": 100, "close": 102},
            {"high": 102, "low": 98, "close": 99}]
    # TR1 = maks(3, |103-100|, |100-100|) = 3 ; TR2 = maks(4, |102-102|, |98-102|) = 4 -> ATR 3.5
    check("ATR(2) = 3.5", close_to(compute_atr(bars, 2), 3.5), str(compute_atr(bars, 2)))
    check("ATR kurang data -> None", compute_atr(bars, 5) is None)

    b = compute_bracket(80000, 1, 200.0, 0.0004, 2.0, 2.0, 2.0)
    check("ATR dominan: SL = 2 x 200 / 80000 = 0.5%", close_to(b["sl_dist"], 0.005), f"{b['sl_dist']:.6f}")
    check("TP = 2 x 0.5% + 3 x 0.08% = 1.24%", close_to(b["tp_dist"], 0.0124), f"{b['tp_dist']:.6f}")
    check("harga SL long di bawah entry", close_to(b["sl_price"], 80000 * 0.995, 1e-6))
    win, loss = b["tp_dist"] - b["fee_rt"], b["sl_dist"] + b["fee_rt"]
    check("rasio UANG BERSIH tepat 2:1", close_to(win / loss, 2.0), f"{win / loss:.12f}")

    f = compute_bracket(80000, -1, 40.0, 0.0004, 2.0, 2.0, 2.0)
    check("ATR kecil (1m): batas bawah fee berlaku, SL = 2 x 0.08% = 0.16%",
          close_to(f["sl_dist"], 0.0016) and "batas bawah" in f["sl_basis"], f"{f['sl_dist']:.6f} {f['sl_basis']}")
    check("short: SL di ATAS entry, TP di BAWAH", f["sl_price"] > 80000 > f["tp_price"])
    win, loss = f["tp_dist"] - f["fee_rt"], f["sl_dist"] + f["fee_rt"]
    check("rasio bersih tetap 2:1 di batas bawah", close_to(win / loss, 2.0))
    r3 = compute_bracket(80000, 1, 200.0, 0.0005, 3.0, 2.0, 2.0)
    check("rasio lain (3:1) juga tepat", close_to((r3["tp_dist"] - r3["fee_rt"]) / (r3["sl_dist"] + r3["fee_rt"]), 3.0))
    n = compute_bracket(80000, 1, None, 0.0004, 2.0, 2.0, 2.0)
    check("ATR belum ada -> pakai batas bawah, tidak crash", close_to(n["sl_dist"], 0.0016))

    bk = {"tp_price": 81000.0, "sl_price": 79800.0}
    check("klasifikasi: keluar dekat TP -> TP", classify_bracket_exit(80990, bk) == "TP")
    check("klasifikasi: keluar dekat SL -> SL", classify_bracket_exit(79795, bk) == "SL")
    check("klasifikasi: harga tak terbaca -> UNKNOWN", classify_bracket_exit(None, bk) == "UNKNOWN")


async def test_entry_and_tp():
    print("\n== 2. Masuk posisi: SL & TP dititipkan ke bursa, lalu TP kena ==")
    r, br = runner([0, 1, 1, 1, 1, 1], stop_after=True)
    log = await feed(r, [80000.0] * 22, rng=40.0)  # 21 bar = cukup untuk ATR20
    tipe = sorted(o["type"] for o in cond_orders(br))
    check("dua order bersyarat terpasang: SL & TP", tipe == ["STOP_MARKET", "TAKE_PROFIT_MARKET"], str(tipe))
    b = r._bracket
    check("SL di bawah entry, TP di atas (long)", b["sl_price"] < r._entry_price < b["tp_price"])
    check("SL terverifikasi di bursa sebelum dianggap aman", "SL terverifikasi" in log)
    check("log menampilkan rugi & untung bersih dalam USDT", "rugi bersih" in log and "untung bersih" in log)

    tp_id = [o["id"] for o in cond_orders(br) if o["type"] == "TAKE_PROFIT_MARKET"][0]
    br.simulate_conditional_trigger(tp_id, b["tp_price"])
    log = await feed(r, [81000.0], start=22)
    check("bot mendeteksi TP kena dari bursa", "[take-profit] TP di BURSA KENA" in log and "TERTUTUP" in log)
    check("SL yang tersisa DIBATALKAN (tidak mengandalkan bursa)", cond_orders(br) == [], str(cond_orders(br)))
    check("posisi FLAT & state bersih", r._current_position == Position.FLAT and r._bracket is None)
    check("stop_after_take_profit: bot berhenti total", r._trading_halted)
    check("baris berhenti memuat penanda auto-stop dashboard",
          "stop_after_take_profit" in log and "BERHENTI trading total" in log)


async def test_sl_then_block_then_reverse():
    print("\n== 3. SL kena -> arah ditahan -> sinyal berbalik -> masuk arah baru ==")
    sig = [0] + [1] * 30 + [-1] * 10
    r, br = runner(sig)
    await feed(r, [80000.0] * 22)
    b = r._bracket
    sl_id = [o["id"] for o in cond_orders(br) if o["type"] == "STOP_MARKET"][0]
    br.simulate_conditional_trigger(sl_id, b["sl_price"])
    log = await feed(r, [79800.0], start=22)
    check("bot mendeteksi SL kena", "[stop-loss] SL di BURSA KENA" in log)
    check("TP yang tersisa dibatalkan", cond_orders(br) == [])
    check("SL TIDAK menghentikan bot", not r._trading_halted)
    check("arah long DITAHAN", r._blocked_direction == Position.LONG)
    await feed(r, [79700.0] * 5, start=23)
    check("sinyal masih long -> TIDAK masuk lagi", r._current_position == Position.FLAT and not cond_orders(br))
    await feed(r, [79000.0] * 4, start=28)
    check("sinyal berbalik short -> posisi short dibuka", r._current_position == Position.SHORT)
    check("SL/TP baru terpasang untuk short", r._bracket and r._bracket["sl_price"] > r._entry_price > r._bracket["tp_price"])


async def test_reversal_cancels_brackets():
    print("\n== 4. Keluar lewat REVERSAL sinyal: SL/TP lama dibatalkan dulu ==")
    sig = [0] + [1] * 21 + [-1] * 10
    r, br = runner(sig)
    await feed(r, [80000.0] * 22)
    check("posisi long dengan 2 order bersyarat", r._current_position == Position.LONG and len(cond_orders(br)) == 2)
    log = await feed(r, [79900.0], start=22)
    check("reversal menutup posisi", r._current_position == Position.FLAT)
    check("SL/TP posisi lama dibatalkan", "order bersyarat (SL/TP) dibatalkan" in log and cond_orders(br) == [])
    await feed(r, [79800.0], start=23)
    check("bar berikutnya: short dibuka dengan SL/TP baru",
          r._current_position == Position.SHORT and len(cond_orders(br)) == 2)


async def test_close_not_filled_restores_sl():
    print("\n== 5. Order penutup BELUM terisi -> SL dipasang ulang ==")
    sig = [0] + [1] * 21 + [-1] * 10
    r, br = runner(sig)
    await feed(r, [80000.0] * 22)
    sl_awal = r._bracket["sl_price"]
    br.fill_immediately = False
    log = await feed(r, [79900.0], start=22)
    sisa = cond_orders(br)
    check("posisi masih long (order penutup belum terisi)", r._current_position == Position.LONG)
    check("SL DIPASANG ULANG di harga yang sama",
          len(sisa) == 1 and sisa[0]["type"] == "STOP_MARKET" and close_to(sisa[0]["trigger_price"], sl_awal, 1e-6),
          str(sisa))
    check("log menjelaskannya", "DIPASANG ULANG" in log)


async def test_sl_rejected_emergency():
    print("\n== 6. SL DITOLAK bursa -> posisi ditutup darurat, bot berhenti ==")
    br = MockBroker(fill_immediately=True, leverage=20, taker_fee_pct=0.0004)
    br.fail_stop_orders = True
    r, _ = runner([0] + [1] * 30, broker=br)
    log = await feed(r, [80000.0] * 22)
    check("log DARURAT tercetak", "[stop-loss] DARURAT" in log)
    check("posisi DITUTUP (tidak dibiarkan tanpa SL)", br.fetch_position(SYM) is None and r._current_position == Position.FLAT)
    check("bot berhenti membuka posisi baru", r._trading_halted)
    check("tidak ada order bersyarat yatim", cond_orders(br) == [])
    await feed(r, [80100.0] * 3, start=22)
    check("bar berikutnya tetap tidak masuk", br.fetch_position(SYM) is None)


async def test_sl_without_trigger_emergency():
    print("\n== 7. SL DITERIMA tapi tanpa harga pemicu (jebakan migrasi Binance) -> darurat ==")
    br = MockBroker(fill_immediately=True, leverage=20, taker_fee_pct=0.0004)
    br.drop_stop_trigger_price = True
    r, _ = runner([0] + [1] * 30, broker=br)
    log = await feed(r, [80000.0] * 22)
    check("verifikasi menangkap SL tanpa pemicu", "TANPA harga pemicu" in log)
    check("posisi ditutup & bot berhenti", br.fetch_position(SYM) is None and r._trading_halted)
    check("order bersyarat dibersihkan", cond_orders(br) == [])


async def test_emergency_close_retry():
    print("\n== 8. Penutupan darurat tidak langsung terisi -> dicoba lagi tiap bar ==")
    r, br = runner([0] + [1] * 40)
    await feed(r, [80000.0] * 22)
    check("posisi long terbuka", br.fetch_position(SYM) is not None)
    br.fill_immediately = False
    out = io.StringIO()
    with redirect_stdout(out):
        await r._emergency_close(80000.0, "simulasi SL gagal")
    check("penutupan darurat belum terisi -> TIDAK dianggap selesai",
          r._emergency_close_pending and not r._trading_halted and br.fetch_position(SYM) is not None)
    br.fill_immediately = True
    log = await feed(r, [80000.0], start=22)
    check("bar berikutnya mencoba lagi dan berhasil",
          "mencoba menutup lagi" in log and br.fetch_position(SYM) is None)
    check("baru SETELAH flat bot berhenti", r._trading_halted and not r._emergency_close_pending)


async def test_restart_adopts_or_places():
    print("\n== 9. Restart saat posisi terbuka ==")
    br = MockBroker(fill_immediately=True, leverage=20, taker_fee_pct=0.0004)
    r1, _ = runner([0] + [1] * 30, broker=br)
    await feed(r1, [80000.0] * 22)
    ids_sebelum = sorted(o["id"] for o in cond_orders(br))
    r2, _ = runner([1] * 30, broker=br)
    out = io.StringIO()
    with redirect_stdout(out):
        r2._recover_position_from_exchange()
        await r2._ensure_bracket_after_recovery()
    check("SL/TP dari sesi lama DIPAKAI, tidak dobel", sorted(o["id"] for o in cond_orders(br)) == ids_sebelum)
    check("state bracket terisi dari bursa", r2._bracket and r2._bracket.get("adopted"))

    br.cancel_conditional_orders(SYM)  # sesi lama mati SEBELUM sempat pasang SL
    r3, _ = runner([1] * 30, broker=br)
    r3._bars = [bar(i, 80000.0) for i in range(25)]
    out = io.StringIO()
    with redirect_stdout(out):
        r3._recover_position_from_exchange()
        await r3._ensure_bracket_after_recovery()
    check("posisi tanpa SL -> SL/TP dipasang saat start", len(cond_orders(br)) == 2, str(cond_orders(br)))
    check("log menjelaskannya", "TERBUKA TANPA SL" in out.getvalue())


async def test_watch_loop_fast_detection():
    print("\n== 10. Pemantau tiap beberapa detik mendeteksi SL tanpa menunggu candle ==")
    r, br = runner([0] + [1] * 30, bracket_poll_seconds=0.01)
    await feed(r, [80000.0] * 22)
    sl_id = [o["id"] for o in cond_orders(br) if o["type"] == "STOP_MARKET"][0]
    br.simulate_conditional_trigger(sl_id, r._bracket["sl_price"])
    out = io.StringIO()
    with redirect_stdout(out):
        task = asyncio.create_task(r._bracket_watch_loop(0.01))
        await asyncio.sleep(0.2)
        task.cancel()
    check("terdeteksi TANPA bar baru", "[stop-loss] SL di BURSA KENA" in out.getvalue())
    check("sisa TP dibatalkan", cond_orders(br) == [])


async def test_default_unchanged():
    print("\n== 11. TANPA --risk-reward: perilaku lama tidak berubah ==")
    br = MockBroker(fill_immediately=True, leverage=20, taker_fee_pct=0.0004)
    panggilan = {"cancel_cond": 0, "stop": 0}
    asli_c, asli_s = br.cancel_conditional_orders, br.place_stop_market
    br.cancel_conditional_orders = lambda s: (panggilan.__setitem__("cancel_cond", panggilan["cancel_cond"] + 1), asli_c(s))[1]
    br.place_stop_market = lambda *a: (panggilan.__setitem__("stop", panggilan["stop"] + 1), asli_s(*a))[1]
    r = PaperRunner(Scripted([0] + [1] * 21 + [-1] * 10), br, symbol=SYM, order_amount=0.01)
    await feed(r, [80000.0] * 24)
    check("buka, tutup, buka tetap jalan", r._current_position == Position.SHORT)
    check("tidak ada SL dipasang", panggilan["stop"] == 0)
    check("tidak ada panggilan order bersyarat sama sekali", panggilan["cancel_cond"] == 0, str(panggilan))

    print("   mode TP lama (--take-profit-pct): celah TP tertinggal kini tertutup")
    br2 = MockBroker(fill_immediately=True, leverage=20, taker_fee_pct=0.0004)
    r2 = PaperRunner(Scripted([0] + [1] * 21 + [-1] * 10), br2, symbol=SYM, order_amount=0.01, take_profit_pct=0.05)
    await feed(r2, [80000.0] * 22)
    check("TP lama tetap dititipkan", len(cond_orders(br2)) == 1)
    await feed(r2, [79950.0], start=22)
    check("reversal: TP lama DIBATALKAN (dulu tertinggal)", r2._current_position == Position.FLAT and cond_orders(br2) == [])


def test_quiet_fetch_position():
    print("\n== 12. fetch_position(quiet=True) tidak membanjiri log ==")

    class FakeEx:
        def fetch_positions(self, symbols):
            return [{"contracts": 0.01, "side": "long", "leverage": None, "entryPrice": 80000,
                     "info": {"notional": "800", "initialMargin": "40"}}]

    b = Broker.__new__(Broker)
    b.exchange = FakeEx()
    out = io.StringIO()
    with redirect_stdout(out):
        p1 = b.fetch_position(SYM)
    check("default (quiet=False): tetap mencetak seperti dulu", "[fetch_position]" in out.getvalue())
    out = io.StringIO()
    with redirect_stdout(out):
        p2 = b.fetch_position(SYM, quiet=True)
    check("quiet=True: tidak mencetak apa pun", out.getvalue() == "")
    check("hasilnya identik", p1 == p2 and close_to(p2["leverage"], 20.0))


def test_real_ccxt_routing():
    print("\n== 13. ccxt ASLI: SL ke endpoint Algo, dibaca & dibatalkan dengan benar ==")
    import ccxt
    ex = ccxt.binanceusdm({"apiKey": "x", "secret": "y"})
    m = {"id": "BTCUSDT", "symbol": SYM, "type": "swap", "swap": True, "linear": True, "inverse": False,
         "spot": False, "margin": False, "future": False, "option": False, "contract": True, "settle": "USDT",
         "quote": "USDT", "base": "BTC", "precision": {"amount": 0.001, "price": 0.1},
         "limits": {"amount": {"min": 0.001}, "price": {}, "cost": {}},
         "info": {"orderTypes": ["LIMIT", "MARKET", "STOP_MARKET", "TAKE_PROFIT_MARKET"]}}
    ex.markets = {SYM: m}
    ex.markets_by_id = {"BTCUSDT": [m]}
    ex.symbols = [SYM]
    terbuka = {}
    rekam = []

    def post_algo(req, *a, **k):
        aid = str(1000 + len(terbuka))
        o = {"algoId": aid, "algoType": "CONDITIONAL", "orderType": req["type"], "symbol": "BTCUSDT",
             "side": req["side"], "algoStatus": "NEW", "triggerPrice": req.get("triggerPrice"),
             "closePosition": True, "createTime": T0, "updateTime": T0}
        terbuka[aid] = o
        rekam.append(("POST algo", dict(req)))
        return o

    ex.fapiPrivatePostAlgoOrder = post_algo
    ex.fapiPrivatePostOrder = lambda req, *a, **k: rekam.append(("POST order LAMA", dict(req))) or {}
    ex.fapiPrivateGetOpenAlgoOrders = lambda req, *a, **k: list(terbuka.values())
    ex.fapiPrivateDeleteAlgoOrder = lambda req, *a, **k: (rekam.append(("DELETE algo", dict(req))), terbuka.pop(req["algoId"]))[1]
    b = Broker.__new__(Broker)
    b.exchange = ex

    sl_id = b.place_stop_market(SYM, "long", 79840.0)
    tp_id = b.place_take_profit_market(SYM, "long", 81000.0)
    check("SL & TP dikirim ke endpoint ALGO, bukan endpoint lama", [r[0] for r in rekam] == ["POST algo", "POST algo"])
    check("SL dikirim dengan triggerPrice", rekam[0][1].get("triggerPrice") == "79840" and "stopPrice" not in rekam[0][1],
          str(rekam[0][1]))
    check("TP (kode lama, stopPrice) tetap diterjemahkan ccxt jadi triggerPrice",
          rekam[1][1].get("triggerPrice") == "81000", str(rekam[1][1]))
    daftar = b.fetch_open_conditional_orders(SYM)
    jenis = {o["id"]: o["type"] for o in daftar}
    check("SL & TP DIBEDAKAN dari info.orderType (ccxt sendiri menyebut keduanya 'market')",
          jenis == {sl_id: "STOP_MARKET", tp_id: "TAKE_PROFIT_MARKET"}, str(jenis))
    check("harga pemicu terbaca", {o["id"]: o["trigger_price"] for o in daftar} == {sl_id: 79840.0, tp_id: 81000.0})
    n = b.cancel_conditional_orders(SYM)
    check("pembatalan lewat endpoint Algo dengan algoId", n == 2 and [r[0] for r in rekam[2:]] == ["DELETE algo"] * 2)
    check("tidak ada yang tersisa", b.fetch_open_conditional_orders(SYM) == [])


def test_launcher():
    print("\n== 14. Launcher ==")
    import argparse
    from scripts import run_paper_donchian_futures as L
    ns = argparse.Namespace(lookback=200, mock=True, symbol=SYM, timeframe="1m", amount=0.01, session_hours=5,
                            min_entry_buffer_hours=None, take_profit_pct=None, stop_after_take_profit=True,
                            backfill_bars=None, live_take_profit_poll_seconds=None, risk_reward=2.0,
                            sl_atr_mult=2.0, sl_atr_period=20, sl_min_fee_mult=2.0, bracket_poll_seconds=5.0)
    with redirect_stdout(io.StringIO()):
        r = L.build_runner(ns)
    check("parameter diteruskan ke PaperRunner",
          (r.risk_reward, r.sl_atr_mult, r.sl_atr_period, r.sl_min_fee_mult) == (2.0, 2.0, 20, 2.0))
    asli = sys.argv
    try:
        sys.argv = ["x", "--mock", "--risk-reward", "2", "--take-profit-pct", "0.003"]
        try:
            with redirect_stdout(io.StringIO()):
                L.main()
            check("--risk-reward + --take-profit-pct ditolak", False)
        except SystemExit as e:
            check("--risk-reward + --take-profit-pct ditolak", "tidak bisa dipakai bersamaan" in str(e))
    finally:
        sys.argv = asli


async def test_notifier_reads_real_bot_logs():
    print("\n== 15. Notifier Gmail membaca alasan dari log ASLI bot ==")
    from scripts.notifier_email import DashboardLog

    def alasan_dari(log_sebelum: str, log_sesudah: str) -> str | None:
        """Suapkan log bot ke DashboardLog seperti dashboard menyajikannya."""
        isi = {"lines": log_sebelum.splitlines()}
        d = DashboardLog("http://dashboard/api", fetch=lambda url: {"bot": {"log": isi["lines"]}})
        d.poll(1000)
        isi["lines"] = (log_sebelum + log_sesudah).splitlines()[-200:]
        d.poll(2000)
        return d.reason(3000)

    r, br = runner([0] + [1] * 30)
    awal = await feed(r, [80000.0] * 22)
    sl_id = [o["id"] for o in cond_orders(br) if o["type"] == "STOP_MARKET"][0]
    br.simulate_conditional_trigger(sl_id, r._bracket["sl_price"])
    akhir = await feed(r, [79800.0], start=22)
    check("SL kena -> email menulis STOP LOSS", alasan_dari(awal, akhir) == "STOP LOSS", alasan_dari(awal, akhir))

    r, br = runner([0] + [1] * 30, stop_after=True)
    awal = await feed(r, [80000.0] * 22)
    tp_id = [o["id"] for o in cond_orders(br) if o["type"] == "TAKE_PROFIT_MARKET"][0]
    br.simulate_conditional_trigger(tp_id, r._bracket["tp_price"])
    akhir = await feed(r, [81000.0], start=22)
    check("TP bursa kena -> email menulis TAKE PROFIT", alasan_dari(awal, akhir) == "TAKE PROFIT")

    r, br = runner([0] + [1] * 21 + [-1] * 10)
    awal = await feed(r, [80000.0] * 22)
    akhir = await feed(r, [79900.0], start=22)
    check("reversal sinyal -> email menulis REVERSAL SINYAL", alasan_dari(awal, akhir) == "REVERSAL SINYAL")

    br = MockBroker(fill_immediately=True, leverage=20, taker_fee_pct=0.0004)
    r, _ = runner([0] * 21 + [1] * 10, broker=br)  # sinyal masuk baru muncul di bar ke-22
    awal = await feed(r, [80000.0] * 21)
    check("(prasyarat) belum ada posisi sebelum simulasi kegagalan", r._current_position == Position.FLAT)
    br.fail_stop_orders = True
    akhir = await feed(r, [80000.0], start=21)
    check("SL gagal terpasang -> email menulis PENUTUPAN DARURAT (bukan reversal)",
          (alasan_dari(awal, akhir) or "").startswith("PENUTUPAN DARURAT"), alasan_dari(awal, akhir))


async def main_async():
    await test_entry_and_tp()
    await test_sl_then_block_then_reverse()
    await test_reversal_cancels_brackets()
    await test_close_not_filled_restores_sl()
    await test_sl_rejected_emergency()
    await test_sl_without_trigger_emergency()
    await test_emergency_close_retry()
    await test_restart_adopts_or_places()
    await test_watch_loop_fast_detection()
    await test_default_unchanged()
    await test_notifier_reads_real_bot_logs()


def main() -> int:
    asal = os.getcwd()
    with tempfile.TemporaryDirectory() as d:
        os.chdir(d)  # order_intents.jsonl & session_state.json ditulis di folder sementara
        try:
            test_math()
            asyncio.run(main_async())
            test_quiet_fetch_position()
            test_real_ccxt_routing()
            test_launcher()
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