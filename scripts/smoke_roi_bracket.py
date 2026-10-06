"""
scripts/smoke_roi_bracket.py

Uji asap SL/TP berbasis ROI KOTOR terhadap margin (--tp-roi-pct / --sl-roi-pct),
termasuk skenario persis posisi Demo 30 Sep: LONG 0.003 @ 84101.80, 20x,
SL/TP lama 83165.30 / 88719.00, harga 84386.60.

    python -m scripts.smoke_roi_bracket
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

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from scripts import dashboard_donchian as dash  # noqa: E402
from scripts import run_paper_donchian_futures as L  # noqa: E402
from src.execution.broker import MockBroker  # noqa: E402
from src.runner.paper import PaperRunner, compute_bracket_roi  # noqa: E402
from src.strategy.base import Position, Strategy  # noqa: E402

import pandas as pd  # noqa: E402

FAILURES: list[str] = []
SYM = "BTC/USDT:USDT"
NODE = shutil.which("node")
ENTRY, QTY, LEV, FEE = 84101.80, 0.003, 20, 0.0004


def check(name, cond, detail=""):
    print(f"  [{'LULUS' if cond else 'GAGAL'}] {name}" + (f" -- {detail}" if detail else ""))
    if not cond:
        FAILURES.append(name)


def dekat(a, b, tol=1e-6):
    return a is not None and b is not None and abs(a - b) <= tol


class Tetap(Strategy):
    def __init__(self, v):
        self.v = v

    def generate_signals(self, df):
        return pd.Series([self.v] * len(df), index=df.index)


def test_rumus():
    print("== 1. Rumus ROI -> harga, dan hasil bersih setelah fee ==")
    b = compute_bracket_roi(ENTRY, 1, LEV, 0.05, 0.015, FEE)
    check("TP ROI 5% di 20x = harga +0,25% -> 84312.05", dekat(b["tp_price"], ENTRY * 1.0025, 1e-6), f"{b['tp_price']:.2f}")
    check("SL ROI 1,5% di 20x = harga -0,075% -> 84038.72", dekat(b["sl_price"], ENTRY * (1 - 0.00075), 1e-6), f"{b['sl_price']:.2f}")
    nilai = ENTRY * QTY
    untung, rugi = nilai * (b["tp_dist"] - b["fee_rt"]), nilai * (b["sl_dist"] + b["fee_rt"])
    check("fee bolak-balik 0,04%/sisi = ROI 1,6% (= 0,202 USDT, sama dengan dashboard Anda)",
          dekat(b["fee_rt"] * LEV, 0.016, 1e-12) and dekat(nilai * b["fee_rt"], 0.2018, 1e-4), f"{nilai * b['fee_rt']:.4f}")
    check("TP bersih = +0,43 USDT (ROI bersih +3,4%) -- asumsi Anda benar, masih untung",
          dekat(untung, 0.4289, 1e-4) and dekat((b["tp_dist"] - b["fee_rt"]) * LEV, 0.034, 1e-12), f"{untung:.4f}")
    check("SL total = -0,39 USDT (ROI -3,1%) -- fee lebih besar dari rugi harganya",
          dekat(rugi, 0.3911, 1e-4) and dekat((b["sl_dist"] + b["fee_rt"]) * LEV, 0.031, 1e-12), f"{rugi:.4f}")
    check("rasio bersih cuma ~1,1 : 1, bukan 5 : 1,5", dekat(b["rr"], 0.0017 / 0.00155, 1e-9), f"{b['rr']:.3f}")
    sh = compute_bracket_roi(ENTRY, -1, LEV, 0.05, 0.015, FEE)
    check("short: TP di bawah, SL di atas", sh["tp_price"] < ENTRY < sh["sl_price"])
    check("leverage tak terbaca -> 1x (TP/SL LEBIH JAUH, bukan lebih dekat)",
          dekat(compute_bracket_roi(ENTRY, 1, None, 0.05, 0.015, FEE)["tp_price"], ENTRY * 1.05, 1e-6))
    lama = (88719.00 - ENTRY) / ENTRY
    check("penjelasan screenshot: TP lama +5,49% HARGA = ROI ~+110%", dekat(lama * LEV, 1.098, 1e-3), f"{lama * LEV:.4f}")


def broker_posisi(harga_kini):
    br = MockBroker(fill_immediately=True, leverage=LEV, taker_fee_pct=FEE)
    br.place_limit_order(SYM, "buy", QTY, ENTRY)
    br.place_stop_market(SYM, "long", 83165.30)
    br.place_take_profit_market(SYM, "long", 88719.00)
    br.set_current_price(harga_kini)
    return br


async def mulai_ulang(br, stop_after=False):
    r = PaperRunner(Tetap(1), br, symbol=SYM, order_amount=QTY, tp_roi=0.05, sl_roi=0.015,
                    stop_after_take_profit=stop_after)
    out = io.StringIO()
    with redirect_stdout(out):
        r._recover_position_from_exchange()
        await r._ensure_bracket_after_recovery()
    return r, out.getvalue()


async def test_skenario_screenshot():
    print("\n== 2. Posisi Anda di screenshot, bot di-restart dengan TP 5% / SL 1,5% ==")
    br = broker_posisi(84386.60)
    r, log = await mulai_ulang(br)
    check("SL/TP lama (83165 / 88719) dikenali TIDAK sesuai pengaturan", "TIDAK sesuai pengaturan" in log)
    check("harga 84386.60 sudah melewati TP baru 84312.05 -> TUTUP PAKSA",
          "SUDAH mencapai target TP" in log and "TUTUP PAKSA" in log)
    check("posisi tertutup di bursa", br.fetch_position(SYM) is None)
    check("SL/TP lama dibersihkan, tidak ada yang tertinggal", br.fetch_open_conditional_orders(SYM) == [])
    check("mode berulang: arah long ditahan (aturan masuk-lagi berlaku)", r._blocked_direction == Position.LONG)

    br2 = broker_posisi(84386.60)
    r2, log2 = await mulai_ulang(br2, stop_after=True)
    check("mode 'sekali saja': bot berhenti + penanda auto-stop dashboard",
          r2._trading_halted and "stop_after_take_profit" in log2 and "BERHENTI trading total" in log2)


async def test_ganti_dan_pakai():
    print("\n== 3. Restart saat harga di antara SL & TP baru ==")
    br = broker_posisi(84150.00)
    r, log = await mulai_ulang(br)
    ords = {o["type"]: o["trigger_price"] for o in br.fetch_open_conditional_orders(SYM)}
    check("SL/TP lama DIGANTI dengan hitungan ROI", "DIGANTI" in log)
    check("SL baru 84038.72, TP baru 84312.05",
          dekat(ords.get("STOP_MARKET"), ENTRY * (1 - 0.00075), 1e-6)
          and dekat(ords.get("TAKE_PROFIT_MARKET"), ENTRY * 1.0025, 1e-6), str(ords))
    check("log menampilkan harga DAN ROI", "= ROI kotor +5.00%" in log and "= ROI kotor -1.50%" in log)
    check("posisi tetap terbuka", br.fetch_position(SYM) is not None)
    ids = sorted(o["id"] for o in br.fetch_open_conditional_orders(SYM))
    r2, log2 = await mulai_ulang(br)
    check("restart lagi dengan pengaturan sama -> DIPAKAI, tidak dipasang ulang",
          "DIPAKAI" in log2 and sorted(o["id"] for o in br.fetch_open_conditional_orders(SYM)) == ids)


async def test_sl_sudah_lewat():
    print("\n== 4. Harga sudah di bawah SL baru saat dipasang ==")
    br = broker_posisi(84000.00)
    r, log = await mulai_ulang(br)
    check("ditutup sebagai STOP LOSS", "SUDAH melewati SL" in log and "[stop-loss] posisi TERTUTUP" in log)
    check("posisi tertutup, tanpa order yatim", br.fetch_position(SYM) is None and br.fetch_open_conditional_orders(SYM) == [])
    check("SL tidak menghentikan bot", not r._trading_halted)


async def test_masuk_baru_dan_peringatan():
    print("\n== 5. Posisi BARU dibuka oleh sinyal ==")
    br = MockBroker(fill_immediately=True, leverage=LEV, taker_fee_pct=FEE)
    r = PaperRunner(Tetap(1), br, symbol=SYM, order_amount=QTY, tp_roi=0.05, sl_roi=0.015)
    out = io.StringIO()
    with redirect_stdout(out):
        for i in range(3):
            await r.process_bar({"timestamp": 1_790_000_000_000 + i * 60_000, "open": ENTRY, "high": ENTRY + 5,
                                 "low": ENTRY - 5, "close": ENTRY, "volume": 1})
    masuk = r._entry_price
    ords = {o["type"]: o["trigger_price"] for o in br.fetch_open_conditional_orders(SYM)}
    check("SL/TP dihitung dari harga masuk SEBENARNYA dan leverage 20x",
          dekat(ords.get("TAKE_PROFIT_MARKET"), masuk * 1.0025, 1e-6)
          and dekat(ords.get("STOP_MARKET"), masuk * (1 - 0.00075), 1e-6), f"masuk {masuk:.2f}: {ords}")
    check("SL terverifikasi di bursa", "SL terverifikasi" in out.getvalue())

    br2 = MockBroker(fill_immediately=True, leverage=LEV, taker_fee_pct=FEE)
    r2 = PaperRunner(Tetap(1), br2, symbol=SYM, order_amount=QTY, tp_roi=0.01, sl_roi=0.015)
    out = io.StringIO()
    with redirect_stdout(out):
        for i in range(3):
            await r2.process_bar({"timestamp": 1_790_000_000_000 + i * 60_000, "open": ENTRY, "high": ENTRY + 5,
                                  "low": ENTRY - 5, "close": ENTRY, "volume": 1})
    check("TP ROI 1% (< fee 1,6%) -> PERINGATAN tetap rugi", "TIDAK menutup fee" in out.getvalue())


def test_validasi():
    print("\n== 6. Validasi bot & launcher ==")
    for kw, nama in [({"tp_roi": 0.05}, "TP tanpa SL ditolak"),
                     ({"tp_roi": 0.05, "sl_roi": 0.015, "risk_reward": 2.0}, "ROI + rasio bersamaan ditolak"),
                     ({"tp_roi": 0.05, "sl_roi": 0.0}, "SL 0 ditolak")]:
        try:
            PaperRunner(Tetap(1), MockBroker(), **kw)
            check(nama, False)
        except ValueError:
            check(nama, True)
    asli = sys.argv
    for argv, pesan, nama in [
        (["--mock", "--tp-roi-pct", "5"], "diisi berdua", "launcher: --tp-roi-pct tanpa --sl-roi-pct ditolak"),
        (["--mock", "--tp-roi-pct", "5", "--sl-roi-pct", "1.5", "--risk-reward", "2"], "tidak bisa digabung",
         "launcher: ROI + --risk-reward ditolak")]:
        try:
            sys.argv = ["x"] + argv
            with redirect_stdout(io.StringIO()):
                L.main()
            check(nama, False)
        except SystemExit as e:
            check(nama, pesan in str(e), str(e))
        finally:
            sys.argv = asli
    ns = argparse.Namespace(lookback=200, mock=True, symbol=SYM, timeframe="1m", amount=QTY, session_hours=None,
                            min_entry_buffer_hours=None, take_profit_pct=None, stop_after_take_profit=False,
                            backfill_bars=200, live_take_profit_poll_seconds=None, risk_reward=None, sl_atr_mult=2.0,
                            sl_atr_period=20, sl_min_fee_mult=2.0, bracket_poll_seconds=5.0, reentry_mode="midline",
                            tp_roi_pct=5.0, sl_roi_pct=1.5)
    with redirect_stdout(io.StringIO()):
        r = L.build_runner(ns)
    check("persen dari CLI diubah ke fraksi (5 -> 0.05, 1.5 -> 0.015)", dekat(r.tp_roi, 0.05) and dekat(r.sl_roi, 0.015))


def js_fn(name):
    src = dash.HTML_PAGE.split("<script>")[1].split("</script>")[0]
    start = src.index(f"function {name}(")
    depth, i = 0, src.index("{", start)
    while True:
        depth += {"{": 1, "}": -1}.get(src[i], 0)
        if depth == 0 and src[i] == "}":
            return src[start:i + 1]
        i += 1


def node(code):
    with tempfile.NamedTemporaryFile("w", suffix=".js", delete=False) as f:
        f.write(code)
    out = subprocess.run([NODE, f.name], capture_output=True, text=True, timeout=20)
    if out.returncode:
        raise RuntimeError(out.stderr)
    return json.loads(out.stdout)


def test_dashboard():
    print("\n== 7. Dashboard ==")
    h = dash.HTML_PAGE
    check("field TP & SL dalam ROI %", 'id="c_tp_roi"' in h and 'id="c_sl_roi"' in h)
    check("field rasio & ATR lama sudah hilang", all(x not in h for x in ('c_untung', 'c_rugi', 'c_slatr')))
    cmd = dash.build_bot_command({"symbol": SYM, "timeframe": "1m", "lookback": 200, "amount": QTY,
                                  "tp_roi_pct": 5, "sl_roi_pct": 1.5, "reentry_mode": "midline", "backfill_bars": 200})
    s = " ".join(cmd)
    check("perintah memuat --tp-roi-pct 5.0 --sl-roi-pct 1.5", "--tp-roi-pct 5.0 --sl-roi-pct 1.5" in s, s)
    argv = ["--mock" if a == "--live" else a for a in cmd[cmd.index("scripts.run_paper_donchian_futures") + 1:]]
    asli = sys.argv
    try:
        sys.argv = ["x"] + argv
        buf = io.StringIO()
        with redirect_stdout(buf):
            L.main()
        check("launcher menerima perintah dari dashboard", "TP +5%" in buf.getvalue() and "SL -1.5%" in buf.getvalue())
    except SystemExit as e:
        check("launcher menerima perintah dari dashboard", False, str(e))
    finally:
        sys.argv = asli

    st = dash.DashboardState(SYM, 5, 1, 200, "1m")

    class B:
        exchange = type("E", (), {"fetch_my_trades": lambda *a, **k: [], "fetch_positions": lambda *a, **k: []})()
        def fetch_balance(self): return {"USDT": {"total": 5241.51, "free": 5000, "used": 12.62}}
        def fetch_position(self, s): return {"side": "long", "contracts": QTY, "leverage": 19.9999999996, "entry_price": ENTRY}
        def fetch_current_price(self, s): return 84386.6
        def fetch_recent_bars(self, s, tf, limit=100): return [{"timestamp": i, "open": 1, "high": 2, "low": 0.5, "close": 1, "volume": 1} for i in range(limit)]
        def fetch_trading_fee_pct(self, s): return FEE
        def fetch_open_conditional_orders(self, s): return []
    st._broker = B()
    st.refresh_once()
    check("leverage terakhir dikirim ke halaman (dibulatkan 20)", st.snapshot.get("leverage_terakhir") == 20,
          str(st.snapshot.get("leverage_terakhir")))

    if not NODE:
        check("node tersedia untuk uji JavaScript", False, "pasang nodejs untuk uji pratinjau")
        return
    kasus = [(84386.6, QTY, 20, FEE, 5, 1.5), (84386.6, 0.01, 20, 0.0005, 10, 3), (60000, 0.02, 10, 0.0004, 1, 2)]
    hasil = node(js_fn("pratinjauROI") + "\nconsole.log(JSON.stringify([" +
                 ",".join(f"pratinjauROI({a},{b},{c},{d},{e},{f})" for a, b, c, d, e, f in kasus) + "]));")
    for (harga, q, lev, fee, tp, sl), j in zip(kasus, hasil):
        b = compute_bracket_roi(harga, 1, lev, tp / 100, sl / 100, fee)
        n = harga * q
        sama = all(dekat(x, y, 1e-9) for x, y in [(j["t"], b["tp_dist"]), (j["s"], b["sl_dist"]),
                                                   (j["untungBersih"], n * (b["tp_dist"] - b["fee_rt"])),
                                                   (j["rugiTotal"], n * (b["sl_dist"] + b["fee_rt"])), (j["rasio"], b["rr"])])
        check(f"pratinjau = bot: TP {tp}% SL {sl}% {lev}x -> bersih {j['untungBersih']:+.3f} / {-j['rugiTotal']:.3f} USDT", sama)
    check("TP 1% di 10x (fee ROI 0,8%) masih untung tipis", hasil[2]["untungBersih"] > 0)
    check("SL 1,5% di 20x ditandai 'lebih kecil dari fee'", hasil[0]["slDiBawahFee"] is True)
    check("titik impas: perlu menang ~47,7%", dekat(hasil[0]["impas"], 0.00155 / (0.0017 + 0.00155), 1e-9),
          f"{hasil[0]['impas']:.4f}")

    panel = node("const pilih=()=>null;\n" + js_fn("panelSlTp") + "\nconsole.log(JSON.stringify(panelSlTp(" + json.dumps(
        {"position": {"entry_price": ENTRY, "leverage": 20}, "bracket_orders": [
            {"type": "STOP_MARKET", "trigger_price": 83165.3}, {"type": "TAKE_PROFIT_MARKET", "trigger_price": 88719.0}]})
        + ", true)));")
    check("panel menampilkan TP lama sebagai 'harga +5.490% · ROI +109.80%'",
          "harga +5.490%" in panel and "ROI +109.80%" in panel, panel[panel.find("harga +"):][:80])
    check("... dan SL lama sebagai ROI -22,27%", "ROI \u221222.27%" in panel)


async def main_async():
    await test_skenario_screenshot()
    await test_ganti_dan_pakai()
    await test_sl_sudah_lewat()
    await test_masuk_baru_dan_peringatan()


def main() -> int:
    asal = os.getcwd()
    with tempfile.TemporaryDirectory() as d:
        os.chdir(d)
        try:
            test_rumus()
            asyncio.run(main_async())
            test_validasi()
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