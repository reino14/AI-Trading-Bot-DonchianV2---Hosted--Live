"""
scripts/smoke_reentry.py

Uji asap: (1) masuk lagi dari tengah channel lalu breakout baru searah,
(2) rasio untung : rugi dua sisi di dashboard.

    python -m scripts.smoke_reentry
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
from src.runner.paper import PaperRunner  # noqa: E402
from src.strategy.base import Position  # noqa: E402
from src.strategy.donchian_close_futures import (DonchianCloseFuturesParams,  # noqa: E402
                                                 DonchianCloseFuturesStrategy)

FAILURES: list[str] = []
SYM = "BTC/USDT:USDT"
T0 = 1_790_000_000_000
LB = 5
NODE = shutil.which("node")


def check(name, cond, detail=""):
    print(f"  [{'LULUS' if cond else 'GAGAL'}] {name}" + (f" -- {detail}" if detail else ""))
    if not cond:
        FAILURES.append(name)


def buat(mode: str):
    br = MockBroker(fill_immediately=True, leverage=20, taker_fee_pct=0.0004)
    r = PaperRunner(DonchianCloseFuturesStrategy(DonchianCloseFuturesParams(lookback=LB)), br,
                    symbol=SYM, order_amount=0.01, risk_reward=2.0, sl_atr_period=3,
                    reentry_mode=mode, reentry_channel_fn=L.make_channel_fn(LB) if mode == "midline" else None)
    return r, br


async def bar(r, i, close):
    out = io.StringIO()
    with redirect_stdout(out):
        await r.process_bar({"timestamp": T0 + i * 60_000, "open": close, "high": close + 0.3,
                             "low": close - 0.3, "close": close, "volume": 1.0})
    return out.getvalue()


def picu(br, jenis, harga):
    oid = [o["id"] for o in br.fetch_open_conditional_orders(SYM) if o["type"] == jenis][0]
    br.simulate_conditional_trigger(oid, harga)


async def test_setelah_tp():
    print("== 1. Setelah TP: siap dari tengah channel, masuk lagi saat breakout baru ==")
    for mode in ("midline", "reversal"):
        r, br = buat(mode)
        for i in range(7):
            await bar(r, i, 100.0)
        await bar(r, 7, 105.0)                      # breakout atas -> masuk long
        if mode == "midline":
            check("breakout pertama membuka LONG", r._current_position == Position.LONG)
        picu(br, "TAKE_PROFIT_MARKET", r._bracket["tp_price"])
        await bar(r, 8, 107.0)                      # TP terdeteksi
        log_naik = await bar(r, 9, 108.0) + await bar(r, 10, 109.0)
        flat_saat_naik, siap_saat_naik = r._current_position == Position.FLAT, r._reentry_armed
        log_tengah = await bar(r, 11, 101.0)        # channel 100..109, tengah 104.5 -> siap
        log_breakout = await bar(r, 12, 110.0)      # tembus atas 109 -> breakout baru
        if mode == "midline":
            check("harga terus naik TANPA kembali ke tengah -> belum masuk (sesuai aturan)",
                  flat_saat_naik and not siap_saat_naik and "[signal] posisi" not in log_naik)
            check("kembali ke tengah (101 <= 104.5) -> status SIAP", "SIAP masuk LONG" in log_tengah, log_tengah.strip()[-160:])
            check("breakout baru (110 > 109) -> masuk LONG lagi, tanpa menunggu batas bawah jebol",
                  "BREAKOUT BARU searah" in log_breakout and r._current_position == Position.LONG)
            check("posisi baru langsung dilindungi SL/TP baru", len(br.fetch_open_conditional_orders(SYM)) == 2)
        else:
            check("mode lama (tunggu berbalik): skenario yang sama TIDAK masuk lagi",
                  r._current_position == Position.FLAT and "DITAHAN" in log_breakout)


async def test_setelah_sl():
    print("\n== 2. Setelah SL: aturan yang sama ==")
    r, br = buat("midline")
    for i in range(7):
        await bar(r, i, 100.0)
    await bar(r, 7, 105.0)
    picu(br, "STOP_MARKET", r._bracket["sl_price"])
    l8 = await bar(r, 8, 104.0)                     # channel 100..105, tengah 102.5 -> 104 belum
    check("SL terdeteksi, belum siap (104 > tengah 102.5)", "SL di BURSA KENA" in l8 and not r._reentry_armed)
    l9 = await bar(r, 9, 102.0)
    check("turun ke tengah -> SIAP", "SIAP masuk LONG" in l9)
    l10 = await bar(r, 10, 106.0)
    check("breakout baru di atas 105 -> masuk LONG lagi", r._current_position == Position.LONG, l10.strip()[-120:])


async def test_berbalik_tetap_jalan():
    print("\n== 3. Saat menunggu, sinyal berbalik -> masuk arah BARU seperti biasa ==")
    r, br = buat("midline")
    for i in range(7):
        await bar(r, i, 100.0)
    await bar(r, 7, 105.0)
    picu(br, "TAKE_PROFIT_MARKET", r._bracket["tp_price"])
    await bar(r, 8, 107.0)
    await bar(r, 9, 102.0)                          # siap
    check("(prasyarat) sedang siap untuk LONG", r._reentry_armed)
    await bar(r, 10, 95.0)                          # tembus bawah -> sinyal short
    check("tembus bawah -> SHORT dibuka", r._current_position == Position.SHORT)
    check("status siap LONG di-reset", not r._reentry_armed and r._blocked_direction is None)


def test_validasi_dan_launcher():
    print("\n== 4. Validasi & launcher ==")
    try:
        PaperRunner(DonchianCloseFuturesStrategy(DonchianCloseFuturesParams(lookback=LB)), MockBroker(),
                    reentry_mode="midline")
        check("midline tanpa fungsi channel ditolak", False)
    except ValueError:
        check("midline tanpa fungsi channel ditolak", True)
    bars = [{"close": c} for c in [10, 30, 20, 50, 40, 5, 99]]
    teks = L.make_channel_debug_fn(5)(bars)
    atas, bawah = L.make_channel_fn(5)(bars)
    # Jendela = 5 close SEBELUM bar terakhir: 30, 20, 50, 40, 5 -> atas 50, bawah 5.
    check("channel masuk-lagi = channel yang dicetak di log (atas 50, bawah 5)",
          (atas, bawah) == (50, 5) and "atas=50.00" in teks and "bawah=5.00" in teks)
    check("data kurang -> None", L.make_channel_fn(10)(bars) is None)

    ns = argparse.Namespace(lookback=200, mock=True, symbol=SYM, timeframe="1h", amount=0.01, session_hours=None,
                            min_entry_buffer_hours=None, take_profit_pct=None, stop_after_take_profit=False,
                            backfill_bars=300, live_take_profit_poll_seconds=None, risk_reward=1.5, sl_atr_mult=2.0,
                            sl_atr_period=20, sl_min_fee_mult=2.0, bracket_poll_seconds=5.0, reentry_mode="midline")
    with redirect_stdout(io.StringIO()):
        r = L.build_runner(ns)
    check("launcher meneruskan mode midline + fungsi channel", r.reentry_mode == "midline" and r.reentry_channel_fn)
    ns.reentry_mode = "reversal"
    with redirect_stdout(io.StringIO()):
        r = L.build_runner(ns)
    check("default reversal: tanpa fungsi channel (perilaku lama)", r.reentry_mode == "reversal" and r.reentry_channel_fn is None)


def js_function(name: str) -> str:
    src = dash.HTML_PAGE.split("<script>")[1].split("</script>")[0]
    start = src.index(f"function {name}(")
    depth, i = 0, src.index("{", start)
    while True:
        depth += {"{": 1, "}": -1}.get(src[i], 0)
        if depth == 0 and src[i] == "}":
            return src[start:i + 1]
        i += 1


def konfig_js(nilai: dict) -> dict:
    stub = "const nilai = " + json.dumps(nilai) + ";\nconst pilih = id => ({value: nilai[id] ?? ''});\n"
    code = stub + js_function("roiKeduanya") + "\n" + js_function("konfig") + "\nconsole.log(JSON.stringify(konfig()));"
    with tempfile.NamedTemporaryFile("w", suffix=".js", delete=False) as f:
        f.write(code)
    out = subprocess.run([NODE, f.name], capture_output=True, text=True, timeout=20)
    if out.returncode:
        raise RuntimeError(out.stderr)
    return json.loads(out.stdout)


def test_dashboard():
    print("\n== 5. Dashboard: TP/SL ROI & pilihan mode ==")
    h = dash.HTML_PAGE
    check("field TP & SL (ROI %) ada", 'id="c_tp_roi"' in h and 'id="c_sl_roi"' in h)
    check("pilihan 'Berulang -- masuk lagi dari tengah channel' ada", 'value="tengah"' in h)
    check("pilihan lama 'tunggu sinyal berbalik' tetap ada", 'value="berulang"' in h)
    if not NODE:
        check("node tersedia untuk uji JavaScript", False, "pasang nodejs untuk uji form")
        return
    dasar = {"c_symbol": SYM, "c_timeframe": "1h", "c_lookback": "238", "c_amount": "0.01", "c_session": "",
             "c_backfill": "300"}
    k = konfig_js({**dasar, "c_tp_roi": "5", "c_sl_roi": "1.5", "c_mode_tp": "tengah"})
    check("TP 5% & SL 1,5% dikirim ke bot", (k["tp_roi_pct"], k["sl_roi_pct"]) == (5, 1.5), str(k))
    check("mode 'dari tengah' -> reentry_mode midline, tidak berhenti setelah TP",
          k["reentry_mode"] == "midline" and k["stop_after_take_profit"] is False)
    k2 = konfig_js({**dasar, "c_tp_roi": "", "c_sl_roi": "1.5", "c_mode_tp": "berulang"})
    check("TP dikosongkan -> tanpa SL/TP sama sekali", k2["tp_roi_pct"] is None and k2["sl_roi_pct"] is None)
    check("mode 'tunggu berbalik' -> tanpa flag midline", k2["reentry_mode"] is None)
    k3 = konfig_js({**dasar, "c_tp_roi": "5", "c_sl_roi": "0", "c_mode_tp": "sekali"})
    check("SL 0 -> tanpa SL/TP (posisi tidak dibuka dengan SL nol)", k3["tp_roi_pct"] is None and k3["sl_roi_pct"] is None)
    check("mode 'sekali saja' -> berhenti setelah TP", k3["stop_after_take_profit"] is True)

    print("   rantai utuh: form -> perintah dashboard -> launcher")
    k["symbol"], k["lookback"] = SYM, 238
    cmd = dash.build_bot_command(k)
    s = " ".join(cmd)
    check("perintah memuat --tp-roi-pct 5.0 --sl-roi-pct 1.5 dan --reentry-mode midline",
          "--tp-roi-pct 5.0 --sl-roi-pct 1.5" in s and "--reentry-mode midline" in s, s)
    argv = ["--mock" if a == "--live" else a for a in cmd[cmd.index("scripts.run_paper_donchian_futures") + 1:]]
    asli = sys.argv
    try:
        sys.argv = ["x"] + argv
        buf = io.StringIO()
        with redirect_stdout(buf):
            L.main()
        check("launcher menerima dan menampilkan TP/SL ROI + aturan masuk lagi",
              "TP +5%" in buf.getvalue() and "tengah channel" in buf.getvalue())
    except SystemExit as e:
        check("launcher menerima dan menampilkan TP/SL ROI + aturan masuk lagi", False, str(e))
    finally:
        sys.argv = asli


async def test_status_note():
    print("\n== 6. Keadaan menunggu tidak lagi diam: status tampil di log & dashboard ==")
    from scripts.notifier_email import DashboardLog

    # Skenario screenshot: sinyal short sudah ada, posisi 0, arah short ditahan
    # (mis. baru kena SL), harga di bawah tengah channel, lalu menembus batas bawah.
    r, br = buat("midline")
    for i in range(7):
        await bar(r, i, 100.0)
    await bar(r, 7, 95.0)                                    # breakout bawah -> SHORT
    check("(prasyarat) short terbuka", r._current_position == Position.SHORT)
    picu(br, "STOP_MARKET", r._bracket["sl_price"])
    await bar(r, 8, 96.0)                                    # SL kena -> arah short ditahan
    check("(prasyarat) arah SHORT ditahan, belum siap", r._blocked_direction == Position.SHORT and not r._reentry_armed)

    log = await bar(r, 9, 94.0) + await bar(r, 10, 93.0)     # sinyal tetap short, harga jauh di bawah tengah
    check("heartbeat menjelaskan kenapa TIDAK masuk (belum siap)",
          "[status] DITAHAN SHORT: belum siap" in log and "naik ke" in log, log.strip()[-230:])
    check("menyebut angka: tengah channel dan batas breakout",
          "(tengah channel)" in log and "di bawah" in log)
    check("posisi memang tetap flat", r._current_position == Position.FLAT)
    ss = json.loads(Path("data/session_state.json").read_text())
    check("session_state.json membawa catatan status untuk dashboard", ss.get("status_note", "").startswith("DITAHAN SHORT"))

    log = await bar(r, 11, 100.0)                            # naik ke tengah -> siap
    log += await bar(r, 12, 100.0)
    check("setelah siap: status berubah 'sudah siap'", "DITAHAN SHORT (sudah siap)" in log, log.strip()[-200:])

    # Tidak boleh memicu mekanisme lain yang membaca log yang sama
    baris = [ln for ln in (await bar(r, 13, 100.0)).splitlines() if "[heartbeat]" in ln]
    semua = "\n".join(baris)
    check("TIDAK memuat pasangan penanda auto-stop dashboard",
          not any("stop_after_take_profit" in b and "BERHENTI trading total" in b for b in baris))
    isi = {"lines": []}
    d = DashboardLog("http://x/api", fetch=lambda u: {"bot": {"log": isi["lines"]}})
    d.poll(1000)
    isi["lines"] = semua.splitlines() * 3
    d.poll(2000)
    check("TIDAK dianggap penutupan oleh pembaca alasan notifier", d.reason(3000) is None)

    # Keadaan berhenti / darurat
    r2, br2 = buat("midline")
    r2.stop_after_take_profit = True
    for i in range(7):
        await bar(r2, i, 100.0)
    await bar(r2, 7, 105.0)
    picu(br2, "TAKE_PROFIT_MARKET", r2._bracket["tp_price"])
    await bar(r2, 8, 107.0)
    log = await bar(r2, 9, 108.0)
    check("bot berhenti: heartbeat menulis DIHENTIKAN (dulu diam)", "[status] DIHENTIKAN" in log and r2._trading_halted)
    heartbeat_halt = [ln for ln in log.splitlines() if "[heartbeat]" in ln]
    check("... tanpa penanda auto-stop (agar tidak memicu ulang)",
          not any("stop_after_take_profit" in b and "BERHENTI trading total" in b for b in heartbeat_halt))

    r3, br3 = buat("midline")
    r3._emergency_close_pending = True
    r3._status_note = r3._compute_status_note(1)
    check("penutupan darurat: catatan DARURAT", r3._status_note.startswith("DARURAT"))

    # Mode lama & posisi terbuka: tidak ada catatan palsu
    r4, br4 = buat("reversal")
    r4._blocked_direction = Position.SHORT
    check("mode 'tunggu berbalik': tidak menambah catatan (log lamanya sudah menjelaskan)",
          r4._compute_status_note(Position.SHORT) == "")
    r5, br5 = buat("midline")
    for i in range(7):
        await bar(r5, i, 100.0)
    log = await bar(r5, 7, 105.0)
    check("posisi terbuka & normal: tanpa catatan", "[status]" not in log and r5._current_position == Position.LONG)


def test_status_dashboard():
    print("\n== 7. Dashboard menampilkan status ==")
    if not NODE:
        check("node tersedia untuk uji JavaScript", False)
        return

    def render(d, b):
        code = js_function("panelStatusBot") + "\nconsole.log(JSON.stringify(panelStatusBot(" + json.dumps(d) + "," + json.dumps(b) + ")));"
        with tempfile.NamedTemporaryFile("w", suffix=".js", delete=False) as f:
            f.write(code)
        out = subprocess.run([NODE, f.name], capture_output=True, text=True, timeout=20)
        if out.returncode:
            raise RuntimeError(out.stderr)
        return json.loads(out.stdout)

    note = "DITAHAN SHORT: belum siap. Tunggu close naik ke 83479.75 (tengah channel) atau lebih tinggi"
    h = render({"session_state": {"status_note": note}}, {"running": True})
    check("bot jalan + ada catatan -> panel 'Status bot' tampil", "Status bot:" in h and "83479.75" in h and 'class="warn"' in h)
    check("bot MATI -> catatan lama disembunyikan (file tidak dibersihkan saat bot mati)",
          render({"session_state": {"status_note": note}}, {"running": False}) == "")
    check("tanpa catatan -> tidak ada panel", render({"session_state": {"status_note": ""}}, {"running": True}) == "")
    check("DIHENTIKAN / DARURAT -> ditampilkan sebagai galat (merah)",
          'class="err"' in render({"session_state": {"status_note": "DIHENTIKAN: x"}}, {"running": True}))
    check("teks disanitasi (tidak menyisipkan HTML)",
          "<script>" not in render({"session_state": {"status_note": "a <script>x</script>"}}, {"running": True}))
    check("panel dipasang di kartu Status", "panelStatusBot(d, b)" in dash.HTML_PAGE.split("<script>")[1])


async def main_async():
    await test_setelah_tp()
    await test_setelah_sl()
    await test_berbalik_tetap_jalan()
    await test_status_note()


def main() -> int:
    asal = os.getcwd()
    with tempfile.TemporaryDirectory() as d:
        os.chdir(d)
        try:
            asyncio.run(main_async())
            test_validasi_dan_launcher()
            test_dashboard()
            test_status_dashboard()
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