"""
scripts/run_paper_donchian_futures.py

Launcher untuk PaperRunner dengan DonchianCloseFuturesStrategy --
MENGIKUTI PERSIS pola yang sudah ada di blok `--live` runner/paper.py
Anda sendiri (untuk DonchianBreakoutStrategy), cuma strategi dan
parameternya diganti. TIDAK menimpa runner/paper.py -- file terpisah,
supaya strategi lama tetap bisa dipakai lewat file aslinya.

BELUM DIUJI END-TO-END oleh saya -- saya tidak punya source
data/stream.py, execution/order_manager.py, execution/slippage_tracker.py
di sandbox saya, jadi tidak bisa membangun PaperRunner untuk tes
langsung seperti integrasi engine.py tadi. WAJIB dites dulu lewat jalur
MockBroker (tanpa --live) sebelum --live sungguhan -- persis anjuran
di paper.py Anda sendiri.

DEFAULT lookback=238 (tengah plateau tervalidasi dari scan 12-672),
BUKAN 8 -- kalau Anda mau override ke 8 atau nilai lain, itu keputusan
eksplisit Anda lewat --lookback, bukan default diam-diam yang mengarah
ke wilayah yang sudah terbukti buruk di scan sebelumnya.
"""

import argparse
import asyncio
import signal
import time
from pathlib import Path

import pandas as pd

from src.execution.account import AccountConfigError, resolve_account
from src.execution.broker import Broker, MockBroker
from src.execution.preflight import run_preflight
from src.strategy.base import Position
from src.strategy.donchian_close_futures import DonchianCloseFuturesParams, DonchianCloseFuturesStrategy
from src.strategy.regime_filtered import RegimeFilteredStrategy
import src.runner.paper as paper_module
from src.runner.paper import PaperRunner


def make_channel_debug_fn(lookback: int):
    """
    Bangun fungsi debug_info_fn untuk PaperRunner -- menghitung channel
    Donchian (atas/bawah) dan jaraknya ke harga sekarang, PERSIS rumus
    yang sama dipakai compute_donchian_signal() (rolling_max/min close
    `lookback` bar SEBELUM bar ini, TIDAK termasuk bar ini sendiri --
    lihat src/strategy/donchian_close.py).

    Dipakai untuk tracking manual: kalau harga sudah lewat "atas" atau
    "bawah" yang dicetak fungsi ini tapi sinyal TIDAK berubah di bar
    berikutnya, itu petunjuk kuat ada bug -- channel di sini dihitung
    dari BUFFER YANG SAMA PERSIS yang dipakai sinyal sungguhan, bukan
    hitungan terpisah yang bisa diam-diam beda.
    """
    def fn(bars: list[dict]) -> str:
        if len(bars) < lookback + 1:
            return f"[channel] belum cukup data ({len(bars)}/{lookback + 1} bar)"
        closes = [b["close"] for b in bars]
        window = closes[-(lookback + 1):-1]  # `lookback` bar SEBELUM bar terakhir
        upper = max(window)
        lower = min(window)
        current = closes[-1]
        dist_upper = (upper - current) / current
        dist_lower = (current - lower) / current
        return (f"[channel lookback={lookback}] atas={upper:.2f} (jarak {dist_upper:+.3%})  "
                f"bawah={lower:.2f} (jarak {dist_lower:+.3%})")
    return fn


def make_channel_fn(lookback: int):
    """
    (atas, bawah) channel Donchian untuk mode masuk-lagi "midline" --
    RUMUS SAMA PERSIS dengan make_channel_debug_fn() di atas dan dengan
    sinyal: max/min close `lookback` bar SEBELUM bar terakhir.
    None kalau data belum cukup.
    """
    def fn(bars: list[dict]):
        if len(bars) < lookback + 1:
            return None
        window = [b["close"] for b in bars[-(lookback + 1):-1]]
        return max(window), min(window)
    return fn


#: buffer PaperRunner saat filter regime aktif (default runner 500) -- lihat
#: BATASAN di src/strategy/regime_filtered.py. 1500 bar 1h = ~62 hari.
REGIME_BUFFER_BARS = 1500

#: batas atas candle backfill (regime timeframe besar di bot timeframe kecil).
#: Binance cuma memberi 1500 candle per permintaan -- di atas itu diambil
#: bertahap lewat paged_fetch_recent_bars().
MAX_BACKFILL_BARS = 12000
BINANCE_KLINE_LIMIT = 1500


def install_paged_backfill(broker, timeframe_minutes: int) -> None:
    """
    Broker.fetch_recent_bars() asli cukup untuk <= 1500 candle. Kalau diminta
    lebih (regime timeframe besar), bagian TERBARU tetap diambil lewat fungsi
    asli (jadi aturan candle-sudah-close tetap sama), lalu candle yang lebih
    lama ditambahkan di depannya lewat ccxt fetch_ohlcv bertahap.
    """
    asli = broker.fetch_recent_bars
    step = timeframe_minutes * 60_000

    def paged(symbol, timeframe, limit):
        if limit is None or limit <= BINANCE_KLINE_LIMIT:
            return asli(symbol, timeframe, limit=limit)
        recent = asli(symbol, timeframe, limit=BINANCE_KLINE_LIMIT)
        if not recent:
            return recent
        older, need, end = [], limit - len(recent), recent[0]["timestamp"]
        while need > 0:
            n = min(BINANCE_KLINE_LIMIT, need)
            raw = broker.exchange.fetch_ohlcv(symbol, timeframe, since=end - n * step, limit=n)
            rows = [r for r in raw if r[0] < end]
            if not rows:
                break
            older = [{"timestamp": int(r[0]), "open": float(r[1]), "high": float(r[2]),
                      "low": float(r[3]), "close": float(r[4]), "volume": float(r[5])}
                     for r in rows] + older
            end, need = rows[0][0], need - len(rows)
        print(f"  [backfill] {len(older)} candle lama ditambahkan bertahap (total {len(older) + len(recent)})")
        return older + recent

    broker.fetch_recent_bars = paged


def make_debug_fn(lookback: int, regime_strategy: RegimeFilteredStrategy | None = None):
    """Debug channel seperti biasa, plus regime terkini kalau filter aktif --
    dihitung dari buffer YANG SAMA dengan sinyal sungguhan."""
    channel_fn = make_channel_debug_fn(lookback)
    if regime_strategy is None:
        return channel_fn

    def fn(bars: list[dict]) -> str:
        text = channel_fn(bars)
        df = pd.DataFrame(bars)
        df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms", utc=True)
        r = regime_strategy.regime_snapshot(df.set_index("timestamp"))
        return (f"{text}  [regime {r['tf']}] {r['regime']} "
                f"(ADX={r['adx']:.1f} CHOP={r['chop']:.1f} ER={r['er']:.2f})")
    return fn


async def replay_historical(runner: PaperRunner, n_bars: int, data_dir: str, symbol_file: str) -> None:
    """
    Suapkan bar 1H BTC SUNGGUHAN (yang sudah ditarik sebelumnya untuk
    Chris Strategy) lewat runner.process_bar() SATU PER SATU, urut
    waktu -- validasi PALING KUAT sebelum --live: bukan cuma "berhasil
    dibuat objeknya", tapi benar-benar melihat sinyal berubah dan order
    (lewat MockBroker) benar-benar terkirim di data harga nyata.
    """
    path = Path(data_dir) / "klines" / symbol_file / "1h.parquet"
    df = pd.read_parquet(path).set_index("open_time")
    df.columns = [c.lower() for c in df.columns]
    df = df.tail(n_bars)

    print(f"\nMemutar ulang {len(df)} bar 1H terakhir dari {path} lewat process_bar()...\n")

    n_signal_changes = 0
    last_position = runner._current_position

    for ts, row in df.iterrows():
        bar = {
            "timestamp": int(ts.timestamp() * 1000),
            "open": float(row["open"]), "high": float(row["high"]),
            "low": float(row["low"]), "close": float(row["close"]),
            "volume": float(row.get("volume", 0.0)),
        }
        await runner.process_bar(bar)
        if runner._current_position != last_position:
            n_signal_changes += 1
            last_position = runner._current_position

    print(f"\n=== Selesai memutar {len(df)} bar ===")
    print(f"  Perubahan posisi terjadi: {n_signal_changes} kali")
    print(f"  Posisi akhir: {runner._current_position}")
    if n_signal_changes == 0:
        print("  PERHATIAN: TIDAK ADA perubahan posisi sama sekali selama replay ini --")
        print("  wajar kalau n_bars < lookback+1, atau kebetulan tidak ada breakout di")
        print("  jendela ini. Coba n_bars lebih besar sebelum menyimpulkan pipa tidak jalan.")


def use_price_pct_brackets() -> None:
    """
    Ubah arti --tp-roi-pct/--sl-roi-pct jadi % PERGERAKAN HARGA (= % dari nilai
    posisi), TIDAK bergantung leverage. Caranya: angka dikali leverage posisi
    SEBENARNYA saat entry sebelum masuk ke compute_bracket_roi() asli, yang
    lalu membaginya lagi dengan leverage yang sama -> harga SL/TP tetap.
    paper.py tidak diubah sama sekali.
    """
    asli = paper_module.compute_bracket_roi

    def by_price(entry, direction, leverage, tp_roi, sl_roi, fee_side):
        lev = leverage if leverage and leverage > 0 else 1.0
        return asli(entry, direction, leverage, tp_roi * lev, sl_roi * lev, fee_side)

    paper_module.compute_bracket_roi = by_price


def install_regime_entry_gate(runner: PaperRunner, regime_strategy: RegimeFilteredStrategy) -> None:
    """
    Pintu terakhir sebelum order PEMBUKA posisi: regime di candle SAAT INI harus
    TREND searah (LONG -> TREND_UP, SHORT -> TREND_DOWN). Kalau tidak:
      - dari FLAT        -> tidak ada order, tetap FLAT, dicek lagi candle berikutnya;
      - pembalikan arah  -> posisi lama TETAP ditutup, tapi tidak membuka arah baru.
    Menutup posisi (target FLAT) tidak pernah ditahan.

    Kenapa perlu: filter di RegimeFilteredStrategy memberi izin per "run" sinyal
    (begitu regime pernah cocok, izin berlaku sampai sinyal berganti). Eksekusi
    yang datang BELAKANGAN di run itu -- langkah ke-2 pembalikan arah, masuk
    lagi setelah TP/SL (midline), atau bot baru start -- bisa terjadi saat
    regime sudah TRANSITION/SIDEWAYS. Gerbang ini menutup celah itu.
    """
    asli = runner._handle_signal_change
    butuh = {Position.LONG: "TREND_UP", Position.SHORT: "TREND_DOWN"}

    async def gated(new_position, df=None, override_price=None):
        if new_position == Position.FLAT or df is None:
            return await asli(new_position, df, override_price=override_price)
        try:
            sekarang = str(regime_strategy.regime_series(df).iloc[-1])
        except Exception as e:
            print(f"  [regime] gagal membaca regime ({e}) -- entry DITAHAN demi aman")
            sekarang = "?"
        if sekarang == butuh.get(new_position):
            return await asli(new_position, df, override_price=override_price)
        arah = "LONG" if new_position == Position.LONG else "SHORT"
        if runner._current_position not in (Position.FLAT, new_position):
            print(f"  [regime] sinyal berbalik ke {arah} tapi regime {sekarang} -- posisi lama DITUTUP, "
                  f"entry {arah} ditahan sampai regime {butuh[new_position]}")
            return await asli(Position.FLAT, df, override_price=override_price)
        print(f"  [regime] entry {arah} DITAHAN -- regime {sekarang}, butuh {butuh[new_position]}")

    runner._handle_signal_change = gated


def _resolve_account_or_exit() -> dict:
    """Akun dari TRADING_ACCOUNT di .env.bot -- tanpa default (lihat src/execution/account.py)."""
    try:
        return resolve_account()
    except AccountConfigError as e:
        raise SystemExit(str(e))


def _stop_signals():
    """Sinyal yang berarti "hentikan bot": SIGTERM (Linux/systemd/dashboard) dan
    SIGBREAK (Windows: tombol Hentikan dashboard mengirim CTRL_BREAK)."""
    sig = [signal.SIGTERM]
    if hasattr(signal, "SIGBREAK"):
        sig.append(signal.SIGBREAK)
    return sig


def _sigterm_to_keyboard_interrupt(signum, frame):
    # Dashboard (Linux) menghentikan bot dengan SIGTERM. Default Python
    # langsung mati tanpa beres-beres; di sini diperlakukan sama dengan Ctrl+C.
    raise KeyboardInterrupt


async def close_position_on_stop(runner: PaperRunner, attempts: int = 5, wait_seconds: float = 3.0) -> None:
    """
    Dipanggil SETELAH loop utama berhenti (tombol Hentikan / Ctrl+C / SIGTERM).
    Posisi di bursa ditutup lewat jalur order yang SAMA dengan bot
    (_handle_signal_change), dicek ulang ke bursa tiap percobaan. Kalau tetap
    gagal, SL dipasang ulang supaya posisi tidak tertinggal tanpa pelindung.
    """
    print("\n=== Bot dihentikan -- menutup posisi di bursa (--close-on-stop) ===")
    for i in range(1, attempts + 1):
        try:
            runner._sync_position_from_exchange()
        except Exception as e:
            print(f"  [stop] gagal membaca posisi bursa ({e})")
        if runner._current_position == Position.FLAT:
            n = runner._cancel_conditional_orders_safely("bot dihentikan, posisi sudah flat")
            print(f"  [stop] posisi FLAT di bursa{' -- sisa SL/TP dibersihkan' if n and n > 0 else ''}. Selesai.")
            return
        try:
            price = runner.broker.fetch_current_price(runner.symbol)
        except Exception as e:
            print(f"  [stop] gagal ambil harga ({e}) -- coba lagi")
            time.sleep(wait_seconds)
            continue
        print(f"  [stop] percobaan {i}/{attempts}: tutup posisi {runner._current_position} di sekitar {price:.2f}")
        try:
            await runner._handle_signal_change(Position.FLAT, override_price=price)
        except Exception as e:
            print(f"  [stop] order penutup gagal ({e})")
        time.sleep(wait_seconds)

    try:
        runner._sync_position_from_exchange()
    except Exception:
        pass
    if runner._current_position != Position.FLAT:
        print("!" * 70)
        print(f"  [stop] POSISI MASIH TERBUKA setelah {attempts} percobaan -- TUTUP MANUAL di Binance.")
        if runner._bracket:
            # _handle_signal_change sudah memasang ulang SL tiap percobaan yang
            # gagal terisi. Pasang lagi HANYA kalau memang tidak ada -- SL dobel
            # ditolak bursa dan membuat log salah bilang posisi tanpa SL.
            ada_sl = False
            try:
                ada_sl = any(str(o.get("type", "")).startswith("STOP")
                             for o in runner.broker.fetch_open_conditional_orders(runner.symbol))
            except Exception:
                ada_sl = False
            if ada_sl:
                print("  [stop] SL masih terpasang di bursa -- posisi tetap terlindungi.")
            else:
                runner._restore_stop_loss()
        print("!" * 70)
    else:
        runner._cancel_conditional_orders_safely("bot dihentikan, posisi sudah flat")
        print("  [stop] posisi FLAT di bursa. Selesai.")


def build_runner(args: argparse.Namespace) -> PaperRunner:
    # getattr: pemanggil lama yang menyusun Namespace sendiri (tanpa field
    # baru ini) tetap jalan dengan perilaku lama, bukan crash.
    reentry_mode = getattr(args, "reentry_mode", "reversal")
    params = DonchianCloseFuturesParams(lookback=args.lookback)
    strategy = DonchianCloseFuturesStrategy(params)
    regime_strategy = None
    if getattr(args, "regime_filter", False):
        regime_strategy = RegimeFilteredStrategy(strategy, regime_timeframe=getattr(args, "regime_timeframe", None))
        strategy = regime_strategy
        need = regime_strategy.required_bars(args.timeframe)
        if need > MAX_BACKFILL_BARS:
            raise SystemExit(f"--regime-timeframe {args.regime_timeframe} butuh {need} candle {args.timeframe} "
                             f"untuk pemanasan -- melebihi batas {MAX_BACKFILL_BARS}. Pakai timeframe regime "
                             f"yang lebih kecil, atau timeframe bot yang lebih besar.")
        if (args.backfill_bars or 0) < need:
            print(f"  (Backfill dinaikkan {args.backfill_bars or 0} -> {need} candle supaya regime "
                  f"{args.regime_timeframe or args.timeframe} langsung siap, tidak menunggu berjam-jam)")
            args.backfill_bars = need
        print(f"  (Filter regime: dihitung di timeframe {args.regime_timeframe or args.timeframe}, "
              f"dari candle yang sudah close)")
    print(f"  (Strategi: {strategy.describe()})")
    print(f"  (ALLOWS_SHORT={strategy.ALLOWS_SHORT} -- wajib True untuk futures selalu-di-pasar ini)")

    if args.mock:
        broker = MockBroker(fill_immediately=True)
        print("  (Broker: MockBroker -- TIDAK menyentuh jaringan sama sekali)")
    else:
        akun = getattr(args, "account", None) or _resolve_account_or_exit()
        broker = Broker(exchange_id="binanceusdm", testnet=akun["testnet"])
        lev_target = getattr(args, "leverage", None)
        if lev_target is not None:
            # Broker tidak punya method set_leverage -- panggil ccxt langsung
            # (pola yang sama dengan dashboard). Gagal = bot TIDAK jalan,
            # supaya tidak diam-diam trading di leverage lama.
            try:
                broker.exchange.set_leverage(int(lev_target), args.symbol)
            except Exception as e:
                raise SystemExit(f"Gagal set leverage {lev_target}x di Binance ({e}). Bot TIDAK dijalankan. "
                                 f"Kalau ada posisi terbuka, tutup dulu lalu coba lagi.")
            print(f"  (Leverage {args.symbol} di bursa diset ke {int(lev_target)}x)")
        print(f"  (Broker: {akun['label']}, futures -- {akun['host']})")
        if akun["is_live"]:
            print("  " + "!" * 66)
            print("  !!  AKUN ASLI -- setiap order bot ini memakai UANG SUNGGUHAN.  !!")
            print("  " + "!" * 66)
        fatal = False
        for tingkat, pesan in run_preflight(broker, args.symbol, args.amount):
            if tingkat == "FATAL" and not akun["is_live"]:
                tingkat = "PERINGATAN"   # akun demo: dicetak saja, pengujian tidak dihentikan
            fatal = fatal or tingkat == "FATAL"
            print(f"  [cek akun] {tingkat}: {pesan}")
        if fatal:
            raise SystemExit("Pemeriksaan akun ASLI gagal (lihat [cek akun] FATAL di atas). Bot TIDAK dijalankan.")

    if args.take_profit_pct is not None:
        print(f"  (Take-profit: {args.take_profit_pct:.1%}, "
              f"{'BERHENTI TOTAL setelah kena' if args.stop_after_take_profit else 'tahan arah sama sampai sinyal berbalik'})")
    if getattr(args, "tp_sl_price_pct", False) and getattr(args, "tp_roi_pct", None) is not None:
        use_price_pct_brackets()
        print(f"  (TP/SL dalam % PERGERAKAN HARGA = % dari nilai posisi: TP +{args.tp_roi_pct:g}%, "
              f"SL -{args.sl_roi_pct:g}% -- tidak bergantung leverage.)")
    if getattr(args, "tp_roi_pct", None) is not None:
        tahan_roi = ("masuk lagi setelah harga kembali ke tengah channel lalu breakout baru searah"
                     if reentry_mode == "midline" else "tahan arah sampai sinyal berbalik")
        if not getattr(args, "tp_sl_price_pct", False):
            print(f"  (SL/TP dari ROI KOTOR terhadap margin, dititipkan ke BURSA: TP +{args.tp_roi_pct:g}%, "
                  f"SL -{args.sl_roi_pct:g}%. Harga dihitung dari leverage posisi yang sebenarnya.)")
        print(f"  (Setelah TP: {'BERHENTI TOTAL' if args.stop_after_take_profit else tahan_roi}. "
              f"Setelah SL: {tahan_roi}.)")
    if args.risk_reward is not None:
        print(f"  (SL + TP rasio {args.risk_reward:g}:1 BERSIH setelah fee, dititipkan ke BURSA:")
        print(f"   SL = maks({args.sl_atr_mult:g} x ATR{args.sl_atr_period}, {args.sl_min_fee_mult:g} x fee bolak-balik), "
              f"TP = {args.risk_reward:g} x SL + {args.risk_reward + 1:g} x fee.")
        tahan = ("masuk lagi setelah harga kembali ke tengah channel lalu breakout baru searah"
                 if reentry_mode == "midline" else "tahan arah sampai sinyal berbalik")
        print(f"   Setelah TP: {'BERHENTI TOTAL' if args.stop_after_take_profit else tahan}. "
              f"Setelah SL: {tahan}.)")

    if regime_strategy is not None and (args.backfill_bars or 0) > BINANCE_KLINE_LIMIT \
            and hasattr(broker, "exchange"):
        from src.strategy.regime_filtered import tf_minutes
        install_paged_backfill(broker, tf_minutes(args.timeframe))

    runner = PaperRunner(
        strategy, broker, symbol=args.symbol, timeframe=args.timeframe,
        order_amount=args.amount, use_reduce_only=True,  # futures -- reduceOnly relevan, beda dari spot
        session_hours=args.session_hours, min_entry_buffer_hours=args.min_entry_buffer_hours,
        take_profit_pct=args.take_profit_pct, stop_after_take_profit=args.stop_after_take_profit,
        backfill_bars=args.backfill_bars, live_take_profit_poll_seconds=args.live_take_profit_poll_seconds,
        debug_info_fn=make_debug_fn(args.lookback, regime_strategy),
        buffer_size=max(REGIME_BUFFER_BARS, (args.backfill_bars or 0) + 1000) if regime_strategy is not None else 500,
        risk_reward=args.risk_reward, sl_atr_mult=args.sl_atr_mult, sl_atr_period=args.sl_atr_period,
        sl_min_fee_mult=args.sl_min_fee_mult, bracket_poll_seconds=args.bracket_poll_seconds,
        reentry_mode=reentry_mode,
        tp_roi=(args.tp_roi_pct / 100) if getattr(args, "tp_roi_pct", None) is not None else None,
        sl_roi=(args.sl_roi_pct / 100) if getattr(args, "sl_roi_pct", None) is not None else None,
        reentry_channel_fn=make_channel_fn(args.lookback) if reentry_mode == "midline" else None,
        state_path=None if args.mock else getattr(args, "state_path", None),
        max_consecutive_sl=getattr(args, "max_consecutive_sl", None),
        max_daily_loss_pct=getattr(args, "max_daily_loss_pct", None),
    )
    if regime_strategy is not None:
        install_regime_entry_gate(runner, regime_strategy)
        print("  (Gerbang regime AKTIF: order pembuka posisi hanya saat regime candle ini TREND searah)")
    return runner


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--live", action="store_true",
                    help="WAJIB diisi eksplisit untuk koneksi ke bursa. AKUN dipilih oleh TRADING_ACCOUNT "
                         "(demo/live) di .env.bot, bukan oleh flag ini. "
                         "Tanpa ini, dan tanpa --mock, program akan MENOLAK jalan -- "
                         "supaya tidak ada kombinasi ambigu yang diam-diam nembak jaringan.")
    p.add_argument("--mock", action="store_true",
                    help="Jalankan dengan MockBroker (tanpa jaringan sama sekali) -- "
                         "WAJIB dicoba dulu sebelum --live, persis pola paper.py asli Anda.")
    p.add_argument("--symbol", default="BTC/USDT:USDT")
    p.add_argument("--timeframe", default="1h", help="WAJIB 1h -- lookback yang divalidasi dihitung dalam jam")
    p.add_argument("--lookback", type=int, default=238,
                    help="default 238 = tengah plateau tervalidasi. WAJIB override eksplisit "
                         "kalau mau nilai lain -- lihat peringatan soal lookback pendek sebelumnya.")
    p.add_argument("--amount", type=float, default=0.001, help="ukuran order dalam BTC")
    p.add_argument("--session-hours", type=float, default=None)
    p.add_argument("--min-entry-buffer-hours", type=float, default=None)
    p.add_argument("--take-profit-pct", type=float, default=None,
                    help="mis. 0.02 untuk 2%% -- posisi ditutup paksa begitu floating PnL "
                         "mencapai ini, tidak menunggu sinyal strategi berbalik")
    p.add_argument("--stop-after-take-profit", action="store_true",
                    help="setelah take-profit kena SEKALI, program BERHENTI trading total "
                         "(bukan cuma menahan arah yang sama -- benar-benar berhenti permanen "
                         "sampai direstart manual)")
    p.add_argument("--backfill-bars", type=int, default=None,
                    help="mis. 200 -- isi buffer dari histori SEBELUM live mulai, supaya sinyal "
                         "pertama bisa langsung dihitung, tidak perlu menunggu lookback bar baru "
                         "satu-satu dari stream")
    p.add_argument("--live-take-profit-poll-seconds", type=float, default=None,
                    help="mis. 5.0 -- pantau take-profit lewat harga LIVE tiap sekian detik, "
                         "TERPISAH dari evaluasi candle (yang cuma sekali per candle tutup). "
                         "Tanpa ini, take-profit tetap jalan tapi cuma dievaluasi sekali per candle.")
    p.add_argument("--risk-reward", type=float, default=None,
                    help="mis. 2 -- pasang STOP LOSS dan TAKE PROFIT di BURSA dengan rasio UANG BERSIH "
                         "(setelah fee) R:1. Tidak bisa digabung dengan --take-profit-pct.")
    p.add_argument("--sl-atr-mult", type=float, default=2.0,
                    help="jarak SL = kelipatan ATR (default 2, aturan 2N sistem Turtle)")
    p.add_argument("--sl-atr-period", type=int, default=20,
                    help="jumlah bar untuk ATR, di timeframe bot (default 20)")
    p.add_argument("--sl-min-fee-mult", type=float, default=2.0,
                    help="batas bawah jarak SL = kelipatan fee bolak-balik (default 2), supaya "
                         "fee paling banyak sepertiga dari kerugian per SL")
    p.add_argument("--bracket-poll-seconds", type=float, default=5.0,
                    help="seberapa sering bot mengecek apakah SL/TP sudah kena, untuk membereskan "
                         "sisa order (eksekusi SL/TP sendiri oleh BURSA, tidak bergantung angka ini)")
    p.add_argument("--tp-roi-pct", type=float, default=None,
                    help="TAKE PROFIT sebagai ROI KOTOR terhadap margin, dalam PERSEN (mis. 5 = +5%%, sama "
                         "dengan ROI di aplikasi Binance). Wajib berpasangan dengan --sl-roi-pct.")
    p.add_argument("--sl-roi-pct", type=float, default=None,
                    help="STOP LOSS sebagai ROI KOTOR terhadap margin, dalam PERSEN (mis. 1.5 = -1,5%%). "
                         "Fee bolak-balik DITAMBAHKAN ke kerugian ini saat SL kena.")
    p.add_argument("--tp-sl-price-pct", action="store_true",
                    help="artikan --tp-roi-pct/--sl-roi-pct sebagai %% PERGERAKAN HARGA (= %% dari nilai "
                         "posisi), bukan ROI terhadap margin. Mis. --tp-roi-pct 5 = harga +5%%.")
    p.add_argument("--regime-timeframe", default=None,
                    help="hitung filter regime di timeframe LEBIH BESAR dari timeframe bot, format timeframe "
                         "Binance (mis. 3m, 5m, 15m, 30m, 1h, 2h, 4h, 1d, 1w). Harus kelipatan "
                         "timeframe bot. Default: sama dengan timeframe bot.")
    p.add_argument("--leverage", type=int, default=None,
                    help="set leverage simbol ini di Binance sebelum bot mulai (mis. 1 = tanpa leverage). "
                         "Tanpa ini, bot memakai leverage yang sedang terpasang di Binance.")
    p.add_argument("--close-on-stop", action="store_true",
                    help="saat bot dihentikan (Ctrl+C / tombol Hentikan / SIGTERM), TUTUP posisi di bursa "
                         "dan bersihkan SL/TP. Tanpa ini posisi dibiarkan terbuka dengan SL/TP di bursa.")
    p.add_argument("--max-consecutive-sl", type=int, default=3,
                    help="kill switch: berhenti membuka posisi sampai hari berganti (WIB) setelah sekian SL "
                         "beruntun di hari yang sama. 0 = mati. Default 3.")
    p.add_argument("--max-daily-loss-pct", type=float, default=2.0,
                    help="kill switch: berhenti membuka posisi sampai hari berganti (WIB) kalau rugi bersih "
                         "hari itu >= sekian %% saldo awal hari. 0 = mati. Default 2.")
    p.add_argument("--state-path", default="data/bot_state.json",
                    help="file status yang bertahan lintas restart (arah ditahan, SL beruntun, batas harian)")
    p.add_argument("--reentry-mode", choices=["reversal", "midline"], default="reversal",
                    help="setelah posisi ditutup TP/SL, kapan boleh masuk lagi ke arah YANG SAMA: "
                         "'reversal' (default) = tunggu sinyal berbalik; 'midline' = siap begitu harga "
                         "kembali ke tengah channel, lalu masuk saat ada breakout baru searah")
    p.add_argument("--regime-filter", action="store_true",
                    help="tahan ENTRY baru sampai market regime TREND searah sinyal "
                         "(ADX/Choppiness/Efficiency Ratio, lihat src/strategy/regime_detector.py). "
                         "Posisi yang sudah terbuka TIDAK ditutup paksa karena regime.")
    p.add_argument("--replay-historical", type=int, default=None,
                    help="jumlah bar 1H BTC SUNGGUHAN terakhir untuk diputar ulang lewat "
                         "process_bar() di mode --mock -- validasi kuat sebelum --live. "
                         "Isi minimal lookback+50 supaya sempat lewati masa pemanasan.")
    p.add_argument("--data-dir", default="data/raw")
    p.add_argument("--symbol-file", default="BTCUSDT", help="nama folder di data-dir/klines/")
    args = p.parse_args()

    if not args.live and not args.mock:
        raise SystemExit(
            "Wajib pilih salah satu eksplisit: --mock (uji tanpa jaringan, WAJIB dicoba dulu) "
            "atau --live (bursa sungguhan; akun dipilih TRADING_ACCOUNT di .env.bot). Tidak ada default diam-diam."
        )
    if args.live and args.mock:
        raise SystemExit("--live dan --mock tidak bisa dipakai bersamaan -- pilih salah satu.")
    if args.max_consecutive_sl < 0 or args.max_daily_loss_pct < 0:
        raise SystemExit("--max-consecutive-sl dan --max-daily-loss-pct tidak boleh negatif (0 = mati).")
    if args.live:
        args.account = _resolve_account_or_exit()
    if (args.tp_roi_pct is None) != (args.sl_roi_pct is None):
        raise SystemExit("--tp-roi-pct dan --sl-roi-pct harus diisi berdua.")
    if args.tp_roi_pct is not None:
        if args.risk_reward is not None or args.take_profit_pct is not None:
            raise SystemExit("--tp-roi-pct/--sl-roi-pct tidak bisa digabung dengan --risk-reward atau "
                             "--take-profit-pct: semuanya memasang TP di bursa. Pilih satu cara.")
        if args.tp_roi_pct <= 0 or args.sl_roi_pct <= 0:
            raise SystemExit("--tp-roi-pct dan --sl-roi-pct harus > 0.")
    if args.risk_reward is not None:
        if args.take_profit_pct is not None:
            raise SystemExit("--risk-reward dan --take-profit-pct tidak bisa dipakai bersamaan: "
                             "keduanya memasang TP di bursa. Pilih salah satu.")
        if args.risk_reward <= 0 or args.sl_atr_mult <= 0 or args.sl_atr_period <= 0 or args.sl_min_fee_mult < 0:
            raise SystemExit("--risk-reward, --sl-atr-mult, --sl-atr-period harus > 0, "
                             "dan --sl-min-fee-mult tidak boleh negatif.")
        if args.live_take_profit_poll_seconds is not None:
            print("  (Catatan: --live-take-profit-poll-seconds tidak berpengaruh di mode --risk-reward; "
                  "SL/TP dieksekusi bursa.)")

    if args.regime_timeframe is not None:
        from src.strategy.regime_filtered import tf_minutes
        if not args.regime_filter:
            raise SystemExit("--regime-timeframe hanya berlaku bersama --regime-filter.")
        try:
            reg_min, bot_min = tf_minutes(args.regime_timeframe), tf_minutes(args.timeframe)
        except ValueError as e:
            raise SystemExit(f"--regime-timeframe: {e}")
        if reg_min <= bot_min:
            raise SystemExit(f"--regime-timeframe ({args.regime_timeframe}) harus LEBIH BESAR dari "
                             f"--timeframe ({args.timeframe}).")
        if reg_min % bot_min:
            raise SystemExit(f"--regime-timeframe ({args.regime_timeframe}) harus KELIPATAN --timeframe "
                             f"({args.timeframe}), mis. 5m/15m/1h untuk bot 1m.")
        from src.strategy.regime_filtered import normalize_tf
        args.regime_timeframe = normalize_tf(args.regime_timeframe)

    if args.lookback < 100:
        print(f"\nPERINGATAN: lookback={args.lookback} jauh di bawah plateau tervalidasi (148-328).")
        print("Scan sebelumnya menunjukkan wilayah ini konsisten buruk (lookback=72 dan 150")
        print("keduanya rugi bersih). Anda tetap bisa lanjut, tapi ini bukan parameter yang")
        print("sudah tervalidasi -- ini eksperimen terpisah, bukan strategi yang sudah teruji.\n")

    runner = build_runner(args)

    if args.mock:
        print("\n=== Mode MOCK -- tidak menyentuh jaringan, cuma verifikasi pipa ===")
        if args.replay_historical:
            asyncio.run(replay_historical(runner, args.replay_historical, args.data_dir, args.symbol_file))
        else:
            print("Panggil runner.process_bar(bar) manual dengan bar tiruan untuk uji,")
            print("persis pola __main__ non---live di runner/paper.py Anda sendiri.")
            print("Atau tambahkan --replay-historical N untuk memutar N bar BTC sungguhan")
            print("lewat pipa ini secara otomatis (rekomendasi: coba ini dulu).")
    else:
        print(f"\n=== Mode LIVE ({args.account['label']}) -- {args.symbol} @ {args.timeframe} ===")
        for sig in _stop_signals():
            signal.signal(sig, _sigterm_to_keyboard_interrupt)
        try:
            asyncio.run(runner.run())
        except KeyboardInterrupt:
            if args.close_on_stop:
                # Abaikan sinyal stop berikutnya selama menutup -- jangan
                # sampai proses penutupan sendiri terpotong di tengah jalan.
                for sig in _stop_signals() + [signal.SIGINT]:
                    signal.signal(sig, signal.SIG_IGN)
                asyncio.run(close_position_on_stop(runner))
            else:
                print("\n=== Bot dihentikan -- posisi & SL/TP di bursa DIBIARKAN (tanpa --close-on-stop) ===")


if __name__ == "__main__":
    main()