"""
src/execution/preflight.py

Pemeriksaan akun SEBELUM bot boleh trading. Hasilnya daftar (tingkat, pesan):
  OK    -- aman
  WARN  -- perlu diperhatikan, bot tetap jalan
  FATAL -- bot TIDAK boleh jalan di akun asli (launcher yang menghentikan)

Di akun demo, FATAL hanya dicetak sebagai peringatan supaya pengujian di
Demo tidak terganggu; di akun live, satu FATAL saja menghentikan bot.

Yang diperiksa:
  1. Saldo terbaca (bukti API key & akun benar).
  2. Position mode ONE-WAY. Bot memakai reduceOnly dan closePosition, yang
     tidak bekerja di Hedge Mode (Binance menolak reduceOnly di hedge mode).
  3. Margin ISOLATED (dicoba dipasang; gagal = peringatan, mis. karena masih
     ada posisi/order terbuka).
  4. Ukuran order di atas batas minimum bursa (jumlah & nilai notional),
     dibaca langsung dari data market bursa, bukan angka hafalan.
"""

from __future__ import annotations

import math

#: Nilai posisi di bawah (minimum x MARGIN) diberi peringatan: harga cukup
#: turun sedikit saja untuk membuat order berikutnya ditolak bursa.
NOTIONAL_SAFETY_MARGIN = 1.03


def saran_amount(min_cost: float, harga: float, langkah: float) -> float:
    """Jumlah terkecil (kelipatan `langkah`) yang nilainya >= min_cost x margin aman."""
    target = min_cost * NOTIONAL_SAFETY_MARGIN / harga
    return round(math.ceil(target / langkah) * langkah, 10)


def run_preflight(broker, symbol: str, amount: float, set_isolated: bool = True) -> list[tuple[str, str]]:
    out: list[tuple[str, str]] = []
    ex = broker.exchange

    try:
        usdt = (broker.fetch_balance() or {}).get("USDT", {})
        out.append(("OK", f"saldo terbaca: {float(usdt.get('total') or 0):.2f} USDT "
                          f"(bebas {float(usdt.get('free') or 0):.2f})"))
    except Exception as e:
        out.append(("FATAL", f"saldo TIDAK bisa dibaca ({type(e).__name__}: {e}) -- cek API key & akun"))

    try:
        mode = ex.fetch_position_mode(symbol)
        if mode.get("hedged"):
            out.append(("FATAL", "akun dalam HEDGE MODE -- bot butuh ONE-WAY MODE (reduceOnly/closePosition "
                                 "ditolak di hedge mode). Ubah di Binance: Futures > Preferences > Position Mode."))
        else:
            out.append(("OK", "position mode: one-way"))
    except Exception as e:
        out.append(("FATAL", f"position mode tidak bisa dibaca ({type(e).__name__}: {e})"))

    if set_isolated:
        try:
            ex.set_margin_mode("isolated", symbol)
            out.append(("OK", "margin mode: isolated"))
        except Exception as e:
            out.append(("WARN", f"margin mode tidak bisa diset ke isolated ({type(e).__name__}: {e}) -- "
                                f"pastikan manual di Binance"))

    try:
        ex.load_markets()
        m = ex.market(symbol)
        lim = m.get("limits") or {}
        min_amt = (lim.get("amount") or {}).get("min")
        min_cost = (lim.get("cost") or {}).get("min")
        langkah = (m.get("precision") or {}).get("amount") or 0.001
        harga = broker.fetch_current_price(symbol)
        nilai = amount * harga
        if min_amt and amount < float(min_amt):
            out.append(("FATAL", f"amount {amount} di bawah minimum jumlah bursa {min_amt}"))
        if min_cost and nilai < float(min_cost):
            out.append(("FATAL", f"nilai order {nilai:.2f} USDT ({amount} x {harga:.2f}) di bawah minimum "
                                 f"notional bursa {float(min_cost):.2f} USDT -- order akan DITOLAK. Pakai amount "
                                 f">= {saran_amount(float(min_cost), harga, float(langkah))}"))
        elif min_cost and nilai < float(min_cost) * NOTIONAL_SAFETY_MARGIN:
            out.append(("WARN", f"nilai order {nilai:.2f} USDT mepet dengan minimum {float(min_cost):.2f} -- "
                                f"turun sedikit saja order ditolak. Saran amount "
                                f">= {saran_amount(float(min_cost), harga, float(langkah))}"))
        elif min_cost:
            out.append(("OK", f"nilai order {nilai:.2f} USDT >= minimum {float(min_cost):.2f}"))
        else:
            out.append(("WARN", "batas minimum notional tidak tersedia dari bursa -- tidak bisa dicek"))
    except Exception as e:
        out.append(("FATAL", f"batas order bursa tidak bisa dibaca ({type(e).__name__}: {e})"))
    return out