"""
src/execution/account.py

SATU sumber kebenaran: akun Binance mana yang dipakai bot, dashboard, dan
notifier. Dibaca dari environment variable TRADING_ACCOUNT di .env.bot:

    TRADING_ACCOUNT=demo   -> Binance Demo Trading (demo-fapi.binance.com)
    TRADING_ACCOUNT=live   -> akun ASLI (fapi.binance.com), uang sungguhan

TIDAK ADA DEFAULT. Kosong atau salah ketik = program menolak jalan. Dulu
pilihan akun ditulis langsung di kode (testnet=True/False) di dua file
berbeda, sehingga folder demo dan live harus punya kode yang berbeda dan
labelnya bisa salah (folder live sempat menulis "Demo Trading" padahal
memakai akun asli). Dengan modul ini kode demo dan live IDENTIK; yang
berbeda hanya isi .env.bot.
"""

from __future__ import annotations

import os

ENV_NAME = "TRADING_ACCOUNT"


class AccountConfigError(RuntimeError):
    pass


def resolve_account(env: dict | None = None) -> dict:
    env = os.environ if env is None else env
    raw = (env.get(ENV_NAME) or "").strip().lower()
    if raw not in ("demo", "live"):
        raise AccountConfigError(
            f"{ENV_NAME} wajib diisi 'demo' atau 'live' di .env.bot (sekarang: {raw or 'kosong'!r}). "
            f"Sengaja tidak ada default, supaya akun uang asli tidak pernah terpakai tanpa disadari."
        )
    live = raw == "live"
    return {
        "mode": raw,
        "is_live": live,
        "testnet": not live,
        "label": "AKUN ASLI (UANG SUNGGUHAN)" if live else "Binance Demo Trading",
        "host": "fapi.binance.com" if live else "demo-fapi.binance.com",
    }