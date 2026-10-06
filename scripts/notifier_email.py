"""
scripts/notifier_email.py

Notifikasi Gmail untuk bot Donchian: MASUK POSISI, UKURAN BERUBAH, dan
POSISI DITUTUP (untung/rugi bersih setelah fee).

=========================================================================
FILE BARU, PROSES TERPISAH. Tidak mengubah bot, broker.py, maupun
dashboard. Semua panggilan ke bursa di sini READ-ONLY (riwayat fill dan
posisi). Notifier tidak pernah mengirim order.
=========================================================================

KENAPA DARI FILL BURSA, BUKAN DARI LOG BOT
-------------------------------------------------------------------------
- Tidak bergantung pada teks log bot atau cara bot dijalankan (tombol
  dashboard, terminal, atau systemd) -- semuanya menghasilkan fill yang
  sama di bursa.
- Kalau posisi ditutup lalu dibuka lagi dalam jeda satu polling, cuplikan
  posisi saja akan melewatkannya; urutan fill tidak.
- Untung bersih per trade dihitung dari angka bursa: jumlah realizedPnl
  DIKURANGI fee SEMUA fill trade itu (fee buka + fee tutup).

ALASAN PENUTUPAN -- dari log bot di dashboard
-------------------------------------------------------------------------
- Bursa tidak mencatat ALASAN penutupan. Kalau NOTIF_DASHBOARD_URL diisi,
  notifier membaca log bot yang ditangkap dashboard: baris
  "[take-profit] ... TUTUP PAKSA" -> TAKE PROFIT, baris "[signal] posisi
  ... reduceOnly" tanpa take profit -> REVERSAL SINYAL, baris
  "[stop-loss] ... TERTUTUP" -> STOP LOSS (fitur --risk-reward).
- Dashboard HANYA dipakai untuk alasan. Deteksi posisi tetap dari fill
  bursa (punya ID order, riwayat lengkap), jadi kalau dashboard mati atau
  restart, email tetap terkirim -- alasannya saja yang tertulis "tidak
  diketahui".
- Alasan hanya terbaca untuk bot yang dijalankan DARI dashboard itu. Bot
  yang dijalankan dari terminal tidak punya log di dashboard.
- Bot saat ini tidak punya stop loss, jadi tidak ada notifikasi "stop loss".

Cara pakai:
    python -m scripts.notifier_email --test-email   # uji kirim email saja
    python -m scripts.notifier_email --dry-run      # jalan, email dicetak, tidak dikirim
    python -m scripts.notifier_email                # jalan sungguhan
"""

from __future__ import annotations

import argparse
import contextlib
import io
import json
import os
import smtplib
import ssl
import time
import urllib.request
from datetime import datetime, timezone
from email.message import EmailMessage
from pathlib import Path

try:
    from zoneinfo import ZoneInfo
    WIB = ZoneInfo("Asia/Jakarta")
except Exception:  # tzdata tidak tersedia -- jatuh ke UTC, tetap jalan
    WIB = timezone.utc

EPS = 1e-9
#: Fill baru diproses setelah berumur sekian ms. Satu order maker bisa
#: terisi dalam belasan fill selama beberapa detik (contoh nyata: order
#: 28587818567, 22:03:01-22:03:07). Tanpa jeda ini, pecahan satu order bisa
#: terbelah di dua putaran dan memicu email "ukuran berubah" palsu.
SETTLE_MS = 20_000


# ---------------------------------------------------------------------------
# Rekonstruksi trade dari fill -- FUNGSI MURNI
# ---------------------------------------------------------------------------

def _num(x, default=0.0) -> float:
    try:
        return float(x) if x not in (None, "") else default
    except (TypeError, ValueError):
        return default


def normalize_fill(t: dict) -> dict:
    """Ambil yang dibutuhkan dari trade ccxt. Fee: fee.cost dulu, fallback info.commission."""
    info = t.get("info") or {}
    fee = (t.get("fee") or {}).get("cost")
    if fee in (None, ""):
        fee = info.get("commission")
    amount = abs(_num(t.get("amount")))
    return {
        "id": str(t.get("id") or f"{t.get('timestamp')}-{t.get('order')}-{amount}"),
        "order": str(t.get("order") or ""),
        "ts": int(t.get("timestamp") or 0),
        "side": t.get("side"),
        "signed": amount if t.get("side") == "buy" else -amount,
        "amount": amount,
        "price": _num(t.get("price")),
        "fee": _num(fee),
        "pnl": _num(info.get("realizedPnl")),
    }


def merge_by_order(fills: list[dict]) -> list[dict]:
    """
    Gabungkan fill BERURUTAN dari order yang sama jadi satu: jumlah, fee,
    dan realizedPnl dijumlah, harga dirata-rata tertimbang. Satu order =
    satu keputusan bot, jadi harus jadi satu event, bukan belasan.
    """
    out: list[dict] = []
    for f in fills:
        if out and f["order"] and out[-1]["order"] == f["order"]:
            m = out[-1]
            tot = m["amount"] + f["amount"]
            m["price"] = (m["price"] * m["amount"] + f["price"] * f["amount"]) / tot if tot else f["price"]
            m["amount"], m["signed"] = tot, m["signed"] + f["signed"]
            m["fee"] += f["fee"]
            m["pnl"] += f["pnl"]
            m["ts"] = max(m["ts"], f["ts"])
            m["ids"].append(f["id"])
        else:
            out.append({**f, "ids": [f["id"]]})
    return out


def _sign(x: float) -> int:
    return 0 if abs(x) < EPS else (1 if x > 0 else -1)


def _clean(x: float) -> float:
    return 0.0 if abs(x) < EPS else round(x, 12)


def _close_event(trip: dict, closed_ts: int) -> dict:
    parts = trip["parts"]
    naik = [p for p in parts if p["kind"] == "buka"]
    turun = [p for p in parts if p["kind"] == "tutup"]
    q_in = sum(p["amount"] for p in naik)
    q_out = sum(p["amount"] for p in turun)
    gross = sum(p["pnl"] for p in parts)
    fees = sum(p["fee"] for p in parts)
    return {
        "type": "CLOSE",
        "side": "long" if trip["dir"] > 0 else "short",
        "max_qty": trip["max_qty"],
        "entry_avg": (sum(p["price"] * p["amount"] for p in naik) / q_in) if q_in else None,
        "exit_avg": (sum(p["price"] * p["amount"] for p in turun) / q_out) if q_out else None,
        "gross": gross, "fees": fees, "net": gross - fees,
        "opened_ts": trip["opened_ts"], "closed_ts": closed_ts,
        "n_fills": len(parts), "complete": trip.get("complete", True),
    }


def apply_fill(state: dict, f: dict) -> list[dict]:
    """
    Terapkan SATU fill ke state {qty, trip}. Kembalikan daftar event.
    qty bertanda: + long, - short. trip menyimpan potongan fill posisi
    yang sedang terbuka, untuk menghitung hasil saat ditutup.

    Fill yang MENYEBERANGI nol (mis. tombol Reverse manual: long 0.01 lalu
    sell 0.03) dipecah: sebagian menutup posisi lama, sisanya membuka
    posisi baru, dengan fee dibagi sebanding jumlahnya. realizedPnl dari
    bursa seluruhnya milik bagian penutup.
    """
    events: list[dict] = []
    q = state["qty"]
    new = _clean(q + f["signed"])
    sq, sn = _sign(q), _sign(new)

    def part(kind, amount, fee, pnl):
        return {"kind": kind, "amount": amount, "price": f["price"], "fee": fee, "pnl": pnl, "ts": f["ts"]}

    if sq == 0:
        if sn == 0:
            return events
        state["trip"] = {"dir": sn, "opened_ts": f["ts"], "max_qty": abs(new), "orders": [f.get("order", "")],
                         "parts": [part("buka", f["amount"], f["fee"], f["pnl"])]}
        events.append({"type": "OPEN", "side": "long" if sn > 0 else "short",
                       "qty": abs(new), "price": f["price"], "ts": f["ts"]})
    elif sn == 0:
        state["trip"]["parts"].append(part("tutup", f["amount"], f["fee"], f["pnl"]))
        events.append(_close_event(state["trip"], f["ts"]))
        state["trip"] = None
    elif sn == sq:
        kind = "buka" if abs(new) > abs(q) else "tutup"
        trip = state["trip"]
        orders = trip.setdefault("orders", [])
        same_order = bool(f.get("order")) and f.get("order") in orders
        if f.get("order") and not same_order:
            orders.append(f["order"])
        trip["parts"].append(part(kind, f["amount"], f["fee"], f["pnl"]))
        trip["max_qty"] = max(trip["max_qty"], abs(new))
        events.append({"type": "SIZE", "side": "long" if sn > 0 else "short", "same_order": same_order,
                       "old_qty": abs(q), "new_qty": abs(new), "price": f["price"], "ts": f["ts"]})
    else:
        frac = abs(q) / f["amount"] if f["amount"] else 1.0
        state["trip"]["parts"].append(part("tutup", f["amount"] * frac, f["fee"] * frac, f["pnl"]))
        events.append(_close_event(state["trip"], f["ts"]))
        rest = f["amount"] - f["amount"] * frac
        state["trip"] = {"dir": sn, "opened_ts": f["ts"], "max_qty": abs(new), "orders": [f.get("order", "")],
                         "parts": [part("buka", rest, f["fee"] * (1 - frac), 0.0)]}
        events.append({"type": "OPEN", "side": "long" if sn > 0 else "short",
                       "qty": abs(new), "price": f["price"], "ts": f["ts"]})
    state["qty"] = new
    return events


def reconstruct_open_trip(fills: list[dict], current_qty: float) -> dict | None:
    """
    Dipakai SEKALI saat notifier pertama aktif dan posisi sudah terbuka:
    telusuri fill MUNDUR dari posisi sekarang sampai posisi kembali nol,
    supaya fee pembuka dan harga masuk trade yang sedang berjalan ikut
    tercatat. Kalau riwayat tidak cukup jauh, trip ditandai tidak lengkap.
    """
    if _sign(current_qty) == 0:
        return None
    urut = sorted(fills, key=lambda f: f["ts"])
    q = current_qty
    start = None
    for i in range(len(urut) - 1, -1, -1):
        before = _clean(q - urut[i]["signed"])
        if _sign(before) != _sign(current_qty):
            start = i
            break
        q = before
    if start is None:
        return {"dir": _sign(current_qty), "opened_ts": urut[0]["ts"] if urut else 0,
                "max_qty": abs(current_qty), "parts": [], "complete": False}
    state = {"qty": _clean(q - urut[start]["signed"]), "trip": None}
    for f in urut[start:]:
        apply_fill(state, f)
    trip = state["trip"]
    if trip is not None:
        trip["complete"] = True
    return trip


# ---------------------------------------------------------------------------
# Alasan penutupan dari log bot di dashboard
# ---------------------------------------------------------------------------

# Alasan penutupan, URUT PRIORITAS. Satu alasan bisa dikenali dari
# beberapa bentuk baris log. Penanda dicocokkan persis (huruf besar-kecil).
#   - DARURAT    : SL gagal terpasang, bot menutup posisi sendiri
#   - STOP LOSS  : SL di bursa kena (fitur --risk-reward)
#   - TAKE PROFIT: TP di bursa kena (--risk-reward) ATAU take profit lama
#                  yang ditutup paksa oleh bot (--take-profit-pct)
#   - REVERSAL   : bot mengirim order penutup karena sinyal berbalik.
#                  Jalur lain di atas juga bisa mencetak baris ini, jadi
#                  prioritasnya paling rendah.
REASON_MARKERS: list[tuple[str, list[tuple[str, ...]]]] = [
    ("PENUTUPAN DARURAT (SL gagal terpasang)", [("[stop-loss]", "DARURAT")]),
    ("STOP LOSS", [("[stop-loss]", "TERTUTUP")]),
    ("TAKE PROFIT", [("[take-profit]", "TUTUP PAKSA"), ("[take-profit]", "TERTUTUP")]),
    ("BOT DIHENTIKAN (posisi ditutup saat bot distop)", [("[stop] percobaan", "tutup posisi")]),
    ("REVERSAL SINYAL", [("[signal] posisi", "reduceOnly")]),
]
REASON_TTL_MS = 10 * 60 * 1000


def resolve_notifier_testnet(env) -> bool:
    """
    Akun yang dipantau notifier. Sumber utama: TRADING_ACCOUNT (sama dengan
    bot & dashboard, dari .env.bot). NOTIF_TESTNET lama tetap didukung,
    tapi kalau keduanya diisi dan BERTENTANGAN, notifier menolak jalan --
    jangan sampai email bilang DEMO padahal yang dipantau akun asli.
    Kalau keduanya kosong: demo (perilaku lama).
    """
    lama = env.get("NOTIF_TESTNET")
    lama_testnet = None if lama in (None, "") else lama.strip() not in ("0", "false", "False", "no")
    akun = (env.get("TRADING_ACCOUNT") or "").strip().lower()
    if akun and akun not in ("demo", "live"):
        raise ValueError(f"TRADING_ACCOUNT harus 'demo' atau 'live' (sekarang {akun!r})")
    akun_testnet = None if not akun else akun == "demo"
    if lama_testnet is not None and akun_testnet is not None and lama_testnet != akun_testnet:
        raise ValueError(f"NOTIF_TESTNET={lama} bertentangan dengan TRADING_ACCOUNT={akun}. Samakan atau "
                         f"hapus NOTIF_TESTNET (TRADING_ACCOUNT sudah cukup).")
    if akun_testnet is not None:
        return akun_testnet
    return True if lama_testnet is None else lama_testnet


def _count(lines: list[str], markers: tuple[str, ...]) -> int:
    return sum(1 for ln in lines if all(m in ln for m in markers))


def _count_reason(lines: list[str], bentuk: list[tuple[str, ...]]) -> int:
    return sum(_count(lines, m) for m in bentuk)


class DashboardLog:
    """
    Membaca log bot dari /api dashboard (bot.log, 200 baris terakhir).
    Log tidak bertimestamp, jadi yang dipantau adalah PERTAMBAHAN jumlah
    baris penanda tiap alasan (lihat REASON_MARKERS) sejak putaran
    sebelumnya. Saat penutupan terdeteksi, alasan dengan prioritas
    tertinggi yang masih segar yang dipakai, lalu semua tanda dihabiskan.
    Tanda kedaluwarsa setelah 10 menit supaya tidak menempel ke penutupan
    yang tidak berhubungan. Kalau jumlahnya MENGECIL (bot di-restart,
    log dikosongkan, atau baris lama tergeser), itu bukan kejadian --
    cukup jadikan patokan baru.
    """

    def __init__(self, url: str, fetch=None):
        self.url = url
        self._fetch = fetch or self._http_get
        self._base: dict[str, int] | None = None  # jumlah baris per alasan, putaran sebelumnya
        self._flags: dict[str, int] = {}  # alasan -> waktu (ms) terakhir terlihat bertambah
        self.reachable = False

    @staticmethod
    def _http_get(url: str) -> dict:
        with urllib.request.urlopen(url, timeout=5) as r:
            return json.loads(r.read().decode())

    def poll(self, now_ms: int) -> None:
        try:
            # from_ms=sekarang: dashboard tidak perlu menyaring riwayat
            # 30 hari untuk permintaan ini -- yang dibutuhkan cuma log bot.
            sep = "&" if "?" in self.url else "?"
            data = self._fetch(f"{self.url}{sep}from_ms={now_ms}")
            lines = list((data.get("bot") or {}).get("log") or [])
            self.reachable = True
        except Exception:
            self.reachable = False
            return
        n = {nama: _count_reason(lines, bentuk) for nama, bentuk in REASON_MARKERS}
        if self._base is not None:
            for nama, jumlah in n.items():
                if jumlah > self._base.get(nama, 0):
                    self._flags[nama] = now_ms
        self._base = n

    def reason(self, now_ms: int) -> str | None:
        """Alasan untuk penutupan yang baru terdeteksi, lalu semua tanda dipakai habis."""
        segar = {k: t for k, t in self._flags.items() if now_ms - t <= REASON_TTL_MS}
        self._flags = {}
        for nama, _ in REASON_MARKERS:
            if nama in segar:
                return nama
        return None


# ---------------------------------------------------------------------------
# Pengirim email Gmail
# ---------------------------------------------------------------------------

class GmailSender:
    """
    SMTP smtp.gmail.com:587 dengan STARTTLS, login memakai App Password
    (16 karakter, butuh 2-Step Verification). Gagal kirim TIDAK boleh
    menghentikan pemantauan -- dicoba ulang beberapa kali lalu dicatat.
    """

    def __init__(self, user: str, app_password: str, to: list[str], *, host="smtp.gmail.com",
                 port=587, retries=3, smtp_factory=smtplib.SMTP, sleep=time.sleep):
        self.user = user.strip()
        self.password = app_password.replace(" ", "").strip()  # Google menampilkannya dengan spasi
        self.to = [t.strip() for t in to if t.strip()]
        self.host, self.port, self.retries = host, port, retries
        self.smtp_factory, self.sleep = smtp_factory, sleep

    def send(self, subject: str, body: str) -> bool:
        msg = EmailMessage()
        msg["From"] = self.user
        msg["To"] = ", ".join(self.to)
        msg["Subject"] = subject
        msg.set_content(body)
        for attempt in range(1, self.retries + 1):
            try:
                with self.smtp_factory(self.host, self.port, timeout=20) as s:
                    s.starttls(context=ssl.create_default_context())
                    s.login(self.user, self.password)
                    s.send_message(msg)
                return True
            except smtplib.SMTPAuthenticationError as e:
                # Salah password tidak akan sembuh dengan dicoba ulang.
                print(f"  [email] DITOLAK Gmail (cek App Password): {e.smtp_code}", flush=True)
                return False
            except Exception as e:
                print(f"  [email] gagal kirim (percobaan {attempt}/{self.retries}): "
                      f"{type(e).__name__}: {e}", flush=True)
                if attempt < self.retries:
                    self.sleep(5 * attempt)
        return False


class PrintSender:
    """Untuk --dry-run: email dicetak ke layar, tidak dikirim."""

    def send(self, subject: str, body: str) -> bool:
        print(f"\n----- EMAIL (dry-run) -----\nSubjek: {subject}\n{body}\n---------------------------", flush=True)
        return True


# ---------------------------------------------------------------------------
# Isi email
# ---------------------------------------------------------------------------

def fmt_ts(ms: int) -> str:
    if not ms:
        return "?"
    return datetime.fromtimestamp(ms / 1000, tz=WIB).strftime("%d %b %Y %H:%M:%S") + " WIB"


def format_event(ev: dict, label: str, symbol: str, leverage: float | None, tp_pct: float | None) -> tuple[str, str]:
    t = ev["type"]
    arah = ev["side"].upper()
    if t == "OPEN":
        subj = f"[{label}] MASUK {arah} {symbol} @ {ev['price']:,.2f}"
        body = (f"Posisi baru dibuka.\n\nSimbol  : {symbol}\nArah    : {arah}\nJumlah  : {ev['qty']}\n"
                f"Harga   : {ev['price']:,.2f}\nWaktu   : {fmt_ts(ev['ts'])}\n")
        if leverage:
            notional = ev["qty"] * ev["price"]
            body += f"Leverage: {leverage:.0f}x (margin sekitar {notional / leverage:,.2f} USDT)\n"
        return subj, body
    if t == "SIZE":
        arah_ubah = "BERTAMBAH" if ev["new_qty"] > ev["old_qty"] else "BERKURANG"
        subj = f"[{label}] UKURAN {arah} {arah_ubah} {ev['old_qty']} -> {ev['new_qty']} {symbol}"
        body = (f"Ukuran posisi berubah tanpa posisi ditutup penuh.\n\nSimbol : {symbol}\n"
                f"Arah   : {arah}\nSebelum: {ev['old_qty']}\nSesudah: {ev['new_qty']}\n"
                f"Harga  : {ev['price']:,.2f}\nWaktu  : {fmt_ts(ev['ts'])}\n\n"
                f"Kalau Anda tidak sengaja menambah posisi, cek apakah ada bot/sesi lain yang "
                f"jalan di akun yang sama.\n")
        return subj, body
    hasil = "UNTUNG" if ev["net"] > 0 else "RUGI"
    alasan = ev.get("reason")
    subj = (f"[{label}] TUTUP {arah}{' (' + alasan + ')' if alasan else ''} -- "
            f"{hasil} {ev['net']:+.4f} USDT {symbol}")
    body = (f"Posisi ditutup.\n\nSimbol       : {symbol}\nArah         : {arah}\n"
            f"Jumlah maks  : {ev['max_qty']}\n")
    if ev["entry_avg"]:
        body += f"Harga masuk  : {ev['entry_avg']:,.2f}\n"
    if ev["exit_avg"]:
        body += f"Harga keluar : {ev['exit_avg']:,.2f}\n"
    body += (f"Dibuka       : {fmt_ts(ev['opened_ts'])}\nDitutup      : {fmt_ts(ev['closed_ts'])}\n\n"
             f"P&L kotor    : {ev['gross']:+.4f} USDT\nFee (buka+tutup): {ev['fees']:.4f} USDT\n"
             f"BERSIH       : {ev['net']:+.4f} USDT\n")
    if leverage and ev["entry_avg"] and ev["max_qty"]:
        margin = ev["max_qty"] * ev["entry_avg"] / leverage
        roi = ev["net"] / margin if margin else 0.0
        body += f"ROI bersih   : {roi:+.2%} terhadap margin ({leverage:.0f}x)\n"
        if tp_pct:
            body += f"Ambang TP bot: {tp_pct:.2%} ROI bersih\n"
    if not ev.get("complete", True):
        body += ("\nCatatan: posisi ini sudah terbuka sebelum notifier aktif dan riwayatnya tidak "
                 "terjangkau, jadi fee pembuka mungkin belum ikut terhitung.\n")
    if alasan:
        body += f"\nAlasan      : {alasan} (dari log bot di dashboard)\n"
    else:
        body += ("\nAlasan      : tidak diketahui. Log bot di dashboard tidak menunjukkan take profit "
                 "maupun order penutup dari bot (dashboard mati, bot tidak dijalankan dari dashboard, "
                 "atau posisi ditutup di luar bot).\n")
    return subj, body


# ---------------------------------------------------------------------------
# Pemantau
# ---------------------------------------------------------------------------

class Notifier:
    def __init__(self, broker, symbol: str, state_path: Path, sender, *, label="DEMO",
                 tp_pct: float | None = None, history_days: int = 7, fetch_trades=None, now_ms=None,
                 dashboard: DashboardLog | None = None):
        self.broker, self.symbol, self.sender = broker, symbol, sender
        self.state_path = Path(state_path)
        self.label, self.tp_pct, self.history_days = label, tp_pct, history_days
        self._now_ms = now_ms or (lambda: int(time.time() * 1000))
        if fetch_trades is None:
            from scripts.dashboard_donchian import fetch_all_trades as fetch_trades
        self._fetch_trades = fetch_trades
        self.dashboard = dashboard
        self.state = self._load()
        self._mismatch = 0

    # -- akses bursa, dibungkam: broker.py di server mencetak banyak baris
    #    [fetch_position] setiap dipanggil; itu dibuang di sini supaya log
    #    notifier tetap terbaca. Bot dan dashboard tidak terpengaruh.
    def _quiet(self, fn, *a, **kw):
        with contextlib.redirect_stdout(io.StringIO()):
            return fn(*a, **kw)

    def _position(self) -> tuple[float, float | None]:
        pos = self._quiet(self.broker.fetch_position, self.symbol)
        if not pos:
            return 0.0, None
        q = abs(_num(pos.get("contracts")))
        return (q if pos.get("side") == "long" else -q), (_num(pos.get("leverage")) or None)

    def _fills_since(self, since_ms: int) -> list[dict]:
        raw = self._quiet(self._fetch_trades, self.broker.exchange, self.symbol, since_ms, self._now_ms())
        return sorted((normalize_fill(t) for t in raw), key=lambda f: f["ts"])

    def _load(self) -> dict | None:
        try:
            return json.loads(self.state_path.read_text())
        except Exception:
            return None

    def _save(self) -> None:
        self.state_path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.state_path.with_suffix(".tmp")
        tmp.write_text(json.dumps(self.state))
        os.replace(tmp, self.state_path)

    def _notify(self, ev: dict, leverage) -> None:
        subj, body = format_event(ev, self.label, self.symbol, leverage or self.state.get("leverage"), self.tp_pct)
        ok = self.sender.send(subj, body)
        print(f"  [notif] {subj} -> {'terkirim' if ok else 'GAGAL'}", flush=True)

    def start(self) -> None:
        """Kalau belum ada state: jangkarkan ke posisi sekarang dan kirim email 'notifier aktif'."""
        if self.state is not None:
            return
        now = self._now_ms()
        qty, lev = self._position()
        trip = None
        if _sign(qty):
            since = now - self.history_days * 24 * 3600 * 1000
            trip = reconstruct_open_trip(self._fills_since(since), qty)
        self.state = {"qty": qty, "trip": trip, "last_ts": now, "seen_at_last_ts": [], "leverage": lev}
        self._save()
        posisi = "KOSONG" if not _sign(qty) else f"{'LONG' if qty > 0 else 'SHORT'} {abs(qty)}"
        self.sender.send(f"[{self.label}] Notifier aktif -- {self.symbol} posisi {posisi}",
                         f"Notifier mulai memantau {self.symbol}.\nPosisi saat ini: {posisi}\n"
                         f"Waktu: {fmt_ts(now)}\n\nEmail berikutnya dikirim saat posisi dibuka, "
                         f"berubah ukuran, atau ditutup.\n")
        print(f"  [notif] aktif, posisi awal {posisi}", flush=True)

    def tick(self) -> list[dict]:
        st = self.state
        now = self._now_ms()
        fills = self._fills_since(st["last_ts"])
        seen = set(st.get("seen_at_last_ts") or [])
        belum = [f for f in fills if not (f["ts"] == st["last_ts"] and f["id"] in seen)]
        siap = [f for f in belum if f["ts"] <= now - SETTLE_MS]
        masih_mengendap = len(siap) < len(belum)
        qty_now, lev = self._position()
        if lev:
            st["leverage"] = lev
        if self.dashboard is not None:
            self.dashboard.poll(now)

        events: list[dict] = []
        for f in merge_by_order(siap):
            events.extend(apply_fill(st, f))
        if siap:
            last = max(f["ts"] for f in siap)
            ids_last = {f["id"] for f in siap if f["ts"] == last}
            if last == st["last_ts"]:
                ids_last |= seen
            st["last_ts"], st["seen_at_last_ts"] = last, sorted(ids_last)

        for ev in events:
            if ev["type"] == "SIZE" and ev.get("same_order"):
                continue  # sisa pecahan order yang sama -- bukan keputusan baru
            if ev["type"] == "CLOSE" and self.dashboard is not None:
                ev["reason"] = self.dashboard.reason(now)
            self._notify(ev, st.get("leverage"))

        if masih_mengendap:
            self._save()
            return events  # posisi bursa sudah maju dari hitungan kita -- jangan dianggap selisih

        # Rekonsiliasi: posisi hasil hitungan fill harus sama dengan posisi
        # di bursa. Beda sesaat wajar (fill baru muncul sedikit terlambat),
        # jadi baru dianggap masalah kalau bertahan dua kali berturut-turut.
        if abs(qty_now - st["qty"]) > EPS:
            self._mismatch += 1
            if self._mismatch >= 2:
                self.sender.send(
                    f"[{self.label}] PERINGATAN: posisi tidak cocok {self.symbol}",
                    f"Hitungan notifier: {st['qty']}\nPosisi di bursa: {qty_now}\n"
                    f"Notifier menyelaraskan diri ke posisi bursa. Event di antaranya mungkin terlewat.\n")
                st["qty"] = qty_now
                st["trip"] = None if not _sign(qty_now) else {
                    "dir": _sign(qty_now), "opened_ts": self._now_ms(), "max_qty": abs(qty_now),
                    "parts": [], "complete": False}
                self._mismatch = 0
        else:
            self._mismatch = 0
        self._save()
        return events


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--test-email", action="store_true", help="kirim satu email uji lalu keluar")
    p.add_argument("--dry-run", action="store_true", help="jalan normal, tapi email dicetak, tidak dikirim")
    args = p.parse_args()

    env = os.environ
    label = env.get("NOTIF_LABEL", "DEMO")
    symbol = env.get("NOTIF_SYMBOL", "BTC/USDT:USDT")
    poll = float(env.get("NOTIF_POLL_SECONDS", "10"))
    tp_pct = float(env["NOTIF_TP_PCT"]) if env.get("NOTIF_TP_PCT") else None
    try:
        testnet = resolve_notifier_testnet(env)
    except ValueError as e:
        raise SystemExit(str(e))
    state_path = Path(env.get("NOTIF_STATE_PATH", f"data/notifier_state_{label.lower()}.json"))

    if args.dry_run:
        sender = PrintSender()
    else:
        missing = [k for k in ("NOTIF_SMTP_USER", "NOTIF_SMTP_APP_PASSWORD", "NOTIF_TO") if not env.get(k)]
        if missing:
            raise SystemExit("Environment variable belum diisi: " + ", ".join(missing))
        sender = GmailSender(env["NOTIF_SMTP_USER"], env["NOTIF_SMTP_APP_PASSWORD"], env["NOTIF_TO"].split(","))

    if args.test_email:
        ok = sender.send(f"[{label}] Uji notifikasi bot Donchian",
                         f"Kalau email ini sampai, pengaturan Gmail sudah benar.\nWaktu: "
                         f"{fmt_ts(int(time.time() * 1000))}\n")
        raise SystemExit(0 if ok else 1)

    from src.execution.broker import Broker
    with contextlib.redirect_stdout(io.StringIO()):
        broker = Broker(exchange_id="binanceusdm", testnet=testnet)

    dash_url = (env.get("NOTIF_DASHBOARD_URL") or "").strip()
    dashboard = DashboardLog(dash_url) if dash_url else None
    n = Notifier(broker, symbol, state_path, sender, label=label, tp_pct=tp_pct, dashboard=dashboard)
    print(f"  Alasan penutupan: {'dari log dashboard ' + dash_url if dash_url else 'tidak dibaca'}", flush=True)
    print(f"=== Notifier Gmail [{label}] {symbol} -- cek tiap {poll:.0f} detik "
          f"({'testnet/demo' if testnet else 'AKUN ASLI'}) ===", flush=True)
    n.start()
    while True:
        try:
            n.tick()
        except Exception as e:
            print(f"  [notif] galat sementara: {type(e).__name__}: {e} -- dicoba lagi", flush=True)
        time.sleep(poll)


if __name__ == "__main__":
    main()