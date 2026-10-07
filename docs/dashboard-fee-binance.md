# Dashboard live: tarif fee Binance, tanpa asumsi

## Perilaku

- Tarif taker akun/simbol berasal dari `Broker.fetch_trading_fee_pct()` → CCXT `fetch_trading_fee()`. Untuk cabang Binance USD-M yang ditinjau, sumbernya endpoint commission rate akun.
- Dashboard meminta tarif sekali pada setiap siklus refresh backend (bukan setiap request halaman), tanpa cache fallback permanen. Gagal berarti `fee_taker: null`, `fee_status: "gagal"`, `fee_source: null`, dan alasan aman di `fee_error`.
- Berhasil berarti `fee_status: "ok"`, `fee_source: "binance"`. Tarif nol yang benar-benar dikembalikan API tetap valid. Nilai non-numerik, boolean, negatif, infinity, NaN, atau >= 1 ditolak.
- Kegagalan pembacaan fee tidak membuat refresh data lain gagal. PnL kotor tetap dapat tampil; estimasi biaya, profit bersih posisi, dan rasio impas tidak ditampilkan tanpa tarif.
- Nilai fee lama dibuang setelah kegagalan refresh/API/koneksi browser. Refresh berikutnya dapat memulihkan tampilan.
- Nominal fee sebelum transaksi tetap **estimasi yang dihitung dashboard**, bukan commission aktual fill. Fee yang sudah dipotong, funding, diskon, dan perbedaan maker/taker bukan angka yang sama.

## Batas perubahan

Hanya dashboard dan tes/documentasi yang diubah. Strategi Donchian, parameter runner, order, SL/TP, akun, `.env`, runtime `data/`, Caddy, dan systemd tidak diubah. Ini bukan perbaikan fallback biaya di runner atau audit seluruh laporan commission transaksi.

Pemetaan deployment yang diberikan pengguna:

| Lingkungan | Service | Folder server | Backend |
|---|---|---|---|
| Demo (jangan diubah) | `dashboard-donchian.service` | `/home/Trabarei/AI-Trading-Bot---Hosted` | 8100 |
| Live | `donchian-live.service` | `/home/Trabarei/test` | 8200, sesuai override yang disarankan |

URL live: `https://139.190.97.44:8443/`. Status penerapan override/port dan pemulihan server tetap perlu diverifikasi di server; perubahan lokal ini tidak menerapkannya.

## Tes lokal offline

```bash
python -B -m scripts.smoke_dashboard_fee
python -B -m scripts.smoke_dashboard_donchian
python -B -m scripts.smoke_dashboard_rr
```

Tes fee memakai FakeBroker, tidak mengirim order atau mengakses exchange. Test suite lengkap terpilih telah dijalankan pada salinan `.py` saja, lingkungan tanpa credential, tanpa state produksi, dengan jaringan diblokir.

Tes browser opsional (Playwright bukan dependensi produksi):

```bash
uv run --no-project --with playwright python -B -m scripts.smoke_dashboard_fee_browser
```

Tes browser memakai Edge terpasang di Windows atau Chromium Playwright di OS lain. Jika Chromium belum tersedia, pasang browser Playwright di lingkungan pengujian. Seluruh request halaman diintersepsi menjadi fixture; tidak ada HTTP server, Binance, start/stop bot, atau order. Screenshot adalah data sintetis, BUKAN bukti tarif akun asli.

## Publikasi dan deployment manual

Perubahan masih lokal sampai di-commit/push atau disalin secara terkontrol. Berkas yang perlu ikut commit:

- `scripts/dashboard_donchian.py`
- `scripts/smoke_dashboard_fee.py`
- `scripts/smoke_dashboard_fee_browser.py`
- `docs/dashboard-fee-binance.md`

Jangan ikutkan `.env`, credential, maupun file runtime `data/`.

Di server, verifikasi identitas dahulu:

```bash
cd /home/Trabarei/test
pwd
git status --short
sudo systemctl show donchian-live.service --no-pager -p WorkingDirectory -p ExecStart
sudo systemctl status donchian-live.service --no-pager -l
```

**Jangan restart ketika runner live/posisi masih aktif tanpa rencana penanganan posisi.** `--close-on-stop` bisa memicu penutupan posisi saat service dihentikan. Pilih maintenance saat runner berhenti dan posisi/order sudah diperiksa di Binance. Jangan menyentuh service demo.

Setelah commit tersedia di remote yang benar, tidak ada konflik lokal, tidak ada perubahan credential/state di commit yang akan ditarik, dan maintenance aman:

```bash
git pull --ff-only
.venv/bin/python -B -m scripts.smoke_dashboard_fee
sudo systemctl restart donchian-live.service
sudo systemctl status donchian-live.service --no-pager -l
```

Tidak perlu `daemon-reload` atau restart Caddy untuk perubahan kode Python ini. Jangan memakai `git reset --hard` untuk mengatasi konflik.

Verifikasi field fee dari backend live (hanya menampilkan field fee, bukan credential/saldo):

```bash
curl -fsS http://127.0.0.1:8200/api | .venv/bin/python -c 'import json,sys; d=json.load(sys.stdin); print(json.dumps({k:d.get(k) for k in ("status","fee_status","fee_source","fee_taker","fee_error")}, indent=2))'
```

- Sukses: `fee_status=ok`, `fee_source=binance`, `fee_taker` numerik.
- Gagal mengambil tarif: `fee_status=gagal`, `fee_taker=null`; UI menampilkan gagal, tanpa angka cadangan.
- Buka URL live dan hard refresh (`Ctrl+Shift+R`). Tidak perlu menyalakan trading untuk memeriksa tampilan fee.

Belum dilakukan pada server oleh asisten: commit/push, pull, restart, autentikasi Binance, atau verifikasi tarif akun sebenarnya.
