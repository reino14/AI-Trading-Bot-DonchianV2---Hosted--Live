"""
execution/broker.py -- VERSI DENGAN fetch_position() DITAMBAHKAN.

Ini SALINAN broker.py Anda dengan dua penambahan:
1. Broker.fetch_position() -- ambil posisi sungguhan lewat ccxt.
2. MockBroker: pelacakan posisi + fetch_position() -- supaya bisa
   dipakai mereproduksi bug reduceOnly di --mock (fill_immediately=False)
   tanpa perlu koneksi sungguhan.

SEMUA method lain PERSIS SAMA seperti file asli Anda -- cuma dua
penambahan ini, tidak ada yang saya ubah/hapus dari yang sudah ada.
"""

import os
import uuid
from dataclasses import dataclass
from enum import Enum


class OrderStatus(Enum):
    OPEN = "open"
    PARTIALLY_FILLED = "partially_filled"
    FILLED = "filled"
    CANCELED = "canceled"
    REJECTED = "rejected"


@dataclass
class OrderResult:
    client_order_id: str
    exchange_order_id: str | None
    symbol: str
    side: str
    price: float
    amount: float
    filled_amount: float
    status: OrderStatus
    fee: float = 0.0
    average_price: float | None = None

    @property
    def remaining_amount(self) -> float:
        return self.amount - self.filled_amount

    @property
    def is_terminal(self) -> bool:
        return self.status in (OrderStatus.FILLED, OrderStatus.CANCELED, OrderStatus.REJECTED)


def _load_credentials(exchange_id: str, testnet: bool) -> tuple[str, str]:
    mode = "TESTNET" if testnet else "LIVE"
    prefix = f"{exchange_id.upper()}_{mode}"
    key = os.environ.get(f"{prefix}_API_KEY")
    secret = os.environ.get(f"{prefix}_API_SECRET")
    if not key or not secret:
        raise RuntimeError(
            f"Kredensial tidak ditemukan. Set environment variable "
            f"{prefix}_API_KEY dan {prefix}_API_SECRET sebelum menjalankan ini.\n"
            f"JANGAN taruh API key di kode atau config.yaml -- bisa ke-commit ke git."
        )
    return key, secret


_EXCHANGE_OPTIONS: dict[str, dict] = {
    "bybit": {"defaultType": "swap"},
    "binanceusdm": {},
    "binance": {"fetchMarkets": ["spot"]},
}


def _redirect_to_binance_demo(exchange, host_map: dict[str, str]) -> None:
    def _patch(node) -> None:
        if not isinstance(node, dict):
            return
        for key, value in list(node.items()):
            if isinstance(value, str):
                for prod_host, demo_host in host_map.items():
                    if prod_host in value:
                        node[key] = value.replace(prod_host, demo_host)
                        break
            elif isinstance(value, dict):
                _patch(value)

    _patch(exchange.urls.get("api"))


_DEMO_HOST_MAP: dict[str, dict[str, str]] = {
    "binanceusdm": {"fapi.binance.com": "demo-fapi.binance.com"},
    "binance": {
        "api-gcp.binance.com": "demo-api.binance.com",
        "api1.binance.com": "demo-api.binance.com",
        "api2.binance.com": "demo-api.binance.com",
        "api3.binance.com": "demo-api.binance.com",
        "api4.binance.com": "demo-api.binance.com",
        "api.binance.com": "demo-api.binance.com",
    },
}


def _disable_fetch_currencies(exchange) -> None:
    exchange.fetch_currencies = lambda params={}: {}


def _disable_margin_endpoints(exchange) -> None:
    original_fetch = exchange.fetch

    def patched_fetch(url, method="GET", headers=None, body=None):
        if "/margin/" in url:
            return []
        return original_fetch(url, method, headers, body)

    exchange.fetch = patched_fetch


class Broker:
    def __init__(self, exchange_id: str = "binanceusdm", testnet: bool = True):
        import ccxt

        api_key, api_secret = _load_credentials(exchange_id, testnet)

        exchange_class = getattr(ccxt, exchange_id)
        options = _EXCHANGE_OPTIONS.get(exchange_id, {})
        self.exchange = exchange_class(
            {"apiKey": api_key, "secret": api_secret, "enableRateLimit": True, "options": options}
        )
        if testnet:
            if exchange_id in _DEMO_HOST_MAP:
                _redirect_to_binance_demo(self.exchange, _DEMO_HOST_MAP[exchange_id])
                _disable_fetch_currencies(self.exchange)
                if exchange_id == "binance":
                    _disable_margin_endpoints(self.exchange)
            else:
                self.exchange.set_sandbox_mode(True)

        self.testnet = testnet
        self.exchange_id = exchange_id

    def place_limit_order(
        self, symbol: str, side: str, amount: float, price: float,
        client_order_id: str | None = None, reduce_only: bool = False,
    ) -> OrderResult:
        client_order_id = client_order_id or f"bot-{uuid.uuid4().hex[:16]}"
        params = {"clientOrderId": client_order_id}
        if reduce_only:
            params["reduceOnly"] = True
        raw = self.exchange.create_order(
            symbol=symbol, type="limit", side=side, amount=amount, price=price, params=params,
        )
        return self._normalize_order(raw, client_order_id)

    def cancel_order(self, exchange_order_id: str, symbol: str) -> None:
        self.exchange.cancel_order(exchange_order_id, symbol)

    def fetch_order_by_client_id(self, client_order_id: str, symbol: str) -> OrderResult | None:
        try:
            open_orders = self.exchange.fetch_open_orders(symbol)
            for o in open_orders:
                if o.get("clientOrderId") == client_order_id:
                    return self._normalize_order(o, client_order_id)
            closed_orders = self.exchange.fetch_closed_orders(symbol, limit=50)
            for o in closed_orders:
                if o.get("clientOrderId") == client_order_id:
                    return self._normalize_order(o, client_order_id)
        except Exception:
            raise
        return None

    def fetch_open_orders(self, symbol: str) -> list[OrderResult]:
        raw_orders = self.exchange.fetch_open_orders(symbol)
        return [self._normalize_order(o, o.get("clientOrderId", o.get("id", ""))) for o in raw_orders]

    def fetch_position(self, symbol: str) -> dict | None:
        """
        Ambil posisi SUNGGUHAN saat ini dari bursa (khusus futures --
        spot tidak punya konsep "posisi" sama sekali, cuma saldo
        wallet). Dipakai untuk REKONSILIASI: kalau sebuah order tidak
        langsung "filled" saat dikirim (limit order pasif yang
        nangkring dulu di order book), program TIDAK BOLEH menebak
        apakah dia akhirnya terisi belakangan -- cek balik ke posisi
        SUNGGUHAN di bursa, bukan asumsi dari status sesaat kirim.

        JUGA kembalikan leverage -- dipakai untuk take-profit berbasis
        ROI/margin (bukan cuma pergerakan harga mentah). ccxt normalnya
        sudah punya field "leverage" di objek posisi unified-nya.
        BELUM DIVERIFIKASI di lapangan (saya tidak punya akses testnet
        sungguhan) -- kalau field ini ternyata None/tidak ada, kita
        FALLBACK ke leverage=1.0 (paling AMAN: bikin ambang take-profit
        JADI LEBIH SULIT tercapai, bukan lebih gampang -- salah ke arah
        yang tidak merugikan kalau asumsinya keliru).

        Return None kalau tidak ada posisi terbuka (flat) untuk symbol
        ini. Kalau ada: {"side": "long"|"short", "contracts": float,
        "leverage": float}.
        """
        positions = self.exchange.fetch_positions([symbol])
        for p in positions:
            contracts = p.get("contracts") or 0.0
            if contracts and abs(contracts) > 0:
                leverage = p.get("leverage")
                info = p.get("info") or {}

                if leverage is None:
                    raw_leverage = info.get("leverage")
                    if raw_leverage is not None:
                        leverage = float(raw_leverage)
                        print(f"  [fetch_position] field unified 'leverage' kosong, tapi ketemu "
                              f"di respons mentah bursa (info.leverage={raw_leverage}) -- dipakai itu.")

                if leverage is None:
                    # Binance Demo Trading TERNYATA tidak kirim field
                    # "leverage" di endpoint positionRisk SAMA SEKALI
                    # (dikonfirmasi lapangan) -- tapi leverage bisa
                    # DIHITUNG dari dua angka yang memang ADA: notional
                    # (nilai posisi) / initialMargin (margin yang
                    # dipakai). Ini definisi matematis leverage itu
                    # sendiri (notional_exposure / margin_posted),
                    # bukan trik spesifik Binance -- diverifikasi cocok
                    # persis 20.0000x di data lapangan Anda.
                    try:
                        notional = float(info.get("notional", 0) or 0)
                        initial_margin = float(info.get("initialMargin", 0) or 0)
                        if initial_margin > 0:
                            leverage = abs(notional) / initial_margin
                            print(f"  [fetch_position] leverage dihitung dari notional/initialMargin "
                                  f"= {leverage:.2f}x (field langsung tidak tersedia dari bursa).")
                    except (TypeError, ValueError):
                        pass

                if leverage is None:
                    print(f"  [fetch_position] PERINGATAN: leverage tidak bisa ditentukan sama sekali "
                          f"(field langsung maupun turunan notional/margin) untuk {symbol} -- pakai "
                          f"fallback 1.0 (take-profit berbasis ROI akan butuh pergerakan harga lebih "
                          f"besar dari seharusnya).")
                    leverage = 1.0

                # entry_price -- untuk PEMULIHAN posisi lama saat program
                # restart (lihat PaperRunner._recover_position_from_exchange).
                # Sama pola fallback seperti leverage: field unified dulu,
                # kalau kosong coba respons mentah.
                entry_price = p.get("entryPrice")
                if entry_price is None:
                    raw_entry = info.get("entryPrice")
                    entry_price = float(raw_entry) if raw_entry is not None else None
                else:
                    entry_price = float(entry_price)

                return {
                    "side": p.get("side"), "contracts": abs(contracts),
                    "leverage": float(leverage), "entry_price": entry_price,
                }
        return None

    def place_take_profit_market(self, symbol: str, position_side: str, stop_price: float) -> str:
        """
        Titipkan order TAKE_PROFIT_MARKET ke BURSA -- begitu harga
        menyentuh stop_price, yang mengeksekusi adalah mesin matching
        Binance, BUKAN bot ini. Tidak ada polling, tidak ada jendela
        buta antar-cek, dan TETAP jalan meski program ini mati atau
        koneksinya putus.

        position_side: "long"/"short" -- arah posisi yang sedang
            dipegang (BUKAN sisi order penutupnya). Sisi ordernya
            diturunkan di sini supaya caller tidak perlu ingat
            membalik sendiri.

        closePosition=True: menutup SELURUH posisi berapa pun
            besarnya, jadi `amount` tidak perlu dihitung dan tidak bisa
            salah hitung. Binance juga otomatis membatalkan order
            seperti ini begitu posisi jadi nol -- PERILAKU INI BELUM
            SAYA VERIFIKASI di akun Demo Anda, wajib dicek langsung:
            buka posisi, tutup manual lewat sinyal, lalu lihat apakah
            order kondisionalnya benar-benar hilang dari open orders.

        workingType=CONTRACT_PRICE: pakai harga LAST, sama dengan yang
            dipakai fetch_current_price() -- supaya ambang di bursa
            konsisten dengan perhitungan internal bot. Default Binance
            adalah MARK_PRICE yang bisa beda beberapa puluh dolar dari
            last, artinya TP bisa terpicu di ROI yang bukan Anda minta.

        Return: id order di bursa -- disimpan caller supaya bisa
        dibatalkan manual kalau perlu.
        """
        side = "sell" if position_side == "long" else "buy"
        order = self.exchange.create_order(
            symbol, type="TAKE_PROFIT_MARKET", side=side, amount=None,
            params={"stopPrice": stop_price, "closePosition": True,
                    "workingType": "CONTRACT_PRICE"},
        )
        return order["id"]

    def fetch_trading_fee_pct(self, symbol: str) -> float | None:
        """
        Ambil taker fee SATU SISI (bukan bolak-balik) untuk symbol ini,
        LANGSUNG dari akun Anda -- bukan angka tebakan/generik. Dipakai
        supaya take-profit bisa memperhitungkan fee SUNGGUHAN, bukan
        estimasi kasar. Return None kalau bursa tidak bisa memberikannya
        (caller yang memutuskan fallback, BUKAN ditebak di sini).
        """
        try:
            info = self.exchange.fetch_trading_fee(symbol)
            taker = info.get("taker")
            return float(taker) if taker is not None else None
        except Exception as e:
            print(f"  [fetch_trading_fee_pct] gagal ambil fee dari bursa ({e})")
            return None

    def fetch_current_price(self, symbol: str) -> float:
        """
        Harga TERKINI (last trade) -- BUKAN dari candle, TIDAK menunggu
        candle tutup. Dipakai untuk pemantauan real-time (mis.
        take-profit yang bereaksi dalam hitungan detik, bukan cuma
        sekali per candle).
        """
        ticker = self.exchange.fetch_ticker(symbol)
        return float(ticker["last"])

    def fetch_balance(self) -> dict:
        return self.exchange.fetch_balance()

    def fetch_free_balance(self, asset: str) -> float:
        balance = self.fetch_balance()
        return float(balance.get(asset, {}).get("free", 0.0) or 0.0)

    def fetch_recent_bars(self, symbol: str, timeframe: str, limit: int = 100) -> list[dict]:
        raw_bars = self.exchange.fetch_ohlcv(symbol, timeframe=timeframe, limit=limit)
        return [
            {"timestamp": b[0], "open": b[1], "high": b[2], "low": b[3], "close": b[4], "volume": b[5]}
            for b in raw_bars
        ]

    @staticmethod
    def _normalize_order(raw: dict, client_order_id: str) -> OrderResult:
        filled = raw.get("filled", 0.0) or 0.0
        amount = raw.get("amount", 0.0) or 0.0
        raw_status = raw.get("status", "open")

        if raw_status in ("rejected", "expired"):
            status = OrderStatus.REJECTED
        elif raw_status == "canceled":
            status = OrderStatus.CANCELED
        elif filled > 0 and filled >= amount:
            status = OrderStatus.FILLED
        elif filled > 0:
            status = OrderStatus.PARTIALLY_FILLED
        elif raw_status == "closed":
            status = OrderStatus.CANCELED
        else:
            status = OrderStatus.OPEN

        fee = 0.0
        fee_info = raw.get("fee")
        if fee_info and isinstance(fee_info, dict):
            fee = fee_info.get("cost", 0.0) or 0.0

        average = raw.get("average")

        return OrderResult(
            client_order_id=client_order_id, exchange_order_id=raw.get("id"),
            symbol=raw.get("symbol", ""), side=raw.get("side", ""), price=raw.get("price", 0.0) or 0.0,
            amount=amount, filled_amount=filled, status=status, fee=fee, average_price=average,
        )


class MockBroker:
    """
    DITAMBAH pelacakan posisi (self._positions) + fetch_position() --
    sebelumnya MockBroker cuma lacak saldo (dibangun untuk uji alur
    SPOT), belum pernah lacak posisi futures. Ditambahkan supaya
    interface-nya BENAR-BENAR simetris dengan Broker sungguhan, dan
    supaya bug reduceOnly-terlambat-fill bisa direproduksi di --mock
    (fill_immediately=False) tanpa koneksi sungguhan sama sekali.
    """

    def __init__(self, fill_immediately: bool = True, leverage: float = 1.0, historical_bars: list[dict] | None = None,
                 taker_fee_pct: float | None = None):
        self._orders: dict[str, OrderResult] = {}
        self._network_up = True
        self.fill_immediately = fill_immediately
        self._balances: dict[str, float] = {"USDT": 10_000.0}
        # side: "long"/"short"/None (flat), contracts: besaran posisi absolut.
        self._positions: dict[str, dict] = {}
        self.leverage = leverage  # SATU nilai untuk semua symbol -- proyek ini trading satu simbol per sesi
        self._historical_bars = historical_bars or []  # untuk uji fetch_recent_bars()/backfill
        self.taker_fee_pct = taker_fee_pct  # untuk uji fetch_trading_fee_pct()
        self._current_price: float | None = None  # untuk uji fetch_current_price()
        self._take_profit_orders: dict[str, dict] = {}  # untuk uji place_take_profit_market()

    def set_current_price(self, price: float) -> None:
        """Bantuan uji -- set harga yang akan dikembalikan fetch_current_price() berikutnya."""
        self._current_price = price

    def fetch_current_price(self, symbol: str) -> float:
        """Padanan Broker.fetch_current_price() -- kembalikan harga yang di-set lewat set_current_price()."""
        self._check_network()
        if self._current_price is None:
            raise ValueError("MockBroker: fetch_current_price dipanggil tapi belum pernah di-set "
                              "lewat set_current_price() -- ini uji, bukan bursa sungguhan.")
        return self._current_price

    def fetch_trading_fee_pct(self, symbol: str) -> float | None:
        """Padanan Broker.fetch_trading_fee_pct() -- kembalikan nilai yang di-set lewat konstruktor."""
        self._check_network()
        return self.taker_fee_pct

    def fetch_recent_bars(self, symbol: str, timeframe: str, limit: int = 100) -> list[dict]:
        """Padanan Broker.fetch_recent_bars() -- kembalikan bar tiruan yang di-set lewat konstruktor."""
        self._check_network()
        return self._historical_bars[-limit:]

    def place_take_profit_market(self, symbol: str, position_side: str, stop_price: float) -> str:
        """
        Padanan Broker.place_take_profit_market() -- WAJIB ada di sini
        juga, kalau tidak jalur --mock akan AttributeError begitu
        posisi dibuka dengan take-profit aktif. Cuma MENCATAT, tidak
        mensimulasikan pemicuannya (MockBroker tidak punya konsep
        "harga bergerak sendiri") -- cukup untuk memverifikasi bahwa
        pipa pemasangannya jalan dan harga triggernya benar.
        """
        self._check_network()
        order_id = f"mock-tp-{uuid.uuid4().hex[:12]}"
        self._take_profit_orders[order_id] = {
            "symbol": symbol, "position_side": position_side, "stop_price": stop_price,
        }
        return order_id

    def simulate_network_down(self) -> None:
        self._network_up = False

    def simulate_network_up(self) -> None:
        self._network_up = True

    def _check_network(self) -> None:
        if not self._network_up:
            raise ConnectionError("MockBroker: simulasi jaringan sedang putus")

    def _apply_fill_to_position(self, symbol: str, side: str, amount: float, price: float) -> None:
        """
        Perbarui posisi tersimulasi setelah SATU fill -- dipanggil dari
        place_limit_order() (fill_immediately=True) DAN
        simulate_delayed_fill() (fill_immediately=False, lihat di
        bawah) supaya keduanya konsisten pakai logika yang SAMA.

        entry_price: PENYEDERHANAAN untuk uji -- dicatat dari harga
        fill PERTAMA yang membuka posisi baru dari flat (atau membalik
        arah), TIDAK dirata-ratakan kalau ada fill tambahan ke arah
        yang SAMA sesudahnya (beda dari bursa sungguhan yang hitung
        rata-rata tertimbang). Cukup untuk uji skenario "satu posisi,
        satu entry" yang dipakai proyek ini.
        """
        current = self._positions.get(symbol, {"side": None, "contracts": 0.0, "entry_price": None})
        was_long, was_short = current["side"] == "long", current["side"] == "short"
        signed = current["contracts"] if was_long else -current["contracts"] if was_short else 0.0
        signed += amount if side == "buy" else -amount

        if abs(signed) < 1e-12:
            self._positions[symbol] = {"side": None, "contracts": 0.0, "entry_price": None}
        elif signed > 0:
            entry = price if not was_long else current["entry_price"]
            self._positions[symbol] = {"side": "long", "contracts": signed, "entry_price": entry}
        else:
            entry = price if not was_short else current["entry_price"]
            self._positions[symbol] = {"side": "short", "contracts": abs(signed), "entry_price": entry}

    def place_limit_order(
        self, symbol: str, side: str, amount: float, price: float,
        client_order_id: str | None = None, reduce_only: bool = False,
    ) -> OrderResult:
        self._check_network()
        client_order_id = client_order_id or f"bot-{uuid.uuid4().hex[:16]}"

        if client_order_id in self._orders:
            raise ValueError(
                f"MockBroker: client_order_id '{client_order_id}' sudah pernah "
                f"dipakai -- bursa sungguhan akan menolak order duplikat begini."
            )

        filled = amount if self.fill_immediately else 0.0
        status = OrderStatus.FILLED if self.fill_immediately else OrderStatus.OPEN
        fee = round(amount * price * 0.0005, 6)

        if self.fill_immediately:
            base, _, quote = symbol.partition("/")
            self._balances.setdefault(base, 0.0)
            self._balances.setdefault(quote, 0.0)
            if side == "buy":
                self._balances[quote] -= amount * price
                self._balances[base] += amount - (fee / price if price else 0.0)
            else:
                self._balances[base] -= amount
                self._balances[quote] += amount * price - fee
            self._apply_fill_to_position(symbol, side, amount, price)

        result = OrderResult(
            client_order_id=client_order_id, exchange_order_id=f"mock-{uuid.uuid4().hex[:12]}",
            symbol=symbol, side=side, price=price, amount=amount, filled_amount=filled,
            status=status, fee=fee, average_price=price if self.fill_immediately else None,
        )
        self._orders[client_order_id] = result
        return result

    def simulate_delayed_fill(self, client_order_id: str) -> None:
        """
        Bantuan uji BARU: paksa order yang tadinya OPEN (fill_immediately=False)
        jadi FILLED belakangan -- persis skenario maker order yang
        nangkring dulu baru terisi setelah beberapa bar, seperti yang
        terjadi di kasus reduceOnly rejected Anda. Posisi ikut
        diperbarui, PERSIS seperti place_limit_order() yang fill
        langsung -- supaya reproduksi bug ini konsisten.
        """
        o = self._orders.get(client_order_id)
        if o is None or o.is_terminal:
            return
        o.filled_amount = o.amount
        o.status = OrderStatus.FILLED
        o.average_price = o.price
        self._apply_fill_to_position(o.symbol, o.side, o.amount, o.price)

    def cancel_order(self, exchange_order_id: str, symbol: str) -> None:
        self._check_network()
        for o in self._orders.values():
            if o.exchange_order_id == exchange_order_id:
                o.status = OrderStatus.CANCELED
                return

    def fetch_order_by_client_id(self, client_order_id: str, symbol: str) -> OrderResult | None:
        self._check_network()
        return self._orders.get(client_order_id)

    def fetch_open_orders(self, symbol: str) -> list[OrderResult]:
        self._check_network()
        return [o for o in self._orders.values() if not o.is_terminal and o.symbol == symbol]

    def fetch_position(self, symbol: str) -> dict | None:
        """Padanan Broker.fetch_position() -- lihat docstring di sana."""
        self._check_network()
        pos = self._positions.get(symbol)
        if pos is None or pos["side"] is None:
            return None
        return {
            "side": pos["side"], "contracts": pos["contracts"],
            "leverage": self.leverage, "entry_price": pos.get("entry_price"),
        }

    def fetch_balance(self) -> dict:
        self._check_network()
        return {asset: {"free": amt, "used": 0.0, "total": amt} for asset, amt in self._balances.items()}

    def fetch_free_balance(self, asset: str) -> float:
        self._check_network()
        return self._balances.get(asset, 0.0)

    def simulate_partial_fill(self, client_order_id: str, filled_amount: float) -> None:
        o = self._orders.get(client_order_id)
        if o:
            o.filled_amount = filled_amount
            o.status = OrderStatus.PARTIALLY_FILLED if filled_amount < o.amount else OrderStatus.FILLED
            if filled_amount > 0 and o.average_price is None:
                o.average_price = o.price