"""Offline regressions for Binance-only dashboard fee. No real broker or orders.

Run: python -m scripts.smoke_dashboard_fee
"""
from __future__ import annotations

import unittest
from scripts import dashboard_donchian as dash


class SequenceFeeBroker:
    def __init__(self, values):
        self.values = iter(values)
        self.calls = []

    def fetch_trading_fee_pct(self, symbol):
        self.calls.append(symbol)
        value = next(self.values)
        if isinstance(value, Exception):
            raise value
        return value


class FeeTests(unittest.TestCase):
    def test_failed_rate_has_no_fallback_and_retries(self):
        state = dash.DashboardState('BTC/USDT:USDT', 5, 1, 200, '1m')
        broker = SequenceFeeBroker([None, 0.00037, ConnectionError('synthetic failure')])
        self.assertIsNone(state._fee_taker(broker, state.symbol))
        self.assertEqual(state._fee_taker(broker, state.symbol), 0.00037)
        self.assertIsNone(state._fee_taker(broker, state.symbol))
        self.assertEqual(len(broker.calls), 3)

    def test_invalid_rates_are_unknown_but_zero_is_valid(self):
        state = dash.DashboardState('BTC/USDT:USDT', 5, 1, 200, '1m')
        for raw in (float('nan'), float('inf'), -0.001, 1.0, 'invalid', True):
            with self.subTest(raw=raw):
                self.assertIsNone(state._fee_taker(SequenceFeeBroker([raw]), state.symbol))
        self.assertEqual(state._fee_taker(SequenceFeeBroker([0.0]), state.symbol), 0.0)

    def test_refresh_error_invalidates_old_fee_in_api_snapshot(self):
        from scripts.smoke_dashboard_rr import FakeBroker
        broker = FakeBroker()
        state = dash.DashboardState('BTC/USDT:USDT', 5, 1, 200, '1m')
        state._broker = broker
        state.refresh_once()
        self.assertEqual(state.snapshot['fee_source'], 'binance')
        previous_pnl = state.snapshot['pnl_berjalan']['pnl']
        def unavailable():
            raise ConnectionError('synthetic balance failure')
        broker.fetch_balance = unavailable
        state.refresh_once()
        self.assertEqual(state.snapshot['status'], 'galat')
        self.assertIsNone(state.snapshot['fee_taker'])
        self.assertEqual(state.snapshot['fee_status'], 'gagal')
        self.assertIsNone(state.snapshot['fee_source'])
        self.assertIsNone(state.snapshot['pnl_berjalan']['fee_total'])
        self.assertIsNone(state.snapshot['pnl_berjalan']['bersih_setelah_fee'])
        self.assertEqual(state.snapshot['pnl_berjalan']['pnl'], previous_pnl)

    def test_refresh_failure_preserves_gross_and_hides_estimates(self):
        from scripts.smoke_dashboard_rr import FakeBroker
        broker = FakeBroker()
        rates = SequenceFeeBroker([None, 0.00037, None])
        broker.fetch_trading_fee_pct = rates.fetch_trading_fee_pct
        state = dash.DashboardState('BTC/USDT:USDT', 5, 1, 200, '1m')
        state._broker = broker
        for rate in (None, 0.00037, None):
            state.refresh_once()
            snap = state.snapshot
            self.assertEqual(snap['status'], 'ok', snap.get('last_error'))
            self.assertEqual(snap['fee_taker'], rate)
            self.assertIsNotNone(snap['pnl_berjalan']['pnl'])
            if rate is None:
                self.assertEqual(snap['fee_status'], 'gagal')
                self.assertIsNone(snap['fee_source'])
                self.assertIn('Binance', snap['fee_error'])
                self.assertIsNone(snap['pnl_berjalan']['fee_total'])
                self.assertIsNone(snap['pnl_berjalan']['bersih_setelah_fee'])
            else:
                self.assertEqual(snap['fee_status'], 'ok')
                self.assertEqual(snap['fee_source'], 'binance')
                self.assertIsNone(snap['fee_error'])
                self.assertGreater(snap['pnl_berjalan']['fee_total'], 0)
        self.assertEqual(len(rates.calls), 3, 'Only one fee request per refresh')


if __name__ == '__main__':
    suite = unittest.defaultTestLoader.loadTestsFromTestCase(FeeTests)
    assert suite.countTestCases() > 0
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    raise SystemExit(0 if result.wasSuccessful() else 1)
