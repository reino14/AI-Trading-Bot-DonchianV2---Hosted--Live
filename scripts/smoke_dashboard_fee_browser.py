"""Real-browser fee regression with intercepted synthetic API responses only.

Requires optional Playwright + Chromium, e.g.:
  uv run --with playwright python -B -m scripts.smoke_dashboard_fee_browser
No HTTP server, exchange credentials, bot start/stop, or real orders are used.
"""
from __future__ import annotations

import argparse
import copy
import json
from pathlib import Path
import tempfile

from playwright.sync_api import sync_playwright, expect
from scripts.dashboard_donchian import HTML_PAGE


def snapshot(rate):
    qty, entry, price = 0.0012, 80000.0, 81000.0  # Deliberately synthetic fixtures.
    pnl = qty * (price - entry)
    fee = qty * (entry + price) * rate if rate is not None else None
    return {
        'status': 'ok', 'last_update': '2026-01-01T00:00:00+00:00', 'symbol': 'BTC/USDT:USDT',
        'price': price, 'wallet': 1000.0, 'fee_taker': rate,
        'fee_status': 'ok' if rate is not None else 'gagal',
        'fee_source': 'binance' if rate is not None else None,
        'fee_error': None if rate is not None else 'Gagal mengambil tarif fee dari Binance.',
        'account': {'is_live': True, 'label': 'AKUN FIXTURE OFFLINE'},
        'bot': {'running': False}, 'channel': None, 'bracket_orders': [],
        'position': {'side': 'long', 'contracts': qty, 'entry_price': entry, 'leverage': 1},
        'pnl_berjalan': {'pnl': pnl, 'roi_pct': pnl / (entry * qty), 'margin_awal': entry * qty,
            'sumber': 'bursa', 'side': 'long', 'entry': entry, 'leverage': 1,
            'harga_acuan': price, 'acuan': 'mark price', 'gerak_harga_pct': (price-entry)/entry,
            'fee_total': fee, 'bersih_setelah_fee': pnl - fee if fee is not None else None},
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--output', type=Path, default=Path(tempfile.mkdtemp(prefix='fee-browser-')))
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=True)
    current = {'data': snapshot(None), 'transport': 'ok'}
    errors, requests = [], []
    with sync_playwright() as pw:
        options = {'headless': True}
        edge = Path('C:/Program Files (x86)/Microsoft/Edge/Application/msedge.exe')
        if edge.exists():
            options['executable_path'] = str(edge)
        browser = pw.chromium.launch(**options)
        context = browser.new_context(viewport={'width': 1400, 'height': 1000}, service_workers='block')
        def intercept(route):
            request = route.request
            requests.append((request.method, request.url))
            if request.method != 'GET' or not request.url.startswith('http://fee-fixture.invalid/'):
                errors.append('Unexpected request: ' + request.method + ' ' + request.url)
                route.abort()
            elif '/api?' in request.url:
                if current['transport'] == 'abort':
                    route.abort()
                elif current['transport'] == 'http500':
                    route.fulfill(status=500, json=current['data'])
                else:
                    route.fulfill(json=current['data'])
            elif request.url == 'http://fee-fixture.invalid/':
                route.fulfill(content_type='text/html', body=HTML_PAGE)
            else:
                route.fulfill(status=204)
        context.route('**/*', intercept)
        page = context.new_page()
        page.on('pageerror', lambda error: errors.append(str(error)))
        # Keep automatic initial load, disable timers; tests advance refresh explicitly.
        page.add_init_script('window.setInterval = () => 0;')
        page.goto('http://fee-fixture.invalid/')
        preview = page.locator('#ket_rr')
        expect(preview).to_contain_text('Gagal mengambil tarif fee dari Binance')
        assert 'BERSIH' not in preview.inner_text()
        assert '0.050%' not in preview.inner_text()

        def refresh(rate):
            current.update(data=snapshot(rate), transport='ok')
            page.evaluate('muat()')
        refresh(0.00037)
        expect(preview).to_contain_text('0.037%')
        expect(preview).to_contain_text('Binance')
        expect(preview).to_contain_text('BERSIH')
        page.screenshot(path=str(args.output / 'fee-success-fixture.png'), full_page=True)
        refresh(None)
        expect(preview).to_contain_text('Gagal mengambil tarif fee dari Binance')
        assert '0.037%' not in preview.inner_text()
        assert 'BERSIH' not in preview.inner_text()
        expect(page.locator('#isi')).to_contain_text('seperti di Binance')  # Gross PnL remains visible.
        expect(page.locator('#isi')).to_contain_text('Estimasi bersih tidak tersedia')
        page.screenshot(path=str(args.output / 'fee-failure-fixture.png'), full_page=True)
        refresh(0.0)
        expect(preview).to_contain_text('0.000%')
        expect(preview).to_contain_text('BERSIH')
        for mode in ('abort', 'http500'):
            refresh(0.00037)
            current['transport'] = mode
            page.evaluate('muat()')
            expect(preview).to_contain_text('Gagal mengambil tarif fee dari Binance')
            expect(page.locator('#isi')).to_contain_text('Estimasi bersih tidak tersedia')
            assert '0.037%' not in preview.inner_text()
        refresh(0.00037)
        current['data'] = {**copy.deepcopy(current['data']), 'status': 'galat', 'last_error': 'synthetic failure'}
        page.evaluate('muat()')
        expect(preview).to_contain_text('Gagal mengambil tarif fee dari Binance')
        assert 'BERSIH' not in preview.inner_text()
        refresh(0.00037)
        expect(preview).to_contain_text('BERSIH')
        assert not errors, errors
        context.close()
        browser.close()
    report = {'passed': True, 'scenarios': ['initial failure', 'success', 'success to failure', 'zero fee',
        'transport failure', 'HTTP error', 'stale failed snapshot', 'recovery'],
        'unexpected_or_page_errors': errors, 'requests': requests, 'data': 'synthetic fixtures, not Binance account data'}
    (args.output / 'browser-results.json').write_text(json.dumps(report, indent=2), encoding='utf-8')
    print(json.dumps(report, indent=2))


if __name__ == '__main__':
    main()
