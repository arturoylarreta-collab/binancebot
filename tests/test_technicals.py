"""Offline tests for feature/indicator math.

No network. Pure functions only.
"""

import numpy as np
import pytest

from crypto_scalper.features import technicals as t


class TestEMA:
    def test_sma_seeded(self):
        # seed = mean([1,2,3]) = 2, alpha = 0.5
        assert t.ema(np.array([1.0, 2.0, 3.0, 4.0, 5.0]), 3) == 4.0

    def test_constant(self):
        assert t.ema(np.array([7.0] * 100), 9) == pytest.approx(7.0, abs=1e-9)

    def test_short_series(self):
        assert t.ema(np.array([1.0]), 14) == 1.0


class TestRSI:
    def test_all_up_is_100(self):
        closes = np.arange(1.0, 40.0)
        assert t.rsi(closes, 14) == 100.0

    def test_all_down_is_0(self):
        closes = np.arange(40.0, 1.0, -1.0)
        assert t.rsi(closes, 14) == 0.0

    def test_flat_is_50(self):
        closes = np.array([5.0] * 40)
        assert t.rsi(closes, 14) == 50.0

    def test_symmetric_avg_is_50(self):
        # alternating equal up/down moves -> RSI ~50
        closes = np.array([100.0, 101.0, 100.0, 101.0, 100.0, 101.0] * 10)
        val = t.rsi(closes, 14)
        assert 45.0 <= val <= 55.0

    def test_insufficient_data(self):
        assert np.isnan(t.rsi(np.array([1.0, 2.0, 3.0]), 14))


class TestATR:
    def test_constant_truerange(self):
        high = np.arange(2.0, 31.0)   # H = low + 1, and close = high, so the
        low = high - 1.0              # previous close sits exactly on the next
        close = high                  # low -> TR == 1 for every candle
        assert t.atr(high, low, close, 14) == pytest.approx(1.0, abs=1e-9)

    def test_insufficient_data(self):
        high = np.array([1.0, 2.0])
        low = np.array([1.0, 2.0])
        close = np.array([1.0, 2.0])
        assert np.isnan(t.atr(high, low, close, 14))


class TestADX:
    def test_monotone_up_is_100(self):
        n = 40
        high = np.arange(1.0, n + 1.0) + 0.5
        low = np.arange(1.0, n + 1.0) - 0.5
        close = np.arange(1.0, n + 1.0)
        assert t.adx(high, low, close, 14) == pytest.approx(100.0, abs=1e-6)

    def test_flat_is_low(self):
        n = 60
        high = np.full(n, 100.5)
        low = np.full(n, 99.5)
        close = np.full(n, 100.0)
        assert t.adx(high, low, close, 14) < 2.0


class TestVWAP:
    def test_session_vwap_one_session(self):
        ts = np.array([0.0, 1000.0, 2000.0])
        typical = np.array([10.0, 10.0, 20.0])
        volume = np.array([1.0, 1.0, 2.0])
        # (10 + 10 + 40) / 4 = 15
        assert t.session_vwap(ts, typical, volume) == 15.0

    def test_session_vwap_resets_on_utc_day(self):
        day_ms = 86_400_000
        ts = np.array([0.0, 1000.0, day_ms + 1000.0, day_ms + 2000.0])
        typical = np.array([10.0, 10.0, 100.0, 100.0])
        volume = np.array([1.0, 1.0, 1.0, 1.0])
        # current session (day 1) -> (100 + 100) / 2 = 100
        assert t.session_vwap(ts, typical, volume) == 100.0

    def test_rolling_vwap(self):
        ts = np.array([0.0, 1000.0, 2000.0, 61_000.0, 62_000.0])
        typical = np.array([10.0, 10.0, 10.0, 20.0, 20.0])
        volume = np.array([1.0, 1.0, 1.0, 1.0, 1.0])
        # trailing 30s -> only the last two samples (older ones outside window)
        assert t.rolling_vwap(ts, typical, volume, window_s=30) == 20.0


class TestBollingerROC:
    def test_zero_vol_bollinger(self):
        closes = np.array([5.0] * 30)
        upper, mid, lower = t.bollinger(closes, 20, 2.0)
        assert (upper, mid, lower) == (5.0, 5.0, 5.0)

    def test_roc(self):
        closes = np.array([10.0, 11.0, 12.0, 13.0])
        assert t.roc(closes, 2) == pytest.approx(13.0 / 11.0 - 1.0)