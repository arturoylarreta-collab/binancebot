"""Offline tests for the UniverseSelector scoring (pure functions only)."""

from crypto_scalper.market_data.universe import UniverseSelector, _book_metrics


class TestBookMetrics:
    def test_prices(self):
        m = _book_metrics(
            bids=[(99.0, 1.0), (98.0, 1.0)],
            asks=[(101.0, 1.0), (102.0, 1.0)],
            quote_notional=10_000.0,
        )
        assert m is not None
        assert abs(m["spread_pct"] - 0.02) < 1e-9
        assert m["depth_0001"] == 0.0  # nothing within ±0.1% of mid 100
        assert m["est_slippage_pct"] > 0.0

    def test_thin_book_penalized(self):
        m = _book_metrics(bids=[(99.0, 1.0)], asks=[(101.0, 1.0)], quote_notional=10_000_000.0)
        assert m is not None
        assert m["est_slippage_pct"] == 0.5  # cannot fill the probe order

    def test_invalid_book(self):
        assert _book_metrics(bids=[], asks=[], quote_notional=1.0) is None


class TestRanking:
    def _rows(self):
        return [
            {
                "symbol": "AAAUSDT", "base": "AAA", "quote_volume": 100_000_000.0,
                "volume": 1.0, "price_change_pct": 1.0, "trade_count": 1_000_000,
                "spread_pct": 0.0001, "depth_0001": 5000.0, "est_slippage_pct": 0.0002,
            },
            {
                "symbol": "BBBUSDT", "base": "BBB", "quote_volume": 1_000_000.0,
                "volume": 1.0, "price_change_pct": 1.0, "trade_count": 50_000,
                "spread_pct": 0.01, "depth_0001": 50.0, "est_slippage_pct": 0.05,
            },
        ]

    def test_best_liquidity_ranks_first(self):
        ranked = UniverseSelector.rank(self._rows(), max_symbols=10)
        assert len(ranked) == 2
        assert ranked[0].symbol == "AAAUSDT"
        assert ranked[0].tradability_score >= ranked[1].tradability_score

    def test_max_symbols_cap(self):
        ranked = UniverseSelector.rank(self._rows() * 5, max_symbols=3)
        assert len(ranked) == 3

    def test_empty(self):
        assert UniverseSelector.rank([], max_symbols=10) == []