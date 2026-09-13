"""Paper trading wrapper (FASE 7).

`PaperTradingEngine` is the main entry point: it wires the signal engine,
simulated execution stack and periodic reconciliation into a single
`run()` coroutine.  The only external input is an
`AsyncIterator[FeatureSnapshot]` source (or the real pipeline in
`main.py --mode paper`).
"""

from crypto_scalper.paper.engine import PaperTradingEngine

__all__ = ["PaperTradingEngine"]