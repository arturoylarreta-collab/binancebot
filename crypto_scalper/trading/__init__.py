"""Paper trading (FASE 7) — simulated account, signals, orchestration.

`trading` implements the offline trading loop exactly like a live bot would,
minus a real venue:
  PaperAccount   simulated balance (cash + fees + equal-and-opposite PnL)
  TradeOrchestrator  Signal → RiskEngine → ExecutionRouter with equity feeding
  PeriodicReconciler internal-vs-venue checks with mitigation + pause
"""

from crypto_scalper.trading.account import PaperAccount
from crypto_scalper.trading.orchestrator import TradeOrchestrator
from crypto_scalper.trading.reconciler import PeriodicReconciler

__all__ = ["PaperAccount", "TradeOrchestrator", "PeriodicReconciler"]