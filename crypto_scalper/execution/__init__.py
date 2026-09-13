"""Execution Engine (FASE 6).

ExchangeAdapter abstraction + simulated offline implementation,
OrderManager (idempotent submit/retry/cancel), PositionManager (mandatory
Signal → Risk → Entry → Fill → Protection → Active sequence) and the
ExecutionRouter that wires RiskEngine approval to order placement.

FASE 6 is fully offline: the only adapter shipped is the simulated one.
"""