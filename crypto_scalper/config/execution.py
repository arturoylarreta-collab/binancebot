"""Execution configuration (FASE 6).

Externalized tuning knobs for the Execution Engine: retry policy, timeouts
and simulated slippage. Nothing here is a secret and everything has a safe
offline default (the simulated adapter is the only adapter wired in FASE 6).
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class ExecutionConfig:
    # Submission retry policy (idempotent: same client_order_id is safe).
    retries: int = 2
    backoff_base_s: float = 0.5
    backoff_max_s: float = 5.0

    # How long to wait for an acknowledgement on submit.
    submit_timeout_s: float = 5.0
    # How long to wait for a market order to reach the terminal FILLED state.
    fill_timeout_s: float = 10.0
    # How long to wait for SL+TP confirmation after an entry fill.
    protection_timeout_s: float = 10.0

    # Simulated market impact/spread: fills slip by this fraction of price.
    slippage_pct: float = 0.0005

    # Bounded event stream behind the adapter (one ExecutionReport per event).
    event_queue_size: int = 1000

    # Default risk-pass-through tuning used by the ExecutionRouter.
    default_rr_ratio: float = 2.0
    default_sl_atr_mult: float = 1.5

    # Taker commission used to estimate fees when the venue report carries none
    # (simulator). Realized PnL fed to risk limits is always NET of fees.
    fee_pct: float = 0.0004
    # After a loss streak pauses trading, resume once this long has passed
    # without a new loss (0 = stay paused until restart).
    loss_streak_cooldown_s: float = 1800.0
    # How long a MARKET entry may stay unfilled before it is cancelled.
    entry_fill_timeout_s: float = 5.0

    def __post_init__(self) -> None:
        if self.retries < 0:
            raise ValueError("retries must be >= 0")
        for name in ("submit_timeout_s", "fill_timeout_s", "protection_timeout_s",
                     "entry_fill_timeout_s"):
            if getattr(self, name) <= 0:
                raise ValueError(f"{name} must be > 0")
        if not 0 <= self.slippage_pct < 0.05:
            raise ValueError("slippage_pct must be in [0, 0.05)")
        if not 0 <= self.fee_pct < 0.01:
            raise ValueError("fee_pct must be in [0, 0.01)")
        if self.event_queue_size < 10:
            raise ValueError("event_queue_size must be >= 10")