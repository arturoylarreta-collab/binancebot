"""Typed error hierarchy.

Every module must raise a subclass of CryptoScalperError and let the
system distinguish recoverable / transient / fatal failures.
"""


class CryptoScalperError(Exception):
    """Base for all domain errors."""


# ── Recoverability markers (mixins, can be applied to any error) ──────────────


class Recoverable:
    """Safe to retry after backoff + state resync."""


class Transient:
    """Expected to succeed on retry soon (network, rate limit)."""


class Fatal:
    """Must never be silently swallowed; triggers Kill Switch if critical."""


# ── Configuration ──────────────────────────────────────────────────────────────


class ConfigurationError(CryptoScalperError, Fatal):
    pass


class EnvNotAllowed(ConfigurationError):
    pass


# ── Exchange ───────────────────────────────────────────────────────────────────


class ExchangeError(CryptoScalperError, Recoverable):
    pass


class ExchangeConnectionError(ExchangeError, Transient):
    pass


class ExchangeTimeoutError(ExchangeError, Transient):
    pass


class ExchangeRateLimitError(ExchangeError, Transient):
    pass


class ExchangeAuthenticationError(ExchangeError, Fatal):
    pass


class ExchangeDataIntegrityError(ExchangeError, Fatal):
    pass


class ExchangeOrderError(ExchangeError, Recoverable):
    pass


# ── Market data ────────────────────────────────────────────────────────────────


class DataError(CryptoScalperError):
    pass


class DataIntegrityError(DataError, Fatal):
    pass


class OrderBookGapError(DataError, Recoverable):
    pass


class MarketDataNotReady(DataError, Recoverable):
    pass


class MalformedMessageError(DataError, Recoverable):
    pass


class UniverseEmptyError(DataError, Recoverable):
    pass


# ── Risk ───────────────────────────────────────────────────────────────────────


class RiskError(CryptoScalperError, Fatal):
    pass


class RiskLimitBreached(RiskError):
    pass


class TradingHalted(RiskError):
    pass


class SafeModeActive(RiskError):
    pass


class KillSwitchActive(RiskError):
    pass


# ── Execution (used from FASE 6 onward) ───────────────────────────────────────


class ExecutionError(CryptoScalperError):
    pass


class OrderRejectedError(ExecutionError, Recoverable):
    pass


class OrderTimeoutError(ExecutionError, Recoverable):
    pass


class DuplicateOrderError(ExecutionError):
    pass


class OrderNotFoundError(ExecutionError):
    """The venue has no order with that client_order_id (Binance -2011/-2013)."""


class AlreadyFlatError(ExecutionError):
    """Reduce-only order with nothing to reduce (Binance -2022): position is flat."""


class PositionNotProtectedError(ExecutionError, Fatal):
    pass


class ReconciliationMismatchError(ExecutionError, Fatal):
    pass


class InvalidOrderError(ExecutionError):
    """Validation failure: missing client_order_id, zero qty, etc."""


class ProtectionTimeoutError(ExecutionError, Fatal):
    """SL/TP not confirmed within timeout after entry fill."""