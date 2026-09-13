from enum import Enum, auto


class Environment(Enum):
    DEV = auto()
    PAPER = auto()
    LIVE = auto()


class RunMode(Enum):
    MARKET_DATA = auto()
    BACKTEST = auto()
    PAPER = auto()
    LIVE = auto()


class Side(Enum):
    BUY = auto()
    SELL = auto()


class AggressorSide(Enum):
    BUY = auto()      # buyer is the taker (aggTrade m == False)
    SELL = auto()     # maker is the buyer -> seller is the taker (aggTrade m == True)
    UNKNOWN = auto()


class OrderType(Enum):
    MARKET = auto()
    LIMIT = auto()
    STOP_MARKET = auto()
    TAKE_PROFIT_MARKET = auto()


class OrderStatus(Enum):
    NEW = auto()
    PARTIALLY_FILLED = auto()
    FILLED = auto()
    PARTIALLY_FILLED_CANCELED = auto()
    CANCELED = auto()
    REJECTED = auto()
    EXPIRED = auto()
    NEW_INSURANCE = auto()
    NEW_ADL = auto()


class Regime(Enum):
    TRENDING_UP = auto()
    TRENDING_DOWN = auto()
    RANGE = auto()
    HIGH_VOLATILITY = auto()
    LOW_VOLATILITY = auto()
    BREAKOUT = auto()
    EXTREME = auto()
    UNKNOWN = auto()


class Impact(Enum):
    LOW = auto()
    MEDIUM = auto()
    HIGH = auto()


class RiskVerdict(Enum):
    APPROVED = auto()
    REJECTED = auto()
    TRADING_HALTED = auto()
    SAFE_MODE = auto()


class RejectReason(Enum):
    NONE = auto()
    SLIPPAGE_ABOVE_THRESHOLD = auto()
    SPREAD_ABOVE_THRESHOLD = auto()
    LOW_CONFIDENCE = auto()
    RISK_EXPOSURE_EXCEEDED = auto()
    DAILY_LOSS_LIMIT = auto()
    MAX_DRAWDOWN = auto()
    CORRELATION_EXCEEDED = auto()
    INVALID_STATE = auto()
    DATA_NOT_READY = auto()
    KILL_SWITCH = auto()
    OTHER = auto()


class SignalType(Enum):
    LONG = auto()
    SHORT = auto()
    FLAT = auto()


class FeatureCategory(Enum):
    PRICE = auto()
    VOLUME = auto()
    VOLATILITY = auto()
    ORDER_FLOW = auto()
    ORDER_BOOK = auto()
    MOMENTUM = auto()
    TREND = auto()
    SOCIAL = auto()
    NEWS = auto()
    MARKET_REGIME = auto()