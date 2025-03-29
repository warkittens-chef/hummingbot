"""
The purpose of this map is to provide a fixed, user-defined filter that should be used by strategies during the step
where the strategy is determining what to analyze and how to analyze it. Right now, this map can be used to:
1. Verify which trading pairs the user has cleared for potentially creating a trade on
2. Apply the risk profile the user has determined for individual trading pairs
3. Any additional static info on trading pairs that for now I haven't figured out how to get dynamically, such as
   individual funding intervals

Future Functionality:
- Make everything about this more dynamic
    - Ping the exchange to see if a trading pair is valid
    - Get funding interval info directly from exchange, possibly via API call
    - Determine volatility rating by getting market data such as OI/daily volume and analyzing it instead of using
      fixed rating


"""

from enum import Enum
from itertools import permutations

from hummingbot.strategy_v2.executors.data_types import ConnectorPair


class VolatilityRating(Enum):
    LOW = 0  # No pair-specific risk management needed
    MEDIUM = 1
    HIGH = 2
    DNU = 3  # Used as override to prevent any controller from using pair


exchange_map = {
    "bybit_perpetual": {
        "ENA": {
            "USDT": {"interval": 60 * 60 * 4, "volatility": VolatilityRating.LOW},
            "USDC": {"interval": 60 * 60 * 8, "volatility": VolatilityRating.LOW},
        },
        "ONDO": {
            "USDT": {"interval": 60 * 60 * 4, "volatility": VolatilityRating.LOW},
            "USDC": {"interval": 60 * 60 * 8, "volatility": VolatilityRating.LOW},
        },
    },
    "hyperliquid_perpetual": {
        "ENA": {"USD": {"interval": 60 * 60 * 1, "volatility": VolatilityRating.LOW}},
        "ONDO": {"USD": {"interval": 60 * 60 * 1, "volatility": VolatilityRating.LOW}},
    },
}


# TODO: memoize this
def get_valid_connector_pairs(
    token: str, connector_names: list[str], quotes: list[str] | None = None
) -> list[ConnectorPair]:
    """
    Returns a complete list of all validated ConnectorPair objects that can be used from the given token and
    additional market info.
    Uses all available quotes if none provided in input.
    """
    cpairs: list[ConnectorPair] = []
    for connector_name, token_map in exchange_map.items():
        if connector_name in connector_names:
            for current_token, quote_map in token_map.items():
                if token == current_token:
                    for quote in quote_map:
                        if not quotes or quote in quotes:
                            cpairs.append(ConnectorPair(connector_name=connector_name, trading_pair=f"{token}-{quote}"))
    return cpairs


# TODO: memoize this
def get_all_valid_trades_for_token(
    token: str, connector_names: list[str], quotes: list[str] | None = None, cross_exchange_only: bool = False
) -> list[tuple[ConnectorPair, ConnectorPair]]:
    """
    Returns a complete list of all validated FundingArbitrageTrade objects that can be executed from the given
    token and additional market info.
    Uses all available quotes if none provided in input.
    """
    trades: list[tuple[ConnectorPair, ConnectorPair]] = []
    for long_pair, short_pair in permutations(get_valid_connector_pairs(token, connector_names, quotes), 2):
        if cross_exchange_only and long_pair.connector_name == short_pair.connector_name:  # Avoid cross-exchange
            continue
        trades.append((long_pair, short_pair))
    return trades
