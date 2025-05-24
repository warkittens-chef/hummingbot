from typing import Any
from unittest import TestCase

from hummingbot.funding_arbitrage.fixed_market_specs import (
    get_valid_connector_pairs,
    get_all_valid_trades_for_token,
    get_market_pair_info,
    VolatilityRating,
    PVPriceType,
)
from hummingbot.strategy_v2.executors.data_types import ConnectorPair


class TestFixedMarketSpecs(TestCase):
    # Valid token returns correct set of ConnectorPair objects for specified exchanges
    def test_get_valid_connector_pairs_returns_correct_pairs(self) -> None:
        # Arrange
        token = "ENA"
        connector_names = ["bybit_perpetual", "hyperliquid_perpetual"]
        quotes = ["USDT", "USD"]

        # Act
        result = get_valid_connector_pairs(token, connector_names, quotes)

        # Assert
        expected = [
            ConnectorPair(connector_name="bybit_perpetual", trading_pair="ENA-USDT"),
            ConnectorPair(connector_name="hyperliquid_perpetual", trading_pair="ENA-USD"),
        ]
        self.assertEqual(result, expected)

    # Token not present in exchange map returns empty set
    def test_get_valid_connector_pairs_returns_empty_for_invalid_token(self) -> None:
        # Arrange
        token = "INVALID_TOKEN"
        connector_names = ["bybit_perpetual", "hyperliquid_perpetual"]
        quotes = ["USDT", "USD"]

        # Act
        result = get_valid_connector_pairs(token, connector_names, quotes)

        # Assert
        self.assertEqual(result, [])

    # Invalid connector names return empty set
    def test_get_valid_connector_pairs_with_invalid_connector_names_returns_empty(self) -> None:
        # Arrange
        token = "ENA"
        connector_names = ["invalid_connector"]
        quotes = ["USDT", "USD"]

        # Act
        result = get_valid_connector_pairs(token, connector_names, quotes)

        # Assert
        expected: list[Any] = []
        self.assertEqual(result, expected)

    # Empty connector names set returns empty set
    def test_get_valid_connector_pairs_with_empty_connector_names(self) -> None:
        # Arrange
        token = "ENA"
        connector_names: list[Any] = []
        quotes = ["USDT", "USD"]

        # Act
        result = get_valid_connector_pairs(token, connector_names, quotes)

        # Assert
        expected: list[Any] = []
        self.assertEqual(result, expected)

    # Non-existent quote currencies return empty set
    def test_get_valid_connector_pairs_with_non_existent_quotes_returns_empty(self) -> None:
        # Arrange
        token = "ENA"
        connector_names = ["bybit_perpetual", "hyperliquid_perpetual"]
        quotes = ["XYZ", "ABC"]  # Non-existent quotes

        # Act
        result = get_valid_connector_pairs(token, connector_names, quotes)

        # Assert
        expected: list[Any] = []
        self.assertEqual(result, expected)

    # Quotes parameter as None includes all available quote pairs
    def test_get_valid_connector_pairs_includes_all_quotes_when_none(self) -> None:
        # Arrange
        token = "ENA"
        connector_names = ["bybit_perpetual", "hyperliquid_perpetual"]
        quotes = None

        # Act
        result = get_valid_connector_pairs(token, connector_names, quotes)

        # Assert
        expected = [
            ConnectorPair(connector_name="bybit_perpetual", trading_pair="ENA-USDT"),
            ConnectorPair(connector_name="bybit_perpetual", trading_pair="ENA-USDC"),
            ConnectorPair(connector_name="hyperliquid_perpetual", trading_pair="ENA-USD"),
        ]
        self.assertEqual(result, expected)

    # Valid token returns correct set of ConnectorPair objects for specified exchanges
    def test_get_valid_connector_pairs_returns_correct_pairs_for_specified_exchanges(self) -> None:
        # Arrange
        token = "ENA"
        connector_names = ["bybit_perpetual", "hyperliquid_perpetual"]
        quotes = ["USDT", "USDC", "USD"]

        # Act
        result = get_valid_connector_pairs(token, connector_names, quotes)

        # Assert
        expected = [
            ConnectorPair(connector_name="bybit_perpetual", trading_pair="ENA-USDT"),
            ConnectorPair(connector_name="bybit_perpetual", trading_pair="ENA-USDC"),
            ConnectorPair(connector_name="hyperliquid_perpetual", trading_pair="ENA-USD"),
        ]
        self.assertEqual(result, expected)

    # Valid token returns correct set of ConnectorPair objects for specified exchanges and quotes
    def test_get_all_valid_trades_for_token_generates_unique_trades(self) -> None:
        # Arrange
        token = "ENA"
        connector_names = ["bybit_perpetual", "hyperliquid_perpetual"]
        quotes = ["USDT", "USD"]

        # Act
        result = get_all_valid_trades_for_token(token, connector_names, quotes)

        # Assert
        expected = [
            (
                ConnectorPair(connector_name="bybit_perpetual", trading_pair="ENA-USDT"),
                ConnectorPair(connector_name="hyperliquid_perpetual", trading_pair="ENA-USD"),
            ),
            (
                ConnectorPair(connector_name="hyperliquid_perpetual", trading_pair="ENA-USD"),
                ConnectorPair(connector_name="bybit_perpetual", trading_pair="ENA-USDT"),
            ),
        ]
        self.assertEqual(len(result), len(expected))
        for trade in expected:
            self.assertIn(trade, result)

    # Valid token returns correct pairs of ConnectorPair objects when cross exchange only is selected
    def test_get_all_valid_trades_for_token_generates_cross_exchange_only_trades(self) -> None:
        # Arrange
        token = "ENA"
        connector_names = ["bybit_perpetual", "hyperliquid_perpetual"]
        quotes = ["USDT", "USDC", "USD"]

        # Act
        result = get_all_valid_trades_for_token(token, connector_names, quotes, cross_exchange_only=True)

        # Assert
        expected = [
            (
                ConnectorPair(connector_name="bybit_perpetual", trading_pair="ENA-USDT"),
                ConnectorPair(connector_name="hyperliquid_perpetual", trading_pair="ENA-USD"),
            ),
            (
                ConnectorPair(connector_name="bybit_perpetual", trading_pair="ENA-USDC"),
                ConnectorPair(connector_name="hyperliquid_perpetual", trading_pair="ENA-USD"),
            ),
            (
                ConnectorPair(connector_name="hyperliquid_perpetual", trading_pair="ENA-USD"),
                ConnectorPair(connector_name="bybit_perpetual", trading_pair="ENA-USDT"),
            ),
            (
                ConnectorPair(connector_name="hyperliquid_perpetual", trading_pair="ENA-USD"),
                ConnectorPair(connector_name="bybit_perpetual", trading_pair="ENA-USDC"),
            ),
        ]
        self.assertEqual(result, expected)

    # Write me a test for get_all_valid_trades_for_token that checks that when cross_exchange_only is False, then the resulting list of ConnectorPair tuples contains all permutations of the provided inputs
    def test_get_all_valid_trades_for_token_includes_all_permutations_when_no_cross_flag(self) -> None:
        # Arrange
        token = "ENA"
        connector_names = ["bybit_perpetual", "hyperliquid_perpetual"]
        quotes = ["USDT", "USDC", "USD"]
        cross_exchange_only = False

        # Act
        result = get_all_valid_trades_for_token(token, connector_names, quotes, cross_exchange_only)

        # Assert
        expected_pairs = [
            (
                ConnectorPair(connector_name="bybit_perpetual", trading_pair="ENA-USDT"),
                ConnectorPair(connector_name="bybit_perpetual", trading_pair="ENA-USDC"),
            ),
            (
                ConnectorPair(connector_name="bybit_perpetual", trading_pair="ENA-USDT"),
                ConnectorPair(connector_name="hyperliquid_perpetual", trading_pair="ENA-USD"),
            ),
            (
                ConnectorPair(connector_name="bybit_perpetual", trading_pair="ENA-USDC"),
                ConnectorPair(connector_name="bybit_perpetual", trading_pair="ENA-USDT"),
            ),
            (
                ConnectorPair(connector_name="bybit_perpetual", trading_pair="ENA-USDC"),
                ConnectorPair(connector_name="hyperliquid_perpetual", trading_pair="ENA-USD"),
            ),
            (
                ConnectorPair(connector_name="hyperliquid_perpetual", trading_pair="ENA-USD"),
                ConnectorPair(connector_name="bybit_perpetual", trading_pair="ENA-USDT"),
            ),
            (
                ConnectorPair(connector_name="hyperliquid_perpetual", trading_pair="ENA-USD"),
                ConnectorPair(connector_name="bybit_perpetual", trading_pair="ENA-USDC"),
            ),
        ]
        self.assertEqual(len(result), len(expected_pairs))
        for pair in expected_pairs:
            self.assertIn(pair, result)

    def test_get_market_pair_info_returns_correct_info(self):
        # Arrange
        market = "bybit_perpetual"
        base = "ENA"
        quote = "USDT"

        # Act
        result = get_market_pair_info(market, base, quote)

        # Assert
        self.assertFalse(result is None)
        # self.assertEqual(result.interval, 60 * 60 * 4) interval might change in future so don't rely on it
        self.assertEqual(result.volatility, VolatilityRating.LOW)
        self.assertEqual(result.price_type, PVPriceType.AVG_ENTRY)

    def test_returns_none_when_market_not_in_exchange_map(self):
        # Arrange
        market = "non_existent_market"
        base = "ENA"
        quote = "USDT"

        # Act
        result = get_market_pair_info(market, base, quote)

        self.assertEqual(None, result)
