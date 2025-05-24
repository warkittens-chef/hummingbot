from unittest import TestCase
from unittest.mock import patch

from sqlalchemy import create_engine

from hummingbot.client.config.client_config_map import ClientConfigMap
from hummingbot.client.config.config_helpers import ClientConfigAdapter
from hummingbot.model.funding_trade import FundingTrade
from hummingbot.model.sql_connection_manager import SQLConnectionType, SQLConnectionManager


class FundingTradeTests(TestCase):

    @patch("hummingbot.model.sql_connection_manager.create_engine")
    def setUp(self, engine_mock) -> None:
        super().setUp()
        self.display_name = "test_market"
        self.config_file_path = "test_config"
        self.strategy_name = "test_strategy"

        engine_mock.return_value = create_engine("sqlite:///:memory:")
        self.manager = SQLConnectionManager(
            ClientConfigAdapter(ClientConfigMap()), SQLConnectionType.TRADE_FILLS, db_name="test_DB"
        )

    def test_find_funding_trade_with_long_match(self):
        # Arrange
        funding_dict = {
            "id": "test_id",
            "controller_id": "test_controller_id",
            "start_time": 1000.0,
            "end_time": 3000.0,
            "long_market": "binance",
            "long_pair": "BTC-USDT",
            "short_market": "kucoin",
            "short_pair": "BTC-USDT"
        }
        new_funding_trade = FundingTrade(**funding_dict)

        with self.manager.get_new_session() as session:
            with session.begin():
                session.add(new_funding_trade)

        # Act
        result = FundingTrade.find_funding_trade(
            sql_session=self.manager.get_new_session(),
            timestamp=2000.0,
            market="binance",
            trading_pair="BTC-USDT"
        )

        # Assert
        self.assertEqual(result.id, funding_dict["id"])

    def test_find_funding_trade_with_short_match(self):
        # Arrange
        funding_dict = {
            "id": "test_id",
            "controller_id": "test_controller_id",
            "start_time": 1000.0,
            "end_time": 3000.0,
            "long_market": "binance",
            "long_pair": "BTC-USDT",
            "short_market": "kucoin",
            "short_pair": "BTC-USDT"
        }
        new_funding_trade = FundingTrade(**funding_dict)

        with self.manager.get_new_session() as session:
            with session.begin():
                session.add(new_funding_trade)

        # Act
        result = FundingTrade.find_funding_trade(
            sql_session=self.manager.get_new_session(),
            timestamp=2000.0,
            market="kucoin",
            trading_pair="BTC-USDT"
        )

        # Assert
        self.assertEqual(result.id, funding_dict["id"])

    def test_find_funding_trade_no_end_time(self):
        # Arrange
        funding_dict = {
            "id": "test_id",
            "controller_id": "test_controller_id",
            "start_time": 1000.0,
            "long_market": "binance",
            "long_pair": "BTC-USDT",
            "short_market": "kucoin",
            "short_pair": "BTC-USDT"
        }
        new_funding_trade = FundingTrade(**funding_dict)

        with self.manager.get_new_session() as session:
            with session.begin():
                session.add(new_funding_trade)

        # Act
        result = FundingTrade.find_funding_trade(
            sql_session=self.manager.get_new_session(),
            timestamp=4000.0,
            market="kucoin",
            trading_pair="BTC-USDT"
        )

        # Assert
        self.assertEqual(result.id, funding_dict["id"])

    def test_find_funding_trade_no_match(self):
        # Arrange
        funding_dict = {
            "id": "test_id",
            "controller_id": "test_controller_id",
            "start_time": 1000.0,
            "end_time": 3000.0,
            "long_market": "binance",
            "long_pair": "BTC-USDT",
            "short_market": "kucoin",
            "short_pair": "BTC-USDT"
        }
        new_funding_trade = FundingTrade(**funding_dict)

        with self.manager.get_new_session() as session:
            with session.begin():
                session.add(new_funding_trade)

        # Act
        result = FundingTrade.find_funding_trade(
            sql_session=self.manager.get_new_session(),
            timestamp=4000.0,
            market="kucoin",
            trading_pair="BTC-USDT"
        )

        # Assert
        self.assertEqual(result, None)

    def test_find_funding_trade_raises_error_on_multiple_matches(self):
        # Arrange
        funding_dict_1 = {
            "id": "test_id_1",
            "controller_id": "test_controller_id",
            "start_time": 1000.0,
            "end_time": 3000.0,
            "long_market": "binance",
            "long_pair": "BTC-USDT",
            "short_market": "kucoin",
            "short_pair": "BTC-USDT"
        }
        new_funding_trade_1 = FundingTrade(**funding_dict_1)

        funding_dict_2 = {
            "id": "test_id_2",
            "controller_id": "test_controller_id",
            "start_time": 1500.0,
            "end_time": 2500.0,
            "short_market": "binance",
            "short_pair": "BTC-USDT",
            "long_market": "hyperliquid",
            "long_pair": "BTC-USDT"
        }
        new_funding_trade_2 = FundingTrade(**funding_dict_2)

        with self.manager.get_new_session() as session:
            with session.begin():
                session.add(new_funding_trade_1)
                session.add(new_funding_trade_2)

        # Act & Assert
        with self.assertRaises(ValueError) as context:
            result = FundingTrade.find_funding_trade(
                sql_session=self.manager.get_new_session(),
                timestamp=2000.0,
                market="binance",
                trading_pair="BTC-USDT"
            )

        self.assertIn(f"Multiple FundingTrade records found for binance BTC-USDT with timestamp 2000.0",
                      str(context.exception))

