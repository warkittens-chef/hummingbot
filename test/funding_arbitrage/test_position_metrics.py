import json
from decimal import Decimal
from unittest import TestCase
from unittest.mock import MagicMock, PropertyMock, patch, Mock

from parameterized import parameterized
from sqlalchemy import create_engine

from hummingbot.client.config.client_config_map import ClientConfigMap
from hummingbot.client.config.config_helpers import ClientConfigAdapter
from hummingbot.funding_arbitrage.position_metrics import PositionMetrics
from hummingbot.model.executors import Executors
from hummingbot.model.sql_connection_manager import SQLConnectionType, SQLConnectionManager
from hummingbot.strategy.script_strategy_base import ScriptStrategyBase
from hummingbot.strategy_v2.executors.arbitrage_executor.arbitrage_executor import ArbitrageExecutor
from hummingbot.strategy_v2.executors.arbitrage_executor.data_types import ArbitrageExecutorConfig
from hummingbot.strategy_v2.executors.data_types import ConnectorPair
from hummingbot.strategy_v2.models.base import RunnableStatus
from hummingbot.strategy_v2.models.executors import TrackedOrder, CloseType


class MarketsRecorderTests(TestCase):
    def print_all_executors(self):
        with self.manager.get_new_session() as session:
            FIVEPLACES = Decimal(10) ** -5
            trim_dec = lambda x: x.quantize(FIVEPLACES) if x else x
            with session.begin():
                executors = session.query(Executors).all()
                for exe in executors:
                    info = exe.to_executor_info()
                    print("===============================================")
                    print(
                        f"buy_market: {info.buy_market}:{info.buy_pair} | size: {trim_dec(info.buy_executed_amount_base)} | price: {trim_dec(info.buy_avg_executed_price)}"
                    )
                    print(
                        f"sell_market: {info.sell_market}:{info.sell_pair} | size: {trim_dec(info.sell_executed_amount_base)} | price: {trim_dec(info.sell_avg_executed_price)}"
                    )
                    print(f"cum_fees: {trim_dec(info.cum_fees_quote)} | net_pnl: {trim_dec(info.net_pnl_quote)}")

    @staticmethod
    def create_mock_strategy():
        market = MagicMock()
        market_info = MagicMock()
        market_info.market = market

        strategy = MagicMock(spec=ScriptStrategyBase)
        type(strategy).market_info = PropertyMock(return_value=market_info)
        type(strategy).trading_pair = PropertyMock(return_value="ETH-USDT")
        strategy.buy.side_effect = ["OID-BUY-1", "OID-BUY-2", "OID-BUY-3"]
        strategy.sell.side_effect = ["OID-SELL-1", "OID-SELL-2", "OID-SELL-3"]
        strategy.cancel.return_value = None
        strategy.connectors = {"binance_perpetual": MagicMock(), "hyperliquid_perpetual": MagicMock()}
        return strategy

    @patch("hummingbot.model.sql_connection_manager.create_engine")
    def setUp(self, engine_mock) -> None:
        super().setUp()
        self.display_name = "test_market"
        self.config_file_path = "test_config"
        self.strategy_name = "test_strategy"
        self.strategy = MarketsRecorderTests.create_mock_strategy()

        engine_mock.return_value = create_engine("sqlite:///:memory:")
        self.manager = SQLConnectionManager(
            ClientConfigAdapter(ClientConfigMap()), SQLConnectionType.TRADE_FILLS, db_name="test_DB"
        )
        self.position_metrics = PositionMetrics(self.manager)

    def get_failed_executor(
        self, close_timestamp: float, buy_side: ConnectorPair, sell_side: ConnectorPair
    ) -> Executors:
        config = ArbitrageExecutorConfig(
            id="123" + str(int(close_timestamp)),
            timestamp=1234,
            controller_id="test",
            buying_market=buy_side,
            selling_market=sell_side,
            order_amount=Decimal("50"),
            min_profitability=Decimal("0.1"),
        )
        executor = ArbitrageExecutor(strategy=self.strategy, config=config)
        executor.buy_order = Mock(spec=TrackedOrder)
        executor.sell_order = Mock(spec=TrackedOrder)
        executor.buy_order.is_filled = False
        executor.sell_order.is_filled = False
        executor.buy_order.executed_amount_base = Decimal("0")
        executor.sell_order.executed_amount_base = Decimal("0")
        executor.buy_order.order.executed_amount_base = Decimal("0")
        executor.sell_order.order.executed_amount_base = Decimal("0")
        executor.buy_order.average_executed_price = Decimal("0")
        executor.sell_order.average_executed_price = Decimal("0")
        executor._status = RunnableStatus.TERMINATED
        executor.close_type = CloseType.FAILED
        executor.close_timestamp = close_timestamp
        executor.buy_order.cum_fees_quote = Decimal("0")
        executor.sell_order.cum_fees_quote = Decimal("0")
        serialized_config = executor.executor_info.json()
        executor_dict = json.loads(serialized_config)
        new_executor = Executors(**executor_dict)
        return new_executor

    def get_one_side_failed_executor(
        self,
        close_timestamp: float,
        buy_side: ConnectorPair,
        sell_side: ConnectorPair,
        size: Decimal = Decimal("10"),
        price=Decimal("1"),
        multiplier: int = 1,
    ) -> Executors:
        """
        The buy side succeeds, sell side fails
        """
        order_size = Decimal(size * multiplier)
        order_price = Decimal(price * multiplier)
        config = ArbitrageExecutorConfig(
            id="123" + str(int(close_timestamp)),
            timestamp=1234,
            controller_id="test",
            buying_market=buy_side,
            selling_market=sell_side,
            order_amount=Decimal("10"),
            min_profitability=Decimal("0.1"),
        )
        executor = ArbitrageExecutor(strategy=self.strategy, config=config)
        executor.buy_order = Mock(spec=TrackedOrder)
        executor.sell_order = Mock(spec=TrackedOrder)
        executor.buy_order.is_filled = True
        executor.sell_order.is_filled = False
        executor.buy_order.executed_amount_base = Decimal(order_size)
        executor.sell_order.executed_amount_base = Decimal("0")
        executor.buy_order.order.executed_amount_base = Decimal(order_size)
        executor.sell_order.order.executed_amount_base = Decimal("0")
        executor.buy_order.average_executed_price = Decimal(order_price)
        executor.sell_order.average_executed_price = Decimal("0")
        executor._status = RunnableStatus.TERMINATED
        executor.close_type = CloseType.ONE_SIDE_FAILED
        executor.close_timestamp = close_timestamp
        executor.buy_order.cum_fees_quote = Decimal(order_size * order_price * Decimal(0.005))
        executor.sell_order.cum_fees_quote = Decimal("0")
        serialized_config = executor.executor_info.json()
        executor_dict = json.loads(serialized_config)
        new_executor = Executors(**executor_dict)
        return new_executor

    def get_completed_executor(
        self,
        close_timestamp: float,
        buy_side: ConnectorPair,
        sell_side: ConnectorPair,
        size: Decimal = Decimal("10"),
        price=Decimal("1"),
        short_to_long_ratio=Decimal("0.99"),
        multiplier: int = 1,
    ) -> Executors:
        order_size = Decimal(size * multiplier)
        order_price = Decimal(price * multiplier)
        order_short_to_long_ratio = short_to_long_ratio * multiplier
        config = ArbitrageExecutorConfig(
            id="123-" + str(multiplier) + str(int(close_timestamp)),
            timestamp=1234 + multiplier,
            controller_id="test",
            buying_market=buy_side,
            selling_market=sell_side,
            order_amount=Decimal(order_size),
            min_profitability=Decimal("0.1"),
        )
        executor = ArbitrageExecutor(strategy=self.strategy, config=config)
        executor.buy_order = Mock(spec=TrackedOrder)
        executor.sell_order = Mock(spec=TrackedOrder)
        executor.buy_order.is_filled = True
        executor.sell_order.is_filled = True
        executor.buy_order.executed_amount_base = Decimal(order_size)
        executor.sell_order.executed_amount_base = Decimal(order_size)
        executor.buy_order.order.executed_amount_base = Decimal(order_size)
        executor.sell_order.order.executed_amount_base = Decimal(order_size)
        executor.buy_order.average_executed_price = Decimal(order_price)
        executor.sell_order.average_executed_price = Decimal(order_price * order_short_to_long_ratio)
        executor._status = RunnableStatus.TERMINATED
        executor.close_type = CloseType.COMPLETED
        executor.close_timestamp = close_timestamp
        executor.buy_order.cum_fees_quote = Decimal(order_size * order_price * Decimal(0.005))
        executor.sell_order.cum_fees_quote = Decimal(
            order_size * order_price * order_short_to_long_ratio * Decimal(0.005)
        )
        serialized_config = executor.executor_info.json()
        executor_dict = json.loads(serialized_config)
        new_executor = Executors(**executor_dict)
        return new_executor

    """
    get_position_size Test Cases:
    - fully upscaled
    - partially downscaled
    - failed executors
    - one side failed executors
    - no executors
    - executors of different market-pair
    - mismatching market-pair
    - no end time
    - outside of time window
    """

    @parameterized.expand([(1,), (2,), (5,)])
    def test_get_position_size_long_all_complete(self, multiplier):
        buy_side = ConnectorPair(connector_name="binance_perpetual", trading_pair="ETH-USDT")
        sell_side = ConnectorPair(connector_name="hyperliquid_perpetual", trading_pair="ETH-USDT")

        start_time = 100.0
        end_time = 1000.0
        base_order_size = Decimal("10.038")
        expected_net_size = Decimal("0")

        # Add all executors to the database
        with self.manager.get_new_session() as session:
            with session.begin():
                for i in range(1, multiplier + 1):
                    executor_record = self.get_completed_executor(
                        close_timestamp=100.0 * i,
                        buy_side=buy_side,
                        sell_side=sell_side,
                        size=base_order_size,
                        multiplier=i,
                    )
                    session.add(executor_record)
                    expected_net_size += base_order_size * i

        # call method
        result = self.position_metrics.get_position_size(
            start_time=start_time, end_time=end_time, target_market="binance_perpetual", target_pair="ETH-USDT"
        )

        # check result
        self.assertAlmostEqual(result, expected_net_size)

    @parameterized.expand([(1,), (2,), (5,)])
    def test_get_position_size_short_all_complete(self, multiplier):
        buy_side = ConnectorPair(connector_name="binance_perpetual", trading_pair="ETH-USDT")
        sell_side = ConnectorPair(connector_name="hyperliquid_perpetual", trading_pair="ETH-USD")

        start_time = 100.0
        end_time = 1000.0
        base_order_size = Decimal("10.038")
        expected_net_size = Decimal("0")

        # Add all executors to the database
        with self.manager.get_new_session() as session:
            with session.begin():
                for i in range(1, multiplier + 1):
                    executor_record = self.get_completed_executor(
                        close_timestamp=100.0 * i,
                        buy_side=buy_side,
                        sell_side=sell_side,
                        size=base_order_size,
                        multiplier=i,
                    )
                    session.add(executor_record)
                    expected_net_size += base_order_size * i

        # call method
        result = self.position_metrics.get_position_size(
            start_time=start_time, end_time=end_time, target_market="hyperliquid_perpetual", target_pair="ETH-USD"
        )

        # check result
        self.assertAlmostEqual(result, expected_net_size)

    @parameterized.expand([(2,), (3,), (5,), (20,)])
    def test_get_position_size_long_all_completed_some_downscaling(self, multiplier):
        buy_side = ConnectorPair(connector_name="binance_perpetual", trading_pair="ETH-USDT")
        sell_side = ConnectorPair(connector_name="hyperliquid_perpetual", trading_pair="ETH-USD")

        start_time = 100.0
        end_time = 100.0 * multiplier * 2
        base_order_size = Decimal("10.038")
        expected_net_size = Decimal("0")

        # Add all executors to the database
        with self.manager.get_new_session() as session:
            with session.begin():
                for i in range(1, multiplier + 1):
                    executor_record = self.get_completed_executor(
                        close_timestamp=100.0 * i,
                        buy_side=buy_side,
                        sell_side=sell_side,
                        size=base_order_size,
                        multiplier=i,
                    )
                    session.add(executor_record)
                    expected_net_size += base_order_size * i

                for i in range(1, int(multiplier / 2)):
                    executor_record = self.get_completed_executor(
                        close_timestamp=(100.0 * multiplier + 100.0 * i),
                        buy_side=sell_side,
                        sell_side=buy_side,
                        size=base_order_size,
                        multiplier=i,
                    )
                    session.add(executor_record)
                    expected_net_size -= base_order_size * i

        # call method
        result = self.position_metrics.get_position_size(
            start_time=start_time, end_time=end_time, target_market="binance_perpetual", target_pair="ETH-USDT"
        )

        # check result
        self.assertAlmostEqual(result, expected_net_size)

    @parameterized.expand([(2,), (3,), (5,), (20,)])
    def test_get_position_size_short_all_completed_some_downscaling(self, multiplier):
        buy_side = ConnectorPair(connector_name="binance_perpetual", trading_pair="ETH-USDT")
        sell_side = ConnectorPair(connector_name="hyperliquid_perpetual", trading_pair="ETH-USD")

        start_time = 100.0
        end_time = 100.0 * multiplier * 2
        base_order_size = Decimal("10.038")
        expected_net_size = Decimal("0")

        # Add all executors to the database
        with self.manager.get_new_session() as session:
            with session.begin():
                for i in range(1, multiplier + 1):
                    executor_record = self.get_completed_executor(
                        close_timestamp=100.0 * i,
                        buy_side=buy_side,
                        sell_side=sell_side,
                        size=base_order_size,
                        multiplier=i,
                    )
                    session.add(executor_record)
                    expected_net_size += base_order_size * i

                for i in range(1, int(multiplier / 2)):
                    executor_record = self.get_completed_executor(
                        close_timestamp=(100.0 * multiplier + 100.0 * i),
                        buy_side=sell_side,
                        sell_side=buy_side,
                        size=base_order_size,
                        multiplier=i,
                    )
                    session.add(executor_record)
                    expected_net_size -= base_order_size * i

        # call method
        result = self.position_metrics.get_position_size(
            start_time=start_time, end_time=end_time, target_market="hyperliquid_perpetual", target_pair="ETH-USD"
        )

        # check result
        self.assertAlmostEqual(result, expected_net_size)

    @parameterized.expand([(5,), (20,)])
    def test_get_position_size_long_some_completed_some_downscaling(self, multiplier):
        buy_side = ConnectorPair(connector_name="binance_perpetual", trading_pair="ETH-USDT")
        sell_side = ConnectorPair(connector_name="hyperliquid_perpetual", trading_pair="ETH-USD")

        start_time = 100.0
        end_time = 100.0 * multiplier * 2
        base_order_size = Decimal("10.038")
        expected_net_size = Decimal("0")

        # Add all executors to the database
        with self.manager.get_new_session() as session:
            with session.begin():
                for i in range(1, multiplier + 1):
                    executor_record = None
                    if i % 3 != 0:
                        executor_record = self.get_completed_executor(
                            close_timestamp=100.0 * i,
                            buy_side=buy_side,
                            sell_side=sell_side,
                            size=base_order_size,
                            multiplier=i,
                        )
                        expected_net_size += base_order_size * i
                    else:
                        executor_record = self.get_failed_executor(
                            close_timestamp=100.0 * i, buy_side=buy_side, sell_side=sell_side
                        )
                    session.add(executor_record)

                for i in range(1, int(multiplier / 2)):
                    executor_record = self.get_completed_executor(
                        close_timestamp=(100.0 * multiplier + 100.0 * i),
                        buy_side=sell_side,
                        sell_side=buy_side,
                        size=base_order_size,
                        multiplier=i,
                    )
                    session.add(executor_record)
                    expected_net_size -= base_order_size * i

        # call method
        result = self.position_metrics.get_position_size(
            start_time=start_time, end_time=end_time, target_market="hyperliquid_perpetual", target_pair="ETH-USD"
        )

        # check result
        self.assertAlmostEqual(result, expected_net_size)

    @parameterized.expand([(5,), (20,)])
    def test_get_position_size_long_some_one_side_failed_some_downscaling(self, multiplier):
        buy_side = ConnectorPair(connector_name="binance_perpetual", trading_pair="ETH-USDT")
        sell_side = ConnectorPair(connector_name="hyperliquid_perpetual", trading_pair="ETH-USD")

        start_time = 100.0
        end_time = 100.0 * multiplier * 5
        base_order_size = Decimal("10.038")
        expected_net_size = Decimal("0")

        # Add all executors to the database
        with self.manager.get_new_session() as session:
            with session.begin():
                for i in range(1, multiplier + 1):
                    executor_record = None
                    if i % 3 != 0:
                        executor_record = self.get_completed_executor(
                            close_timestamp=100.0 * i,
                            buy_side=buy_side,
                            sell_side=sell_side,
                            size=base_order_size,
                            multiplier=i,
                        )
                    else:
                        executor_record = self.get_one_side_failed_executor(
                            close_timestamp=100.0 * i,
                            buy_side=buy_side,
                            sell_side=sell_side,
                            size=base_order_size,
                            multiplier=i,
                        )
                    expected_net_size += base_order_size * i
                    session.add(executor_record)

                for i in range(1, int(multiplier / 2)):
                    executor_record = self.get_completed_executor(
                        close_timestamp=(100.0 * multiplier + 100.0 * i),
                        buy_side=sell_side,
                        sell_side=buy_side,
                        size=base_order_size,
                        multiplier=i,
                    )
                    session.add(executor_record)
                    expected_net_size -= base_order_size * i

        # call method
        result = self.position_metrics.get_position_size(
            start_time=start_time, end_time=end_time, target_market="binance_perpetual", target_pair="ETH-USDT"
        )

        # check result
        self.assertAlmostEqual(result, expected_net_size)

    @parameterized.expand([(5,), (20,)])
    def test_get_position_size_short_some_one_side_failed_some_downscaling(self, multiplier):
        buy_side = ConnectorPair(connector_name="binance_perpetual", trading_pair="ETH-USDT")
        sell_side = ConnectorPair(connector_name="hyperliquid_perpetual", trading_pair="ETH-USD")

        start_time = 100.0
        end_time = 100.0 * multiplier * 5
        base_order_size = Decimal("10")
        expected_net_size = Decimal("0")

        # Add all executors to the database
        with self.manager.get_new_session() as session:
            with session.begin():
                for i in range(1, multiplier + 1):
                    executor_record = None
                    if i % 3 != 0:
                        executor_record = self.get_completed_executor(
                            close_timestamp=100.0 * i,
                            buy_side=buy_side,
                            sell_side=sell_side,
                            size=base_order_size,
                            multiplier=i,
                        )
                        expected_net_size += base_order_size * i
                    else:
                        executor_record = self.get_one_side_failed_executor(
                            close_timestamp=100.0 * i,
                            buy_side=buy_side,
                            sell_side=sell_side,
                            size=base_order_size,
                            multiplier=i,
                        )
                    session.add(executor_record)

                for i in range(1, int(multiplier / 2)):
                    executor_record = self.get_completed_executor(
                        close_timestamp=(100.0 * multiplier + 100.0 * i),
                        buy_side=sell_side,
                        sell_side=buy_side,
                        size=base_order_size,
                        multiplier=i,
                    )
                    session.add(executor_record)
                    expected_net_size -= base_order_size * i

        # call method
        result = self.position_metrics.get_position_size(
            start_time=start_time, end_time=end_time, target_market="hyperliquid_perpetual", target_pair="ETH-USD"
        )

        # check result
        self.assertAlmostEqual(result, expected_net_size)

    @parameterized.expand([(1,), (2,), (5,)])
    def test_get_position_size_none_complete(self, multiplier):
        buy_side = ConnectorPair(connector_name="binance_perpetual", trading_pair="ETH-USDT")
        sell_side = ConnectorPair(connector_name="hyperliquid_perpetual", trading_pair="ETH-USDT")

        start_time = 100.0
        end_time = 100.0 * multiplier * 2
        base_order_size = Decimal("10.038")
        expected_net_size = Decimal("0")

        # Add all executors to the database
        with self.manager.get_new_session() as session:
            with session.begin():
                for i in range(1, multiplier + 1):
                    executor_record = self.get_failed_executor(
                        close_timestamp=100.0 * i, buy_side=buy_side, sell_side=sell_side
                    )
                    session.add(executor_record)

        # call method
        result = self.position_metrics.get_position_size(
            start_time=start_time, end_time=end_time, target_market="binance_perpetual", target_pair="ETH-USDT"
        )

        # check result
        self.assertAlmostEqual(result, expected_net_size)

    @parameterized.expand([(1,), (2,), (5,)])
    def test_get_position_size_wrong_market_pair(self, multiplier):
        buy_side = ConnectorPair(connector_name="binance_perpetual", trading_pair="ETH-USDT")
        sell_side = ConnectorPair(connector_name="hyperliquid_perpetual", trading_pair="ETH-USDT")

        start_time = 100.0
        end_time = 100.0 * multiplier * 2
        base_order_size = Decimal("10.038")
        expected_net_size = Decimal("0")

        # Add all executors to the database
        with self.manager.get_new_session() as session:
            with session.begin():
                for i in range(1, multiplier + 1):
                    executor_record = self.get_failed_executor(
                        close_timestamp=100.0 * i, buy_side=buy_side, sell_side=sell_side
                    )
                    session.add(executor_record)

        # call method
        result_wrong_market = self.position_metrics.get_position_size(
            start_time=start_time, end_time=end_time, target_market="some_market", target_pair="ETH-USDT"
        )
        result_wrong_pair = self.position_metrics.get_position_size(
            start_time=start_time, end_time=end_time, target_market="binance_perpetual", target_pair="FAKE-USDT"
        )
        result_wrong_market_pair = self.position_metrics.get_position_size(
            start_time=start_time, end_time=end_time, target_market="some_market", target_pair="FAKE-USDT"
        )

        # check result
        self.assertAlmostEqual(result_wrong_market, expected_net_size)
        self.assertAlmostEqual(result_wrong_pair, expected_net_size)
        self.assertAlmostEqual(result_wrong_market_pair, expected_net_size)

    def test_get_position_size_no_end_time(self):
        buy_side = ConnectorPair(connector_name="binance_perpetual", trading_pair="ETH-USDT")
        sell_side = ConnectorPair(connector_name="hyperliquid_perpetual", trading_pair="ETH-USDT")

        start_time = 100.0
        end_time = None
        base_order_size = Decimal("10.038")
        expected_net_size = Decimal("0")
        multiplier = 3

        # Add all executors to the database
        with self.manager.get_new_session() as session:
            with session.begin():
                for i in range(1, multiplier + 1):
                    executor_record = self.get_completed_executor(
                        close_timestamp=100.0 * i,
                        buy_side=buy_side,
                        sell_side=sell_side,
                        size=base_order_size,
                        multiplier=i,
                    )
                    session.add(executor_record)
                    expected_net_size += base_order_size * i

        # call method
        result = self.position_metrics.get_position_size(
            start_time=start_time, end_time=end_time, target_market="binance_perpetual", target_pair="ETH-USDT"
        )

        # check result
        self.assertAlmostEqual(result, expected_net_size)

    def test_get_position_size_outside_time_window(self):
        buy_side = ConnectorPair(connector_name="binance_perpetual", trading_pair="ETH-USDT")
        sell_side = ConnectorPair(connector_name="hyperliquid_perpetual", trading_pair="ETH-USDT")

        start_time = 100.0
        end_time = 1000.0
        base_order_size = Decimal("10.038")
        expected_net_size = Decimal("0")
        multiplier = 3

        # Add all executors to the database
        with self.manager.get_new_session() as session:
            with session.begin():
                for i in range(1, multiplier + 1):
                    close_timestamp = None
                    if i == 1:
                        close_timestamp = 50.0
                    elif i == 2:
                        close_timestamp = 200.0
                        expected_net_size += base_order_size * i
                    elif i == 3:
                        close_timestamp = 2000.0

                    executor_record = self.get_completed_executor(
                        close_timestamp=close_timestamp,
                        buy_side=buy_side,
                        sell_side=sell_side,
                        size=base_order_size,
                        multiplier=i,
                    )
                    session.add(executor_record)

        # call method
        result = self.position_metrics.get_position_size(
            start_time=start_time, end_time=end_time, target_market="binance_perpetual", target_pair="ETH-USDT"
        )

        # check result
        self.assertAlmostEqual(result, expected_net_size)

    """
    get_position_avg_entry_price Test Cases:
    - fully upscaled
    - partially downscaled
        - Ignores downscale orders
    - failed executors
    - one side failed executors
    - no executors
    - no end time
    - outside of time window
    """

    @parameterized.expand([(1,), (2,), (5,)])
    def test_get_entry_price_long_all_complete(self, multiplier):
        buy_side = ConnectorPair(connector_name="binance_perpetual", trading_pair="ETH-USDT")
        sell_side = ConnectorPair(connector_name="hyperliquid_perpetual", trading_pair="ETH-USDT")

        start_time = 100.0
        end_time = 1000.0
        base_order_size = Decimal("10.038")
        base_entry_price = Decimal("4.7197")
        short_to_long_ratio = Decimal("0.99")
        expected_net_size = Decimal("0")
        expected_initial_exposure = Decimal("0")

        # Add all executors to the database
        with self.manager.get_new_session() as session:
            with session.begin():
                for i in range(1, multiplier + 1):
                    executor_record = self.get_completed_executor(
                        close_timestamp=100.0 * i,
                        buy_side=buy_side,
                        sell_side=sell_side,
                        size=base_order_size,
                        price=base_entry_price,
                        short_to_long_ratio=short_to_long_ratio,
                        multiplier=i,
                    )
                    session.add(executor_record)
                    expected_net_size += base_order_size * i
                    expected_initial_exposure += (base_order_size * i) * (base_entry_price * i)

        # call method
        result = self.position_metrics.get_position_avg_entry_price(
            start_time=start_time,
            end_time=end_time,
            target_market="binance_perpetual",
            target_pair="ETH-USDT",
            target_net_side="long",
        )

        incorrect_side_result = self.position_metrics.get_position_avg_entry_price(
            start_time=start_time,
            end_time=end_time,
            target_market="binance_perpetual",
            target_pair="ETH-USDT",
            target_net_side="short",
        )

        expected_price = expected_initial_exposure / expected_net_size
        # check result
        self.assertAlmostEqual(expected_price, result)

        self.assertAlmostEqual(Decimal("0"), incorrect_side_result)

    @parameterized.expand([(1,), (2,), (5,)])
    def test_get_entry_price_short_all_complete(self, multiplier):
        buy_side = ConnectorPair(connector_name="binance_perpetual", trading_pair="ETH-USDT")
        sell_side = ConnectorPair(connector_name="hyperliquid_perpetual", trading_pair="ETH-USDT")

        start_time = 100.0
        end_time = 1000.0
        base_order_size = Decimal("10.038")
        base_entry_price = Decimal("4.7197")
        short_to_long_ratio = Decimal("0.99")
        expected_net_size = Decimal("0")
        expected_initial_exposure = Decimal("0")

        # Add all executors to the database
        with self.manager.get_new_session() as session:
            with session.begin():
                for i in range(1, multiplier + 1):
                    executor_record = self.get_completed_executor(
                        close_timestamp=100.0 * i,
                        buy_side=buy_side,
                        sell_side=sell_side,
                        size=base_order_size,
                        price=base_entry_price,
                        short_to_long_ratio=short_to_long_ratio,
                        multiplier=i,
                    )
                    session.add(executor_record)
                    expected_net_size += base_order_size * i
                    expected_initial_exposure += (
                        (base_order_size * i) * (base_entry_price * i) * (short_to_long_ratio * i)
                    )

        # call method
        result = self.position_metrics.get_position_avg_entry_price(
            start_time=start_time,
            end_time=end_time,
            target_market="hyperliquid_perpetual",
            target_pair="ETH-USDT",
            target_net_side="short",
        )

        incorrect_side_result = self.position_metrics.get_position_avg_entry_price(
            start_time=start_time,
            end_time=end_time,
            target_market="hyperliquid_perpetual",
            target_pair="ETH-USDT",
            target_net_side="long",
        )

        expected_price = expected_initial_exposure / expected_net_size
        # check result
        self.assertAlmostEqual(expected_price, result)

        self.assertAlmostEqual(Decimal("0"), incorrect_side_result)

    @parameterized.expand([(2,), (3,), (5,), (20,)])
    def test_get_entry_price_long_all_completed_some_downscaling(self, multiplier):
        buy_side = ConnectorPair(connector_name="binance_perpetual", trading_pair="ETH-USDT")
        sell_side = ConnectorPair(connector_name="hyperliquid_perpetual", trading_pair="ETH-USD")

        start_time = 10.0
        end_time = 100.0 * multiplier * 5 + 100.0
        base_order_size = Decimal("10.038")
        base_entry_price = Decimal("4.7197")
        short_to_long_ratio = Decimal("0.99")
        expected_initial_size = Decimal("0")
        expected_initial_exposure = Decimal("0")

        # Add all executors to the database
        with self.manager.get_new_session() as session:
            with session.begin():
                for i in range(1, multiplier + 1):
                    executor_record = self.get_completed_executor(
                        close_timestamp=100.0 * i,
                        buy_side=buy_side,
                        sell_side=sell_side,
                        size=base_order_size,
                        price=base_entry_price,
                        multiplier=i,
                        short_to_long_ratio=short_to_long_ratio,
                    )
                    session.add(executor_record)
                    expected_initial_size += base_order_size * i
                    expected_initial_exposure += (base_order_size * i) * (base_entry_price * i)

                for i in range(1, int(multiplier / 2)):
                    executor_record = self.get_completed_executor(
                        close_timestamp=(100.0 * multiplier + 100.0 * i),
                        buy_side=sell_side,
                        sell_side=buy_side,
                        size=base_order_size,
                        price=base_entry_price,
                        multiplier=i,
                        short_to_long_ratio=short_to_long_ratio,
                    )
                    session.add(executor_record)

        # call method
        result = self.position_metrics.get_position_avg_entry_price(
            start_time=start_time,
            end_time=end_time,
            target_market="binance_perpetual",
            target_pair="ETH-USDT",
            target_net_side="long",
        )
        expected_price = expected_initial_exposure / expected_initial_size

        # check result
        self.assertAlmostEqual(expected_price, result)

    @parameterized.expand([(2,), (3,), (5,), (20,)])
    def test_get_entry_price_short_all_completed_some_downscaling(self, multiplier):
        buy_side = ConnectorPair(connector_name="binance_perpetual", trading_pair="ETH-USDT")
        sell_side = ConnectorPair(connector_name="hyperliquid_perpetual", trading_pair="ETH-USD")

        start_time = 100.0
        end_time = 100.0 * multiplier * 2 + 100.0
        base_order_size = Decimal("10.038")
        base_entry_price = Decimal("4.7197")
        short_to_long_ratio = Decimal("0.99")
        expected_initial_size = Decimal("0")
        expected_initial_exposure = Decimal("0")

        # Add all executors to the database
        with self.manager.get_new_session() as session:
            with session.begin():
                for i in range(1, multiplier + 1):
                    executor_record = self.get_completed_executor(
                        close_timestamp=100.0 * i,
                        buy_side=buy_side,
                        sell_side=sell_side,
                        size=base_order_size,
                        price=base_entry_price,
                        multiplier=i,
                        short_to_long_ratio=short_to_long_ratio,
                    )
                    session.add(executor_record)
                    expected_initial_size += base_order_size * i
                    expected_initial_exposure += (
                        (base_order_size * i) * (base_entry_price * i) * (short_to_long_ratio * i)
                    )

                for i in range(1, int(multiplier / 2)):
                    executor_record = self.get_completed_executor(
                        close_timestamp=(100.0 * multiplier + 100.0 * i),
                        buy_side=sell_side,
                        sell_side=buy_side,
                        size=base_order_size,
                        price=base_entry_price,
                        multiplier=i,
                        short_to_long_ratio=short_to_long_ratio,
                    )
                    session.add(executor_record)

        # call method
        result = self.position_metrics.get_position_avg_entry_price(
            start_time=start_time,
            end_time=end_time,
            target_market="hyperliquid_perpetual",
            target_pair="ETH-USD",
            target_net_side="short",
        )
        expected_price = expected_initial_exposure / expected_initial_size
        # check result
        self.assertAlmostEqual(expected_price, result)

    @parameterized.expand([(5,), (20,)])
    def test_get_entry_price_long_some_completed_some_downscaling(self, multiplier):
        buy_side = ConnectorPair(connector_name="binance_perpetual", trading_pair="ETH-USDT")
        sell_side = ConnectorPair(connector_name="hyperliquid_perpetual", trading_pair="ETH-USD")

        start_time = 100.0
        end_time = 100.0 * multiplier * 2 + 100.0
        base_order_size = Decimal("10.038")
        base_entry_price = Decimal("4.7197")
        short_to_long_ratio = Decimal("0.99")
        expected_initial_size = Decimal("0")
        expected_initial_exposure = Decimal("0")

        # Add all executors to the database
        with self.manager.get_new_session() as session:
            with session.begin():
                for i in range(1, multiplier + 1):
                    executor_record = None
                    if i % 3 != 0:
                        executor_record = self.get_completed_executor(
                            close_timestamp=100.0 * i,
                            buy_side=buy_side,
                            sell_side=sell_side,
                            size=base_order_size,
                            price=base_entry_price,
                            multiplier=i,
                            short_to_long_ratio=short_to_long_ratio,
                        )
                        session.add(executor_record)
                        expected_initial_size += base_order_size * i
                        expected_initial_exposure += (base_order_size * i) * (base_entry_price * i)
                    else:
                        executor_record = self.get_failed_executor(
                            close_timestamp=100.0 * i, buy_side=buy_side, sell_side=sell_side
                        )
                    session.add(executor_record)

                for i in range(1, int(multiplier / 2)):
                    executor_record = self.get_completed_executor(
                        close_timestamp=(100.0 * multiplier + 100.0 * i),
                        buy_side=sell_side,
                        sell_side=buy_side,
                        size=base_order_size,
                        price=base_entry_price,
                        multiplier=i,
                        short_to_long_ratio=short_to_long_ratio,
                    )
                    session.add(executor_record)

        # call method
        result = self.position_metrics.get_position_avg_entry_price(
            start_time=start_time,
            end_time=end_time,
            target_market="binance_perpetual",
            target_pair="ETH-USDT",
            target_net_side="long",
        )
        expected_price = expected_initial_exposure / expected_initial_size
        # check result
        self.assertAlmostEqual(expected_price, result)

    @parameterized.expand([(5,), (20,)])
    def test_get_entry_price_short_some_completed_some_downscaling(self, multiplier):
        buy_side = ConnectorPair(connector_name="binance_perpetual", trading_pair="ETH-USDT")
        sell_side = ConnectorPair(connector_name="hyperliquid_perpetual", trading_pair="ETH-USD")

        start_time = 100.0
        end_time = 100.0 * multiplier * 2 + 100.0
        base_order_size = Decimal("10.038")
        base_entry_price = Decimal("4.7197")
        short_to_long_ratio = Decimal("0.99")
        expected_initial_size = Decimal("0")
        expected_initial_exposure = Decimal("0")

        # Add all executors to the database
        with self.manager.get_new_session() as session:
            with session.begin():
                for i in range(1, multiplier + 1):
                    executor_record = None
                    if i % 3 != 0:
                        executor_record = self.get_completed_executor(
                            close_timestamp=100.0 * i,
                            buy_side=buy_side,
                            sell_side=sell_side,
                            size=base_order_size,
                            price=base_entry_price,
                            multiplier=i,
                            short_to_long_ratio=short_to_long_ratio,
                        )
                        session.add(executor_record)
                        expected_initial_size += base_order_size * i
                        expected_initial_exposure += (
                            (base_order_size * i) * (base_entry_price * i) * (short_to_long_ratio * i)
                        )
                    else:
                        executor_record = self.get_failed_executor(
                            close_timestamp=100.0 * i, buy_side=buy_side, sell_side=sell_side
                        )
                    session.add(executor_record)

                for i in range(1, int(multiplier / 2)):
                    executor_record = self.get_completed_executor(
                        close_timestamp=(100.0 * multiplier + 100.0 * i),
                        buy_side=sell_side,
                        sell_side=buy_side,
                        size=base_order_size,
                        price=base_entry_price,
                        multiplier=i,
                        short_to_long_ratio=short_to_long_ratio,
                    )
                    session.add(executor_record)

        # call method
        result = self.position_metrics.get_position_avg_entry_price(
            start_time=start_time,
            end_time=end_time,
            target_market="hyperliquid_perpetual",
            target_pair="ETH-USD",
            target_net_side="short",
        )
        expected_price = expected_initial_exposure / expected_initial_size
        # check result
        self.assertAlmostEqual(expected_price, result)

    @parameterized.expand([(5,), (20,)])
    def test_get_entry_price_long_some_one_side_failed_some_downscaling(self, multiplier):
        buy_side = ConnectorPair(connector_name="binance_perpetual", trading_pair="ETH-USDT")
        sell_side = ConnectorPair(connector_name="hyperliquid_perpetual", trading_pair="ETH-USD")

        start_time = 100.0
        end_time = 100.0 * multiplier * 2 + 100.0
        base_order_size = Decimal("10.038")
        base_entry_price = Decimal("4.7197")
        short_to_long_ratio = Decimal("0.99")
        expected_initial_size = Decimal("0")
        expected_initial_exposure = Decimal("0")

        # Add all executors to the database
        with self.manager.get_new_session() as session:
            with session.begin():
                for i in range(1, multiplier + 1):
                    executor_record = None
                    if i % 3 != 0:
                        executor_record = self.get_completed_executor(
                            close_timestamp=100.0 * i,
                            buy_side=buy_side,
                            sell_side=sell_side,
                            size=base_order_size,
                            price=base_entry_price,
                            multiplier=i,
                            short_to_long_ratio=short_to_long_ratio,
                        )

                    else:
                        executor_record = self.get_one_side_failed_executor(
                            close_timestamp=100.0 * i,
                            buy_side=buy_side,
                            sell_side=sell_side,
                            size=base_order_size,
                            price=base_entry_price,
                            multiplier=i,
                        )
                    session.add(executor_record)
                    expected_initial_size += base_order_size * i
                    expected_initial_exposure += (base_order_size * i) * (base_entry_price * i)

                for i in range(1, int(multiplier / 2)):
                    executor_record = self.get_completed_executor(
                        close_timestamp=(100.0 * multiplier + 100.0 * i),
                        buy_side=sell_side,
                        sell_side=buy_side,
                        size=base_order_size,
                        price=base_entry_price,
                        multiplier=i,
                        short_to_long_ratio=short_to_long_ratio,
                    )
                    session.add(executor_record)

        # call method
        result = self.position_metrics.get_position_avg_entry_price(
            start_time=start_time,
            end_time=end_time,
            target_market="binance_perpetual",
            target_pair="ETH-USDT",
            target_net_side="long",
        )

        expected_price = expected_initial_exposure / expected_initial_size
        # check result
        self.assertAlmostEqual(expected_price, result)

    @parameterized.expand([(5,), (20,)])
    def test_get_entry_price_short_some_one_side_failed_some_downscaling(self, multiplier):
        buy_side = ConnectorPair(connector_name="binance_perpetual", trading_pair="ETH-USDT")
        sell_side = ConnectorPair(connector_name="hyperliquid_perpetual", trading_pair="ETH-USD")

        start_time = 100.0
        end_time = 100.0 * multiplier * 2 + 100.0
        base_order_size = Decimal("10.038")
        base_entry_price = Decimal("4.7197")
        short_to_long_ratio = Decimal("0.99")
        expected_initial_size = Decimal("0")
        expected_initial_exposure = Decimal("0")

        # Add all executors to the database
        with self.manager.get_new_session() as session:
            with session.begin():
                for i in range(1, multiplier + 1):
                    executor_record = None
                    if i % 3 != 0:
                        executor_record = self.get_completed_executor(
                            close_timestamp=100.0 * i,
                            buy_side=buy_side,
                            sell_side=sell_side,
                            size=base_order_size,
                            price=base_entry_price,
                            multiplier=i,
                            short_to_long_ratio=short_to_long_ratio,
                        )
                        expected_initial_size += base_order_size * i
                        expected_initial_exposure += (
                            (base_order_size * i) * (base_entry_price * i) * (short_to_long_ratio * i)
                        )
                    else:
                        executor_record = self.get_one_side_failed_executor(
                            close_timestamp=100.0 * i,
                            buy_side=buy_side,
                            sell_side=sell_side,
                            size=base_order_size,
                            price=base_entry_price,
                            multiplier=i,
                        )
                    session.add(executor_record)

                for i in range(1, int(multiplier / 2)):
                    executor_record = self.get_completed_executor(
                        close_timestamp=(100.0 * multiplier + 100.0 * i),
                        buy_side=sell_side,
                        sell_side=buy_side,
                        size=base_order_size,
                        price=base_entry_price,
                        multiplier=i,
                        short_to_long_ratio=short_to_long_ratio,
                    )
                    session.add(executor_record)

        # call method
        result = self.position_metrics.get_position_avg_entry_price(
            start_time=start_time,
            end_time=end_time,
            target_market="hyperliquid_perpetual",
            target_pair="ETH-USD",
            target_net_side="short",
        )

        expected_price = expected_initial_exposure / expected_initial_size
        # check result
        self.assertAlmostEqual(expected_price, result)

    @parameterized.expand([(1,), (2,), (5,)])
    def test_get_entry_price_none_complete(self, multiplier):
        buy_side = ConnectorPair(connector_name="binance_perpetual", trading_pair="ETH-USDT")
        sell_side = ConnectorPair(connector_name="hyperliquid_perpetual", trading_pair="ETH-USDT")

        start_time = 100.0
        end_time = 100.0 * multiplier * 2
        base_order_size = Decimal("10.038")
        expected_price = Decimal("0")

        # Add all executors to the database
        with self.manager.get_new_session() as session:
            with session.begin():
                for i in range(1, multiplier + 1):
                    executor_record = self.get_failed_executor(
                        close_timestamp=100.0 * i, buy_side=buy_side, sell_side=sell_side
                    )
                    session.add(executor_record)

        # call method
        result = self.position_metrics.get_position_avg_entry_price(
            start_time=start_time,
            end_time=end_time,
            target_market="binance_perpetual",
            target_pair="ETH-USDT",
            target_net_side="long",
        )

        # check result
        self.assertAlmostEqual(expected_price, result)
