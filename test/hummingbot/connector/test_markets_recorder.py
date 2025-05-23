import asyncio
import time
from decimal import Decimal
from typing import Awaitable, List
from unittest import TestCase
from unittest.mock import MagicMock, PropertyMock, patch, Mock

import numpy as np
from hummingbot.core.data_type.order_book import OrderBook
from sqlalchemy import create_engine

from hummingbot.client.config.client_config_map import ClientConfigMap, MarketDataCollectionConfigMap
from hummingbot.client.config.config_helpers import ClientConfigAdapter
from hummingbot.connector.markets_recorder import MarketsRecorder
from hummingbot.core.data_type.common import OrderType, PositionAction, PriceType, TradeType
from hummingbot.core.data_type.trade_fee import AddedToCostTradeFee
from hummingbot.core.event.events import (
    BuyOrderCompletedEvent,
    BuyOrderCreatedEvent,
    MarketEvent,
    OrderFilledEvent,
    SellOrderCreatedEvent,
    FundingPaymentCompletedEvent,
)
from hummingbot.funding_arbitrage.fixed_market_specs import PVPriceType
from hummingbot.logger import HummingbotLogger
from hummingbot.model.executors import Executors
from hummingbot.model.funding_payment import FundingPayment
from hummingbot.model.funding_trade import FundingTrade
from hummingbot.model.market_data import MarketData
from hummingbot.model.order import Order
from hummingbot.model.position import Position
from hummingbot.model.sql_connection_manager import SQLConnectionManager, SQLConnectionType
from hummingbot.model.trade_fill import TradeFill
from hummingbot.strategy.script_strategy_base import ScriptStrategyBase
from hummingbot.strategy_v2.executors.arbitrage_executor.arbitrage_executor import ArbitrageExecutor
from hummingbot.strategy_v2.executors.arbitrage_executor.data_types import ArbitrageExecutorConfig
from hummingbot.strategy_v2.executors.data_types import ConnectorPair
from hummingbot.strategy_v2.executors.position_executor.data_types import PositionExecutorConfig, TripleBarrierConfig
from hummingbot.strategy_v2.executors.position_executor.position_executor import PositionExecutor
from hummingbot.strategy_v2.models.base import RunnableStatus
from hummingbot.strategy_v2.models.executors import CloseType, TrackedOrder
from hummingbot.strategy_v2.models.executors_info import ExecutorInfo


class MarketsRecorderTests(TestCase):
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
        strategy.connectors = {
            "binance_perpetual": MagicMock(),
        }
        return strategy

    @staticmethod
    def async_run_with_timeout(coroutine: Awaitable, timeout: int = 1):
        ret = asyncio.get_event_loop().run_until_complete(asyncio.wait_for(coroutine, timeout))
        return ret

    def get_price_by_type(self, trading_pair, price_type):
        pass

    def get_order_book(self, trading_pair):
        pass

    @patch("hummingbot.model.sql_connection_manager.create_engine")
    def setUp(self, engine_mock) -> None:
        super().setUp()
        self.display_name = "test_market"
        self.config_file_path = "test_config"
        self.strategy_name = "test_strategy"

        self.symbol = "COINALPHAHBOT"
        self.base = "COINALPHA"
        self.quote = "HBOT"
        self.trading_pair = f"{self.base}-{self.quote}"
        self.ready = True
        self.trading_pairs = [self.trading_pair]

        engine_mock.return_value = create_engine("sqlite:///:memory:")
        self.manager = SQLConnectionManager(
            ClientConfigAdapter(ClientConfigMap()), SQLConnectionType.TRADE_FILLS, db_name="test_DB"
        )

        self.tracking_states = dict()

    def add_trade_fills_from_market_recorder(self, current_trade_fills):
        pass

    def add_exchange_order_ids_from_market_recorder(self, current_exchange_order_ids):
        pass

    def test_properties(self):
        recorder = MarketsRecorder(
            sql=self.manager,
            markets=[self],
            config_file_path=self.config_file_path,
            strategy_name=self.strategy_name,
            market_data_collection=MarketDataCollectionConfigMap(
                market_data_collection_enabled=False,
                market_data_collection_interval=60,
                market_data_collection_depth=20,
            ),
        )

        self.assertEqual(self.manager, recorder.sql_manager)
        self.assertEqual(self.config_file_path, recorder.config_file_path)
        self.assertEqual(self.strategy_name, recorder.strategy_name)
        self.assertIsInstance(recorder.logger(), HummingbotLogger)

    def test_get_trade_for_config(self):
        recorder = MarketsRecorder(
            sql=self.manager,
            markets=[self],
            config_file_path=self.config_file_path,
            strategy_name=self.strategy_name,
            market_data_collection=MarketDataCollectionConfigMap(
                market_data_collection_enabled=False,
                market_data_collection_interval=60,
                market_data_collection_depth=20,
            ),
        )

        with self.manager.get_new_session() as session:
            with session.begin():
                trade_fill_record = TradeFill(
                    config_file_path=self.config_file_path,
                    strategy=self.strategy_name,
                    market=self.display_name,
                    symbol=self.symbol,
                    base_asset=self.base,
                    quote_asset=self.quote,
                    timestamp=int(time.time()),
                    order_id="OID1",
                    trade_type=TradeType.BUY.name,
                    order_type=OrderType.LIMIT.name,
                    price=Decimal(1000),
                    amount=Decimal(1),
                    leverage=1,
                    trade_fee=AddedToCostTradeFee().to_json(),
                    exchange_trade_id="EOID1",
                    position=PositionAction.NIL.value,
                )
                session.add(trade_fill_record)

            fill_id = trade_fill_record.exchange_trade_id

        trades = recorder.get_trades_for_config("test_config")
        self.assertEqual(1, len(trades))
        self.assertEqual(fill_id, trades[0].exchange_trade_id)

    def test_buy_order_created_event_creates_order_record(self):
        recorder = MarketsRecorder(
            sql=self.manager,
            markets=[self],
            config_file_path=self.config_file_path,
            strategy_name=self.strategy_name,
            market_data_collection=MarketDataCollectionConfigMap(
                market_data_collection_enabled=False,
                market_data_collection_interval=60,
                market_data_collection_depth=20,
            ),
        )

        event = BuyOrderCreatedEvent(
            timestamp=int(time.time()),
            type=OrderType.LIMIT,
            trading_pair=self.trading_pair,
            amount=Decimal(1),
            price=Decimal(1000),
            order_id="OID1",
            creation_timestamp=1640001112.223,
            exchange_order_id="EOID1",
        )

        recorder._did_create_order(MarketEvent.BuyOrderCreated.value, self, event)

        with self.manager.get_new_session() as session:
            query = session.query(Order)
            orders = query.all()
            order = orders[0]
            order_status = order.status
            trade_fills = order.trade_fills

        self.assertEqual(1, len(orders))
        self.assertEqual(self.config_file_path, orders[0].config_file_path)
        self.assertEqual(event.order_id, orders[0].id)
        self.assertEqual(1640001112223, orders[0].creation_timestamp)
        self.assertEqual(1, len(order_status))
        self.assertEqual(MarketEvent.BuyOrderCreated.name, order_status[0].status)
        self.assertEqual(0, len(trade_fills))

    def test_sell_order_created_event_creates_order_record(self):
        recorder = MarketsRecorder(
            sql=self.manager,
            markets=[self],
            config_file_path=self.config_file_path,
            strategy_name=self.strategy_name,
            market_data_collection=MarketDataCollectionConfigMap(
                market_data_collection_enabled=False,
                market_data_collection_interval=60,
                market_data_collection_depth=20,
            ),
        )

        event = SellOrderCreatedEvent(
            timestamp=int(time.time()),
            type=OrderType.LIMIT,
            trading_pair=self.trading_pair,
            amount=Decimal(1),
            price=Decimal(1000),
            order_id="OID1",
            creation_timestamp=1640001112.223,
            exchange_order_id="EOID1",
        )

        recorder._did_create_order(MarketEvent.SellOrderCreated.value, self, event)

        with self.manager.get_new_session() as session:
            query = session.query(Order)
            orders = query.all()
            order = orders[0]
            order_status = order.status
            trade_fills = order.trade_fills

        self.assertEqual(1, len(orders))
        self.assertEqual(self.config_file_path, orders[0].config_file_path)
        self.assertEqual(event.order_id, orders[0].id)
        self.assertEqual(1640001112223, orders[0].creation_timestamp)
        self.assertEqual(1, len(order_status))
        self.assertEqual(MarketEvent.SellOrderCreated.name, order_status[0].status)
        self.assertEqual(0, len(trade_fills))

    def test_create_order_and_process_fill(self):
        recorder = MarketsRecorder(
            sql=self.manager,
            markets=[self],
            config_file_path=self.config_file_path,
            strategy_name=self.strategy_name,
            market_data_collection=MarketDataCollectionConfigMap(
                market_data_collection_enabled=False,
                market_data_collection_interval=60,
                market_data_collection_depth=20,
            ),
        )

        create_event = BuyOrderCreatedEvent(
            timestamp=1642010000,
            type=OrderType.LIMIT,
            trading_pair=self.trading_pair,
            amount=Decimal(1),
            price=Decimal(1000),
            order_id="OID1-1642010000000000",
            creation_timestamp=1640001112.223,
            exchange_order_id="EOID1",
        )

        recorder._did_create_order(MarketEvent.BuyOrderCreated.value, self, create_event)

        fill_event = OrderFilledEvent(
            timestamp=1642020000,
            order_id=create_event.order_id,
            trading_pair=create_event.trading_pair,
            trade_type=TradeType.BUY,
            order_type=create_event.type,
            price=Decimal(1010),
            amount=create_event.amount,
            trade_fee=AddedToCostTradeFee(),
            exchange_trade_id="TradeId1",
        )

        recorder._did_fill_order(MarketEvent.OrderFilled.value, self, fill_event)

        with self.manager.get_new_session() as session:
            query = session.query(Order)
            orders = query.all()
            order = orders[0]
            order_status = order.status
            trade_fills = order.trade_fills

        self.assertEqual(1, len(orders))
        self.assertEqual(self.config_file_path, orders[0].config_file_path)
        self.assertEqual(create_event.order_id, orders[0].id)
        self.assertEqual(2, len(order_status))
        self.assertEqual(MarketEvent.BuyOrderCreated.name, order_status[0].status)
        self.assertEqual(MarketEvent.OrderFilled.name, order_status[1].status)
        self.assertEqual(1, len(trade_fills))
        self.assertEqual(self.config_file_path, trade_fills[0].config_file_path)
        self.assertEqual(fill_event.order_id, trade_fills[0].order_id)

    def test_trade_fee_in_quote_not_available(self):
        recorder = MarketsRecorder(
            sql=self.manager,
            markets=[self],
            config_file_path=self.config_file_path,
            strategy_name=self.strategy_name,
            market_data_collection=MarketDataCollectionConfigMap(
                market_data_collection_enabled=False,
                market_data_collection_interval=60,
                market_data_collection_depth=20,
            ),
        )

        create_event = BuyOrderCreatedEvent(
            timestamp=1642010000,
            type=OrderType.LIMIT,
            trading_pair=self.trading_pair,
            amount=Decimal(1),
            price=Decimal(1000),
            order_id="OID1-1642010000000000",
            creation_timestamp=1640001112.223,
            exchange_order_id="EOID1",
        )

        recorder._did_create_order(MarketEvent.BuyOrderCreated.value, self, create_event)

        trade_fee = MagicMock()
        trade_fee.fee_amount_in_token = MagicMock(side_effect=[Exception("Fee amount in quote not available")])
        trade_fee.to_json = MagicMock(return_value={"test": "test"})
        fill_event = OrderFilledEvent(
            timestamp=1642020000,
            order_id=create_event.order_id,
            trading_pair=create_event.trading_pair,
            trade_type=TradeType.BUY,
            order_type=create_event.type,
            price=Decimal(1010),
            amount=create_event.amount,
            trade_fee=trade_fee,
            exchange_trade_id="TradeId1",
        )

        recorder._did_fill_order(MarketEvent.OrderFilled.value, self, fill_event)

        with self.manager.get_new_session() as session:
            query = session.query(Order)
            orders = query.all()
            order = orders[0]
            order_status = order.status
            trade_fills = order.trade_fills

        self.assertEqual(1, len(orders))
        self.assertEqual(self.config_file_path, orders[0].config_file_path)
        self.assertEqual(create_event.order_id, orders[0].id)
        self.assertEqual(2, len(order_status))
        self.assertEqual(MarketEvent.BuyOrderCreated.name, order_status[0].status)
        self.assertEqual(MarketEvent.OrderFilled.name, order_status[1].status)
        self.assertEqual(1, len(trade_fills))
        self.assertEqual(self.config_file_path, trade_fills[0].config_file_path)
        self.assertEqual(fill_event.order_id, trade_fills[0].order_id)

    def test_create_order_and_completed(self):
        recorder = MarketsRecorder(
            sql=self.manager,
            markets=[self],
            config_file_path=self.config_file_path,
            strategy_name=self.strategy_name,
            market_data_collection=MarketDataCollectionConfigMap(
                market_data_collection_enabled=False,
                market_data_collection_interval=60,
                market_data_collection_depth=20,
            ),
        )

        create_event = BuyOrderCreatedEvent(
            timestamp=1642010000,
            type=OrderType.LIMIT,
            trading_pair=self.trading_pair,
            amount=Decimal(1),
            price=Decimal(1000),
            order_id="OID1-1642010000000000",
            creation_timestamp=1640001112.223,
            exchange_order_id="EOID1",
        )

        recorder._did_create_order(MarketEvent.BuyOrderCreated.value, self, create_event)

        complete_event = BuyOrderCompletedEvent(
            timestamp=1642020000,
            order_id=create_event.order_id,
            base_asset=self.base,
            quote_asset=self.quote,
            base_asset_amount=create_event.amount,
            quote_asset_amount=create_event.amount * create_event.price,
            order_type=create_event.type,
        )

        recorder._did_complete_order(MarketEvent.BuyOrderCompleted.value, self, complete_event)

        with self.manager.get_new_session() as session:
            query = session.query(Order)
            orders = query.all()
            order = orders[0]
            order_status = order.status
            trade_fills = order.trade_fills

        self.assertEqual(1, len(orders))
        self.assertEqual(self.config_file_path, orders[0].config_file_path)
        self.assertEqual(create_event.order_id, orders[0].id)
        self.assertEqual(2, len(order_status))
        self.assertEqual(MarketEvent.BuyOrderCreated.name, order_status[0].status)
        self.assertEqual(MarketEvent.BuyOrderCompleted.name, order_status[1].status)
        self.assertEqual(0, len(trade_fills))

    @patch("hummingbot.connector.markets_recorder.MarketsRecorder._sleep")
    def test_market_data_collection_enabled(self, sleep_mock):
        sleep_mock.side_effect = [0.1, asyncio.CancelledError]
        recorder = MarketsRecorder(
            sql=self.manager,
            markets=[self],
            config_file_path=self.config_file_path,
            strategy_name=self.strategy_name,
            market_data_collection=MarketDataCollectionConfigMap(
                market_data_collection_enabled=True,
                market_data_collection_interval=1,
                market_data_collection_depth=20,
            ),
        )
        with patch.object(self, "get_price_by_type") as get_price_by_type:
            # Set the side_effect function to determine return values
            def side_effect(trading_pair, price_type):
                if price_type == PriceType.MidPrice:
                    return Decimal("100")
                elif price_type == PriceType.BestBid:
                    return Decimal("99")
                elif price_type == PriceType.BestAsk:
                    return Decimal("101")

            # Assign the side_effect function to the mock method
            get_price_by_type.side_effect = side_effect
            with patch.object(self, "get_order_book") as get_order_book:
                order_book = OrderBook(dex=False)
                bids_array = np.array([[1, 1, 1], [2, 1, 2], [3, 1, 3]], dtype=np.float64)
                asks_array = np.array([[4, 1, 1], [5, 1, 2], [6, 1, 3], [7, 1, 4]], dtype=np.float64)
                order_book.apply_numpy_snapshot(bids_array, asks_array)
                get_order_book.return_value = order_book
                with self.assertRaises(asyncio.CancelledError):
                    self.async_run_with_timeout(recorder._record_market_data())
        with self.manager.get_new_session() as session:
            query = session.query(MarketData)
            market_data = query.all()
        self.assertEqual(market_data[0].best_ask, Decimal("101"))
        self.assertEqual(market_data[0].best_bid, Decimal("99"))
        self.assertEqual(market_data[0].mid_price, Decimal("100"))

    def test_store_position(self):
        recorder = MarketsRecorder(
            sql=self.manager,
            markets=[self],
            config_file_path=self.config_file_path,
            strategy_name=self.strategy_name,
            market_data_collection=MarketDataCollectionConfigMap(
                market_data_collection_enabled=False,
                market_data_collection_interval=60,
                market_data_collection_depth=20,
            ),
        )

        position = Position(
            id="123",
            timestamp=123,
            controller_id="test_controller",
            connector_name="binance",
            trading_pair="ETH-USDT",
            side=TradeType.BUY.name,
            amount=Decimal("1"),
            breakeven_price=Decimal("1000"),
            unrealized_pnl_quote=Decimal("0"),
            cum_fees_quote=Decimal("0"),
            volume_traded_quote=Decimal("10"),
        )
        recorder.store_position(position)
        with self.manager.get_new_session() as session:
            query = session.query(Position)
            positions = query.all()
        self.assertEqual(1, len(positions))

    def test_store_or_update_executor(self):
        recorder = MarketsRecorder(
            sql=self.manager,
            markets=[self],
            config_file_path=self.config_file_path,
            strategy_name=self.strategy_name,
            market_data_collection=MarketDataCollectionConfigMap(
                market_data_collection_enabled=False,
                market_data_collection_interval=60,
                market_data_collection_depth=20,
            ),
        )
        position_executor_mock = MagicMock(spec=PositionExecutor)
        position_executor_config = PositionExecutorConfig(
            id="123",
            timestamp=1234,
            trading_pair="ETH-USDT",
            connector_name="binance",
            side=TradeType.BUY,
            entry_price=Decimal("1000"),
            amount=Decimal("1"),
            leverage=1,
            triple_barrier_config=TripleBarrierConfig(take_profit=Decimal("0.1"), stop_loss=Decimal("0.2")),
        )
        position_executor_mock.config = position_executor_config
        position_executor_mock.executor_info = ExecutorInfo(
            id="123",
            timestamp=1234,
            type="position_executor",
            close_timestamp=1235,
            close_type=CloseType.TAKE_PROFIT,
            status=RunnableStatus.TERMINATED,
            controller_id="test_controller",
            custom_info={},
            config=position_executor_config,
            net_pnl_pct=Decimal("0.1"),
            net_pnl_quote=Decimal("10"),
            cum_fees_quote=Decimal("0.1"),
            filled_amount_quote=Decimal("1"),
            is_active=False,
            is_trading=False,
        )

        recorder.store_or_update_executor(position_executor_mock)
        with self.manager.get_new_session() as session:
            query = session.query(Executors)
            executors = query.all()
        self.assertEqual(1, len(executors))

    def test_store_completed_arbitrage_executor_in_database(self):
        """
        Tests that the completed arbitrage executor is stored in the database correctly.
        """
        # Setup
        market = MagicMock()
        market_info = MagicMock()
        market_info.market = market
        strategy = MagicMock(spec=ScriptStrategyBase)
        type(strategy).market_info = PropertyMock(return_value=market_info)
        type(strategy).trading_pair = PropertyMock(return_value="BTC-USDT")
        strategy.connectors = {
            "binance_perpetual": MagicMock(),
            "hyperliquid_perpetual": MagicMock(),
        }

        recorder = MarketsRecorder(
            sql=self.manager,
            markets=[self],
            config_file_path=self.config_file_path,
            strategy_name=self.strategy_name,
            market_data_collection=MarketDataCollectionConfigMap(
                market_data_collection_enabled=False,
                market_data_collection_interval=60,
                market_data_collection_depth=20,
            ),
        )

        config = ArbitrageExecutorConfig(
            id="123",
            timestamp=1234,
            controller_id="test",
            buying_market=ConnectorPair(connector_name="binance_perpetual", trading_pair="BTC-USDT"),
            selling_market=ConnectorPair(connector_name="hyperliquid_perpetual", trading_pair="BTC-USD"),
            order_amount=Decimal("1"),
            min_profitability=Decimal("0.1"),
        )
        completed_arbitrage_executor = ArbitrageExecutor(strategy=strategy, config=config)
        completed_arbitrage_executor.buy_order = Mock(spec=TrackedOrder)
        completed_arbitrage_executor.sell_order = Mock(spec=TrackedOrder)
        completed_arbitrage_executor.buy_order.is_filled = True
        completed_arbitrage_executor.sell_order.is_filled = True
        completed_arbitrage_executor.buy_order.executed_amount_base = Decimal("5")
        completed_arbitrage_executor.sell_order.executed_amount_base = Decimal("5")
        completed_arbitrage_executor.buy_order.average_executed_price = Decimal("100")
        completed_arbitrage_executor.sell_order.average_executed_price = Decimal("101")
        completed_arbitrage_executor._status = RunnableStatus.TERMINATED
        completed_arbitrage_executor.close_type = CloseType.COMPLETED
        completed_arbitrage_executor.buy_order.cum_fees_quote = Decimal("1")
        completed_arbitrage_executor.sell_order.cum_fees_quote = Decimal("1")

        completed_arbitrage_executor.buy_order.order.executed_amount_base = Decimal("5")
        completed_arbitrage_executor.sell_order.order.executed_amount_base = Decimal("5")

        # Act
        recorder.store_or_update_executor(completed_arbitrage_executor)

        # Validate
        with self.manager.get_new_session() as session:
            query = session.query(Executors)
            executors: List[Executors] = query.all()
        self.assertEqual(1, len(executors))
        self.assertEqual("123", executors[0].id)
        self.assertEqual("test", executors[0].controller_id)
        self.assertEqual("arbitrage_executor", executors[0].type)
        self.assertEqual(9, executors[0].close_type)
        self.assertEqual(4, executors[0].status)
        self.assertEqual("binance_perpetual", executors[0].buy_market)
        self.assertEqual("hyperliquid_perpetual", executors[0].sell_market)
        self.assertEqual("BTC-USDT", executors[0].buy_pair)
        self.assertEqual("BTC-USD", executors[0].sell_pair)
        self.assertEqual(Decimal("5"), executors[0].buy_executed_amount_base)
        self.assertEqual(Decimal("5"), executors[0].sell_executed_amount_base)
        self.assertEqual(Decimal("100"), executors[0].buy_avg_executed_price)
        self.assertEqual(Decimal("101"), executors[0].sell_avg_executed_price)

    def test_store_failed_arbitrage_executor_in_database(self):
        """
        Tests that the failed arbitrage executor is stored in the database correctly.
        """
        # Setup
        market = MagicMock()
        market_info = MagicMock()
        market_info.market = market
        strategy = MagicMock(spec=ScriptStrategyBase)
        type(strategy).market_info = PropertyMock(return_value=market_info)
        type(strategy).trading_pair = PropertyMock(return_value="BTC-USDT")
        strategy.connectors = {
            "binance_perpetual": MagicMock(),
            "hyperliquid_perpetual": MagicMock(),
        }

        recorder = MarketsRecorder(
            sql=self.manager,
            markets=[self],
            config_file_path=self.config_file_path,
            strategy_name=self.strategy_name,
            market_data_collection=MarketDataCollectionConfigMap(
                market_data_collection_enabled=False,
                market_data_collection_interval=60,
                market_data_collection_depth=20,
            ),
        )

        config = ArbitrageExecutorConfig(
            id="123",
            timestamp=1234,
            controller_id="test",
            buying_market=ConnectorPair(connector_name="binance_perpetual", trading_pair="BTC-USDT"),
            selling_market=ConnectorPair(connector_name="hyperliquid_perpetual", trading_pair="BTC-USD"),
            order_amount=Decimal("1"),
            min_profitability=Decimal("0.1"),
        )
        completed_arbitrage_executor = ArbitrageExecutor(strategy=strategy, config=config)
        completed_arbitrage_executor.buy_order = Mock(spec=TrackedOrder)
        completed_arbitrage_executor.sell_order = Mock(spec=TrackedOrder)
        completed_arbitrage_executor.buy_order.is_filled = False
        completed_arbitrage_executor.sell_order.is_filled = False
        completed_arbitrage_executor.buy_order.executed_amount_base = Decimal("0")
        completed_arbitrage_executor.sell_order.executed_amount_base = Decimal("0")
        completed_arbitrage_executor.buy_order.average_executed_price = Decimal("0")
        completed_arbitrage_executor.sell_order.average_executed_price = Decimal("0")
        completed_arbitrage_executor._status = RunnableStatus.TERMINATED
        completed_arbitrage_executor.close_type = CloseType.FAILED
        completed_arbitrage_executor.buy_order.cum_fees_quote = Decimal("0")
        completed_arbitrage_executor.sell_order.cum_fees_quote = Decimal("0")

        completed_arbitrage_executor.buy_order.order.executed_amount_base = Decimal("0")
        completed_arbitrage_executor.sell_order.order.executed_amount_base = Decimal("0")

        # Act
        recorder.store_or_update_executor(completed_arbitrage_executor)

        # Validate
        with self.manager.get_new_session() as session:
            query = session.query(Executors)
            executors: List[Executors] = query.all()
        self.assertEqual(1, len(executors))
        self.assertEqual("123", executors[0].id)
        self.assertEqual("test", executors[0].controller_id)
        self.assertEqual("arbitrage_executor", executors[0].type)
        self.assertEqual(8, executors[0].close_type)
        self.assertEqual(4, executors[0].status)
        self.assertEqual("binance_perpetual", executors[0].buy_market)
        self.assertEqual("hyperliquid_perpetual", executors[0].sell_market)
        self.assertEqual("BTC-USDT", executors[0].buy_pair)
        self.assertEqual("BTC-USD", executors[0].sell_pair)
        self.assertEqual(Decimal("0"), executors[0].buy_executed_amount_base)
        self.assertEqual(Decimal("0"), executors[0].sell_executed_amount_base)
        self.assertEqual(Decimal("0"), executors[0].buy_avg_executed_price)
        self.assertEqual(Decimal("0"), executors[0].sell_avg_executed_price)

    def test_store_one_side_failed_arbitrage_executor_in_database(self):
        """
        Tests that the half failed arbitrage executor is stored in the database correctly.
        """
        # Setup
        market = MagicMock()
        market_info = MagicMock()
        market_info.market = market
        strategy = MagicMock(spec=ScriptStrategyBase)
        type(strategy).market_info = PropertyMock(return_value=market_info)
        type(strategy).trading_pair = PropertyMock(return_value="BTC-USDT")
        strategy.connectors = {
            "binance_perpetual": MagicMock(),
            "hyperliquid_perpetual": MagicMock(),
        }

        recorder = MarketsRecorder(
            sql=self.manager,
            markets=[self],
            config_file_path=self.config_file_path,
            strategy_name=self.strategy_name,
            market_data_collection=MarketDataCollectionConfigMap(
                market_data_collection_enabled=False,
                market_data_collection_interval=60,
                market_data_collection_depth=20,
            ),
        )

        config = ArbitrageExecutorConfig(
            id="123",
            timestamp=1234,
            controller_id="test",
            buying_market=ConnectorPair(connector_name="binance_perpetual", trading_pair="BTC-USDT"),
            selling_market=ConnectorPair(connector_name="hyperliquid_perpetual", trading_pair="BTC-USD"),
            order_amount=Decimal("1"),
            min_profitability=Decimal("0.1"),
        )
        completed_arbitrage_executor = ArbitrageExecutor(strategy=strategy, config=config)
        completed_arbitrage_executor.buy_order = Mock(spec=TrackedOrder)
        completed_arbitrage_executor.sell_order = Mock(spec=TrackedOrder)
        completed_arbitrage_executor.buy_order.is_filled = True
        completed_arbitrage_executor.sell_order.is_filled = False
        completed_arbitrage_executor.buy_order.executed_amount_base = Decimal(10)
        completed_arbitrage_executor.sell_order.executed_amount_base = Decimal("0")
        completed_arbitrage_executor.buy_order.average_executed_price = Decimal(45)
        completed_arbitrage_executor.sell_order.average_executed_price = Decimal("0")
        completed_arbitrage_executor._status = RunnableStatus.TERMINATED
        completed_arbitrage_executor.close_type = CloseType.ONE_SIDE_FAILED
        completed_arbitrage_executor.buy_order.cum_fees_quote = Decimal(1)
        completed_arbitrage_executor.sell_order.cum_fees_quote = Decimal("0")

        completed_arbitrage_executor.buy_order.order.executed_amount_base = Decimal(10)
        completed_arbitrage_executor.sell_order.order.executed_amount_base = Decimal("0")

        # Act
        recorder.store_or_update_executor(completed_arbitrage_executor)

        # Validate
        with self.manager.get_new_session() as session:
            query = session.query(Executors)
            executors: List[Executors] = query.all()
        self.assertEqual(1, len(executors))
        self.assertEqual("123", executors[0].id)
        self.assertEqual("test", executors[0].controller_id)
        self.assertEqual("arbitrage_executor", executors[0].type)
        self.assertEqual(11, executors[0].close_type)
        self.assertEqual(4, executors[0].status)
        self.assertEqual("binance_perpetual", executors[0].buy_market)
        self.assertEqual("hyperliquid_perpetual", executors[0].sell_market)
        self.assertEqual("BTC-USDT", executors[0].buy_pair)
        self.assertEqual("BTC-USD", executors[0].sell_pair)
        self.assertEqual(Decimal(10), executors[0].buy_executed_amount_base)
        self.assertEqual(Decimal("0"), executors[0].sell_executed_amount_base)
        self.assertEqual(Decimal(45), executors[0].buy_avg_executed_price)
        self.assertEqual(Decimal("0"), executors[0].sell_avg_executed_price)

    def test_records_funding_payment_with_associated_trade_details(self):
        # Arrange
        mock_market = MagicMock()
        mock_market.display_name = "binance"

        mock_event = MagicMock(spec=FundingPaymentCompletedEvent)
        mock_event.timestamp = 2000
        mock_event.trading_pair = "BTC-USDT"
        mock_event.amount = Decimal("10.5")
        mock_event.funding_rate = Decimal("0.001")

        mock_funding_trade = FundingTrade(
            id="trade123",
            start_time=1000,
            end_time=None,
            long_market="binance",
            long_pair="BTC-USDT",
            short_market="binance",
            short_pair="BTC-USDT",
        )
        with self.manager.get_new_session() as session:
            session.add(mock_funding_trade)
            session.commit()

        mock_market_pair_info = MagicMock()
        mock_market_pair_info.price_type = PVPriceType.AVG_ENTRY

        mock_position_metrics = MagicMock()
        mock_position_metrics.get_position_size.return_value = Decimal("1.5")
        mock_position_metrics.get_position_avg_entry_price.return_value = Decimal("20000")

        recorder = MarketsRecorder(
            sql=self.manager,
            markets=[self],
            config_file_path=self.config_file_path,
            strategy_name=self.strategy_name,
            market_data_collection=MarketDataCollectionConfigMap(
                market_data_collection_enabled=False,
                market_data_collection_interval=60,
                market_data_collection_depth=20,
            ),
        )
        recorder._position_metrics = mock_position_metrics
        recorder._ev_loop = MagicMock()

        # Act
        with patch("hummingbot.connector.markets_recorder.get_market_pair_info", return_value=mock_market_pair_info):
            recorder._did_complete_funding_payment(1, mock_market, mock_event)

        # Assert
        with self.manager.get_new_session() as session:
            query = session.query(FundingPayment)
            added_records: List[FundingPayment] = query.all()
        self.assertEqual(len(added_records), 1)
        added_record = added_records[0]
        self.assertEqual(added_record.timestamp, 2000)
        self.assertEqual(added_record.config_file_path, self.config_file_path)
        self.assertEqual(added_record.market, "binance")
        self.assertEqual(added_record.rate, 0.001)
        self.assertEqual(added_record.symbol, "BTC-USDT")
        self.assertEqual(added_record.amount, float(Decimal("10.5")))
        self.assertEqual(added_record.trade_id, "trade123")
        self.assertEqual(added_record.price_type, PVPriceType.AVG_ENTRY.value)
        self.assertEqual(added_record.trade_position_value, float(Decimal("30000")))
        self.assertEqual(added_record.trade_position_exposure, float(Decimal("30000")))

    def test_records_funding_payment_no_trade_details_out_of_time_window_or_dif_pair(self):
        # Arrange
        mock_market = MagicMock()
        mock_market.display_name = "binance"

        mock_event = MagicMock(spec=FundingPaymentCompletedEvent)
        mock_event.timestamp = 500
        mock_event.trading_pair = "BTC-USDT"
        mock_event.amount = Decimal("10.5")
        mock_event.funding_rate = Decimal("0.001")

        funding_trade_out_of_time_window = FundingTrade(
            id="trade123",
            start_time=1000,
            end_time=None,
            long_market="binance",
            long_pair="BTC-USDT",
            short_market="binance",
            short_pair="BTC-USDT",
        )
        funding_trade_different_pair = FundingTrade(
            id="trade1234",
            start_time=10,
            end_time=1100,
            long_market="binance",
            long_pair="ETH-USDT",
            short_market="binance",
            short_pair="ETH-USDT",
        )
        with self.manager.get_new_session() as session:
            session.add(funding_trade_out_of_time_window)
            session.add(funding_trade_different_pair)
            session.commit()

        mock_market_pair_info = MagicMock()
        mock_market_pair_info.price_type = PVPriceType.AVG_ENTRY

        recorder = MarketsRecorder(
            sql=self.manager,
            markets=[self],
            config_file_path=self.config_file_path,
            strategy_name=self.strategy_name,
            market_data_collection=MarketDataCollectionConfigMap(
                market_data_collection_enabled=False,
                market_data_collection_interval=60,
                market_data_collection_depth=20,
            ),
        )
        recorder._ev_loop = MagicMock()

        # Act
        with patch("hummingbot.connector.markets_recorder.get_market_pair_info", return_value=mock_market_pair_info):
            recorder._did_complete_funding_payment(1, mock_market, mock_event)

        # Assert
        with self.manager.get_new_session() as session:
            query = session.query(FundingPayment)
            added_records: List[FundingPayment] = query.all()
        self.assertEqual(len(added_records), 1)
        added_record = added_records[0]
        self.assertEqual(added_record.timestamp, 500)
        self.assertEqual(added_record.config_file_path, self.config_file_path)
        self.assertEqual(added_record.market, "binance")
        self.assertEqual(added_record.rate, 0.001)
        self.assertEqual(added_record.symbol, "BTC-USDT")
        self.assertEqual(added_record.amount, float(Decimal("10.5")))
        self.assertEqual(added_record.trade_id, None)
        self.assertEqual(added_record.price_type, PVPriceType.AVG_ENTRY.value)
        self.assertEqual(added_record.trade_position_value, None)
        self.assertEqual(added_record.trade_position_exposure, None)

    # Handles different price type calculations (AVG_ENTRY vs other types)
    def test_handles_different_price_type_calculations(self):
        # Arrange
        mock_sql_manager = MagicMock()
        mock_session = MagicMock()
        mock_sql_manager.get_new_session.return_value.__enter__.return_value = mock_session

        mock_market = MagicMock()
        mock_market.display_name = "binance"

        mock_event = MagicMock(spec=FundingPaymentCompletedEvent)
        mock_event.timestamp = 1234567890
        mock_event.trading_pair = "BTC-USDT"
        mock_event.amount = Decimal("10.5")
        mock_event.funding_rate = Decimal("0.001")

        mock_position_metrics = MagicMock()
        mock_position_metrics.get_position_size.return_value = Decimal("2.0")
        mock_position_metrics.get_position_avg_entry_price.return_value = Decimal("50000.0")

        recorder = MarketsRecorder(
            sql=mock_sql_manager,
            markets=[mock_market],
            config_file_path="conf/config.yml",
            strategy_name="test_strategy",
            market_data_collection=MarketDataCollectionConfigMap(
                market_data_collection_enabled=False,
                market_data_collection_interval=60,
                market_data_collection_depth=20,
            ),
        )
        recorder._ev_loop = MagicMock()
        recorder._position_metrics = mock_position_metrics

        # No existing payment record
        mock_session.query().filter().one_or_none.return_value = None

        # Mock associated trade
        mock_associated_trade = FundingTrade(
            id="trade123",
            start_time=1234567000,
            end_time=1234568000,
            long_market="binance",
            long_pair="BTC-USDT",
            short_market="binance",
            short_pair="BTC-USDT",
        )

        with patch(
            "hummingbot.model.funding_trade.FundingTrade.find_funding_trade", return_value=mock_associated_trade
        ):
            # Test with AVG_ENTRY price type
            mock_market_pair_info = MagicMock()
            mock_market_pair_info.price_type = PVPriceType.AVG_ENTRY
            with patch(
                "hummingbot.connector.markets_recorder.get_market_pair_info",
                return_value=mock_market_pair_info,
            ):
                # Act
                recorder._did_complete_funding_payment(1, mock_market, mock_event)

                # Assert for AVG_ENTRY price type
                mock_session.add.assert_called_once()
                added_record = mock_session.add.call_args[0][0]
                self.assertEqual(added_record.price_type, PVPriceType.AVG_ENTRY.value)
                # For AVG_ENTRY, both values should be position_size * avg_entry_price
                expected_value = float(Decimal("2.0") * Decimal("50000.0"))
                self.assertEqual(added_record.trade_position_value, expected_value)
                self.assertEqual(added_record.trade_position_exposure, expected_value)

            # Reset mocks for second test
            mock_session.reset_mock()

            # Test with a different price type
            mock_market_pair_info.price_type = PVPriceType.UNKNOWN

            with patch(
                "hummingbot.connector.markets_recorder.get_market_pair_info",
                return_value=mock_market_pair_info,
            ):
                # Act again
                recorder._did_complete_funding_payment(1, mock_market, mock_event)

                # Assert for other price type
                added_record = mock_session.add.call_args[0][0]
                self.assertEqual(added_record.price_type, PVPriceType.UNKNOWN.value)
                # For other price types, position_value is calculated from funding payment
                expected_position_value = float(Decimal("10.5") / Decimal("0.001"))
                expected_position_exposure = float(Decimal("2.0") * Decimal("50000.0"))
                self.assertEqual(added_record.trade_position_value, expected_position_value)
                self.assertEqual(added_record.trade_position_exposure, expected_position_exposure)
