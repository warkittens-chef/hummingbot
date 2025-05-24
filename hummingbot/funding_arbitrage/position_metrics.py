import logging
from decimal import Decimal

from sqlalchemy import func
from sqlalchemy.orm import Query

from hummingbot.logger import HummingbotLogger
from hummingbot.model.executors import Executors
from hummingbot.model.sql_connection_manager import SQLConnectionManager


class PositionMetrics:
    _logger = None
    _shared_instance: "PositionMetrics" = None

    @classmethod
    def logger(cls) -> HummingbotLogger:
        if cls._logger is None:
            cls._logger = logging.getLogger(__name__)
        return cls._logger

    @classmethod
    def get_instance(cls, *args, **kwargs) -> "PositionMetrics":
        if cls._shared_instance is None:
            cls._shared_instance = PositionMetrics(*args, **kwargs)
        return cls._shared_instance

    def __init__(self, sql: SQLConnectionManager):
        self._sql_manager = sql
        self._shared_instance = self

    def get_position_size(
        self, start_time: float, end_time: float | None, target_market: str, target_pair: str
    ) -> Decimal:
        """
        Given start timestamp, end timestamp, target market and pair combo, and other market and pair combo,
        use sqlalchemy to find all executors of type 'arbitrage_executor',
                            that have a close_type of 9 or 11,
                            that have a close timestamp between the start and end timestamp,
                            that have matching connector pairs.
        Then, aggregates current net size on target connector pair, where if its buy-side, its positive and if its
        sell-side then its negative.

        """

        with self._sql_manager.get_new_session() as session:
            net_buy_size: Query = session.query(func.sum(Executors.buy_executed_amount_base).label("buy_sizes")).filter(
                Executors.type == "arbitrage_executor",
                Executors.close_timestamp.between(start_time, end_time)
                if end_time
                else Executors.close_timestamp >= start_time,
                (Executors.buy_market == target_market),
                (Executors.buy_pair == target_pair),
                Executors.close_type.in_([9, 11]),
            )
            net_sell_size: Query = session.query(
                func.sum(Executors.sell_executed_amount_base).label("sell_sizes")
            ).filter(
                Executors.type == "arbitrage_executor",
                Executors.close_timestamp.between(start_time, end_time)
                if end_time
                else Executors.close_timestamp >= start_time,
                (Executors.sell_market == target_market),
                (Executors.sell_pair == target_pair),
                Executors.close_type.in_([9, 11]),
            )

            net_buy_size_amt = net_buy_size.one_or_none()[0]
            net_buy_size_amt = Decimal(net_buy_size_amt) if net_buy_size_amt else Decimal("0")
            net_sell_size_amt = net_sell_size.one_or_none()[0]
            net_sell_size_amt = Decimal(net_sell_size_amt) if net_sell_size_amt else Decimal("0")

            if net_buy_size_amt or net_sell_size_amt:
                return abs(net_buy_size_amt - net_sell_size_amt)
            else:
                return Decimal("0")

    def get_position_avg_entry_price(
        self, start_time: float, end_time: float | None, target_market: str, target_pair: str, target_net_side: str
    ) -> Decimal:
        """
        Given start and end timestamp, two connector pairs, and a market-pair, finds all applicable arbitrage executors
        and calculates the average entry price of all orders from those executors. Keep in mind downscale orders do not
        affect average entry price.
        Position average entry price is calculated as:
            sum of initial order position values / sum of initial order quantities

        Concerns:
        - if order size and order price are 0, will this raise an error
            - filter out any executors with an order size of 0
        - needs to accommodate short side and long side queries
            - Either find the side that has a bigger net size, or include a side parameter
            - Issue with first is what happens when both sides have the same size
                - The only way to address this is to already know which net-side the target connector pair is on,
                  so go with option 2
        """
        if target_net_side not in ["long", "short"]:
            raise ValueError("Invalid target_net_side. Must be 'long' or 'short'.")

        with self._sql_manager.get_new_session() as session:
            query: Query = (
                session.query(
                    func.sum(
                        (Executors.buy_executed_amount_base * Executors.buy_avg_executed_price).label(
                            "initial_long_exposures"
                        )
                    ),
                    func.sum(Executors.buy_executed_amount_base.label("initial_long_sizes")),
                ).filter(
                    Executors.type == "arbitrage_executor",
                    Executors.close_timestamp.between(start_time, end_time)
                    if end_time
                    else Executors.close_timestamp >= start_time,
                    (Executors.buy_market == target_market),
                    (Executors.buy_pair == target_pair),
                    Executors.close_type.in_([9, 11]),
                    Executors.buy_executed_amount_base > 0,
                )
                if target_net_side == "long"
                else session.query(
                    func.sum(
                        (Executors.sell_executed_amount_base * Executors.sell_avg_executed_price).label(
                            "initial_short_exposures"
                        )
                    ),
                    func.sum(Executors.sell_executed_amount_base.label("initial_short_sizes")),
                ).filter(
                    Executors.type == "arbitrage_executor",
                    Executors.close_timestamp.between(start_time, end_time)
                    if end_time
                    else Executors.close_timestamp >= start_time,
                    (Executors.sell_market == target_market),
                    (Executors.sell_pair == target_pair),
                    Executors.close_type.in_([9, 11]),
                    Executors.sell_executed_amount_base > 0,
                )
            )

            net_initial_exposure, net_initial_size = query.one_or_none()

            net_initial_exposure_amt = Decimal(net_initial_exposure) if net_initial_exposure else Decimal("0")
            net_initial_size_amt = Decimal(net_initial_size) if net_initial_size else Decimal("0")

            if net_initial_exposure_amt and net_initial_size_amt:
                return net_initial_exposure_amt / net_initial_size_amt
            else:
                return Decimal("0")
