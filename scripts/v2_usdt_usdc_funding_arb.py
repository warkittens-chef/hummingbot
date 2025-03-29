import os
from decimal import Decimal
from typing import Dict, List, Set

import pandas as pd  # type: ignore
from hummingbot.connector.connector_base import ConnectorBase  # type: ignore
from hummingbot.core.clock import Clock  # type: ignore
from pydantic import Field, validator

from hummingbot.client.config.config_data_types import ClientFieldData
from hummingbot.client.ui.interface_utils import format_df_for_printout
from hummingbot.core.data_type.common import OrderType, PositionAction, PositionMode, PriceType, TradeType
from hummingbot.core.data_type.funding_info import FundingInfo
from hummingbot.core.data_type.order_candidate import PerpetualOrderCandidate
from hummingbot.core.event.events import FundingPaymentCompletedEvent
from hummingbot.data_feed.candles_feed.data_types import CandlesConfig
from hummingbot.strategy.strategy_v2_base import StrategyV2Base, StrategyV2ConfigBase
from hummingbot.strategy_v2.executors.position_executor.data_types import PositionExecutorConfig, TripleBarrierConfig
from hummingbot.strategy_v2.models.executor_actions import CreateExecutorAction, StopExecutorAction


class FundingInfoReport:
    """
    This is a little wrapper class to concretely define funding info maps. A report is is meant to contain info
    for a single base token on a single exchange, where funding_info_dict is:
    trading pair --> FundingInfo
    """

    def __init__(self, connector_name: str, base_token: str, funding_info: tuple[FundingInfo, FundingInfo]):
        self.connector_name = connector_name
        self.base_token = base_token
        self.funding_rates = funding_info


class FundingArbitrageTradeInfo:
    def __init__(
        self,
        connector_name: str,
        base_token: str,
        pairs: tuple[str, str],
        executor_ids: tuple[str, str],
        first_executor_side: TradeType,
        funding_payments: list[FundingPaymentCompletedEvent],
    ):
        self.connector_name = connector_name
        self.base_token = base_token
        self.pairs = pairs
        self.executor_ids = executor_ids
        self.first_executor_side = first_executor_side
        self.funding_payments = funding_payments

    def add_payment(self, payment_event: FundingPaymentCompletedEvent) -> None:
        self.funding_payments.append(payment_event)


class Percent:
    def __init__(self, percent_value: float):
        self.val = percent_value

    def as_fraction(self) -> float:
        return self.val * 100


class QuoteFundingRateArbitrageConfig(StrategyV2ConfigBase):
    script_file_name: str = Field(default_factory=lambda: os.path.basename(__file__))
    candles_config: List[CandlesConfig] = []
    controllers_config: List[str] = []
    markets: Dict[str, Set[str]] = {}
    leverage: int = Field(
        default=20,
        gt=0,
        client_data=ClientFieldData(prompt=lambda mi: "Enter the leverage (e.g. 20): ", prompt_on_new=True),
    )

    min_funding_rate_profitability: Decimal = Field(
        default=0.001,
        client_data=ClientFieldData(
            prompt=lambda mi: "Enter the min funding rate profitability to enter in a position: ", prompt_on_new=True
        ),
    )
    connector_names: Set[str] = Field(
        default="hyperliquid_perpetual,binance_perpetual",
        client_data=ClientFieldData(
            prompt_on_new=True,
            prompt=lambda mi: "Enter the connectors separated by commas:",
        ),
    )
    tokens: Set[str] = Field(
        default="WIF,FET",
        client_data=ClientFieldData(
            prompt_on_new=True,
            prompt=lambda mi: "Enter the tokens separated by commas:",
        ),
    )
    position_size_quote: Decimal = Field(
        default=100,
        client_data=ClientFieldData(
            prompt_on_new=True,
            prompt=lambda mi: "Enter the position size for each token and exchange (e.g. order amount 100 will open 100 long on hyperliquid and 100 short on binance):",
        ),
    )
    profitability_to_take_profit: Decimal = Field(
        default=0.01,
        client_data=ClientFieldData(
            prompt_on_new=True,
            prompt=lambda mi: "Enter the profitability to take profit (including PNL of positions and fundings received): ",
        ),
    )
    funding_rate_diff_stop_loss: Decimal = Field(
        default=-0.001,
        client_data=ClientFieldData(
            prompt=lambda mi: "Enter the funding rate difference to stop the position: ", prompt_on_new=True
        ),
    )
    trade_profitability_condition_to_enter: bool = Field(
        default=False,
        client_data=ClientFieldData(
            prompt=lambda mi: "Create the position if the trade profitability is positive only: ", prompt_on_new=True
        ),
    )

    @validator("connector_names", "tokens", pre=True, allow_reuse=True, always=True)
    def validate_sets(cls, v):  # type: ignore
        if isinstance(v, str):
            return set(v.split(","))
        return v


class QuoteFundingRateArbitrage(StrategyV2Base):
    """
    A strategy that seeks funding arbitrage opportunities where a single exchange offers both USDC and USDT quote pairs
    of the same base currency.
    """

    funding_payment_interval_map = {"bybit_perpetual": {"ENA": {"USDT": 60 * 60 * 4, "USDC": 60 * 60 * 8}}}
    funding_profitability_interval = 60 * 60 * 24

    @classmethod
    def get_trading_pairs_for_token(cls, token: str) -> tuple[str, str]:
        return f"{token}-USDT", f"{token}-USDC"

    @classmethod
    def init_markets(cls, config: QuoteFundingRateArbitrageConfig) -> None:  # type: ignore
        """
        Creates a dict for basic market info mapping the exchange name to the trading pair names. This is used by
        start_command.py to establish connectors prior to initializing this class
        """
        markets: dict[str, set[str]] = {}
        for connector_name in config.connector_names:
            for token in config.tokens:
                trading_pairs = cls.get_trading_pairs_for_token(token)
                if not markets[connector_name]:
                    markets[connector_name] = set()
                if trading_pairs:
                    markets[connector_name].add(trading_pairs[0])
                    markets[connector_name].add(trading_pairs[1])
        cls.markets = markets

    def __init__(self, connectors: Dict[str, ConnectorBase], config: QuoteFundingRateArbitrageConfig):
        """
        Every strategy upon initialization is given a map of online connectors and a filled out config map based on
        the chosen conf file in the user-defined CLI start statement.
        """
        super().__init__(connectors, config)
        self.config: QuoteFundingRateArbitrageConfig = config
        self.active_funding_arbitrages: dict[str, FundingArbitrageTradeInfo] = {}
        self.stopped_funding_arbitrages: dict[str, list[FundingArbitrageTradeInfo]] = {
            token: [] for token in self.config.tokens
        }

    def start(self, clock: Clock, timestamp: float) -> None:
        """
        Start the strategy.
        :param clock: Clock to use.
        :param timestamp: Current time.
        """
        self._last_timestamp = timestamp
        self.apply_initial_setting()

    def apply_initial_setting(self) -> None:
        for connector_name, connector in self.connectors.items():
            if self.is_perpetual(connector_name):
                position_mode = PositionMode.ONEWAY
                connector.set_position_mode(position_mode)
                for trading_pair in self.market_data_provider.get_trading_pairs(connector_name):
                    connector.set_leverage(trading_pair, self.config.leverage)

    def get_funding_info_by_token_and_connector(self, token: str, connector_name: str) -> FundingInfoReport:
        """
        This method provides the funding rates across all the connectors. Returns a map of connector to funding info
        of a particular token.

        Refactored:
        Returns a map of trading_pair to funding info of a particular token for a particular connector.
        """
        connector = self.connectors.get(connector_name)
        if connector is None:
            raise Exception(f'Could not find connector "{connector_name}".')
        trading_pairs = self.get_trading_pairs_for_token(token)
        funding_rates: tuple[FundingInfo, FundingInfo] = (
            connector.get_funding_info(trading_pairs[0]),
            connector.get_funding_info(trading_pairs[1]),
        )
        funding_info_report = FundingInfoReport(connector_name, token, funding_rates)
        return funding_info_report

    def get_current_profitability_after_fees(
        self, connector_name: str, trading_pair_1: str, trading_pair_2: str, trading_pair_1_side: TradeType
    ) -> Decimal:
        """
        This method compares the profitability of buying at market in the two exchanges. If the side is TradeType.BUY
        means that the operation is long on connector 1 and short on connector 2.

        Refactored:
        This method estimates the profitability of opening a funding basis trade on two pairs on the same exchange.
        """

        pair_1_price = Decimal(
            self.market_data_provider.get_price_for_quote_volume(
                connector_name=connector_name,
                trading_pair=trading_pair_1,
                quote_volume=self.config.position_size_quote,  # type: ignore
                is_buy=trading_pair_1_side == TradeType.BUY,
            ).result_price
        )
        pair_2_price = Decimal(
            self.market_data_provider.get_price_for_quote_volume(
                connector_name=connector_name,
                trading_pair=trading_pair_2,
                quote_volume=self.config.position_size_quote,  # type: ignore
                is_buy=trading_pair_1_side != TradeType.BUY,
            ).result_price
        )
        estimated_fees_pair_1 = (
            self.connectors[connector_name]
            .get_fee(
                base_currency=trading_pair_1.split("-")[0],
                quote_currency=trading_pair_1.split("-")[1],
                order_type=OrderType.MARKET,
                order_side=TradeType.BUY,  # TODO: Might want to revisit if these estimated fees are accurate
                amount=self.config.position_size_quote / pair_1_price,
                price=pair_1_price,
                is_maker=False,
                position_action=PositionAction.OPEN,
            )
            .percent
        )
        estimated_fees_pair_2 = (
            self.connectors[connector_name]
            .get_fee(
                base_currency=trading_pair_2.split("-")[0],
                quote_currency=trading_pair_2.split("-")[1],
                order_type=OrderType.MARKET,
                order_side=TradeType.BUY,
                amount=self.config.position_size_quote / pair_2_price,
                price=pair_2_price,
                is_maker=False,
                position_action=PositionAction.OPEN,
            )
            .percent
        )

        if trading_pair_1_side == TradeType.BUY:
            estimated_trade_pnl_pct = (pair_2_price - pair_1_price) / pair_1_price
        else:
            estimated_trade_pnl_pct = (pair_1_price - pair_2_price) / pair_2_price
        return estimated_trade_pnl_pct - Decimal(estimated_fees_pair_1) - Decimal(estimated_fees_pair_2)

    def get_most_profitable_combination(
        self, funding_info_report: FundingInfoReport
    ) -> tuple[FundingInfo, FundingInfo, TradeType, Decimal] | None:
        """
        This method returns the most profitable funding basis trade across two exchanges for a given token.

        Refactored:
        This method returns the most profitable quote pair for a funding basis trade on a single exchange for a given
        base token.
        """
        best_combination = None
        highest_profitability = Decimal(0)
        connector_name = funding_info_report.connector_name
        for pair_1_funding in funding_info_report.funding_rates:
            for pair_2_funding in funding_info_report.funding_rates:
                if pair_1_funding != pair_2_funding:
                    rate_for_pair_1 = self.get_normalized_funding_rate_in_seconds(connector_name, pair_1_funding)
                    rate_for_pair_2 = self.get_normalized_funding_rate_in_seconds(connector_name, pair_2_funding)
                    funding_rate_diff: Decimal = (
                        abs(rate_for_pair_1 - rate_for_pair_2) * self.funding_profitability_interval
                    )
                    if funding_rate_diff > highest_profitability:
                        trade_side = TradeType.BUY if rate_for_pair_1 < rate_for_pair_2 else TradeType.SELL
                        highest_profitability = funding_rate_diff
                        best_combination = (pair_1_funding, pair_2_funding, trade_side, funding_rate_diff)
        return best_combination

    def get_normalized_funding_rate_in_seconds(self, connector_name: str, funding_info: FundingInfo) -> Decimal:
        return (
            funding_info.rate
            / self.funding_payment_interval_map[connector_name][funding_info.trading_pair.split("-")[0]][
                funding_info.trading_pair.split("-")[0]
            ]
        )

    def create_actions_proposal(self) -> List[CreateExecutorAction]:
        """
        In this method we are going to evaluate if a new set of positions has to be created for each of the tokens that
        don't have an active arbitrage.
        More filters can be applied to limit the creation of the positions, since the current logic is only checking for
        positive pnl between funding rate. Is logged and computed the trading profitability at the time for entering
        at market to open the possibilities for other people to create variations like sending limit position executors
        and if one gets filled buy market the other one to improve the entry prices.
        """
        create_actions: list[CreateExecutorAction] = []
        for connector_name in self.connectors:
            for token in self.config.tokens:
                if token not in self.active_funding_arbitrages:
                    funding_info_report: FundingInfoReport = self.get_funding_info_by_token_and_connector(
                        token, connector_name
                    )
                    best_combination = self.get_most_profitable_combination(funding_info_report)
                    if not best_combination:
                        continue
                    self.logger().info(best_combination)
                    trading_pair_1, trading_pair_2, trade_side, expected_profitability = best_combination
                    if expected_profitability >= self.config.min_funding_rate_profitability:
                        current_profitability = self.get_current_profitability_after_fees(
                            token, trading_pair_1.trading_pair, trading_pair_2.trading_pair, trade_side
                        )
                        if self.config.trade_profitability_condition_to_enter:
                            if current_profitability < 0:  # 0.0005 = 0.05%
                                self.logger().info(
                                    f"Best Combination: {trading_pair_1} | {trading_pair_2} | {trade_side}"
                                    f"Funding rate profitability: {expected_profitability}"
                                    f"Trading profitability after fees: {current_profitability}"
                                    f"Trade profitability is negative, skipping..."
                                )
                                continue
                        self.logger().info(
                            f"Best Combination: {trading_pair_1} | {trading_pair_2} | {trade_side}"
                            f"Funding rate profitability: {expected_profitability}"
                            f"Trading profitability after fees: {current_profitability}"
                            f"Starting executors..."
                        )
                        position_executor_config_1, position_executor_config_2 = self.get_position_executors_config(
                            connector_name, trading_pair_1.trading_pair, trading_pair_2.trading_pair, trade_side
                        )
                        self.active_funding_arbitrages[token] = FundingArbitrageTradeInfo(
                            connector_name,
                            token,
                            (trading_pair_1.trading_pair, trading_pair_2.trading_pair),
                            (position_executor_config_1.id, position_executor_config_2.id),
                            trade_side,
                            [],
                        )
                        return [
                            CreateExecutorAction(executor_config=position_executor_config_1),
                            CreateExecutorAction(executor_config=position_executor_config_2),
                        ]
        return create_actions

    def check_if_both_sides_executable(
        self, executor_config_1: PositionExecutorConfig, executor_config_2: PositionExecutorConfig
    ) -> bool:
        """
        This method is meant to preliminarily check if user wallet balances on both exchanges are sufficient for the
        proposed arbitrage open trade
        """
        open_order_price_type = PriceType.BestBid if executor_config_1.side == TradeType.BUY else PriceType.BestAsk
        executor_1_entry_price = self.connectors[executor_config_1.connector_name].get_price_by_type(
            executor_config_1.trading_pair, open_order_price_type
        )
        executor_1_order = PerpetualOrderCandidate(
            trading_pair=executor_config_1.trading_pair,
            is_maker=executor_config_1.triple_barrier_config.open_order_type.is_limit_type(),  # type: ignore
            order_type=executor_config_1.triple_barrier_config.open_order_type,
            order_side=executor_config_1.side,
            amount=executor_config_1.amount,
            price=executor_1_entry_price,
            leverage=Decimal(executor_config_1.leverage),
        )

        open_order_price_type = PriceType.BestBid if executor_config_1.side == TradeType.BUY else PriceType.BestAsk
        executor_2_entry_price = self.connectors[executor_config_2.connector_name].get_price_by_type(
            executor_config_2.trading_pair, open_order_price_type
        )
        executor_2_order = PerpetualOrderCandidate(
            trading_pair=executor_config_2.trading_pair,
            is_maker=executor_config_2.triple_barrier_config.open_order_type.is_limit_type(),  # type: ignore
            order_type=executor_config_2.triple_barrier_config.open_order_type,
            order_side=executor_config_2.side,
            amount=executor_config_2.amount,
            price=executor_2_entry_price,
            leverage=Decimal(executor_config_2.leverage),
        )

        order_1_available = True
        order_2_available = True
        adjusted_order_candidates = self.connectors[executor_config_1.connector_name].budget_checker.adjust_candidates(
            [executor_1_order]
        )
        if adjusted_order_candidates[0].amount == Decimal("0"):
            order_1_available = False
            self.logger().error(f"Not enough budget to open position for {executor_config_1.connector_name}")
        adjusted_order_candidates = self.connectors[executor_config_2.connector_name].budget_checker.adjust_candidates(
            [executor_2_order]
        )
        if adjusted_order_candidates[0].amount == Decimal("0"):
            order_2_available = False
            self.logger().error(f"Not enough budget to open position for {executor_config_2.connector_name}")

        return order_1_available and order_2_available

    def stop_actions_proposal(self) -> List[StopExecutorAction]:
        """
        Once the funding rate arbitrage is created we are going to control the funding payments pnl and the current
        pnl of each of the executors at the cost of closing the open position at market.
        If that PNL is greater than the profitability_to_take_profit
        """
        stop_executor_actions: List[StopExecutorAction] = []
        funding_arbitrage_info: FundingArbitrageTradeInfo
        for token, funding_arbitrage_info in self.active_funding_arbitrages.items():
            executors = self.filter_executors(
                executors=self.get_all_executors(), filter_func=lambda x: x.id in funding_arbitrage_info.executor_ids
            )
            funding_payments_pnl = sum(
                funding_payment.amount for funding_payment in funding_arbitrage_info.funding_payments
            )
            executors_pnl = sum(executor.net_pnl_quote for executor in executors)
            take_profit_condition = (
                executors_pnl + funding_payments_pnl
                > self.config.profitability_to_take_profit * self.config.position_size_quote
            )
            connector_name = funding_arbitrage_info.connector_name
            funding_info_report = self.get_funding_info_by_token_and_connector(token, connector_name)
            first_funding_info = funding_info_report.funding_rates[0]
            second_funding_info = funding_info_report.funding_rates[1]
            if funding_arbitrage_info.first_executor_side == TradeType.BUY:
                funding_rate_diff = self.get_normalized_funding_rate_in_seconds(
                    connector_name, second_funding_info
                ) - self.get_normalized_funding_rate_in_seconds(connector_name, first_funding_info)
            else:
                funding_rate_diff = self.get_normalized_funding_rate_in_seconds(
                    connector_name, first_funding_info
                ) - self.get_normalized_funding_rate_in_seconds(connector_name, second_funding_info)
            current_funding_condition = (
                funding_rate_diff * self.funding_profitability_interval < self.config.funding_rate_diff_stop_loss
            )
            if take_profit_condition:
                self.logger().info("Take profit profitability reached, stopping executors (sike not really)")
                self.stopped_funding_arbitrages[token].append(funding_arbitrage_info)
                # stop_executor_actions.extend([StopExecutorAction(executor_id=executor.id) for executor in executors])
            elif current_funding_condition:
                self.logger().info(
                    "Funding rate difference reached for stop loss, stopping executors (sike not really)"
                )
                self.stopped_funding_arbitrages[token].append(funding_arbitrage_info)
                # stop_executor_actions.extend([StopExecutorAction(executor_id=executor.id) for executor in executors])
        return stop_executor_actions

    def did_complete_funding_payment(self, funding_payment_completed_event: FundingPaymentCompletedEvent) -> None:
        """
        Based on the funding payment event received, check if one of the active arbitrages matches to add the event
        to the list.
        """
        token = funding_payment_completed_event.trading_pair.split("-")[0]
        if token in self.active_funding_arbitrages:
            self.active_funding_arbitrages[token].funding_payments.append(funding_payment_completed_event)

    def get_position_executors_config(
        self, connector_name: str, trading_pair_1: str, trading_pair_2: str, trade_side: TradeType
    ) -> tuple[PositionExecutorConfig, PositionExecutorConfig]:
        price = self.market_data_provider.get_price_by_type(
            connector_name=connector_name, trading_pair=trading_pair_1, price_type=PriceType.MidPrice
        )
        # This is the quantity to buy/sell, not dollar amount
        position_amount = self.config.position_size_quote / price

        position_executor_config_1 = PositionExecutorConfig(
            timestamp=self.current_timestamp,
            connector_name=connector_name,
            trading_pair=trading_pair_1,
            side=trade_side,
            amount=position_amount,
            leverage=self.config.leverage,
            triple_barrier_config=TripleBarrierConfig(open_order_type=OrderType.MARKET),  # type: ignore
        )
        position_executor_config_2 = PositionExecutorConfig(
            timestamp=self.current_timestamp,
            connector_name=connector_name,
            trading_pair=trading_pair_2,
            side=TradeType.BUY if trade_side == TradeType.SELL else TradeType.SELL,
            amount=position_amount,
            leverage=self.config.leverage,
            triple_barrier_config=TripleBarrierConfig(open_order_type=OrderType.MARKET),  # type: ignore
        )
        return position_executor_config_1, position_executor_config_2

    def format_status(self) -> str:
        original_status = super().format_status()
        funding_rate_status = []
        if self.ready_to_trade:
            all_funding_info = []
            all_best_paths = []
            for connector_name in self.config.connector_names:
                for token in self.config.tokens:
                    token_info = {"token": token}
                    best_paths_info = {"token": token}
                    funding_info_report = self.get_funding_info_by_token_and_connector(token, connector_name)
                    best_combination = self.get_most_profitable_combination(funding_info_report)
                    if not best_combination:
                        continue
                    for funding_info in funding_info_report.funding_rates:
                        token_info[f"{funding_info.trading_pair} Rate (%)"] = str(
                            self.get_normalized_funding_rate_in_seconds(connector_name, funding_info)
                            * self.funding_profitability_interval
                            * 100
                        )
                    trading_pair_1, trading_pair_2, first_pair_side, funding_rate_diff = best_combination
                    profitability_after_fees = self.get_current_profitability_after_fees(
                        token, trading_pair_1.trading_pair, trading_pair_2.trading_pair, first_pair_side
                    )
                    best_paths_info["Best Path"] = f"{trading_pair_1}_{trading_pair_2}"
                    best_paths_info["Best Rate Diff (%)"] = str(funding_rate_diff * 100)
                    best_paths_info["Trade Profitability (%)"] = str(profitability_after_fees * 100)
                    best_paths_info["Days Trade Prof"] = str(-profitability_after_fees / funding_rate_diff)
                    best_paths_info["Days to TP"] = str(
                        (self.config.profitability_to_take_profit - profitability_after_fees) / funding_rate_diff
                    )

                    time_to_next_funding_info_c1 = (
                        funding_info_report.funding_rates[0].next_funding_utc_timestamp - self.current_timestamp
                    )
                    time_to_next_funding_info_c2 = (
                        funding_info_report.funding_rates[1].next_funding_utc_timestamp - self.current_timestamp
                    )
                    best_paths_info["Min to Funding 1"] = time_to_next_funding_info_c1 / 60
                    best_paths_info["Min to Funding 2"] = time_to_next_funding_info_c2 / 60

                    all_funding_info.append(token_info)
                    all_best_paths.append(best_paths_info)
                funding_rate_status.append(
                    f"\n\n\nMin Funding Rate Profitability: {self.config.min_funding_rate_profitability:.2%}"
                )
                funding_rate_status.append(
                    f"Profitability to Take Profit: {self.config.profitability_to_take_profit:.2%}\n"
                )
                funding_rate_status.append("Funding Rate Info (Funding Profitability in Days): ")
                funding_rate_status.append(
                    format_df_for_printout(
                        df=pd.DataFrame(all_funding_info),
                        table_format="psql",  # type: ignore
                    )
                )
                funding_rate_status.append(
                    format_df_for_printout(
                        df=pd.DataFrame(all_best_paths),
                        table_format="psql",  # type: ignore
                    )
                )
                for token, funding_arbitrage_info in self.active_funding_arbitrages.items():
                    long_pair = (
                        funding_arbitrage_info.pairs[0]
                        if funding_arbitrage_info.first_executor_side == TradeType.BUY
                        else funding_arbitrage_info.pairs[1]
                    )
                    short_pair = (
                        funding_arbitrage_info.pairs[1]
                        if funding_arbitrage_info.first_executor_side == TradeType.BUY
                        else funding_arbitrage_info.pairs[0]
                    )
                    funding_rate_status.append(f"Token: {token}")
                    funding_rate_status.append(f"Long pair: {long_pair} | Short pair: {short_pair}")
                    funding_rate_status.append(f"Funding Payments Collected: {funding_arbitrage_info.funding_payments}")
                    funding_rate_status.append(f"Executors: {funding_arbitrage_info.executor_ids}")
                    funding_rate_status.append("-" * 50 + "\n")
        return original_status + "\n".join(funding_rate_status)
