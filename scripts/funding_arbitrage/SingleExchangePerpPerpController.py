from decimal import Decimal
from enum import Enum
from typing import List, Set, Dict

from pydantic import Field

from hummingbot.client.config.config_data_types import ClientFieldData
from hummingbot.strategy_v2.controllers import ControllerBase, ControllerConfigBase
from hummingbot.strategy_v2.executors.arbitrage_executor.data_types import ArbitrageExecutorConfig
from hummingbot.strategy_v2.executors.data_types import ConnectorPair
from hummingbot.strategy_v2.models.executor_actions import ExecutorAction
from hummingbot.strategy_v2.models.executors_info import ExecutorInfo
from scripts.funding_arbitrage.FundingTrade import FundingTrade
from scripts.funding_arbitrage.fixed_market_specs import get_all_valid_trades_for_token

"""
Nomenclature:
 - Trade
    - In this case, a trade stands for arbitrage trade
    - It is a single arbitrage setup, defined by 2 positions, 
 - Position
    - i.e. a "side" of a trade
    - It is the aggregate open contract order (in the case of perps) of a single trading pair
 - Amount
    - i.e. value
    - This is the dollar denominated value of X token
 - Size
    - i.e. quantity
    - This is the number of units of X token
"""


class SingleExchangePerpPerpConfig(ControllerConfigBase):
    controller_name: str = "single_exchange_perp_perp"
    leverage: int = Field(
        default=10,
        gt=0,
        client_data=ClientFieldData(
            prompt=lambda mi: "Enter the leverage to make positions with (e.g. 20): ", prompt_on_new=True
        ),
    )
    connector_name: str = Field(
        default="bybit_perpetual",
        client_data=ClientFieldData(
            prompt=lambda mi: "Enter the connector name of the exchange to arbitrage (e.g. hyperliquid_perpetual): ",
            prompt_on_new=True,
        ),
    )
    tokens: Set[str] = Field(
        default="ENA,ONDO",
        client_data=ClientFieldData(
            prompt_on_new=True,
            prompt=lambda mi: "Enter the tokens separated by commas (e.g. ENA,ONDO):",
        ),
    )
    quotes: Set[str] = Field(
        default="USDT, USDC",
        client_data=ClientFieldData(
            prompt_on_new=True,
            prompt=lambda mi: "Enter the quote currencies separated by commas (e.g. USDT,USDC):",
        ),
    )
    """
    This is meant to be the absolute fixed limit of a controller to prevent it from adding positions regardless of
    changes in the balances on the connected exchanges.
    
    In the future, would like to add a feature that dynamically scales active trades up and down based on changes in
    the cross account leverage (ex. non-arb positions were added to account, or funds were deposited or withdrawn). 
    When that feature is added, this field will maintain highest order for added risk management.
    """
    max_controller_allocation_amount: int = Field(
        default=500,
        gt=0,
        client_data=ClientFieldData(
            prompt=lambda mi: "Enter the max dollar amount of the open positions this controller can have at a given time (e.g. 20): ",
            prompt_on_new=True,
        ),
    )
    """
    This is meant to provide risk management localized to individual trades.
    
    In the future, would like the option to have many trades active at once. Would also like to have further risk
    management based on the exact trading pair for a trade. For example, if trading a highly volatile asset like a meme
    or recent token launch that has insane APY, want to have a position in it, but also want a small allocation so that
    I can get out quickly. 
    """
    max_trade_allocation_amount: int = Field(
        default=200,
        gt=0,
        client_data=ClientFieldData(
            prompt=lambda mi: "Enter the max dollar amount a single trade can have active positions with at a given time (e.g. 20): ",
            prompt_on_new=True,
        ),
    )
    inc_order_amount: int = Field(
        default=50,
        gt=0,
        client_data=ClientFieldData(
            prompt=lambda mi: "Enter the upper-bound dollar amount each order in a position (e.g. 20): ",
            prompt_on_new=True,
        ),
    )
    max_order_cost_as_percentage: Decimal = Field(
        default=0.0,
        client_data=ClientFieldData(
            prompt=lambda mi: "Enter the max allowable cost of opening or closing an order (both sides) as a percentage of order: ",
            prompt_on_new=True,
        ),
    )

    # TODO: Write validator to confirm at least two quotes

    def get_trading_pairs(self, token: str) -> Set[str]:
        return {f"{token}-{quote}" for quote in self.quotes}

    def update_markets(self, markets: Dict[str, Set[str]]) -> Dict[str, Set[str]]:
        if self.connector_name not in markets:
            markets[self.connector_name] = set()
        for token in self.tokens:
            for trading_pair in self.get_trading_pairs(token):
                markets[self.connector_name].add(trading_pair)
        return markets


class ControllerState(Enum):
    NO_ACTIVE_TRADES = 0
    SCALING_IN = 1
    ACTIVE_TRADE = 2
    SCALING_OUT = 3
    SWAPPING_TRADE = 4


class SingleExchangePerpPerpController(ControllerBase):
    def __init__(self, config: SingleExchangePerpPerpConfig, *args, **kwargs):
        super().__init__(config, *args, **kwargs)
        self.curr_controller_position_amount = None
        self.config = config
        self.state = ControllerState.NO_ACTIVE_TRADES
        self.opening_trade: FundingTrade = None
        self.closing_trade: FundingTrade = None
        self.active_trades: List[FundingTrade] = []

    def update_processed_data(self):
        """Determine the current state of the controller based on most recent market data i.e. determine the info:
        - Current cross account leverage
        - Total active trade amounts
        - Total controller trade amounts
        - Cumulative historic funding fees for controller and trades
        - Cumulative daily funding fees over the last 14 days for controller and trades
        - Average price discrepancy PnL for opening trades
        - Average price discrepancy PnL for closing trades
        - Current trade net PnL
        - Current trade funding fee PnL
        """
        self.current_controller_position_amount = Decimal(100000)

    def determine_executor_actions(self) -> List[ExecutorAction]:
        if self.state == ControllerState.NO_ACTIVE_TRADES:
            """ Search for new trade provided controller has sufficient funds. """

            # 1. Check if state of wallets allow for more exposure
            if not self.accepting_new_trade_proposals():
                return []

            # 2. Find the best possible trade at this moment
            projected_revenues = {}
            trade_with_best_projection = None
            for token in self.config.tokens:
                for long_pair, short_pair in get_all_valid_trades_for_token(
                    token, self.config.quotes, self.config.connector_name
                ):
                    projected_revenues[(long_pair, short_pair)] = self.determine_projected_revenue(
                        long_pair, short_pair
                    )
                    if not trade_with_best_projection:
                        trade_with_best_projection = (long_pair, short_pair)
                    elif projected_revenues[(long_pair, short_pair)] > projected_revenues[trade_with_best_projection]:
                        trade_with_best_projection = (long_pair, short_pair)

            # 3. Check if the potential trade meets minimum APY requirements and start execution if so
            # 4. TODO: Could add additional step here to have volatility-specific APY requirements
            if self.meets_minimum_projected_revenue_requirement(trade_with_best_projection):
                self.opening_trade = FundingTrade(trade_with_best_projection[0], trade_with_best_projection[1])
                self.state = ControllerState.SCALING_IN
            pass
        elif self.state == ControllerState.SCALING_IN:
            """ New trade in progress. Continually open incremental orders provided previous order finished gracefully 
                and new order meets minimum price discrepancy/trading fee PnL requirements. """

            # 1. Check if an incremental order is already open
            active_orders: List[ExecutorInfo] = self.filter_executors(
                executors=self.executors_info, filter_func=lambda e: not e.is_done
            )
            if active_orders:
                return []

            # 2. Check if maximum allocation has been reached.
            #    This includes checking sufficient balance on wallets, max controller allocation, max trade allocation
            # TODO: Add conditional for checking sufficient wallet balances
            if (
                self.curr_controller_position_amount + self.config.inc_order_amount
                >= self.config.max_controller_allocation_amount
            ):
                self.active_trades.append(self.opening_trade)
                self.opening_trade = None
                self.state = ControllerState.ACTIVE_TRADE
                return []

            if (
                self.opening_trade.current_long_position_amount + self.config.inc_order_amount
                >= self.config.max_trade_allocation_amount
            ) or (
                self.opening_trade.current_short_position_amount + self.config.inc_order_amount
                >= self.config.max_trade_allocation_amount
            ):
                self.active_trades.append(self.opening_trade)
                self.opening_trade = None
                self.state = ControllerState.ACTIVE_TRADE
                return []

            # 3. Check if next order meets trading fee/price diff PnL requirements
            expected_order_cost_as_percent = self.determine_projected_order_cost()
            if expected_order_cost_as_percent >= self.config.max_order_cost_as_percentage:
                return []

            # 4. Everything is good. Open next order
            # TODO: Not sure if ArbitrageExecutor can even execute perps. Where is the leverage setting? Or is that a
            #       setting that is made within connector?
            arbitrage_config = ArbitrageExecutorConfig(
                buying_market=self.opening_trade.long_pair,
                selling_market=self.opening_trade.short_pair,
                order_amount=Decimal(0),
                min_profitability=Decimal(10),
            )

            pass
        elif self.state == ControllerState.ACTIVE_TRADE:
            """ Fully deployed trade in action. Monitor market activity to determine either
                scaling out, swapping for a new trade, or staying in current trade. """
            pass
        elif self.state == ControllerState.SCALING_OUT:
            """ Closing active trade. Permission to create incremental close orders provided
                order meets minimum price discrepancy/trading fee PnL requirements. """
            pass
        elif self.state == ControllerState.SWAPPING_TRADE:
            """ Better trade found with favorable conditions. Permission to follow procedures 
                for SCALING_IN to better trade and SCALING_OUT of active trade. """
            pass
        return []

    def accepting_new_trade_proposals(self) -> bool:
        """Determine if available funds, total active trade amount, and max number of active trades
        allows room for opening a new trade"""
        return False

    def meets_minimum_projected_revenue_requirement(self, trade) -> bool:
        """Determine if proposed trade meets minimum requirements for projected profitability"""
        return False

    def determine_projected_trade_revenue(self, long_pair: ConnectorPair, short_pair: ConnectorPair) -> Decimal:
        """Estimate a funding rate differential APY"""
        return Decimal(0.0)

    def determine_projected_order_cost(self) -> Decimal:
        """Estimate the cost of opening or closing an order. Accounts for trading fees and price diffs"""
        return Decimal(0.0)
