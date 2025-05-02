from decimal import Decimal

from hummingbot.strategy_v2.executors.arbitrage_executor.data_types import ArbitrageExecutorConfig
from hummingbot.strategy_v2.executors.data_types import ConnectorPair
from hummingbot.strategy_v2.models.executor_actions import CreateExecutorAction
from hummingbot.strategy_v2.models.executors_info import ExecutorInfo


class FundingTrade:
    """
    This class maintains the state of a single funding rate arbitrage trade. It manages all executors related to the
    trade and provides an interface for getting data on the state of the trade.

    Tech Requirements:
     - Maintains up-to-date PnL from funding payments per side
     - Maintains up-to-date PnL from trading fees per side
     - Maintains up-to-date PnL from execution price discrepancies
     - Maintains up-to-date average entry price per side
     - Maintains up-to-date open position size
     - Maintains current state of most recent executor
     - Maintains any other up-to-date data that might be useful for determining if you should close the trade
        - The actual order analysis will be delegated to the controller
     - Keep open and close order methods separate so that this class can be subclassed easier
     - Use a getter/setter method for most recent executor for easier subclassing
     - Prevents adding to open position size when current size is too close to maximum position size
     - Prevents adding to/removing from open position size when recent order failed in any way
        - Logs error type and reason for error
        - Notifies user in some way
        - This will be used to start collecting expected exceptions
     - Validation method to ensure most recent open order is complete before making a new one
     - Validation method to ensure most recent close order is complete before making a new one

     Basically, methods for data analysis, and action analysis, but no actual state management of the trade since that
     has to be done by the controller.

    Initialization
     - On instantiation, should load the current progress of the trade from the database
        - Active executor(s)
        - All variables defining state of trade

    ---------------

    I want the controller to defer all decision-making for a single trade to this class, where it will call methods
    to get answers: a method for checking that all trade-wide rules are met, methods that return ExecutorAction objects.
    """

    def __init__(
        self,
        long_pair: ConnectorPair,
        short_pair: ConnectorPair,
        incremental_order_amount: Decimal,
        max_total_value_investable: Decimal,
    ) -> None:
        self.long_pair = long_pair
        self.short_pair = short_pair
        self.incremental_order_amount = incremental_order_amount
        self.max_total_value_investable = max_total_value_investable

        self.current_long_side_size = Decimal(0)
        self.current_short_side_size = Decimal(0)
        self.current_long_side_avg_entry_price = Decimal(0)
        self.current_short_side_avg_entry_price = Decimal(0)
        self.cumm_pnl_trading_fees = Decimal(0)
        self.cumm_pnl_price_gaps = Decimal(0)
        self.cumm_pnl_funding_fees = Decimal(0)

        self.order_in_progress = False

    def executor_belongs_to_trade(self, executor: ExecutorInfo) -> bool:
        if not isinstance(executor.config, ArbitrageExecutorConfig):
            return False

        executor_config: ArbitrageExecutorConfig = executor.config
        executor_market_pairs = [executor_config.buying_market, executor_config.selling_market]

        if self.long_pair in executor_market_pairs and self.short_pair in executor_market_pairs:
            return True

        return False

    def get_current_total_value_invested(self) -> Decimal:
        """
        This returns the total amount of money invested in the trade. Excludes PnL from price gaps because it is an
        initial cost of starting the trade.
        """
        return (
            self.current_long_side_size * self.current_long_side_avg_entry_price
            + self.current_short_side_size * self.current_short_side_avg_entry_price
            - self.cumm_pnl_trading_fees
            - self.cumm_pnl_price_gaps
        )

    def reload_trade(self, db_client: dict) -> None:
        """
        The approach to keeping analysis and state up-to-date is load everything available from the database, and then
        every tick
        """

    def _add_finalized_order_to_trade_metrics(self, finalized_order: ExecutorInfo) -> None:
        pass

    def update_order_status(self, order: ExecutorInfo) -> None:
        """
        The controller only has available to it all active executors from executor_orchestrator, where the active
        executor list is always up-to-date (as far as the controller is concerned) with only executors that haven't
        been marked as is_done.
        """
        if not self.executor_belongs_to_trade(order):
            return

        if order.is_done:
            self._add_finalized_order_to_trade_metrics(order)
            self.order_in_progress = False
        else:
            self.order_in_progress = True

    def decide_on_next_scale_up_order(self) -> CreateExecutorAction | None:
        """
        When returning an action proposal, this should set proposed_executor_id to the newly generated executor config
        id. This function returns the action proposal, it gets sent to executor_orchestrator, who executes it and adds
        it to the controller's executors_info list. By the next tick, the controller will iterate through each executor
        in executors_info
        """
        if self.order_in_progress:
            return None

        # TODO: Add code to check Trade rules and run evaluation, and return an action
        #       Remember to set order_in_progress to True if creating an action
        pass
