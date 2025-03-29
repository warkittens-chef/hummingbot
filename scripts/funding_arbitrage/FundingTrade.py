from decimal import Decimal

from hummingbot.strategy_v2.executors.data_types import ConnectorPair


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

    def __init__(self, long_pair: ConnectorPair, short_pair: ConnectorPair) -> None:
        self.long_pair = long_pair
        self.short_pair = short_pair
        self.current_long_position_amount = Decimal(0)
        self.current_short_position_amount = Decimal(0)
