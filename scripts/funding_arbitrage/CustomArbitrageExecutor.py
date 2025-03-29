import logging

import hummingbot.strategy_v2.executors.arbitrage_executor.arbitrage_executor as arbitrage_executor
from hummingbot.logger import HummingbotLogger
from hummingbot.strategy.script_strategy_base import ScriptStrategyBase
from hummingbot.strategy_v2.executors.arbitrage_executor.data_types import ArbitrageExecutorConfig


class CustomArbitrageExecutor(arbitrage_executor.ArbitrageExecutor):
    """
    The existing ArbitrageExecutor has some limitations when it comes to arbing funding rates. Mods include:
     [ ]  Adding a wallet balance checker that accommodates same exchange arbitrage
     [ ]  Removing profitability checker logic to offload it to controller analysis
     [ ]  Refactor logic to catch when one side of trade fails

    Just realized I could only use this if I make some refactors to the hummingbot library (see executor_orchestrator
    as an example). So, for now implement this logic in the controller or trade manager as needed instead.
    """

    _logger = None

    @classmethod
    def logger(cls) -> HummingbotLogger:
        if cls._logger is None:
            cls._logger = logging.getLogger(__name__)
        return cls._logger  # type: ignore

    def __init__(
        self, strategy: ScriptStrategyBase, config: ArbitrageExecutorConfig, update_interval: float = 1.0
    ) -> None:
        super().__init__(strategy, config, update_interval)
