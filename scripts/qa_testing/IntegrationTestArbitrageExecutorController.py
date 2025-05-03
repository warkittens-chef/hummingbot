from typing import List

from hummingbot.strategy_v2.controllers import ControllerBase, ControllerConfigBase
from hummingbot.strategy_v2.models.executor_actions import ExecutorAction


class TestArbitrageExecutorController(ControllerBase):
    def __init__(self, config: ControllerConfigBase, *args, **kwargs):
        super().__init__(config, *args, **kwargs)
        self.config = config

    def determine_executor_actions(self) -> List[ExecutorAction]:
        """
        This is to test that when an arbitrage executor completes, the execution data successfully makes its way to
        the database Executor table. Therefore, it is testing that:
        1. By the time the executor completes, the execution data is available in executor_info
        2. When we attempt to store executor_info with execution data in the database, it's successful
        """
