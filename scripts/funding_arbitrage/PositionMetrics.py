from decimal import Decimal


class PositionMetrics:
    def __init__(
        self,
        start_time: int,
        end_time: int,
        market: str,
        long_side_size: Decimal,
        short_side_size: Decimal,
        long_side_avg_entry_price: Decimal,
        short_side_avg_entry_price: Decimal,
        cumm_pnl_trading_fees: Decimal,
        cumm_pnl_price_gaps: Decimal,
        cumm_pnl_funding_fees: Decimal,
    ):
        self.start_time = start_time
        self.end_time = end_time
        self.market = market
        self.long_side_size = long_side_size
        self.short_side_size = short_side_size
        self.long_side_avg_entry_price = long_side_avg_entry_price
        self.short_side_avg_entry_price = short_side_avg_entry_price
        self.cumm_pnl_trading_fees = cumm_pnl_trading_fees
        self.cumm_pnl_price_gaps = cumm_pnl_price_gaps
        self.cumm_pnl_funding_fees = cumm_pnl_funding_fees
