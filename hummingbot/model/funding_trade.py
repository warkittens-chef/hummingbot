from typing import List, Optional

from sqlalchemy import BigInteger, Column, Index, Text, or_, and_
from sqlalchemy.orm import Session

from hummingbot.model import HummingbotBase

"""
Include uniqueness between each individual connector pair, time window combo
    - This is to avoid Trade overlap in database
    - Want to be able to query the table using a timestamp and a connector pair to find the FundingTrade associated
      with this. Currently used for FundingPayment, where we only have a timestamp and connector pair

"""


class FundingTrade(HummingbotBase):
    __tablename__ = "FundingTrade"
    __table_args__ = (
        Index("ex_long_time_window", "long_market", "long_pair", "start_time", "end_time"),
        Index("ex_short_time_window", "short_market", "short_pair", "start_time", "end_time"),
    )
    id = Column(Text, primary_key=True)
    controller_id = Column(Text, nullable=True)
    start_time = Column(BigInteger, nullable=False)
    end_time = Column(BigInteger, nullable=True)
    long_market = Column(Text, nullable=False)
    long_pair = Column(Text, nullable=False)
    short_market = Column(Text, nullable=False)
    short_pair = Column(Text, nullable=False)

    @staticmethod
    def find_funding_trade(
        sql_session: Session,
        timestamp: float,
        market: str,
        trading_pair: str,
    ):
        """
        Returns all FundingTrade records where the given market and trading_pair match either the long or short side,
        and the timestamp is between start_time and end_time (inclusive).
        """
        print("find_funding_trade: ", timestamp, market, trading_pair)
        filters = or_(
            and_(
                FundingTrade.long_market == market,
                FundingTrade.long_pair == trading_pair,
                FundingTrade.start_time <= timestamp,
                or_(None == FundingTrade.end_time, FundingTrade.end_time >= timestamp),
            ),
            and_(
                FundingTrade.short_market == market,
                FundingTrade.short_pair == trading_pair,
                FundingTrade.start_time <= timestamp,
                or_(None == FundingTrade.end_time, FundingTrade.end_time >= timestamp),
            ),
        )
        result: Optional[List[FundingTrade]] = sql_session.query(FundingTrade).filter(filters).all()

        if result and len(result) > 1:
            # This should never happen but adding this in case it does. If so, need to add validations to this table
            raise ValueError(
                "Multiple FundingTrade records found for {} {} with timestamp {}.".format(
                    market, trading_pair, timestamp
                )
            )

        if result:
            return result[0]
        return None
