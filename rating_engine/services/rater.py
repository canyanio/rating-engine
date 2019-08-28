from datetime import datetime
from math import ceil
from typing import Tuple

from dateutil import parser
from pytz import timezone


class RaterService(object):
    """Rater service"""

    MAX_UNITS_FOR_TRANSACTIONS = 3600 * 4

    def __init__(self, tz=None):
        self.tz = tz or timezone('UTC')

    def tz_localize(self, dt: datetime) -> datetime:
        return self.tz.localize(dt) if dt.tzinfo is None else dt

    def get_transaction_fee_and_duration(self, transaction: dict) -> Tuple[int, int]:
        timestamp_begin = (
            transaction['timestamp_begin']
            if isinstance(transaction['timestamp_begin'], datetime)
            else parser.parse(transaction['timestamp_begin'])
        )
        timestamp_end = (
            transaction['timestamp_end']
            if isinstance(transaction['timestamp_end'], datetime)
            else (
                parser.parse(transaction['timestamp_end'])
                if transaction['timestamp_end']
                else datetime.utcnow()
            )
        )
        timestamp_begin = self.tz_localize(timestamp_begin)
        timestamp_end = self.tz_localize(timestamp_end)
        if timestamp_end <= timestamp_begin:
            return (0, 0)
        timestamp_delta = timestamp_end - timestamp_begin
        duration = timestamp_delta.seconds + (1 if timestamp_delta.microseconds else 0)
        destination_rate = transaction.get('destination_rate') or {}
        connect_fee = destination_rate.get('connect_fee', 0)
        interval_start = destination_rate.get('interval_start', 0)
        rate = destination_rate.get('rate', 0)
        rate_increment = destination_rate.get('rate_increment') or 1
        return (
            connect_fee
            + int(max(0, ceil(duration / rate_increment) - interval_start)) * rate,
            duration,
        )

    def get_transaction_fee(self, transaction: dict) -> int:
        fee, _ = self.get_transaction_fee_and_duration(transaction)
        return fee

    def get_maximum_allowed_units_for_transaction(
        self, balance: int, destination_rate: dict
    ) -> Tuple[bool, int]:
        if not destination_rate:
            return (False, 0)
        connect_fee = destination_rate.get('connect_fee', 0)
        interval_start = destination_rate.get('interval_start', 0)
        rate = destination_rate.get('rate', 0)
        rate_increment = destination_rate.get('rate_increment') or 1
        allowed_units = (
            int((balance - connect_fee) / rate) * rate_increment
            if rate
            else self.MAX_UNITS_FOR_TRANSACTIONS
        )
        allowed_units = (
            min(allowed_units + interval_start, self.MAX_UNITS_FOR_TRANSACTIONS)
            if allowed_units
            else 0
        )
        max_allowed_units = allowed_units if allowed_units and allowed_units > 0 else 0
        authorized = balance > 0 or (connect_fee == 0 and rate == 0)
        return (authorized, max_allowed_units)
