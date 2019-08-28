import unittest

from datetime import datetime
from pytz import timezone

from rating_engine.services.rater import RaterService


class ServicesRaterTest(unittest.TestCase):
    def setUp(self):
        self.tz = timezone('UTC')
        self.service = RaterService(tz=self.tz)

    def test_rater_one_cent_per_second(self):
        transaction = {
            'timestamp_begin': self.tz.localize(datetime(2019, 1, 1, 10, 0, 0)),
            'timestamp_end': self.tz.localize(datetime(2019, 1, 1, 10, 1, 30)),
            'destination_rate': {
                'connect_fee': 0,
                'rate': 1,
                'rate_increment': 1,
                'interval_start': 0,
            },
        }
        fee = self.service.get_transaction_fee(transaction=transaction)
        self.assertEqual(90, fee)

    def test_rater_one_cent_per_second_with_30_seconds_free(self):
        transaction = {
            'timestamp_begin': self.tz.localize(datetime(2019, 1, 1, 10, 0, 0)),
            'timestamp_end': self.tz.localize(datetime(2019, 1, 1, 10, 1, 30)),
            'destination_rate': {
                'connect_fee': 0,
                'rate': 1,
                'rate_increment': 1,
                'interval_start': 30,
            },
        }
        fee = self.service.get_transaction_fee(transaction=transaction)
        self.assertEqual(60, fee)

    def test_rater_one_euro_per_minute(self):
        transaction = {
            'timestamp_begin': self.tz.localize(datetime(2019, 1, 1, 10, 0, 0)),
            'timestamp_end': self.tz.localize(datetime(2019, 1, 1, 10, 1, 30)),
            'destination_rate': {
                'connect_fee': 0,
                'rate': 100,
                'rate_increment': 60,
                'interval_start': 0,
            },
        }
        fee = self.service.get_transaction_fee(transaction=transaction)
        self.assertEqual(200, fee)

    def test_rater_one_euro_per_minute_with_one_free_minute(self):
        transaction = {
            'timestamp_begin': self.tz.localize(datetime(2019, 1, 1, 10, 0, 0)),
            'timestamp_end': self.tz.localize(datetime(2019, 1, 1, 10, 1, 30)),
            'destination_rate': {
                'connect_fee': 0,
                'rate': 100,
                'rate_increment': 60,
                'interval_start': 1,
            },
        }
        fee = self.service.get_transaction_fee(transaction=transaction)
        self.assertEqual(100, fee)

    def test_rater_one_euro_per_minute_with_begin_fee(self):
        transaction = {
            'timestamp_begin': self.tz.localize(datetime(2019, 1, 1, 10, 0, 0)),
            'timestamp_end': self.tz.localize(datetime(2019, 1, 1, 10, 1, 30)),
            'destination_rate': {
                'connect_fee': 100,
                'rate': 100,
                'rate_increment': 60,
                'interval_start': 0,
            },
        }
        fee = self.service.get_transaction_fee(transaction=transaction)
        self.assertEqual(300, fee)

    def test_rater_one_euro_per_minute_with_two_free_minute(self):
        transaction = {
            'timestamp_begin': self.tz.localize(datetime(2019, 1, 1, 10, 0, 0)),
            'timestamp_end': self.tz.localize(datetime(2019, 1, 1, 10, 0, 30)),
            'destination_rate': {
                'connect_fee': 0,
                'rate': 100,
                'rate_increment': 60,
                'interval_start': 2,
            },
        }
        fee = self.service.get_transaction_fee(transaction=transaction)
        self.assertEqual(0, fee)

    def test_rater_get_maximum_allowed_units_for_transaction(self):
        destination_rate = {
            'connect_fee': 0,
            'rate': 1,
            'rate_increment': 1,
            'interval_start': 60,
        }
        (
            authorized,
            allowed_units,
        ) = self.service.get_maximum_allowed_units_for_transaction(
            balance=50, destination_rate=destination_rate
        )
        self.assertTrue(authorized)
        self.assertEqual(110, allowed_units)

    def test_rater_get_maximum_allowed_units_for_transaction_without_destination_rate(
        self,
    ):
        authorized, _ = self.service.get_maximum_allowed_units_for_transaction(
            balance=50, destination_rate=None
        )
        self.assertFalse(authorized)

    def test_rater_get_maximum_allowed_units_for_transaction_with_balance_zero_and_interval_start(
        self,
    ):
        destination_rate = {
            'connect_fee': 0,
            'rate': 1,
            'rate_increment': 1,
            'interval_start': 60,
        }
        (
            authorized,
            allowed_units,
        ) = self.service.get_maximum_allowed_units_for_transaction(
            balance=0, destination_rate=destination_rate
        )
        self.assertFalse(authorized)
        self.assertEqual(0, allowed_units)

    def test_rater_get_maximum_allowed_units_for_transaction_with_begin_fee(self):
        destination_rate = {
            'connect_fee': 10,
            'rate': 1,
            'rate_increment': 1,
            'interval_start': 0,
        }
        (
            authorized,
            allowed_units,
        ) = self.service.get_maximum_allowed_units_for_transaction(
            balance=60, destination_rate=destination_rate
        )
        self.assertTrue(authorized)
        self.assertEqual(50, allowed_units)

    def test_rater_get_maximum_allowed_units_for_transaction_with_balance_zero_and_no_fee(
        self,
    ):
        destination_rate = {
            'connect_fee': 0,
            'rate': 0,
            'rate_increment': 1,
            'interval_start': 60,
        }
        (
            authorized,
            allowed_units,
        ) = self.service.get_maximum_allowed_units_for_transaction(
            balance=0, destination_rate=destination_rate
        )
        self.assertTrue(authorized)
        self.assertEqual(self.service.MAX_UNITS_FOR_TRANSACTIONS, allowed_units)

    def test_rater_with_timestamp_begin_newer_than_end(self):
        transaction = {
            'timestamp_begin': self.tz.localize(datetime(2019, 1, 1, 10, 0, 30)),
            'timestamp_end': self.tz.localize(datetime(2019, 1, 1, 10, 0, 0)),
            'destination_rate': {
                'connect_fee': 0,
                'rate': 0,
                'rate_increment': 0,
                'interval_start': 0,
            },
        }
        fee = self.service.get_transaction_fee(transaction=transaction)
        self.assertEqual(0, fee)
