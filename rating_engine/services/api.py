import aiohttp

from datetime import datetime
from json import dumps
from typing import Any, List, Optional, Tuple
from pytz import timezone


def _dumps(val: Any, d: Any = ''):
    return dumps(val if val is not None else d, default=_dumps_converter)


UTC = timezone('UTC')


def _dumps_converter(v: Any):
    if isinstance(v, datetime):
        return v.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


class APIService(object):

    _api_url: str
    _api_usename: Optional[str]
    _api_password: Optional[str]
    _session: Optional[aiohttp.ClientSession]

    QUERY_GET_ACCOUNT_BY_ID_WRAPPER = """query {
        %(query)s
}"""

    QUERY_GET_ACCOUNT_BY_ID = """Account(tenant: %(tenant)s, account_tag: %(account_tag)s) {
        account_tag
        type
        name
        balance
        active
        notification_email
        notification_mobile
        max_concurrent_transactions
        running_transactions {
            destination_rate {
                carrier_tag
                pricelist_tag
                prefix
                description
                connect_fee
                rate
                rate_increment
                interval_start
            }
            transaction_tag
            source
            source_ip
            carrier_ip
            destination
            tags
            in_progress
            inbound
            timestamp_begin
            timestamp_end
        }
        carrier_tags
        carrier_tags_override
        %(destination_rate)s
        %(least_cost_routing)s
        linked_accounts {
            account_tag
            type
            name
            balance
            active
            notification_email
            notification_mobile
            max_concurrent_transactions
            running_transactions {
                destination_rate {
                    carrier_tag
                    pricelist_tag
                    prefix
                    description
                    connect_fee
                    rate
                    rate_increment
                    interval_start
                }
                transaction_tag
                destination
                tags
                in_progress
                inbound
                timestamp_begin
                timestamp_end
            }
            carrier_tags
            carrier_tags_override
            pricelist_tags
            tags
            %(destination_rate)s
        }
        pricelist_tags
        tags
    }
"""

    QUERY_GET_ACCOUNT_BY_ID_DESTINATION_RATE = """
    destination_rate(destination: %(destination)s) {
        carrier_tag
        pricelist_tag
        prefix
        description
        connect_fee
        rate
        rate_increment
        interval_start
    }
"""

    QUERY_GET_ACCOUNT_BY_ID_LEAST_COST_ROUTING = """
    least_cost_routing(destination: %(destination)s) {
        host
        port
        protocol
    }
"""

    QUERY_DESTINATION_RATE = """
        destination_rate: {
            carrier_tag: %(destination_rate_carrier_tag)s,
            pricelist_tag: %(destination_rate_pricelist_tag)s,
            prefix: %(destination_rate_prefix)s,
            description: %(destination_rate_description)s,
            connect_fee: %(destination_rate_connect_fee)s,
            rate: %(destination_rate_rate)s,
            rate_increment: %(destination_rate_rate_increment)s,
            interval_start: %(destination_rate_interval_start)s
        },
    """

    QUERY_BEGIN_ACCOUNT_TRANSACTION = """mutation {
    beginAccountTransaction(
        tenant: %(tenant)s,
        account_tag: %(account_tag)s,
        transaction: {
            transaction_tag: %(transaction_tag)s,
            %(destination_rate)s
            source: %(source)s,
            source_ip: %(source_ip)s,
            destination: %(destination)s,
            carrier_ip: %(carrier_ip)s,
            timestamp_begin: %(timestamp_begin)s,
            primary: %(primary)s,
            inbound: %(inbound)s
        }
    ) {
        ok
        transaction {
            destination_rate {
                carrier_tag
                pricelist_tag
                prefix
                description
                connect_fee
                rate
                rate_increment
                interval_start
            }
            transaction_tag
            destination
            tags
            in_progress
            inbound
            primary
            timestamp_begin
            timestamp_end
        }
    }
}"""

    QUERY_ROLLBACK_ACCOUNT_TRANSACTION = """mutation {
    rollbackAccountTransaction(
        tenant: %(tenant)s,
        account_tag: %(account_tag)s,
        transaction_tag: %(transaction_tag)s
    ) {
        ok
    }
}"""

    QUERY_END_ACCOUNT_TRANSACTION = """mutation {
    endAccountTransaction(
        tenant: %(tenant)s,
        account_tag: %(account_tag)s,
        transaction_tag: %(transaction_tag)s,
        timestamp_end: %(timestamp_end)s
    ) {
        ok
        transaction {
            destination_rate {
                carrier_tag
                pricelist_tag
                prefix
                description
                connect_fee
                rate
                rate_increment
                interval_start
            }
            transaction_tag
            source
            source_ip
            carrier_ip
            destination
            tags
            in_progress
            primary
            inbound
            timestamp_begin
            timestamp_end
        }
    }
}"""

    QUERY_COMMIT_ACCOUNT_TRANSACTION = """mutation {
    commitAccountTransaction(
        tenant: %(tenant)s,
        account_tag: %(account_tag)s,
        transaction_tag: %(transaction_tag)s,
        fee: %(fee)s
    ) {
        ok
    }
}"""

    QUERY_GET_PRIMARY_TRANSACTIONS_BY_TENANT_AND_TAG = """query {
    allTransactions(filter:{tenant: %(tenant)s, transaction_tag: %(transaction_tag)s, primary: true}) {
        tenant
        transaction_tag
        account_tag
        source
        source_ip
        destination
        carrier_ip
        inbound
        primary
    }
}"""

    QUERY_UPSERT_TRANSACTION = """mutation {
    upsertTransaction (
        tenant: %(tenant)s,
        transaction_tag: %(transaction_tag)s,
        account_tag: %(account_tag)s,
        %(destination_rate)s
        source: %(source)s,
        source_ip: %(source_ip)s,
        destination: %(destination)s,
        carrier_ip: %(carrier_ip)s,
        tags: %(tags)s,
        timestamp_begin: %(timestamp_begin)s,
        timestamp_end: %(timestamp_end)s,
        primary: %(primary)s,
        inbound: %(inbound)s,
        duration: %(duration)s,
        fee: %(fee)s
    ) {
        id
    }
}"""

    QUERY_UPSERT_AUTHORIZATION_TRANSACTION = """mutation {
    upsertTransaction (
        tenant: %(tenant)s,
        transaction_tag: %(transaction_tag)s,
        account_tag: %(account_tag)s,
        source: %(source)s,
        destination: %(destination)s,
        tags: %(tags)s,
        timestamp_auth: %(timestamp_auth)s
        primary: %(primary)s,
        inbound: %(inbound)s,
    ) {
        id
    }
}"""

    def __init__(
        self,
        api_url: str,
        api_username: Optional[str] = None,
        api_password: Optional[str] = None,
    ):
        self._api_url = api_url
        self._api_username = api_username
        self._api_password = api_password
        self._session = None

    async def _query(self, query: str) -> Optional[dict]:
        if self._session is None:
            self._session = aiohttp.ClientSession()
        json = {'query': query}
        try:
            async with self._session.post(self._api_url, json=json) as r:
                if r.status == 200:
                    return await r.json()
        except aiohttp.ClientError:  # pragma: no cover
            pass
        return None

    async def close(self):
        if self._session is not None:
            await self._session.close()

    async def get_account_and_destination_account_by_id(
        self,
        tenant: str,
        account_tag: Optional[str] = None,
        destination: Optional[str] = None,
        destination_account_tag: Optional[str] = None,
    ) -> Tuple[Optional[dict], Optional[dict]]:
        destination_rate = (
            self.QUERY_GET_ACCOUNT_BY_ID_DESTINATION_RATE
            % dict(destination=_dumps(destination))
            if destination is not None
            else ''
        )
        least_cost_routing = (
            self.QUERY_GET_ACCOUNT_BY_ID_LEAST_COST_ROUTING
            % dict(destination=_dumps(destination))
            if destination is not None
            else ''
        )
        account = (
            self.QUERY_GET_ACCOUNT_BY_ID
            % dict(
                tenant=_dumps(tenant),
                account_tag=_dumps(account_tag),
                destination_rate=destination_rate,
                least_cost_routing=least_cost_routing,
            )
            if account_tag is not None
            else None
        )
        destination_account = (
            'DestinationAccount:'
            + self.QUERY_GET_ACCOUNT_BY_ID
            % dict(
                tenant=_dumps(tenant),
                account_tag=_dumps(destination_account_tag),
                destination_rate='',
                least_cost_routing='',
                destination_account='',
            )
            if destination_account_tag is not None
            else None
        )
        query = self.QUERY_GET_ACCOUNT_BY_ID_WRAPPER % dict(
            query='\n'.join(filter(None, (account, destination_account)))
        )
        result = await self._query(query=query)
        return (
            (result['data'].get('Account'), result['data'].get('DestinationAccount'))
            if result is not None
            else (None, None)
        )

    async def begin_account_transaction(
        self,
        tenant: str,
        account_tag: str,
        destination_rate: dict,
        transaction_tag: str,
        source: Optional[str],
        source_ip: Optional[str],
        destination: Optional[str],
        carrier_ip: Optional[str],
        timestamp_begin: datetime,
        primary: bool = False,
        inbound: bool = False,
    ) -> Optional[dict]:
        query_destination_rate = (
            self.QUERY_DESTINATION_RATE
            % dict(
                destination_rate_pricelist_tag=_dumps(
                    destination_rate['pricelist_tag']
                ),
                destination_rate_carrier_tag=_dumps(destination_rate['carrier_tag']),
                destination_rate_prefix=_dumps(destination_rate['prefix']),
                destination_rate_description=_dumps(destination_rate['description']),
                destination_rate_connect_fee=_dumps(destination_rate['connect_fee'], 0),
                destination_rate_rate=_dumps(destination_rate['rate'], 0),
                destination_rate_rate_increment=_dumps(
                    destination_rate['rate_increment'], 0
                ),
                destination_rate_interval_start=_dumps(
                    destination_rate['interval_start'], 0
                ),
            )
            if destination_rate
            else ''
        )
        query = self.QUERY_BEGIN_ACCOUNT_TRANSACTION % dict(
            tenant=_dumps(tenant),
            account_tag=_dumps(account_tag),
            transaction_tag=_dumps(transaction_tag),
            source=_dumps(source),
            source_ip=_dumps(source_ip),
            destination=_dumps(destination),
            carrier_ip=_dumps(carrier_ip),
            timestamp_begin=_dumps(timestamp_begin),
            destination_rate=query_destination_rate,
            primary='true' if primary else 'false',
            inbound='true' if inbound else 'false',
        )
        result = await self._query(query=query)
        return (
            result['data']['beginAccountTransaction']['transaction']
            if result is not None
            else None
        )

    async def rollback_account_transaction(
        self, tenant: str, account_tag: Optional[str], transaction_tag: str,
    ) -> Optional[dict]:
        query = self.QUERY_ROLLBACK_ACCOUNT_TRANSACTION % dict(
            tenant=_dumps(tenant),
            account_tag=_dumps(account_tag),
            transaction_tag=_dumps(transaction_tag),
        )
        result = await self._query(query=query)
        return (
            result['data']['rollbackAccountTransaction']['ok']
            if result is not None
            else None
        )

    async def end_account_transaction(
        self,
        tenant: str,
        account_tag: str,
        transaction_tag: str,
        timestamp_end: datetime,
    ) -> Optional[dict]:
        query = self.QUERY_END_ACCOUNT_TRANSACTION % dict(
            tenant=_dumps(tenant),
            account_tag=_dumps(account_tag),
            transaction_tag=_dumps(transaction_tag),
            timestamp_end=_dumps(timestamp_end),
        )
        result = await self._query(query=query)
        return (
            result['data']['endAccountTransaction']['transaction']
            if result is not None
            else None
        )

    async def get_primary_transactions_by_tenant_and_tag(
        self, tenant: str, transaction_tag: str,
    ) -> List[dict]:
        query = self.QUERY_GET_PRIMARY_TRANSACTIONS_BY_TENANT_AND_TAG % dict(
            tenant=_dumps(tenant), transaction_tag=_dumps(transaction_tag),
        )
        result = await self._query(query=query)
        return list(result['data']['allTransactions']) if result is not None else []

    async def upsert_transaction(
        self,
        tenant: str,
        account_tag: str,
        transaction: dict,
        duration: int = 0,
        fee: int = 0,
    ) -> Optional[bool]:
        destination_rate = transaction['destination_rate']
        query_destination_rate = (
            self.QUERY_DESTINATION_RATE
            % dict(
                destination_rate_pricelist_tag=_dumps(
                    destination_rate['pricelist_tag']
                ),
                destination_rate_carrier_tag=_dumps(destination_rate['carrier_tag']),
                destination_rate_prefix=_dumps(destination_rate['prefix']),
                destination_rate_description=_dumps(destination_rate['description']),
                destination_rate_connect_fee=_dumps(destination_rate['connect_fee'], 0),
                destination_rate_rate=_dumps(destination_rate['rate'], 0),
                destination_rate_rate_increment=_dumps(
                    destination_rate['rate_increment'], 0
                ),
                destination_rate_interval_start=_dumps(
                    destination_rate['interval_start'], 0
                ),
            )
            if destination_rate
            else ''
        )
        query = self.QUERY_UPSERT_TRANSACTION % dict(
            tenant=_dumps(tenant),
            account_tag=_dumps(account_tag),
            transaction_tag=_dumps(transaction['transaction_tag']),
            source=_dumps(transaction['source']),
            source_ip=_dumps(transaction.get('source_ip')),
            carrier_ip=_dumps(transaction.get('carrier_ip')),
            destination=_dumps(transaction['destination']),
            destination_rate=query_destination_rate,
            tags=_dumps(transaction['tags'] or []),
            timestamp_begin=_dumps(transaction['timestamp_begin']),
            timestamp_end=_dumps(transaction['timestamp_end']),
            primary='true' if transaction.get('primary') else 'false',
            inbound='true' if transaction.get('inbound') else 'false',
            duration=_dumps(duration, 0),
            fee=_dumps(fee),
        )
        result = await self._query(query=query)
        return (
            result['data']['upsertTransaction']['id'] is not None
            if result is not None
            else None
        )

    async def upsert_authorization_transaction(
        self, tenant: str, account_tag: str, transaction: dict
    ) -> Optional[bool]:
        query = self.QUERY_UPSERT_AUTHORIZATION_TRANSACTION % dict(
            tenant=_dumps(tenant),
            account_tag=_dumps(account_tag),
            transaction_tag=_dumps(transaction['transaction_tag']),
            source=_dumps(transaction['source']),
            destination=_dumps(transaction['destination']),
            tags=_dumps(transaction['tags'] or []),
            timestamp_auth=_dumps(transaction['timestamp_auth']),
            primary='true',
            inbound='true' if transaction.get('inbound') else 'false',
        )
        result = await self._query(query=query)
        return (
            result['data']['upsertTransaction']['id'] is not None
            if result is not None
            else None
        )

    async def commit_account_transaction(
        self, tenant: str, account_tag: str, transaction_tag: str, fee: int
    ) -> Optional[dict]:
        query = self.QUERY_COMMIT_ACCOUNT_TRANSACTION % dict(
            tenant=_dumps(tenant),
            account_tag=_dumps(account_tag),
            transaction_tag=_dumps(transaction_tag),
            fee=_dumps(fee, 0),
        )
        result = await self._query(query=query)
        return (
            result['data']['commitAccountTransaction']['ok']
            if result is not None
            else None
        )
