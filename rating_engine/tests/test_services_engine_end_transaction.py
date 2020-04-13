import pytest  # type: ignore

from datetime import datetime
from json import dumps
from pytz import timezone

from ..schema import engine as schema


@pytest.mark.asyncio
async def test_end_transaction(engine):
    request = schema.EndTransactionRequest(transaction_tag="100")
    result = await engine.end_transaction(request)
    assert result == schema.EndTransactionResponse(ok=False)


@pytest.mark.asyncio
async def test_end_transaction_failed_no_accounts_provided(engine):
    tenant = "default"
    transaction_tag = "100"
    #
    request = schema.EndTransactionRequest(
        tenant=tenant, transaction_tag=transaction_tag
    )
    response = await engine.end_transaction(request)
    assert response.ok is False


@pytest.mark.asyncio
async def test_end_transaction_failed_account_not_found(engine):
    tenant = "default"
    transaction_tag = "100"
    account_tag = "1000"
    #
    request = schema.EndTransactionRequest(
        tenant=tenant, transaction_tag=transaction_tag, account_tag=account_tag
    )
    response = await engine.end_transaction(request)
    assert response.ok is False
    assert account_tag == response.failed_account_tag
    assert 'NOT_FOUND' == response.failed_reason


@pytest.mark.asyncio
async def test_end_transaction_failed_destination_account_not_found(engine):
    tenant = "default"
    transaction_tag = "100"
    destination_account_tag = "1001"
    #
    request = schema.EndTransactionRequest(
        tenant=tenant,
        transaction_tag=transaction_tag,
        destination_account_tag=destination_account_tag,
    )
    response = await engine.end_transaction(request)
    assert response.ok is False
    assert destination_account_tag == response.failed_account_tag
    assert 'NOT_FOUND' == response.failed_reason


@pytest.mark.asyncio
async def test_end_transaction_failed_account_and_destination_account_not_found(engine):
    tenant = "default"
    transaction_tag = "100"
    account_tag = "1000"
    destination_account_tag = "1001"
    #
    request = schema.EndTransactionRequest(
        tenant=tenant,
        transaction_tag=transaction_tag,
        account_tag=account_tag,
        destination_account_tag=destination_account_tag,
    )
    response = await engine.end_transaction(request)
    assert response.ok is False
    assert account_tag == response.failed_account_tag
    assert 'NOT_FOUND' == response.failed_reason


@pytest.mark.asyncio
async def test_end_transaction_failed_transaction_not_found(engine, graphql):
    tenant = "default"
    transaction_tag = "100"
    account_tag = "1000"
    #
    response = await graphql(
        """
        mutation {
            upsertAccount(
                tenant: %(tenant)s,
                account_tag: %(account_tag)s,
                type: PREPAID,
                balance: 1000000,
                active: false
            ) {
                id
            }
        }"""
        % {'tenant': dumps(tenant), 'account_tag': dumps(account_tag)}
    )
    request = schema.EndTransactionRequest(
        tenant=tenant, transaction_tag=transaction_tag, account_tag=account_tag
    )
    response = await engine.end_transaction(request)
    assert response.ok is False


@pytest.mark.asyncio
async def test_end_transaction_with_transaction_id(engine, graphql):
    tenant = "default"
    transaction_tag = "100"
    account_tag = "1000"
    source = "1000"
    destination = "393291234567"
    timestamp_auth = timezone("UTC").localize(datetime.utcnow())
    #
    await graphql(
        """
        mutation {
            upsertCarrier(
                tenant:%(tenant)s,
                carrier_tag:"TESTS",
                host:"carrier1.canyan.io",
                port:5060,
                protocol:UDP
                active:true
            ) {
                id
            }
            upsertPricelist(
                tenant:%(tenant)s,
                pricelist_tag:"TESTS",
                currency:EUR
            ) {
                id
            }
            upsertPricelistRate(
                tenant:%(tenant)s,
                pricelist_tag:"TESTS",
                carrier_tag:"TESTS",
                prefix:"39"
                active:true,
                connect_fee:0,
                rate:1,
                rate_increment:1,
                interval_start:0,
                description:"TESTS_ALL_DESTINATIONS"
            ) {
                id
            }
            upsertAccount(
                tenant: %(tenant)s,
                account_tag: %(account_tag)s,
                type: PREPAID,
                pricelist_tags: ["TESTS"]
                balance: 100,
                active: true
            ) {
                id
            }
            upsertTransaction(
                tenant: %(tenant)s,
                transaction_tag: %(transaction_tag)s,
                account_tag: %(account_tag)s,
                source: %(source)s,
                destination: %(destination)s,
                timestamp_auth: %(timestamp_auth)s,
                primary: true,
                inbound: false,
            ) {
                id
            }
            beginAccountTransaction(
                tenant: %(tenant)s,
                account_tag: %(account_tag)s,
                transaction: {
                    transaction_tag: %(transaction_tag)s,
                    destination_rate: {
                        pricelist_tag:"TESTS"
                        prefix:"39"
                        connect_fee:0
                        rate:1
                        rate_increment:1
                        interval_start:0
                        carrier_tag:"TESTS"
                        description:"TESTS_ALL_DESTINATIONS"
                    }
                    source: %(source)s,
                    source_ip: "127.0.0.1",
                    destination: %(destination)s,
                    carrier_ip: "192.168.0.1",
                    timestamp_begin: %(timestamp_begin)s,
                    primary: true,
                    inbound: false
                }
            ) {
                ok
            }
        }"""
        % {
            'tenant': dumps(tenant),
            'account_tag': dumps(account_tag),
            'transaction_tag': dumps(transaction_tag),
            'source': dumps(source),
            'destination': dumps(destination),
            'timestamp_auth': dumps(timestamp_auth.strftime("%Y-%m-%dT%H:%M:%SZ")),
            'timestamp_begin': dumps(timestamp_auth.strftime("%Y-%m-%dT%H:%M:%SZ")),
        }
    )
    request = schema.EndTransactionRequest(transaction_tag=transaction_tag)
    result = await engine.end_transaction(request)
    assert result == schema.EndTransactionResponse(ok=True)


@pytest.mark.asyncio
async def test_end_transaction_with_tags(engine, graphql):
    tenant = "default"
    transaction_tag = "100"
    account_tag = "1000"
    source = "1000"
    destination = "393291234567"
    timestamp_auth = timezone("UTC").localize(datetime.utcnow())
    #
    await graphql(
        """
        mutation {
            upsertCarrier(
                tenant:%(tenant)s,
                carrier_tag:"TESTS",
                host:"carrier1.canyan.io",
                port:5060,
                protocol:UDP
                active:true
            ) {
                id
            }
            upsertPricelist(
                tenant:%(tenant)s,
                pricelist_tag:"TESTS",
                currency:EUR
            ) {
                id
            }
            upsertPricelistRate(
                tenant:%(tenant)s,
                pricelist_tag:"TESTS",
                carrier_tag:"TESTS",
                prefix:"39"
                active:true,
                connect_fee:0,
                rate:1,
                rate_increment:1,
                interval_start:0,
                description:"TESTS_ALL_DESTINATIONS"
            ) {
                id
            }
            upsertAccount(
                tenant: %(tenant)s,
                account_tag: %(account_tag)s,
                type: PREPAID,
                tags: ["A1"],
                pricelist_tags: ["TESTS"]
                balance: 100,
                active: true
            ) {
                id
            }
            upsertTransaction(
                tenant: %(tenant)s,
                transaction_tag: %(transaction_tag)s,
                account_tag: %(account_tag)s,
                source: %(source)s,
                destination: %(destination)s,
                timestamp_auth: %(timestamp_auth)s,
                primary: true,
                inbound: false,
            ) {
                id
            }
            beginAccountTransaction(
                tenant: %(tenant)s,
                account_tag: %(account_tag)s,
                transaction: {
                    transaction_tag: %(transaction_tag)s,
                    tags: ["T1"],
                    destination_rate: {
                        pricelist_tag:"TESTS"
                        prefix:"39"
                        connect_fee:0
                        rate:1
                        rate_increment:1
                        interval_start:0
                        carrier_tag:"TESTS"
                        description:"TESTS_ALL_DESTINATIONS"
                    }
                    source: %(source)s,
                    source_ip: "127.0.0.1",
                    destination: %(destination)s,
                    carrier_ip: "192.168.0.1",
                    timestamp_begin: %(timestamp_begin)s,
                    primary: true,
                    inbound: false
                }
            ) {
                ok
            }
        }"""
        % {
            'tenant': dumps(tenant),
            'account_tag': dumps(account_tag),
            'transaction_tag': dumps(transaction_tag),
            'source': dumps(source),
            'destination': dumps(destination),
            'timestamp_auth': dumps(timestamp_auth.strftime("%Y-%m-%dT%H:%M:%SZ")),
            'timestamp_begin': dumps(timestamp_auth.strftime("%Y-%m-%dT%H:%M:%SZ")),
        }
    )
    request = schema.EndTransactionRequest(transaction_tag=transaction_tag)
    result = await engine.end_transaction(request)
    assert result == schema.EndTransactionResponse(ok=True)
    #
    response = await graphql(
        """
        query {
            Transaction(
                tenant: %(tenant)s,
                transaction_tag: %(transaction_tag)s,
                account_tag: %(account_tag)s
            ) {
                tenant
                transaction_tag
                account_tag
                source
                source_ip
                destination
                carrier_ip
                duration
                inbound
                primary
                fee
                tags
            }
        }"""
        % {
            'tenant': dumps(tenant),
            'account_tag': dumps(account_tag),
            'transaction_tag': dumps(transaction_tag),
        }
    )
    assert response['data']['Transaction'] == {
        'account_tag': '1000',
        'carrier_ip': '192.168.0.1',
        'destination': '393291234567',
        'duration': 1,
        'fee': 1,
        'inbound': False,
        'primary': True,
        'source': '1000',
        'source_ip': '127.0.0.1',
        'tags': ['T1', 'A1'],
        'tenant': 'default',
        'transaction_tag': '100',
    }


@pytest.mark.asyncio
async def test_end_transaction_with_transaction_id_internal_error(engine, graphql):
    tenant = "default"
    transaction_tag = "100"
    account_tag = "1000"
    source = "1000"
    destination = "393291234567"
    timestamp_auth = timezone("UTC").localize(datetime.utcnow())
    #
    await graphql(
        """
        mutation {
            upsertCarrier(
                tenant:%(tenant)s,
                carrier_tag:"TESTS",
                host:"carrier1.canyan.io",
                port:5060,
                protocol:UDP
                active:true
            ) {
                id
            }
            upsertPricelist(
                tenant:%(tenant)s,
                pricelist_tag:"TESTS",
                currency:EUR
            ) {
                id
            }
            upsertPricelistRate(
                tenant:%(tenant)s,
                pricelist_tag:"TESTS",
                carrier_tag:"TESTS",
                prefix:"39"
                active:true,
                connect_fee:0,
                rate:1,
                rate_increment:1,
                interval_start:0,
                description:"TESTS_ALL_DESTINATIONS"
            ) {
                id
            }
            upsertAccount(
                tenant: %(tenant)s,
                account_tag: %(account_tag)s,
                type: PREPAID,
                pricelist_tags: ["TESTS"]
                balance: 100,
                active: true
            ) {
                id
            }
            upsertTransaction(
                tenant: %(tenant)s,
                transaction_tag: %(transaction_tag)s,
                account_tag: %(account_tag)s,
                source: %(source)s,
                destination: %(destination)s,
                timestamp_auth: %(timestamp_auth)s,
                primary: true,
                inbound: false,
            ) {
                id
            }
            beginAccountTransaction(
                tenant: %(tenant)s,
                account_tag: %(account_tag)s,
                transaction: {
                    transaction_tag: %(transaction_tag)s,
                    destination_rate: {
                        pricelist_tag:"TESTS"
                        prefix:"39"
                        connect_fee:0
                        rate:1
                        rate_increment:1
                        interval_start:0
                        carrier_tag:"TESTS"
                        description:"TESTS_ALL_DESTINATIONS"
                    }
                    source: %(source)s,
                    source_ip: "127.0.0.1",
                    destination: %(destination)s,
                    carrier_ip: "192.168.0.1",
                    timestamp_begin: %(timestamp_begin)s,
                    primary: true,
                    inbound: false
                }
            ) {
                ok
            }
        }"""
        % {
            'tenant': dumps(tenant),
            'account_tag': dumps(account_tag),
            'transaction_tag': dumps(transaction_tag),
            'source': dumps(source),
            'destination': dumps(destination),
            'timestamp_auth': dumps(timestamp_auth.strftime("%Y-%m-%dT%H:%M:%SZ")),
            'timestamp_begin': dumps(timestamp_auth.strftime("%Y-%m-%dT%H:%M:%SZ")),
        }
    )
    original_api = engine.get_api()

    class MockedAPI:
        def __getattribute__(self, item):
            if item == 'end_account_transaction':
                return super(MockedAPI, self).__getattribute__('void')
            return getattr(original_api, item)

        async def void(self, *args, **kw):
            return None

    mocked_api = MockedAPI()
    engine.set_api(mocked_api)
    #
    request = schema.EndTransactionRequest(transaction_tag=transaction_tag)
    result = await engine.end_transaction(request)
    assert result == schema.EndTransactionResponse(
        ok=False, failed_account_tag='1000', failed_reason='INTERNAL_ERROR'
    )


@pytest.mark.asyncio
async def test_end_transaction_with_transaction_id_internal_error_upsert(
    engine, graphql
):
    tenant = "default"
    transaction_tag = "100"
    account_tag = "1000"
    source = "1000"
    destination = "393291234567"
    timestamp_auth = timezone("UTC").localize(datetime.utcnow())
    #
    await graphql(
        """
        mutation {
            upsertCarrier(
                tenant:%(tenant)s,
                carrier_tag:"TESTS",
                host:"carrier1.canyan.io",
                port:5060,
                protocol:UDP
                active:true
            ) {
                id
            }
            upsertPricelist(
                tenant:%(tenant)s,
                pricelist_tag:"TESTS",
                currency:EUR
            ) {
                id
            }
            upsertPricelistRate(
                tenant:%(tenant)s,
                pricelist_tag:"TESTS",
                carrier_tag:"TESTS",
                prefix:"39"
                active:true,
                connect_fee:0,
                rate:1,
                rate_increment:1,
                interval_start:0,
                description:"TESTS_ALL_DESTINATIONS"
            ) {
                id
            }
            upsertAccount(
                tenant: %(tenant)s,
                account_tag: %(account_tag)s,
                type: PREPAID,
                pricelist_tags: ["TESTS"]
                balance: 100,
                active: true
            ) {
                id
            }
            upsertTransaction(
                tenant: %(tenant)s,
                transaction_tag: %(transaction_tag)s,
                account_tag: %(account_tag)s,
                source: %(source)s,
                destination: %(destination)s,
                timestamp_auth: %(timestamp_auth)s,
                primary: true,
                inbound: false,
            ) {
                id
            }
            beginAccountTransaction(
                tenant: %(tenant)s,
                account_tag: %(account_tag)s,
                transaction: {
                    transaction_tag: %(transaction_tag)s,
                    destination_rate: {
                        pricelist_tag:"TESTS"
                        prefix:"39"
                        connect_fee:0
                        rate:1
                        rate_increment:1
                        interval_start:0
                        carrier_tag:"TESTS"
                        description:"TESTS_ALL_DESTINATIONS"
                    }
                    source: %(source)s,
                    source_ip: "127.0.0.1",
                    destination: %(destination)s,
                    carrier_ip: "192.168.0.1",
                    timestamp_begin: %(timestamp_begin)s,
                    primary: true,
                    inbound: false
                }
            ) {
                ok
            }
        }"""
        % {
            'tenant': dumps(tenant),
            'account_tag': dumps(account_tag),
            'transaction_tag': dumps(transaction_tag),
            'source': dumps(source),
            'destination': dumps(destination),
            'timestamp_auth': dumps(timestamp_auth.strftime("%Y-%m-%dT%H:%M:%SZ")),
            'timestamp_begin': dumps(timestamp_auth.strftime("%Y-%m-%dT%H:%M:%SZ")),
        }
    )
    original_api = engine.get_api()

    class MockedAPI:
        def __getattribute__(self, item):
            if item == 'upsert_transaction':
                return super(MockedAPI, self).__getattribute__('void')
            return getattr(original_api, item)

        async def void(self, *args, **kw):
            return None

    mocked_api = MockedAPI()
    engine.set_api(mocked_api)
    #
    request = schema.EndTransactionRequest(transaction_tag=transaction_tag)
    result = await engine.end_transaction(request)
    assert result == schema.EndTransactionResponse(
        ok=False, failed_account_tag='1000', failed_reason='INTERNAL_ERROR'
    )


@pytest.mark.asyncio
async def test_end_transaction_with_transaction_id_internal_error_commit(
    engine, graphql
):
    tenant = "default"
    transaction_tag = "100"
    account_tag = "1000"
    source = "1000"
    destination = "393291234567"
    timestamp_auth = timezone("UTC").localize(datetime.utcnow())
    #
    await graphql(
        """
        mutation {
            upsertCarrier(
                tenant:%(tenant)s,
                carrier_tag:"TESTS",
                host:"carrier1.canyan.io",
                port:5060,
                protocol:UDP
                active:true
            ) {
                id
            }
            upsertPricelist(
                tenant:%(tenant)s,
                pricelist_tag:"TESTS",
                currency:EUR
            ) {
                id
            }
            upsertPricelistRate(
                tenant:%(tenant)s,
                pricelist_tag:"TESTS",
                carrier_tag:"TESTS",
                prefix:"39"
                active:true,
                connect_fee:0,
                rate:1,
                rate_increment:1,
                interval_start:0,
                description:"TESTS_ALL_DESTINATIONS"
            ) {
                id
            }
            upsertAccount(
                tenant: %(tenant)s,
                account_tag: %(account_tag)s,
                type: PREPAID,
                pricelist_tags: ["TESTS"]
                balance: 100,
                active: true
            ) {
                id
            }
            upsertTransaction(
                tenant: %(tenant)s,
                transaction_tag: %(transaction_tag)s,
                account_tag: %(account_tag)s,
                source: %(source)s,
                destination: %(destination)s,
                timestamp_auth: %(timestamp_auth)s,
                primary: true,
                inbound: false,
            ) {
                id
            }
            beginAccountTransaction(
                tenant: %(tenant)s,
                account_tag: %(account_tag)s,
                transaction: {
                    transaction_tag: %(transaction_tag)s,
                    destination_rate: {
                        pricelist_tag:"TESTS"
                        prefix:"39"
                        connect_fee:0
                        rate:1
                        rate_increment:1
                        interval_start:0
                        carrier_tag:"TESTS"
                        description:"TESTS_ALL_DESTINATIONS"
                    }
                    source: %(source)s,
                    source_ip: "127.0.0.1",
                    destination: %(destination)s,
                    carrier_ip: "192.168.0.1",
                    timestamp_begin: %(timestamp_begin)s,
                    primary: true,
                    inbound: false
                }
            ) {
                ok
            }
        }"""
        % {
            'tenant': dumps(tenant),
            'account_tag': dumps(account_tag),
            'transaction_tag': dumps(transaction_tag),
            'source': dumps(source),
            'destination': dumps(destination),
            'timestamp_auth': dumps(timestamp_auth.strftime("%Y-%m-%dT%H:%M:%SZ")),
            'timestamp_begin': dumps(timestamp_auth.strftime("%Y-%m-%dT%H:%M:%SZ")),
        }
    )
    original_api = engine.get_api()

    class MockedAPI:
        def __getattribute__(self, item):
            if item == 'commit_account_transaction':
                return super(MockedAPI, self).__getattribute__('void')
            return getattr(original_api, item)

        async def void(self, *args, **kw):
            return None

    mocked_api = MockedAPI()
    engine.set_api(mocked_api)
    #
    request = schema.EndTransactionRequest(transaction_tag=transaction_tag)
    result = await engine.end_transaction(request)
    assert result == schema.EndTransactionResponse(
        ok=False, failed_account_tag='1000', failed_reason='INTERNAL_ERROR'
    )
