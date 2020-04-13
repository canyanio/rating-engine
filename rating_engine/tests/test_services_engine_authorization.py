import pytest  # type: ignore

from datetime import datetime, timedelta
from json import dumps
from pytz import timezone

from ..enums import MethodName, RPCCallPriority
from ..schema import engine as schema


@pytest.mark.asyncio
async def test_authorization(engine, mocked_bus):
    request = schema.AuthorizationRequest(transaction_tag="100")
    result = await engine.authorization(request)
    assert result == schema.AuthorizationResponse(
        transaction_tag="100", authorized=False
    )
    #
    assert len(mocked_bus.calls) == 0


@pytest.mark.asyncio
async def test_authorization_failed_no_accounts_provided(engine, mocked_bus):
    tenant = "default"
    transaction_tag = "100"
    destination = "393291234567"
    #
    request = schema.AuthorizationRequest(
        tenant=tenant, transaction_tag=transaction_tag, destination=destination
    )
    response = await engine.authorization(request)
    assert response.authorized is False
    assert response.authorized_destination is False
    #
    assert len(mocked_bus.calls) == 0


@pytest.mark.asyncio
async def test_authorization_failed_account_not_found(engine, mocked_bus):
    tenant = "default"
    transaction_tag = "100"
    account_tag = "1000"
    destination = "393291234567"
    #
    request = schema.AuthorizationRequest(
        tenant=tenant,
        transaction_tag=transaction_tag,
        account_tag=account_tag,
        destination=destination,
    )
    response = await engine.authorization(request)
    assert response.authorized is False
    assert response.authorized_destination is False
    assert account_tag == response.unauthorized_account_tag
    assert 'NOT_FOUND' == response.unauthorized_reason
    #
    assert len(mocked_bus.calls) == 0


@pytest.mark.asyncio
async def test_authorization_failed_account_not_active(engine, graphql, mocked_bus):
    tenant = "default"
    transaction_tag = "100"
    account_tag = "1000"
    destination = "393291234567"
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
    #
    request = schema.AuthorizationRequest(
        tenant=tenant,
        transaction_tag=transaction_tag,
        account_tag=account_tag,
        destination=destination,
    )
    response = await engine.authorization(request)
    assert response.authorized is False
    assert response.authorized_destination is False
    assert account_tag == response.unauthorized_account_tag
    assert 'NOT_ACTIVE' == response.unauthorized_reason
    #
    assert len(mocked_bus.calls) == 0


@pytest.mark.asyncio
async def test_authorization_failed_unreacheable_destination(
    engine, graphql, mocked_bus
):
    tenant = "default"
    transaction_tag = "100"
    account_tag = "1000"
    destination = "393291234567"
    #
    response = await graphql(
        """
        mutation {
            upsertAccount(
                tenant: %(tenant)s,
                account_tag: %(account_tag)s,
                type: PREPAID,
                balance: 1000000,
                pricelist_tags: ["DOES_NOT_EXIST"],
                active: true
            ) {
                id
            }
        }"""
        % {'tenant': dumps(tenant), 'account_tag': dumps(account_tag)}
    )
    #
    request = schema.AuthorizationRequest(
        tenant=tenant,
        transaction_tag=transaction_tag,
        account_tag=account_tag,
        destination=destination,
    )
    response = await engine.authorization(request)
    assert response.authorized is False
    assert response.authorized_destination is False
    assert account_tag == response.unauthorized_account_tag
    assert 'UNREACHEABLE_DESTINATION' == response.unauthorized_reason
    #
    assert len(mocked_bus.calls) == 0


@pytest.mark.asyncio
async def test_authorization_failed_destination_account_not_found(engine, mocked_bus):
    tenant = "default"
    transaction_tag = "100"
    destination_account_tag = "1001"
    destination = "393291234567"
    #
    request = schema.AuthorizationRequest(
        tenant=tenant,
        transaction_tag=transaction_tag,
        destination_account_tag=destination_account_tag,
        destination=destination,
    )
    response = await engine.authorization(request)
    assert response.authorized is False
    assert response.authorized_destination is False
    assert destination_account_tag == response.unauthorized_account_tag
    assert 'NOT_FOUND' == response.unauthorized_reason
    #
    assert len(mocked_bus.calls) == 0


@pytest.mark.asyncio
async def test_authorization_failed_destination_account_not_active(
    engine, graphql, mocked_bus
):
    tenant = "default"
    transaction_tag = "100"
    destination_account_tag = "1001"
    destination = "393291234567"
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
        % {'tenant': dumps(tenant), 'account_tag': dumps(destination_account_tag)}
    )
    #
    request = schema.AuthorizationRequest(
        tenant=tenant,
        transaction_tag=transaction_tag,
        destination_account_tag=destination_account_tag,
        destination=destination,
    )
    response = await engine.authorization(request)
    assert response.authorized is False
    assert response.authorized_destination is False
    assert destination_account_tag == response.unauthorized_account_tag
    assert 'NOT_ACTIVE' == response.unauthorized_reason
    #
    assert len(mocked_bus.calls) == 0


@pytest.mark.asyncio
async def test_authorization_failed_account_and_destination_account_not_found(
    engine, mocked_bus
):
    tenant = "default"
    transaction_tag = "100"
    account_tag = "1000"
    destination_account_tag = "1001"
    destination = "393291234567"
    #
    request = schema.AuthorizationRequest(
        tenant=tenant,
        transaction_tag=transaction_tag,
        account_tag=account_tag,
        destination_account_tag=destination_account_tag,
        destination=destination,
    )
    response = await engine.authorization(request)
    assert response.authorized is False
    assert response.authorized_destination is False
    assert account_tag == response.unauthorized_account_tag
    assert 'NOT_FOUND' == response.unauthorized_reason
    #
    assert len(mocked_bus.calls) == 0


@pytest.mark.asyncio
async def test_authorization_failed_account_balance_insufficient(
    engine, graphql, mocked_bus
):
    tenant = "default"
    transaction_tag = "100"
    account_tag = "1000"
    destination = "393291234567"
    #
    response = await graphql(
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
                balance: 0,
                active: true
            ) {
                id
            }
        }"""
        % {'tenant': dumps(tenant), 'account_tag': dumps(account_tag)}
    )
    #
    timestamp_auth = timezone("UTC").localize(datetime.utcnow())
    request = schema.AuthorizationRequest(
        tenant=tenant,
        transaction_tag=transaction_tag,
        account_tag=account_tag,
        destination=destination,
        timestamp_auth=timestamp_auth,
    )
    response = await engine.authorization(request)
    assert response.authorized is False
    assert response.authorized_destination is False
    assert account_tag == response.unauthorized_account_tag
    assert 'BALANCE_INSUFFICIENT' == response.unauthorized_reason
    #
    assert len(mocked_bus.calls) == 1
    assert {
        'expiration': 10,
        'kwargs': {
            'request': {
                'account_tag': '1000',
                'authorized': False,
                'authorized_destination': False,
                'balance': 0,
                'carrier_ip': None,
                'carriers': [],
                'destination': '393291234567',
                'destination_account_tag': None,
                'max_available_units': 0,
                'source': None,
                'source_ip': None,
                'tags': [],
                'tenant': 'default',
                'timestamp_auth': timestamp_auth,
                'transaction_tag': '100',
                'unauthorized_reason': None,
                'unauthorized_account_tag': None,
            }
        },
        'method': MethodName.AUTHORIZATION_TRANSACTION.value,
        'priority': RPCCallPriority.LOW,
    } == mocked_bus.calls[0]


@pytest.mark.asyncio
async def test_authorization_failed_account_virtual_balance_insufficient(
    engine, graphql, mocked_bus
):
    tenant = "default"
    transaction_tag = "100"
    account_tag = "1000"
    destination = "393291234567"
    #
    response = await graphql(
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
                balance: 10,
                active: true
            ) {
                id
            }
        }"""
        % {'tenant': dumps(tenant), 'account_tag': dumps(account_tag)}
    )
    #
    timestamp_begin = timezone("UTC").localize(datetime.utcnow()) - timedelta(
        seconds=15
    )
    request = schema.BeginTransactionRequest(
        tenant=tenant,
        transaction_tag=transaction_tag,
        account_tag=account_tag,
        destination=destination,
        timestamp_begin=timestamp_begin.strftime("%Y-%m-%dT%H:%M:%SZ"),
    )
    response = await engine.begin_transaction(request)
    assert response.ok is True
    #
    timestamp_auth = timezone("UTC").localize(datetime.utcnow())
    request = schema.AuthorizationRequest(
        tenant=tenant,
        transaction_tag=transaction_tag,
        account_tag=account_tag,
        destination=destination,
        timestamp_auth=timestamp_auth,
    )
    response = await engine.authorization(request)
    assert response.authorized is False
    assert response.authorized_destination is False
    assert account_tag == response.unauthorized_account_tag
    assert 'BALANCE_INSUFFICIENT' == response.unauthorized_reason
    #
    assert len(mocked_bus.calls) == 1
    assert {
        'expiration': 10,
        'kwargs': {
            'request': {
                'account_tag': '1000',
                'authorized': False,
                'authorized_destination': False,
                'balance': 0,
                'carrier_ip': None,
                'carriers': [],
                'destination': '393291234567',
                'destination_account_tag': None,
                'max_available_units': 0,
                'source': None,
                'source_ip': None,
                'tags': [],
                'tenant': 'default',
                'timestamp_auth': timestamp_auth,
                'transaction_tag': '100',
                'unauthorized_reason': None,
                'unauthorized_account_tag': None,
            }
        },
        'method': MethodName.AUTHORIZATION_TRANSACTION.value,
        'priority': RPCCallPriority.LOW,
    } == mocked_bus.calls[0]


@pytest.mark.asyncio
async def test_authorization_successful_account_virtual_balance_sufficient(
    engine, graphql, mocked_bus
):
    tenant = "default"
    transaction_tag = "100"
    account_tag = "1000"
    destination = "393291234567"
    #
    response = await graphql(
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
                balance: 20,
                active: true
            ) {
                id
            }
        }"""
        % {'tenant': dumps(tenant), 'account_tag': dumps(account_tag)}
    )
    #
    timestamp_begin = timezone("UTC").localize(datetime.utcnow()) - timedelta(
        seconds=15
    )
    request = schema.BeginTransactionRequest(
        tenant=tenant,
        transaction_tag=transaction_tag,
        account_tag=account_tag,
        destination=destination,
        timestamp_begin=timestamp_begin.strftime("%Y-%m-%dT%H:%M:%SZ"),
    )
    response = await engine.begin_transaction(request)
    assert response.ok is True
    #
    timestamp_auth = timezone("UTC").localize(datetime.utcnow())
    request = schema.AuthorizationRequest(
        tenant=tenant,
        transaction_tag=transaction_tag,
        account_tag=account_tag,
        destination=destination,
        timestamp_auth=timestamp_auth,
    )
    response = await engine.authorization(request)
    assert response.authorized is True
    #
    assert len(mocked_bus.calls) == 1
    # balance is ephemeral, can either by 3 or 4 based on timing of the test
    assert mocked_bus.calls[0]['kwargs']['request']['balance'] in (3, 4)
    mocked_bus.calls[0]['kwargs']['request']['balance'] = 4
    assert {
        'expiration': 10,
        'kwargs': {
            'request': {
                'account_tag': '1000',
                'authorized': True,
                'authorized_destination': False,
                'balance': 4,
                'carrier_ip': None,
                'carriers': ['UDP:carrier1.canyan.io:5060'],
                'destination': '393291234567',
                'destination_account_tag': None,
                'max_available_units': 4,
                'source': None,
                'source_ip': None,
                'tags': [],
                'tenant': 'default',
                'timestamp_auth': timestamp_auth,
                'transaction_tag': '100',
                'unauthorized_reason': None,
                'unauthorized_account_tag': None,
            }
        },
        'method': MethodName.AUTHORIZATION_TRANSACTION.value,
        'priority': RPCCallPriority.LOW,
    } == mocked_bus.calls[0]


@pytest.mark.asyncio
async def test_authorization_successful_with_timestamp(engine, graphql, mocked_bus):
    tenant = "default"
    transaction_tag = "100"
    account_tag = "1000"
    destination = "393291234567"
    #
    response = await graphql(
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
                balance: 20,
                active: true
            ) {
                id
            }
        }"""
        % {'tenant': dumps(tenant), 'account_tag': dumps(account_tag)}
    )
    #
    timestamp_auth = timezone("UTC").localize(datetime.utcnow())
    timestamp_begin = timestamp_auth - timedelta(seconds=15)
    request = schema.BeginTransactionRequest(
        tenant=tenant,
        transaction_tag=transaction_tag,
        account_tag=account_tag,
        destination=destination,
        timestamp_auth=timestamp_auth,
        timestamp_begin=timestamp_begin.strftime("%Y-%m-%dT%H:%M:%SZ"),
    )
    response = await engine.begin_transaction(request)
    assert response.ok is True
    #
    request = schema.AuthorizationRequest(
        tenant=tenant,
        transaction_tag=transaction_tag,
        account_tag=account_tag,
        destination=destination,
        timestamp_auth=timestamp_auth,
    )
    response = await engine.authorization(request)
    assert response.authorized is True
    #
    assert len(mocked_bus.calls) == 1
    # balance is ephemeral, can either by 3 or 4 based on timing of the test
    assert mocked_bus.calls[0]['kwargs']['request']['balance'] in (3, 4)
    mocked_bus.calls[0]['kwargs']['request']['balance'] = 4
    assert {
        'expiration': 10,
        'kwargs': {
            'request': {
                'account_tag': '1000',
                'authorized': True,
                'authorized_destination': False,
                'balance': 4,
                'carrier_ip': None,
                'carriers': ['UDP:carrier1.canyan.io:5060'],
                'destination': '393291234567',
                'destination_account_tag': None,
                'max_available_units': 4,
                'source': None,
                'source_ip': None,
                'tags': [],
                'tenant': 'default',
                'timestamp_auth': timestamp_auth,
                'transaction_tag': '100',
                'unauthorized_reason': None,
                'unauthorized_account_tag': None,
            }
        },
        'method': MethodName.AUTHORIZATION_TRANSACTION.value,
        'priority': RPCCallPriority.LOW,
    } == mocked_bus.calls[0]


@pytest.mark.asyncio
async def test_authorization_failed_account_too_many_running_transactions(
    engine, graphql, mocked_bus
):
    tenant = "default"
    transaction_tag = "100"
    account_tag = "1000"
    destination = "393291234567"
    #
    response = await graphql(
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
                balance: 1000000,
                max_concurrent_transactions: 0,
                active: true
            ) {
                id
            }
        }"""
        % {'tenant': dumps(tenant), 'account_tag': dumps(account_tag)}
    )
    #
    timestamp_auth = timezone("UTC").localize(datetime.utcnow())
    request = schema.AuthorizationRequest(
        tenant=tenant,
        transaction_tag=transaction_tag,
        account_tag=account_tag,
        destination=destination,
        timestamp_auth=timestamp_auth,
    )
    response = await engine.authorization(request)
    assert response.authorized is False
    assert response.authorized_destination is False
    assert account_tag == response.unauthorized_account_tag
    assert 'TOO_MANY_RUNNING_TRANSACTIONS' == response.unauthorized_reason
    #
    assert len(mocked_bus.calls) == 1
    assert {
        'expiration': 10,
        'kwargs': {
            'request': {
                'account_tag': '1000',
                'authorized': False,
                'authorized_destination': False,
                'balance': 0,
                'carrier_ip': None,
                'carriers': [],
                'destination': '393291234567',
                'destination_account_tag': None,
                'max_available_units': 0,
                'source': None,
                'source_ip': None,
                'tags': [],
                'tenant': 'default',
                'timestamp_auth': timestamp_auth,
                'transaction_tag': '100',
                'unauthorized_reason': None,
                'unauthorized_account_tag': None,
            }
        },
        'method': MethodName.AUTHORIZATION_TRANSACTION.value,
        'priority': RPCCallPriority.LOW,
    } == mocked_bus.calls[0]


@pytest.mark.asyncio
async def test_authorization_failed_account_and_destination_account_too_many_running_transactions(
    engine, graphql, mocked_bus
):
    tenant = "default"
    transaction_tag = "100"
    account_tag = "1000"
    destination_account_tag = "1001"
    destination = "393291234567"
    #
    response = await graphql(
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
            a1:upsertAccount(
                tenant: %(tenant)s,
                account_tag: %(account_tag)s,
                type: PREPAID,
                pricelist_tags: ["TESTS"]
                balance: 1000000,
                active: true
            ) {
                id
            }
            a2:upsertAccount(
                tenant: %(tenant)s,
                account_tag: %(destination_account_tag)s,
                type: PREPAID,
                pricelist_tags: ["TESTS"]
                balance: 1000000,
                max_concurrent_transactions: 0,
                active: true
            ) {
                id
            }
        }"""
        % {
            'tenant': dumps(tenant),
            'account_tag': dumps(account_tag),
            'destination_account_tag': dumps(destination_account_tag),
        }
    )
    #
    timestamp_auth = timezone("UTC").localize(datetime.utcnow())
    request = schema.AuthorizationRequest(
        tenant=tenant,
        transaction_tag=transaction_tag,
        account_tag=account_tag,
        destination_account_tag=destination_account_tag,
        destination=destination,
        timestamp_auth=timestamp_auth,
    )
    response = await engine.authorization(request)
    assert response.authorized is False
    assert response.authorized_destination is False
    assert destination_account_tag == response.unauthorized_account_tag
    assert 'TOO_MANY_RUNNING_TRANSACTIONS' == response.unauthorized_reason
    #
    assert len(mocked_bus.calls) == 1
    assert {
        'expiration': 10,
        'kwargs': {
            'request': {
                'account_tag': '1000',
                'authorized': False,
                'authorized_destination': False,
                'balance': 0,
                'carrier_ip': None,
                'carriers': [],
                'destination': '393291234567',
                'destination_account_tag': '1001',
                'max_available_units': 0,
                'source': None,
                'source_ip': None,
                'tags': [],
                'tenant': 'default',
                'timestamp_auth': timestamp_auth,
                'transaction_tag': '100',
                'unauthorized_reason': None,
                'unauthorized_account_tag': None,
            }
        },
        'method': MethodName.AUTHORIZATION_TRANSACTION.value,
        'priority': RPCCallPriority.LOW,
    } == mocked_bus.calls[0]


@pytest.mark.asyncio
async def test_authorization_failed_destination_account_too_many_running_transactions(
    engine, graphql, mocked_bus
):
    tenant = "default"
    transaction_tag = "100"
    destination_account_tag = "1001"
    destination = "393291234567"
    #
    response = await graphql(
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
                account_tag: %(destination_account_tag)s,
                type: PREPAID,
                pricelist_tags: ["TESTS"]
                balance: 1000000,
                max_concurrent_transactions: 0,
                active: true
            ) {
                id
            }
        }"""
        % {
            'tenant': dumps(tenant),
            'destination_account_tag': dumps(destination_account_tag),
        }
    )
    #
    timestamp_auth = timezone("UTC").localize(datetime.utcnow())
    request = schema.AuthorizationRequest(
        tenant=tenant,
        transaction_tag=transaction_tag,
        destination_account_tag=destination_account_tag,
        destination=destination,
        timestamp_auth=timestamp_auth,
    )
    response = await engine.authorization(request)
    assert response.authorized is False
    assert response.authorized_destination is False
    assert destination_account_tag == response.unauthorized_account_tag
    assert 'TOO_MANY_RUNNING_TRANSACTIONS' == response.unauthorized_reason
    #
    assert len(mocked_bus.calls) == 1
    assert {
        'expiration': 10,
        'kwargs': {
            'request': {
                'account_tag': None,
                'authorized': False,
                'authorized_destination': False,
                'balance': 0,
                'carrier_ip': None,
                'carriers': [],
                'destination': '393291234567',
                'destination_account_tag': '1001',
                'max_available_units': 0,
                'source': None,
                'source_ip': None,
                'tags': [],
                'tenant': 'default',
                'timestamp_auth': timestamp_auth,
                'transaction_tag': '100',
                'unauthorized_reason': None,
                'unauthorized_account_tag': None,

            }
        },
        'method': MethodName.AUTHORIZATION_TRANSACTION.value,
        'priority': RPCCallPriority.LOW,
    } == mocked_bus.calls[0]
