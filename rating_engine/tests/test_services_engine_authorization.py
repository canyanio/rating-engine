import pytest  # type: ignore

from datetime import datetime, timedelta
from json import dumps
from pytz import timezone

from ..schema import engine as schema


@pytest.mark.asyncio
async def test_authorization(engine):
    request = schema.AuthorizationRequest(transaction_tag="100",)
    result = await engine.authorization(request)
    assert result == schema.AuthorizationResponse(
        transaction_tag="100", authorized=False,
    )


@pytest.mark.asyncio
async def test_authorization_failed_no_accounts_provided(engine):
    tenant = "default"
    transaction_tag = "100"
    destination = "393291234567"
    #
    request = schema.AuthorizationRequest(
        tenant=tenant, transaction_tag=transaction_tag, destination=destination,
    )
    response = await engine.authorization(request)
    assert response.authorized is False
    assert response.authorized_destination is False


@pytest.mark.asyncio
async def test_authorization_failed_account_not_found(engine):
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
    assert 'NOT_FOUND' == response.unauthorized_account_reason


@pytest.mark.asyncio
async def test_authorization_failed_account_not_active(engine, graphql):
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
    assert 'NOT_ACTIVE' == response.unauthorized_account_reason


@pytest.mark.asyncio
async def test_authorization_failed_unreacheable_destination(engine, graphql):
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
    assert 'UNREACHEABLE_DESTINATION' == response.unauthorized_account_reason


@pytest.mark.asyncio
async def test_authorization_failed_destination_account_not_found(engine):
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
    assert 'NOT_FOUND' == response.unauthorized_account_reason


@pytest.mark.asyncio
async def test_authorization_failed_destination_account_not_active(engine, graphql):
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
    assert 'NOT_ACTIVE' == response.unauthorized_account_reason


@pytest.mark.asyncio
async def test_authorization_failed_account_and_destination_account_not_found(engine):
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
    assert 'NOT_FOUND' == response.unauthorized_account_reason


@pytest.mark.asyncio
async def test_authorization_failed_account_balance_insufficient(engine, graphql):
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
    assert 'BALANCE_INSUFFICIENT' == response.unauthorized_account_reason


@pytest.mark.asyncio
async def test_authorization_failed_account_virtual_balance_insufficient(
    engine, graphql
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
    assert 'BALANCE_INSUFFICIENT' == response.unauthorized_account_reason


@pytest.mark.asyncio
async def test_authorization_successful_account_virtual_balance_sufficient(
    engine, graphql
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
    request = schema.AuthorizationRequest(
        tenant=tenant,
        transaction_tag=transaction_tag,
        account_tag=account_tag,
        destination=destination,
    )
    response = await engine.authorization(request)
    assert response.authorized is True


@pytest.mark.asyncio
async def test_authorization_failed_account_too_many_running_transactions(
    engine, graphql
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
    assert 'TOO_MANY_RUNNING_TRANSACTIONS' == response.unauthorized_account_reason


@pytest.mark.asyncio
async def test_authorization_failed_account_and_destination_account_too_many_running_transactions(
    engine, graphql
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
    assert destination_account_tag == response.unauthorized_account_tag
    assert 'TOO_MANY_RUNNING_TRANSACTIONS' == response.unauthorized_account_reason


@pytest.mark.asyncio
async def test_authorization_failed_destination_account_too_many_running_transactions(
    engine, graphql
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
    assert 'TOO_MANY_RUNNING_TRANSACTIONS' == response.unauthorized_account_reason
