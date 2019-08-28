import pytest  # type: ignore

from json import dumps

from ..services import engine as engine_service
from ..schema import engine as schema


@pytest.mark.asyncio
async def test_transaction_successful_account_and_destination_account(
    engine: engine_service.EngineService, graphql
):
    tenant = 'default'
    account_tag = "1000"
    destination_account_tag = "2000"
    transaction_tag = "100"
    source = "393291234557"
    destination = "393291234557"
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
                balance: 1000000,
                pricelist_tags: ["TESTS"]
            ) {
                id
            }
            a2:upsertAccount(
                tenant: %(tenant)s,
                account_tag: %(destination_account_tag)s,
                type: PREPAID,
                balance: 1000000,
                pricelist_tags: ["TESTS"]
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
        source=source,
        destination=destination,
    )
    response = await engine.authorization(request)
    assert response.authorized is True
    assert response.authorized_destination is True
    #
    request_begin = schema.BeginTransactionRequest(
        tenant=tenant,
        transaction_tag=transaction_tag,
        account_tag=account_tag,
        destination_account_tag=destination_account_tag,
        source=source,
        destination=destination,
    )
    response = await engine.begin_transaction(request_begin)
    assert response.ok is True
    #
    request_end = schema.EndTransactionRequest(
        tenant=tenant,
        transaction_tag=transaction_tag,
        account_tag=account_tag,
        destination_account_tag=destination_account_tag,
        source=source,
        destination=destination,
    )
    response = await engine.end_transaction(request_end)
    assert response.ok is True


@pytest.mark.asyncio
async def test_transaction_successful_account(
    engine: engine_service.EngineService, graphql
):
    tenant = 'default'
    account_tag = "1000"
    transaction_tag = "100"
    source = "393291234557"
    destination = "393291234557"
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
                balance: 1000000,
                pricelist_tags: ["TESTS"]
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
        source=source,
        destination=destination,
    )
    response = await engine.authorization(request)
    assert response.authorized is True
    assert response.authorized_destination is False
    #
    request_begin = schema.BeginTransactionRequest(
        tenant=tenant,
        transaction_tag=transaction_tag,
        account_tag=account_tag,
        source=source,
        destination=destination,
    )
    response = await engine.begin_transaction(request_begin)
    assert response.ok is True
    #
    request_end = schema.EndTransactionRequest(
        tenant=tenant,
        transaction_tag=transaction_tag,
        account_tag=account_tag,
        source=source,
        destination=destination,
    )
    response = await engine.end_transaction(request_end)
    assert response.ok is True


@pytest.mark.asyncio
async def test_transaction_successful_destination_account(
    engine: engine_service.EngineService, graphql
):
    tenant = 'default'
    destination_account_tag = "2000"
    transaction_tag = "100"
    source = "393291234557"
    destination = "393291234557"
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
            a2:upsertAccount(
                tenant: %(tenant)s,
                account_tag: %(destination_account_tag)s,
                type: PREPAID,
                balance: 1000000,
                pricelist_tags: ["TESTS"]
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
        source=source,
        destination=destination,
    )
    response = await engine.authorization(request)
    assert response.authorized is False
    assert response.authorized_destination is True
    #
    request_begin = schema.BeginTransactionRequest(
        tenant=tenant,
        transaction_tag=transaction_tag,
        destination_account_tag=destination_account_tag,
        source=source,
        destination=destination,
    )
    response = await engine.begin_transaction(request_begin)
    assert response.ok is True
    #
    request_end = schema.EndTransactionRequest(
        tenant=tenant,
        transaction_tag=transaction_tag,
        destination_account_tag=destination_account_tag,
        source=source,
        destination=destination,
    )
    response = await engine.end_transaction(request_end)
    assert response.ok is True
