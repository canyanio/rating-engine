import pytest  # type: ignore

from datetime import datetime
from json import dumps

from ..schema import engine as schema
from ..services.api import APIService


@pytest.mark.asyncio
async def test_authorization_transaction(engine):
    request = schema.AuthorizationTransactionRequest(
        transaction_tag="100", timestamp_auth=datetime.utcnow()
    )
    result = await engine.authorization_transaction(request)
    assert result == schema.AuthorizationTransactionResponse(ok=True)


@pytest.mark.asyncio
async def test_authorization_transaction_account_tag(engine, graphql):
    tenant = "default"
    account_tag = "1000"
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
        }"""
        % {'tenant': dumps(tenant), 'account_tag': dumps(account_tag)}
    )
    #
    request = schema.AuthorizationTransactionRequest(
        transaction_tag="100", account_tag=account_tag, timestamp_auth=datetime.utcnow()
    )
    result = await engine.authorization_transaction(request)
    assert result == schema.AuthorizationTransactionResponse(ok=True)


@pytest.mark.asyncio
async def test_authorization_transaction_destination_account_tag(engine, graphql):
    tenant = "default"
    account_tag = "1000"
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
        }"""
        % {'tenant': dumps(tenant), 'account_tag': dumps(account_tag)}
    )
    #
    request = schema.AuthorizationTransactionRequest(
        transaction_tag="100",
        destination_account_tag=account_tag,
        timestamp_auth=datetime.utcnow(),
    )
    result = await engine.authorization_transaction(request)
    assert result == schema.AuthorizationTransactionResponse(ok=True)


@pytest.mark.asyncio
async def test_authorization_transaction_account_tag_internal_error(engine):
    dummy_api = APIService("http://127.0.0.1:81")
    engine.set_api(dummy_api)
    #
    request = schema.AuthorizationTransactionRequest(
        transaction_tag="100", account_tag="1000", timestamp_auth=datetime.utcnow()
    )
    result = await engine.authorization_transaction(request)
    assert result == schema.AuthorizationTransactionResponse(ok=False)
