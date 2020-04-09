import pytest  # type: ignore

from datetime import datetime
from json import dumps
from pytz import timezone

from ..schema import engine as schema


@pytest.mark.asyncio
async def test_rollback_transaction(engine):
    request = schema.RollbackTransactionRequest(transaction_tag="100")
    result = await engine.rollback_transaction(request)
    assert result == schema.RollbackTransactionResponse(ok=False)


@pytest.mark.asyncio
async def test_rollback_transaction_failed(engine):
    tenant = "default"
    transaction_tag = "100"
    account_tag = "1000"
    #
    request = schema.RollbackTransactionRequest(
        tenant=tenant, transaction_tag=transaction_tag, account_tag=account_tag
    )
    response = await engine.rollback_transaction(request)
    assert response.ok is False


@pytest.mark.asyncio
async def test_rollback_transaction_with_transaction_id_inbound(engine, graphql):
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
                inbound: true,
            ) {
                id
            }
        }"""
        % {
            'tenant': dumps(tenant),
            'account_tag': dumps(account_tag),
            'transaction_tag': dumps(transaction_tag),
            'source': dumps(source),
            'destination': dumps(destination),
            'timestamp_auth': dumps(timestamp_auth.strftime("%Y-%m-%dT%H:%M:%SZ")),
        }
    )
    request = schema.BeginTransactionRequest(transaction_tag=transaction_tag)
    result = await engine.begin_transaction(request)
    assert result == schema.BeginTransactionResponse(ok=True)
    #
    request = schema.RollbackTransactionRequest(transaction_tag=transaction_tag)
    result = await engine.rollback_transaction(request)
    assert result == schema.RollbackTransactionResponse(ok=True)
