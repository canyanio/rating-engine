import pytest  # type: ignore

from json import dumps

from ..schema import engine as schema


@pytest.mark.asyncio
async def test_end_transaction(engine):
    request = schema.EndTransactionRequest(transaction_tag="100",)
    result = await engine.end_transaction(request)
    assert result == schema.EndTransactionResponse(ok=False)


@pytest.mark.asyncio
async def test_end_transaction_failed_no_accounts_provided(engine):
    tenant = "default"
    transaction_tag = "100"
    #
    request = schema.EndTransactionRequest(
        tenant=tenant, transaction_tag=transaction_tag,
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
        tenant=tenant, transaction_tag=transaction_tag, account_tag=account_tag,
    )
    response = await engine.end_transaction(request)
    assert response.ok is False
    assert account_tag == response.failed_account_tag
    assert 'NOT_FOUND' == response.failed_account_reason


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
    assert 'NOT_FOUND' == response.failed_account_reason


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
    assert 'NOT_FOUND' == response.failed_account_reason


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
        tenant=tenant, transaction_tag=transaction_tag, account_tag=account_tag,
    )
    response = await engine.end_transaction(request)
    assert response.ok is False
