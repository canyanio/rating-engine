import pytest  # type: ignore

from json import dumps

from ..schema import engine as schema


@pytest.mark.asyncio
async def test_begin_transaction(engine):
    request = schema.BeginTransactionRequest(transaction_tag="100",)
    result = await engine.begin_transaction(request)
    assert result == schema.BeginTransactionResponse(ok=False)


@pytest.mark.asyncio
async def test_begin_transaction_failed_no_accounts_provided(engine):
    tenant = "default"
    transaction_tag = "100"
    destination = "393291234567"
    #
    request = schema.BeginTransactionRequest(
        tenant=tenant, transaction_tag=transaction_tag, destination=destination,
    )
    response = await engine.begin_transaction(request)
    assert response.ok is False


@pytest.mark.asyncio
async def test_begin_transaction_failed_account_not_found(engine):
    tenant = "default"
    transaction_tag = "100"
    account_tag = "1000"
    destination = "393291234567"
    #
    request = schema.BeginTransactionRequest(
        tenant=tenant,
        transaction_tag=transaction_tag,
        account_tag=account_tag,
        destination=destination,
    )
    response = await engine.begin_transaction(request)
    assert response.ok is False
    assert account_tag == response.failed_account_tag
    assert 'NOT_FOUND' == response.failed_account_reason


@pytest.mark.asyncio
async def test_begin_transaction_failed_account_not_active(engine, graphql):
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
    request = schema.BeginTransactionRequest(
        tenant=tenant,
        transaction_tag=transaction_tag,
        account_tag=account_tag,
        destination=destination,
    )
    response = await engine.begin_transaction(request)
    assert response.ok is False
    assert account_tag == response.failed_account_tag
    assert 'NOT_ACTIVE' == response.failed_account_reason


@pytest.mark.asyncio
async def test_begin_transaction_failed_destination_account_not_found(engine):
    tenant = "default"
    transaction_tag = "100"
    destination_account_tag = "1001"
    destination = "393291234567"
    #
    request = schema.BeginTransactionRequest(
        tenant=tenant,
        transaction_tag=transaction_tag,
        destination_account_tag=destination_account_tag,
        destination=destination,
    )
    response = await engine.begin_transaction(request)
    assert response.ok is False
    assert destination_account_tag == response.failed_account_tag
    assert 'NOT_FOUND' == response.failed_account_reason


@pytest.mark.asyncio
async def test_begin_transaction_failed_destination_account_not_active(engine, graphql):
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
    request = schema.BeginTransactionRequest(
        tenant=tenant,
        transaction_tag=transaction_tag,
        destination_account_tag=destination_account_tag,
        destination=destination,
    )
    response = await engine.begin_transaction(request)
    assert response.ok is False
    assert destination_account_tag == response.failed_account_tag
    assert 'NOT_ACTIVE' == response.failed_account_reason


@pytest.mark.asyncio
async def test_begin_transaction_failed_account_and_destination_account_not_found(
    engine,
):
    tenant = "default"
    transaction_tag = "100"
    account_tag = "1000"
    destination_account_tag = "1001"
    destination = "393291234567"
    #
    request = schema.BeginTransactionRequest(
        tenant=tenant,
        transaction_tag=transaction_tag,
        account_tag=account_tag,
        destination_account_tag=destination_account_tag,
        destination=destination,
    )
    response = await engine.begin_transaction(request)
    assert response.ok is False
    assert account_tag == response.failed_account_tag
    assert 'NOT_FOUND' == response.failed_account_reason
