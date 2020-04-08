import pytest  # type: ignore

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
