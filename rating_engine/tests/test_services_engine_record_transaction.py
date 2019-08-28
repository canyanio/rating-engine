import pytest  # type: ignore

from ..schema import engine as schema


@pytest.mark.asyncio
async def test_record_transaction(engine):
    request = schema.RecordTransactionRequest(transaction_tag="100",)
    result = await engine.record_transaction(request)
    assert result == schema.RecordTransactionResponse()
