import pytest  # type: ignore

from datetime import datetime, timedelta
from json import dumps
from pytz import timezone

from ..schema import engine as schema


@pytest.mark.asyncio
async def test_record_transaction(engine):
    request = schema.RecordTransactionRequest(transaction_tag="100")
    result = await engine.record_transaction(request)
    assert result == schema.RecordTransactionResponse()


@pytest.mark.asyncio
async def test_record_transaction_failed_no_accounts_provided(engine):
    tenant = "default"
    transaction_tag = "100"
    destination = "393291234567"
    #
    request = schema.RecordTransactionRequest(
        tenant=tenant, transaction_tag=transaction_tag, destination=destination
    )
    response = await engine.record_transaction(request)
    assert response.ok is False


@pytest.mark.asyncio
async def test_record_transaction_failed_account_not_found(engine):
    tenant = "default"
    transaction_tag = "100"
    account_tag = "1000"
    destination = "393291234567"
    #
    request = schema.RecordTransactionRequest(
        tenant=tenant,
        transaction_tag=transaction_tag,
        account_tag=account_tag,
        destination=destination,
    )
    response = await engine.record_transaction(request)
    assert response.ok is False
    assert account_tag == response.failed_account_tag
    assert 'NOT_FOUND' == response.failed_reason


@pytest.mark.asyncio
async def test_record_transaction_failed_account_not_active(engine, graphql):
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
    request = schema.RecordTransactionRequest(
        tenant=tenant,
        transaction_tag=transaction_tag,
        account_tag=account_tag,
        destination=destination,
    )
    response = await engine.record_transaction(request)
    assert response.ok is False
    assert account_tag == response.failed_account_tag
    assert 'NOT_ACTIVE' == response.failed_reason


@pytest.mark.asyncio
async def test_record_transaction_failed_destination_account_not_found(engine):
    tenant = "default"
    transaction_tag = "100"
    destination_account_tag = "1001"
    destination = "393291234567"
    #
    request = schema.RecordTransactionRequest(
        tenant=tenant,
        transaction_tag=transaction_tag,
        destination_account_tag=destination_account_tag,
        destination=destination,
    )
    response = await engine.record_transaction(request)
    assert response.ok is False
    assert destination_account_tag == response.failed_account_tag
    assert 'NOT_FOUND' == response.failed_reason


@pytest.mark.asyncio
async def test_record_transaction_failed_destination_account_not_active(
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
    request = schema.RecordTransactionRequest(
        tenant=tenant,
        transaction_tag=transaction_tag,
        destination_account_tag=destination_account_tag,
        destination=destination,
    )
    response = await engine.record_transaction(request)
    assert response.ok is False
    assert destination_account_tag == response.failed_account_tag
    assert 'NOT_ACTIVE' == response.failed_reason


@pytest.mark.asyncio
async def test_record_transaction_failed_account_and_destination_account_not_found(
    engine,
):
    tenant = "default"
    transaction_tag = "100"
    account_tag = "1000"
    destination_account_tag = "1001"
    destination = "393291234567"
    #
    request = schema.RecordTransactionRequest(
        tenant=tenant,
        transaction_tag=transaction_tag,
        account_tag=account_tag,
        destination_account_tag=destination_account_tag,
        destination=destination,
    )
    response = await engine.record_transaction(request)
    assert response.ok is False
    assert account_tag == response.failed_account_tag
    assert 'NOT_FOUND' == response.failed_reason


@pytest.mark.asyncio
async def test_record_transaction_with_tags(engine, graphql):
    tenant = "default"
    transaction_tag = "100"
    account_tag = "1000"
    source = "1000"
    destination = "393291234567"
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
        }"""
        % {'tenant': dumps(tenant), 'account_tag': dumps(account_tag)}
    )
    timestamp_begin = timezone("UTC").localize(datetime.utcnow())
    timestamp_end = timestamp_begin + timedelta(seconds=1)
    request = schema.RecordTransactionRequest(
        tenant=tenant,
        transaction_tag=transaction_tag,
        account_tag=account_tag,
        tags=['T1'],
        source=source,
        source_ip='source_ip',
        destination=destination,
        carrier_ip='carrier_ip',
        timestamp_begin=timestamp_begin,
        timestamp_end=timestamp_end,
    )
    result = await engine.record_transaction(request)
    assert result == schema.RecordTransactionResponse(ok=True)
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
        'carrier_ip': 'carrier_ip',
        'destination': '393291234567',
        'duration': 1,
        'fee': 1,
        'inbound': False,
        'primary': True,
        'source': '1000',
        'source_ip': 'source_ip',
        'tags': ['T1', 'A1'],
        'tenant': 'default',
        'transaction_tag': '100',
    }


@pytest.mark.asyncio
async def test_record_transaction_with_transaction_id_internal_error(engine, graphql):
    tenant = "default"
    transaction_tag = "100"
    account_tag = "1000"
    source = "1000"
    destination = "393291234567"
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
        }"""
        % {'tenant': dumps(tenant), 'account_tag': dumps(account_tag)}
    )
    #
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
    timestamp_begin = timezone("UTC").localize(datetime.utcnow())
    timestamp_end = timestamp_begin + timedelta(seconds=1)
    request = schema.RecordTransactionRequest(
        tenant=tenant,
        transaction_tag=transaction_tag,
        account_tag=account_tag,
        tags=['T1'],
        source=source,
        source_ip='source_ip',
        destination=destination,
        carrier_ip='carrier_ip',
        timestamp_begin=timestamp_begin,
        timestamp_end=timestamp_end,
    )
    result = await engine.record_transaction(request)
    assert result == schema.RecordTransactionResponse(
        ok=False, failed_account_tag='1000', failed_reason='INTERNAL_ERROR'
    )
