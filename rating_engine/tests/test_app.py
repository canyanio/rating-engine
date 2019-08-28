import pytest  # type: ignore

from rating_engine.enums import MethodName
from rating_engine.schema import engine as schema
from rating_engine.services import bus as bus_service


@pytest.mark.asyncio
async def test_app_authorization(app):
    bus = bus_service.BusService(messagebus_uri=app.config['messagebus_uri'])
    await bus.connect()
    #
    request = schema.AuthorizationRequest(
        transaction_tag="100",
    )
    response = await bus.rpc_call(
        method=MethodName.AUTHORIZATION.value,
        kwargs={'transaction': dict(request)},
    )
    assert response is not None
    assert response['authorized'] is False
    #
    await bus.close()


@pytest.mark.asyncio
async def test_app_authorization_invalid_data(app):
    bus = bus_service.BusService(messagebus_uri=app.config['messagebus_uri'])
    await bus.connect()
    #
    request = {}
    response = await bus.rpc_call(
        method=MethodName.AUTHORIZATION.value,
        kwargs={'transaction': dict(request)},
    )
    assert response is not None
    assert response.get('errors') is not None
    #
    await bus.close()


@pytest.mark.asyncio
async def test_app_begin_transaction(app):
    bus = bus_service.BusService(messagebus_uri=app.config['messagebus_uri'])
    await bus.connect()
    #
    request = schema.BeginTransactionRequest(
        transaction_tag="100",
    )
    response = await bus.rpc_call(
        method=MethodName.BEGIN_TRANSACTION.value,
        kwargs={'transaction': dict(request)},
    )
    assert response is not None
    assert response['ok'] is False
    #
    await bus.close()


@pytest.mark.asyncio
async def test_app_begin_transaction_invalid_data(app):
    bus = bus_service.BusService(messagebus_uri=app.config['messagebus_uri'])
    await bus.connect()
    #
    request = {}
    response = await bus.rpc_call(
        method=MethodName.BEGIN_TRANSACTION.value,
        kwargs={'transaction': dict(request)},
    )
    assert response is not None
    assert response.get('errors') is not None
    #
    await bus.close()


@pytest.mark.asyncio
async def test_app_rollback_transaction(app):
    bus = bus_service.BusService(messagebus_uri=app.config['messagebus_uri'])
    await bus.connect()
    #
    request = schema.RollbackTransactionRequest(
        transaction_tag="100",
    )
    response = await bus.rpc_call(
        method=MethodName.ROLLBACK_TRANSACTION.value,
        kwargs={'transaction': dict(request)},
    )
    assert response is not None
    assert response['ok'] is False
    #
    await bus.close()


@pytest.mark.asyncio
async def test_app_rollback_transaction_invalid_data(app):
    bus = bus_service.BusService(messagebus_uri=app.config['messagebus_uri'])
    await bus.connect()
    #
    request = {}
    response = await bus.rpc_call(
        method=MethodName.ROLLBACK_TRANSACTION.value,
        kwargs={'transaction': dict(request)},
    )
    assert response is not None
    assert response.get('errors') is not None
    #
    await bus.close()


@pytest.mark.asyncio
async def test_app_end_transaction(app):
    bus = bus_service.BusService(messagebus_uri=app.config['messagebus_uri'])
    await bus.connect()
    #
    request = schema.EndTransactionRequest(
        transaction_tag="100",
    )
    response = await bus.rpc_call(
        method=MethodName.END_TRANSACTION.value,
        kwargs={'transaction': dict(request)},
    )
    assert response is not None
    assert response['ok'] is False
    #
    await bus.close()


@pytest.mark.asyncio
async def test_app_end_transaction_invalid_data(app):
    bus = bus_service.BusService(messagebus_uri=app.config['messagebus_uri'])
    await bus.connect()
    #
    request = {}
    response = await bus.rpc_call(
        method=MethodName.END_TRANSACTION.value,
        kwargs={'transaction': dict(request)},
    )
    assert response is not None
    assert response.get('errors') is not None
    #
    await bus.close()


@pytest.mark.asyncio
async def test_app_record_transaction(app):
    bus = bus_service.BusService(messagebus_uri=app.config['messagebus_uri'])
    await bus.connect()
    #
    request = schema.RecordTransactionRequest(
        transaction_tag="100",
    )
    response = await bus.rpc_call(
        method=MethodName.RECORD_TRANSACTION.value,
        kwargs={'transaction': dict(request)},
    )
    assert response is not None
    assert response['ok'] is False
    #
    await bus.close()


@pytest.mark.asyncio
async def test_app_record_transaction_invalid_data(app):
    bus = bus_service.BusService(messagebus_uri=app.config['messagebus_uri'])
    await bus.connect()
    #
    request = {}
    response = await bus.rpc_call(
        method=MethodName.RECORD_TRANSACTION.value,
        kwargs={'transaction': dict(request)},
    )
    assert response is not None
    assert response.get('errors') is not None
    #
    await bus.close()
