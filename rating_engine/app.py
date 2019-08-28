import asyncio

from pydantic import ValidationError

from .enums import MethodName
from .schema import engine as schema
from .services import api as api_service
from .services import bus as bus_service
from .services import engine as engine_service


class App(object):
    _bus: bus_service.BusService
    _rating: engine_service.EngineService
    _config: dict

    def __init__(self, config: dict):
        self._config = config
        self._bus = bus_service.BusService(messagebus_uri=config["messagebus_uri"])
        api = api_service.APIService(
            api_url=config['api_url'],
            api_username=config['api_username'],
            api_password=config['api_password'],
        )
        self._rating = engine_service.EngineService(api)

    @property
    def config(self) -> dict:
        return self._config

    def run(self):
        try:
            loop = asyncio.get_event_loop()
        except RuntimeError:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
        loop.run_until_complete(self._run())
        try:
            loop.run_forever()
        finally: # pragma: no cover
            loop.shutdown_asyncgens()

    async def _run(self):
        await self._bus.connect()
        await self._bus.rpc_register(
            MethodName.AUTHORIZATION.value, self._authorization
        )
        await self._bus.rpc_register(
            MethodName.BEGIN_TRANSACTION.value, self._begin_transaction
        )
        await self._bus.rpc_register(
            MethodName.ROLLBACK_TRANSACTION.value, self._rollback_transaction
        )
        await self._bus.rpc_register(
            MethodName.END_TRANSACTION.value, self._end_transaction
        )
        await self._bus.rpc_register(
            MethodName.RECORD_TRANSACTION.value, self._record_transaction
        )

    async def _authorization(self, transaction: dict) -> dict:
        try:
            request = schema.AuthorizationRequest(**transaction)
        except ValidationError as e:
            return {"errors": e.errors()}
        response = await self._rating.authorization(request)
        return dict(response)

    async def _begin_transaction(self, transaction: dict) -> dict:
        try:
            request = schema.BeginTransactionRequest(**transaction)
        except ValidationError as e:
            return {"errors": e.errors()}
        response = await self._rating.begin_transaction(request)
        return dict(response)

    async def _end_transaction(self, transaction: dict) -> dict:
        try:
            request = schema.EndTransactionRequest(**transaction)
        except ValidationError as e:
            return {"errors": e.errors()}
        response = await self._rating.end_transaction(request)
        return dict(response)

    async def _rollback_transaction(self, transaction: dict) -> dict:
        try:
            request = schema.RollbackTransactionRequest(**transaction)
        except ValidationError as e:
            return {"errors": e.errors()}
        response = await self._rating.rollback_transaction(request)
        return dict(response)

    async def _record_transaction(self, transaction: dict) -> dict:
        try:
            request = schema.RecordTransactionRequest(**transaction)
        except ValidationError as e:
            return {"errors": e.errors()}
        response = await self._rating.record_transaction(request)
        return dict(response)


def get_app(config: dict):
    return App(config=config)
