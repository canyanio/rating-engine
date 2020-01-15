import asyncio
import logging
import os
import sys

from functools import wraps
from pydantic import ValidationError

from .enums import MethodName
from .schema import engine as schema
from .services import api as api_service
from .services import bus as bus_service
from .services import engine as engine_service


def log_request_and_response(f):
    @wraps(f)
    async def wrapper(self, request):
        name = f.__name__.lstrip('_')
        logger = self.logger
        logger.debug("%s < %s", name, request)
        response = await f(self, request)
        logger.debug("%s > %s", name, response)
        return response

    return wrapper


class App(object):
    _bus: bus_service.BusService
    _rating: engine_service.EngineService
    _config: dict
    logger: logging.Logger

    LOGGING_FORMAT: str = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'

    def __init__(self, config: dict):
        self._config = config
        self._bus = bus_service.BusService(messagebus_uri=config["messagebus_uri"])
        api = api_service.APIService(
            api_url=config['api_url'],
            api_username=config['api_username'],
            api_password=config['api_password'],
        )
        self._rating = engine_service.EngineService(api)
        self._setup_logger(config)

    def _setup_logger(self, config: dict):
        logger = logging.getLogger('%s[%s]' % (self.__class__.__name__, os.getpid()))
        logger.setLevel(logging.DEBUG if config.get('debug') else logging.INFO)
        handler = logging.StreamHandler(sys.stdout)
        handler.setLevel(logging.DEBUG if config.get('debug') else logging.INFO)
        formatter = logging.Formatter(self.LOGGING_FORMAT)
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        self.logger = logger

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
        finally:  # pragma: no cover
            loop.run_until_complete(loop.shutdown_asyncgens())

    async def _run(self):
        self.logger.info("Connecting to RabbitMQ: %s", self._config["messagebus_uri"])
        await self._bus.connect()
        #
        self.logger.info("Registering RPC methods:")
        for method, callback in (
            (MethodName.AUTHORIZATION.value, self._authorization),
            (MethodName.BEGIN_TRANSACTION.value, self._begin_transaction),
            (MethodName.ROLLBACK_TRANSACTION.value, self._rollback_transaction),
            (MethodName.END_TRANSACTION.value, self._end_transaction),
            (MethodName.RECORD_TRANSACTION.value, self._record_transaction),
        ):
            self.logger.info("* %s", method)
            await self._bus.rpc_register(method, callback)
        self.logger.info("Ready")

    @log_request_and_response
    async def _authorization(self, request: dict) -> dict:
        try:
            request_obj = schema.AuthorizationRequest(**request)
        except ValidationError as e:
            return {"errors": e.errors()}
        response = await self._rating.authorization(request_obj)
        return dict(response)

    @log_request_and_response
    async def _begin_transaction(self, request: dict) -> dict:
        try:
            request_obj = schema.BeginTransactionRequest(**request)
        except ValidationError as e:
            return {"errors": e.errors()}
        response = await self._rating.begin_transaction(request_obj)
        return dict(response)

    @log_request_and_response
    async def _end_transaction(self, request: dict) -> dict:
        try:
            request_obj = schema.EndTransactionRequest(**request)
        except ValidationError as e:
            return {"errors": e.errors()}
        response = await self._rating.end_transaction(request_obj)
        return dict(response)

    @log_request_and_response
    async def _rollback_transaction(self, request: dict) -> dict:
        try:
            request_obj = schema.RollbackTransactionRequest(**request)
        except ValidationError as e:
            return {"errors": e.errors()}
        response = await self._rating.rollback_transaction(request_obj)
        return dict(response)

    @log_request_and_response
    async def _record_transaction(self, request: dict) -> dict:
        try:
            request_obj = schema.RecordTransactionRequest(**request)
        except ValidationError as e:
            return {"errors": e.errors()}
        response = await self._rating.record_transaction(request_obj)
        return dict(response)


def get_app(config: dict):
    return App(config=config)
