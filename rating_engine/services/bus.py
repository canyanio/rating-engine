import json

from datetime import datetime
from pytz import timezone
from time import time
from typing import Any, Callable, Optional

from aio_pika import connect_robust, Channel, Connection
from aio_pika.message import Message
from aio_pika.patterns import RPC
from aio_pika.patterns.rpc import RPCMessageTypes
from pamqp.specification import Basic  # type: ignore

from ..enums import RPCCallPriority


UTC = timezone('UTC')


class JsonRPC(RPC):
    SERIALIZER = json
    CONTENT_TYPE = 'application/json'

    def deserialize(self, data: bytes) -> Any:
        value = self.SERIALIZER.loads(data.decode('utf-8'))
        if type(value) == dict and value.get('error'):
            return RuntimeError(value['error']['message'])
        return value

    def serialize(self, data: Any) -> bytes:
        return self.SERIALIZER.dumps(
            data, ensure_ascii=False, default=self.serialize_default
        ).encode('utf-8')

    def serialize_default(self, v: Any):
        if isinstance(v, datetime):
            return v.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
        return repr(v)

    def serialize_exception(self, exception: Exception) -> bytes:
        return self.serialize(
            {
                "error": {
                    "type": exception.__class__.__name__,
                    "message": repr(exception),
                    "args": exception.args,
                }
            }
        )


class BusService(object):
    connection: Connection
    channel: Channel
    rpc: JsonRPC

    def __init__(self, messagebus_uri: str):
        self._messagebus_uri = messagebus_uri

    async def connect(self):
        self.connection = await connect_robust(self._messagebus_uri)
        self.channel = await self.connection.channel()
        self.rpc = await JsonRPC.create(self.channel)

    async def close(self):
        await self.connection.close()

    async def rpc_call(
        self,
        method: str,
        kwargs: dict,
        expiration: int = 10,
        priority: RPCCallPriority = RPCCallPriority.MEDIUM,
    ) -> Optional[dict]:
        return await self.rpc.call(
            method, kwargs=kwargs, expiration=expiration, priority=priority.value
        )

    async def rpc_call_async(
        self,
        method: str,
        kwargs: dict,
        expiration: int = 10,
        priority: RPCCallPriority = RPCCallPriority.MEDIUM,
    ) -> bool:
        message = Message(
            body=self.rpc.serialize(kwargs or {}),
            type=RPCMessageTypes.call.value,
            timestamp=time(),
            priority=priority.value,
            delivery_mode=self.rpc.DELIVERY_MODE,
        )
        if expiration is not None:
            message.expiration = expiration
        response = await self.channel.default_exchange.publish(
            message, routing_key=method, mandatory=True,
        )
        return isinstance(response, Basic.Ack)

    async def rpc_register(self, method: str, func: Callable, auto_delete: bool = True):
        await self.rpc.register(method, func, auto_delete=auto_delete)
