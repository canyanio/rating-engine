from datetime import datetime
from pytz import timezone


class MockChannel(object):
    loop = None


def test_jsonrpc_serialize_exception():
    from rating_engine.services.bus import JsonRPC

    jsonrpc = JsonRPC(MockChannel())
    data = jsonrpc.serialize_exception(ValueError("test"))
    assert (
        data
        == b'{"error": {"type": "ValueError", "message": "ValueError(\'test\')", "args": ["test"]}}'
    )


def test_jsonrpc_serialize_object():
    from rating_engine.services.bus import JsonRPC

    jsonrpc = JsonRPC(MockChannel())
    data = jsonrpc.serialize(datetime)
    assert data == b'"<class \'datetime.datetime\'>"'


def test_jsonrpc_serialize_datetime():
    from rating_engine.services.bus import JsonRPC

    jsonrpc = JsonRPC(MockChannel())
    data = jsonrpc.serialize(timezone('UTC').localize(datetime(2020, 1, 1, 0, 0, 0)))
    assert data == b'"2020-01-01T00:00:00Z"'


def test_jsonrpc_deserialize_exception():
    from rating_engine.services.bus import JsonRPC

    jsonrpc = JsonRPC(MockChannel())
    data = jsonrpc.deserialize(
        b'{"error": {"type": "ValueError", "message": "ValueError(\'test\')", "args": ["test"]}}'
    )
    assert isinstance(data, Exception)
