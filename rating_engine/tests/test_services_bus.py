def test_jsonrpc_serialize_exception(app):
    from rating_engine.services.bus import JsonRPC

    bus_service = getattr(app, '_bus')
    jsonrpc = JsonRPC(bus_service.channel)

    data = jsonrpc.serialize_exception(ValueError("test"))
    assert (
        data
        == b'{"error": {"type": "ValueError", "message": "ValueError(\'test\')", "args": ["test"]}}'
    )


def test_jsonrpc_deserialize_exception(app):
    from rating_engine.services.bus import JsonRPC

    bus_service = getattr(app, '_bus')
    jsonrpc = JsonRPC(bus_service.channel)

    data = jsonrpc.deserialize(
        b'{"error": {"type": "ValueError", "message": "ValueError(\'test\')", "args": ["test"]}}'
    )
    assert isinstance(data, Exception)
