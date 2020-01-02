import aiohttp
import asyncio
import os
import pytest

from threading import Thread

from rating_engine.app import get_app
from rating_engine.services import api as api_service
from rating_engine.services import engine as engine_service

API_URL = os.getenv("API_URL", "http://localhost:8000/graphql")
MESSAGEBUS_URI = os.getenv("MESSAGEBUS_URI", "pyamqp://user:password@localhost:5672//")
MONGODB_URI = os.getenv("MONGODB_URI", "mongodb://localhost:27017")
MONGODB_DB = os.getenv("MONGODB_DB", "rating_api")


@pytest.fixture(scope="function")
async def app():
    app_obj = get_app(
        dict(
            api_url=API_URL,
            api_username=None,
            api_password=None,
            messagebus_uri=MESSAGEBUS_URI,
        )
    )
    thread = Thread(target=app_obj.run, daemon=True)
    thread.start()
    await asyncio.sleep(0.2)
    #
    yield app_obj


@pytest.fixture
async def api():
    api_obj = api_service.APIService(api_url=API_URL)
    yield api_obj
    await api_obj.close()


@pytest.fixture
async def engine(api):
    from pymongo import MongoClient

    mongoclient = MongoClient(MONGODB_URI)
    mongoclient.drop_database(MONGODB_DB)
    #
    engine = engine_service.EngineService(api)
    yield engine


@pytest.fixture
async def graphql():
    session = aiohttp.ClientSession()

    async def _graphql(query):
        json = {'query': query}
        async with session.post(API_URL, json=json) as r:
            assert r.status == 200
            return await r.json()

    yield _graphql
    await session.close()
