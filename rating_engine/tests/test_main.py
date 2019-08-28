import asyncio

from click.testing import CliRunner


def test_main():
    from rating_engine.main import main

    async def shutdown(loop):
        loop.stop()

    loop = asyncio.get_event_loop()
    loop.run_until_complete(shutdown(loop))

    runner = CliRunner()
    result = runner.invoke(main)
    assert result.exit_code == 1
