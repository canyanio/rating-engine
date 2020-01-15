import click

from typing import Optional

from .app import get_app


@click.command()
@click.option(
    "--messagebus-uri",
    type=click.STRING,
    default="pyamqp://user:password@localhost:5672//",
    show_default=True,
)
@click.option(
    "--api-url",
    type=click.STRING,
    default="http://localhost:8000/graphql",
    show_default=True,
)
@click.option(
    "--api-username", type=click.STRING, default=None,
)
@click.option(
    "--api-password", type=click.STRING, default=None,
)
@click.option("-d", "--debug/--no-debug", default=False)
def main(
    host: str = "0.0.0.0",
    port: int = 8000,
    messagebus_uri: str = "pyamqp://user:password@localhost:5672//",
    api_url: str = None,
    api_username: Optional[str] = None,
    api_password: Optional[str] = None,
    debug: bool = False,
    **kw,
):
    config = dict(
        host=host,
        port=port,
        messagebus_uri=messagebus_uri,
        api_url=api_url,
        api_username=api_username,
        api_password=api_password,
        debug=debug,
    )
    app = get_app(config)
    app.run()


def main_with_env():  # pragma: no cover
    main(auto_envvar_prefix="RATING_ENGINE")
