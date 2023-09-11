import click

from .app import Forerunner
from .utils import import_from_string


@click.command()
@click.argument("app", type=str)
def main(app: str):
    loaded_app = import_from_string(app)
    if not isinstance(loaded_app, Forerunner):
        raise Exception("Loaded app is not a Forerunner App")
    loaded_app.run()
