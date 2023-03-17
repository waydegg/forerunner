import click

from .app import App
from .utils import import_from_string


@click.command()
@click.argument("app", type=str)
def main(app: str):
    loaded_app = import_from_string(app)
    if not isinstance(loaded_app, App):
        raise Exception("Loaded app is not a Forerunner App")
    loaded_app.run()
