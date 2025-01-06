import logging

import rich
import typer

from .convert import (
    convert_interactive_display_to_thumbnail,
    convert_interactive_display_to_static_display
)


app = typer.Typer()


logger = logging.getLogger("bia-converter")


SUPPORTED_OUTPUTS = {
    "thumbnail": convert_interactive_display_to_thumbnail,
    "static_display": convert_interactive_display_to_static_display
}

@app.command()
def hello(image_rep_uuid: str, output_rep_type: str):

    logging.basicConfig(level=logging.INFO)

    if output_rep_type not in SUPPORTED_OUTPUTS:
        raise NotImplementedError

    conversion_function = SUPPORTED_OUTPUTS[output_rep_type]
    image_rep = conversion_function(image_rep_uuid)

    rich.print(image_rep)


if __name__ == "__main__":
    app()