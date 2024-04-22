import argparse
from datetime import datetime
from typing import Union
from _pytest.config.argparsing import Parser


def define_arguments(parser: Union[argparse.ArgumentParser, Parser]):
    """
    Defines the arguments of the Python script

    :return: the argument parser
    :rtype: ArgumentParser
    """

    timestamp = datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
    add_arg = parser.addoption if isinstance(parser, Parser) else parser.add_argument

    add_arg(
        "--bucket", help="The bucket against which to run the check",
        type=str, default="bbp-external/seu"
    )
    add_arg(
        "--token", help="The nexus token", type=str, required=True
    )
    add_arg(
        "--output_dir", help="The path to load schemas from.",
        default=f'./output/{timestamp}', type=str
    )

    return parser