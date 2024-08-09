import argparse
from typing import Union
from _pytest.config.argparsing import Parser

from src.arguments import define_arguments


def define_morphology_arguments(parser: Union[argparse.ArgumentParser, Parser]):
    """
    Defines the arguments of the Python script

    :return: the argument parser
    :rtype: ArgumentParser
    """
    parser = define_arguments(parser)
    add_arg = parser.addoption if isinstance(parser, Parser) else parser.add_argument

    add_arg(
        "--curated", help="Whether to only check curated data are all",
        type=str, choices=["yes", "no", "both"], default="yes"
    )

    add_arg(
        "--really_update", help="Whether to really update data being modified by the pipeline in production",
        type=str, choices=["yes", "no"], default="no"
    )

    add_arg(
        "--push_to_staging", help="Whether to push to staging, if really-update is True",
        type=str, choices=["yes", "no"], default="yes"
    )

    return parser
