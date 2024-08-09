import argparse
from typing import Union
from _pytest.config.argparsing import Parser

from src.arguments import define_arguments


def define_schemas_arguments(parser: Union[argparse.ArgumentParser, Parser]):
    """
    Defines the arguments of the Python script

    :return: the argument parser
    :rtype: ArgumentParser
    """
    parser = define_arguments(parser)
    add_arg = parser.addoption if isinstance(parser, Parser) else parser.add_argument

    add_arg(
        "--forge_validation", help="Whether to use forge.validate, or delta trial validation",
        type=str, choices=["yes", "no", "both"], default="yes"
    )

    add_arg(
        "--elastic", help="Whether to use Elastic Search to retrieve resources, default uses forge.search",
        type=str, choices=["yes", "no"], default="yes"
    )

    add_arg(
        "--changed_schemas_filepath", help="Path of a json file with a list of schemas that have recently changed, produced by the bmo pipeline",
        type=str, default="./bmo_changed_schemas.json"
    )

    return parser
