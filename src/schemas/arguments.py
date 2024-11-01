import argparse
from typing import Union

from src.arguments import define_arguments


def define_schemas_arguments(parser: argparse.ArgumentParser):
    """
    Defines the arguments of the Python script

    :return: the argument parser
    :rtype: ArgumentParser
    """
    parser = define_arguments(parser)

    parser.add_argument(
        "--forge_validation", help="Whether to use forge.validate, or delta trial validation",
        type=str, choices=["yes", "no", "both"], default="yes"
    )

    parser.add_argument(
        "--elastic", help="Whether to use Elastic Search to retrieve resources, default uses forge.search",
        type=str, choices=["yes", "no"], default="yes"
    )

    parser.add_argument(
        "--changed_schemas_filepath", help="Path of a json file with a list of schemas that have recently changed, produced by the bmo pipeline",
        type=str, default="./bmo_changed_schemas.json"
    )

    return parser
