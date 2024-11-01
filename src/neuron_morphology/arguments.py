import argparse
from typing import Union

from src.arguments import define_arguments


def define_morphology_arguments(parser: argparse.ArgumentParser):
    """
    Defines the arguments of the Python script

    :return: the argument parser
    :rtype: ArgumentParser
    """
    parser = define_arguments(parser)

    parser.add_argument(
        "--curated", help="Whether to only check curated data are all",
        type=str, choices=["yes", "no", "both"], default="yes"
    )

    parser.add_argument(
        "--really_update", help="Whether to really update data being modified by the pipeline in production",
        type=str, choices=["yes", "no"], default="no"
    )

    parser.add_argument(
        "--push_to_staging", help="Whether to push to staging, if really-update is True",
        type=str, choices=["yes", "no"], default="yes"
    )

    parser.add_argument(
        "--morphology_tag", help="Tag of the morphology Resources to require",
        type=str
    )

    return parser
