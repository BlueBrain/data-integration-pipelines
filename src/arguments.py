import argparse
from datetime import datetime
from typing import Union

from src.helpers import Deployment


def define_arguments(parser: argparse.ArgumentParser, with_bucket=True):
    """
    Defines the arguments of the Python script

    :return: the argument parser
    :rtype: ArgumentParser
    """

    timestamp = datetime.now().strftime("%Y-%m-%d-%H-%M-%S")

    if with_bucket:
        parser.add_argument(
            "--bucket", help="The bucket against which to run the check",
            type=str, default="bbp/mmb-point-neuron-framework-model"
        )

    parser.add_argument(
        "--username", help="Service account username", type=str, required=True
    )
    parser.add_argument(
        "--password", help="Service account password", type=str, required=True
    )

    parser.add_argument(
        "--output_dir", help="The path to dump log and data files.",
        default=f'./output/{timestamp}', type=str
    )
    parser.add_argument(
        "--limit", help="Query limit for resources, defaults to 10000",
        type=int, default=10000
    )

    parser.add_argument(
        "--is_service_account", help="Is a service account. Valid values: yes, no",
        type=str, choices=["yes", "no"]
    )

    parser.add_argument(
        "--deployment", type=str, default="PRODUCTION",
        choices=Deployment._member_names_
    )

    return parser
