import os
import json
import argparse
import pandas as pd

from kgforge.core import KnowledgeGraphForge
from src.helpers import authenticate_from_parser_arguments
from src.logger import logger
from src.arguments import define_arguments
from src.schemas.getters import TypeGetter
from src.schemas.schema_validation import UNCONSTRAINED_SCHEMA


if __name__ == "__main__":
    parser = define_arguments(argparse.ArgumentParser())

    received_args, leftovers = parser.parse_known_args()
    org, project = received_args.bucket.split("/")
    output_dir = received_args.output_dir

    deployment, auth_token = authenticate_from_parser_arguments(received_args)

    working_directory = os.path.join(os.getcwd(), output_dir)
    os.makedirs(working_directory, exist_ok=True)

    logger.info(f"Working directory {working_directory}")

    type_getter = TypeGetter(token=auth_token, deployment=deployment).get_unconstrained_types

    types, _ = type_getter(org, project)

    with open(os.path.join(working_directory, f"{org}_{project}_unconstrained_types.json"), "w") as f:
        json.dump(types, f, indent=4)
