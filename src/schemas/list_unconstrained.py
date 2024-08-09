import os
import json
import argparse
import pandas as pd

from kgforge.core import KnowledgeGraphForge
from src.helpers import allocate, authenticate
from src.logger import logger
from src.arguments import define_arguments
from src.schemas.getters import TypeGetter
from src.schemas.schema_validation import UNCONSTRAINED_SCHEMA


if __name__ == "__main__":
    parser = define_arguments(argparse.ArgumentParser())

    received_args, leftovers = parser.parse_known_args()
    org, project = received_args.bucket.split("/")
    output_dir = received_args.output_dir
    token = authenticate(username=received_args.username, password=received_args.password)
    is_prod = True

    working_directory = os.path.join(os.getcwd(), output_dir)
    os.makedirs(working_directory, exist_ok=True)

    logger.info(f"Working directory {working_directory}")

    type_getter = TypeGetter(token).get_unconstrained_types

    types, _ = type_getter(org, project)

    with open(os.path.join(working_directory, f"{org}_{project}_unconstrained_types.json"), "w") as f:
        json.dump(types, f, indent=4)