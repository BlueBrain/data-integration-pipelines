import argparse
import json
from typing import List, Dict

from kgforge.core import KnowledgeGraphForge, Resource
import cachetools
import os
import pandas as pd

from src.helpers import allocate, get_token, _as_list, _download_from, _format_boolean, authenticate
from src.logger import logger
from src.neuron_morphology.arguments import define_morphology_arguments
from src.neuron_morphology.query_data import get_neuron_morphologies
from src.schemas.schema_validation import check_schema


if __name__ == "__main__":
    parser = define_morphology_arguments(argparse.ArgumentParser())

    received_args, leftovers = parser.parse_known_args()
    org, project = received_args.bucket.split("/")
    output_dir = received_args.output_dir
    token = authenticate(username=received_args.username, password=received_args.password)
    is_prod = True

    working_directory = os.path.join(os.getcwd(), output_dir)
    os.makedirs(working_directory, exist_ok=True)

    logger.info(f"Working directory {working_directory}")

    forge_bucket = allocate(org, project, is_prod=is_prod, token=token)
    forge = allocate("bbp", "atlas", is_prod=is_prod, token=token)

    temp = forge.retrieve("https://bbp.epfl.ch/nexus/v1/resources/neurosciencegraph/datamodels/_/schema_to_type_mapping", cross_bucket=True)
    resources = get_neuron_morphologies(forge=forge_bucket, curated=received_args.curated)

    rows, failed = check_schema(resources, forge, schema_to_type_mapping_value=forge.as_json(temp.value))
    df = pd.DataFrame(rows)

    with open(os.path.join(working_directory, "error_reports.json"), "w") as f:
        json.dump(failed, f, indent=4)

    df.to_csv(os.path.join(working_directory, 'check_schema.csv'))
