import argparse
import json
from typing import List, Dict

from kgforge.core import KnowledgeGraphForge, Resource
import cachetools
import os
import pandas as pd

from src.helpers import allocate_by_deployment, _as_list, _download_from, _format_boolean, authenticate_from_parser_arguments
from src.logger import logger
from src.neuron_morphology.arguments import define_morphology_arguments
from src.neuron_morphology.query_data import get_neuron_morphologies
from src.schemas.schema_validation import check_schema


if __name__ == "__main__":
    parser = define_morphology_arguments(argparse.ArgumentParser())

    received_args, leftovers = parser.parse_known_args()
    org, project = received_args.bucket.split("/")
    output_dir = received_args.output_dir

    deployment, auth_token = authenticate_from_parser_arguments(received_args)

    working_directory = os.path.join(os.getcwd(), output_dir)
    os.makedirs(working_directory, exist_ok=True)

    logger.info(f"Working directory {working_directory}")

    forge_bucket = allocate_by_deployment(org, project, deployment=deployment, token=auth_token)
    forge = allocate_by_deployment("bbp", "atlas", deployment=deployment, token=auth_token)

    temp = forge.retrieve("https://bbp.epfl.ch/nexus/v1/resources/neurosciencegraph/datamodels/_/schema_to_type_mapping", cross_bucket=True)
    resources = get_neuron_morphologies(forge=forge_bucket, curated=received_args.curated)

    rows, failed = check_schema(resources, forge, schema_to_type_mapping_value=forge.as_json(temp.value))
    df = pd.DataFrame(rows)

    with open(os.path.join(working_directory, "error_reports.json"), "w") as f:
        json.dump(failed, f, indent=4)

    df.to_csv(os.path.join(working_directory, 'check_schema.csv'))
