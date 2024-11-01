
import os
import json
import argparse
import pandas as pd
from typing import List, Dict

from kgforge.core import KnowledgeGraphForge, Resource
from src.helpers import DEFAULT_ES_VIEW, allocate_by_deployment, authenticate_from_parser_arguments
from src.logger import logger
from src.arguments import define_arguments
from src.schemas.query_data import (
    get_resources_by_type_es,
    get_resources_by_type_search,
    _delta_es,
    _payload_to_resource
)
from src.schemas.getters import TypeGetter
from src.schemas.schema_validation import check_schema


if __name__ == "__main__":
    parser = define_arguments(argparse.ArgumentParser())

    received_args, leftovers = parser.parse_known_args()
    bucket = received_args.bucket
    org, project = bucket.split("/")
    output_dir = received_args.output_dir

    deployment, auth_token = authenticate_from_parser_arguments(received_args)

    forge_bucket = allocate_by_deployment(org, project, deployment=deployment, token=auth_token)
    forge_atlas = allocate_by_deployment("bbp", "atlas", deployment=deployment, token=auth_token)

    errors = []

    mapping_source = forge_atlas.retrieve("https://bbp.epfl.ch/nexus/v1/resources/neurosciencegraph/datamodels/_/schema_to_type_mapping", cross_bucket=True)
    schema_to_type_mapping = forge_atlas.as_json(mapping_source.value)

    type_ = "https://neuroshapes.org/Annotation"

    es_query = {"query": {
        "bool": {
          "must": [
            {
              "term": {
                "@type": f"{type_}"
              }
            },
            {
              "term": {
                "_deprecated": False
              }
            }
          ]
        }
      }
    }

    endpoint = forge_bucket._store.endpoint
    resources, error = get_resources_by_type_es(forge_bucket, type_, limit=10000)
    print(len(resources))
    print(error)
    rows, failed = check_schema(resources, forge_atlas, schema_to_type_mapping_value=schema_to_type_mapping,
                                use_forge=True)
    # print(resources[0])

    # try:
    #     resources = get_resources_by_type_es(forge, type_, limit=10000)
    # except Exception as exc:
    #     if 'The provided token is invalid for user' in str(exc):
    #       deployment, auth_token = authenticate_from_parser_arguments(received_args)
    #       forge_bucket = allocate_by_deployment(org, project, deployment=deployment, token=auth_token)
    #       forge_atlas = allocate_by_deployment("bbp", "atlas", deployment=deployment, token=auth_token)

    #         try: 
    #             resources = get_resources_by_type_es(forge_atlas, type_, limit=10000)
    #         except Exception as exc:
    #             error = str(exc)
    #     else:
    #         error = str(exc)

    # if error:
    #     errors.append(error)

    # if len(resources) > 0:
    #     working_directory = os.path.join(os.getcwd(), output_dir, type_.split('/')[-1])
    #     os.makedirs(working_directory, exist_ok=True)

    #     logger.info(f"Working directory {working_directory}")

    #     rows, failed = check_schema(resources, forge, schema_to_type_mapping_value=schema_to_type_mapping,
    #                                 use_forge=True)
    #     df = pd.DataFrame(rows)

    #     df.to_csv(os.path.join(working_directory, 'check_schema.csv'))
    #     with open(os.path.join(working_directory, "errors_schema_validation.json"), "w") as f:
    #         json.dump(failed, f, indent=4)

    # if errors:
    #     with open(os.path.join(output_directory, "errors_searching.json"), "w") as f:
    #         json.dump(errors, f, indent=4)
