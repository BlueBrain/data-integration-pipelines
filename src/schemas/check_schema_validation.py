
import os
import json
import argparse
import pandas as pd
from typing import List, Dict

from kgforge.core import KnowledgeGraphForge, Resource
from src.helpers import allocate_by_deployment, authenticate_from_parser_arguments
from src.logger import logger
from src.schemas.arguments import define_schemas_arguments
from src.schemas.query_data import get_resources_by_type_es, get_resources_by_type_search
from src.schemas.getters import TypeGetter
from src.schemas.schema_validation import check_schema


def run_validation(
    resources: List[Resource],
    forge: KnowledgeGraphForge,
    schema_to_type_mapping: dict,
    working_directory: str,
    use_forge: bool
):

    rows, failed = check_schema(
        resources, forge, schema_to_type_mapping_value=schema_to_type_mapping,
        use_forge=use_forge
    )
    df = pd.DataFrame(rows)

    validation_type = "forge" if use_forge else "delta"
    df.to_csv(os.path.join(working_directory, f"check_schema_{validation_type}.csv"))
    with open(os.path.join(working_directory, f"errors_schema_validation_{validation_type}.json"), "w") as f:
        json.dump(failed, f, indent=4)


if __name__ == "__main__":
    parser = define_schemas_arguments(argparse.ArgumentParser())

    received_args, leftovers = parser.parse_known_args()
    org, project = received_args.bucket.split("/")
    output_dir = received_args.output_dir
    forge_validation = received_args.forge_validation
    use_elastic = received_args.elastic

    if use_elastic:
        search_method = get_resources_by_type_es
    else:
        search_method = get_resources_by_type_search

    deployment, auth_token = authenticate_from_parser_arguments(received_args)

    forge_bucket = allocate_by_deployment(org, project, deployment=deployment, token=auth_token)
    forge_atlas = allocate_by_deployment("bbp", "atlas", deployment=deployment, token=auth_token)

    mapping_source = forge_atlas.retrieve("https://bbp.epfl.ch/nexus/v1/resources/neurosciencegraph/datamodels/_/schema_to_type_mapping", cross_bucket=True)
    schema_to_type_mapping = forge_atlas.as_json(mapping_source.value)
    type_getter = TypeGetter(token=auth_token, deployment=deployment).get_types_delta

    all_types, _ = type_getter(org, project)

    errors = []

    output_directory = os.path.join(os.getcwd(), output_dir)
    os.makedirs(output_directory, exist_ok=True)

    for type_, type_info in all_types.items():
        if "Density" in type_ or "Volumetric" in type_:
            continue
        resource_count, _ = type_info
        logger.info(f" Expecting {resource_count} resources")

        error = None
        resources = []

        try:
            resources, error = search_method(forge_bucket, type_, limit=resource_count)
        except Exception as exc:
            if 'The provided token is invalid for user' in str(exc):

                deployment, auth_token = authenticate_from_parser_arguments(received_args)

                forge_bucket = allocate_by_deployment(org, project, deployment=deployment, token=auth_token)
                forge_atlas = allocate_by_deployment("bbp", "atlas", deployment=deployment, token=auth_token)

                try:
                    resources, error = search_method(forge_bucket, type_, limit=resource_count)
                except Exception as exc:
                    error = str(exc)
            else:
                error = str(exc)

        if error:
            errors.append(error)

        if len(resources) > 0:
            type_name = type_.split('/')[-1] 
            working_directory = os.path.join(os.getcwd(), output_dir, type_name)
            os.makedirs(working_directory, exist_ok=True)

            logger.info(f"Current type {type_name}")

            if forge_validation == "both":
                run_validation(resources, forge_bucket, schema_to_type_mapping, working_directory, True)
                run_validation(resources, forge_bucket, schema_to_type_mapping, working_directory, False)
            else:
                use_forge = True if forge_validation == "yes" else False
                run_validation(resources, forge_bucket, schema_to_type_mapping, working_directory, use_forge)

    if errors:
        with open(os.path.join(output_directory, "errors_searching.json"), "w") as f:
            json.dump(errors, f, indent=4)
