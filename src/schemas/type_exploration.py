import argparse
import os

from typing import Tuple, List

from src.helpers import Deployment, authenticate_from_parser_arguments
from src.get_projects import _get_obp_projects
from src.logger import logger
from src.arguments import define_arguments
from src.schemas.getters import (
    TypeGetter,
    SchemaGetter,
    bucket_to_type_schema_dict,
    get_org_project_to_types,
    get_missing_schemas,
    write_into_xls
)


def type_to_schema_exploration(
        org_project_list: List[Tuple[str, str]],
        dir_path: str, token: str,
        schema_getter_deployment: Deployment,
        write_intermediate_steps: bool
):

    os.makedirs(dir_path, exist_ok=True)

    # Only in prod - not enough real resources in staging
    type_getter = TypeGetter(token, deployment=Deployment.PRODUCTION)

    types_sparql, types_sparql_flattened = get_org_project_to_types(
        org_project_list=org_project_list, getter=type_getter.get_types_sparql,
        filename_per_bucket="types_per_bucket", filename_flattened="types_flattened",
        dir_path=dir_path,
        write_into_file=write_intermediate_steps
    )

    # Can be done in staging
    schema_getter = SchemaGetter(token, deployment=schema_getter_deployment)

    _ = get_missing_schemas(
        schema_getter=schema_getter,
        types_sparql_flattened=types_sparql_flattened,
        dir_path=dir_path,
        filename="types_without_schema",
        write_into_file=write_intermediate_steps
    )  # Find out how many schema need to exist, vs currently exist

    schema_dict_sparql, flattened = bucket_to_type_schema_dict(
        bucket_to_type=types_sparql,
        # using sparql because with delta you cannot get the schema used by the resources
        schema_getter=schema_getter.get_schema_from_type_nd,
        filename_per_bucket="type_to_schema_per_bucket",
        filename_flattened="type_to_schema_flattened",
        dir_path=dir_path,
        write_into_file=write_intermediate_steps
    )

    write_into_xls(
        filename="type_to_schema",
        dir_path=dir_path,
        data=schema_dict_sparql,
        data_flattened=flattened
    )


if __name__ == "__main__":
    parser = define_arguments(argparse.ArgumentParser())

    received_args, leftovers = parser.parse_known_args()
    org, project = received_args.bucket.split("/")
    output_dir = received_args.output_dir

    deployment, auth_token = authenticate_from_parser_arguments(received_args)

    working_directory = os.path.join(os.getcwd(), output_dir)
    os.makedirs(working_directory, exist_ok=True)

    logger.info(f"Working directory {working_directory}")

    obp: List[Tuple[str, str]] = _get_obp_projects(token=auth_token, deployment=deployment)

    type_to_schema_exploration(
        org_project_list=obp,
        token=auth_token, dir_path=working_directory,
        schema_getter_deployment=deployment,
        write_intermediate_steps=True
    )
