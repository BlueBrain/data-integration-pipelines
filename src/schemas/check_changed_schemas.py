
import os
import json
import argparse
from typing import List, Dict

from kgforge.core import KnowledgeGraphForge
from src.helpers import allocate, authenticate, initialize_objects
from src.schemas.arguments import define_schemas_arguments
from src.schemas.schema_validation import _check_schema


CONSTRAINED_QUERY = """
SELECT DISTINCT ?id
    WHERE {{
        GRAPH ?g {{
            ?id _constrainedBy <{0}> ;
            _deprecated false .
        }}
    }}
"""

def check_changed_schemas_in_project(forge: KnowledgeGraphForge,
                                     schema_list: List[str],
                                     schema_to_type_mapping: Dict,
                                     use_forge: bool = True,
                                     limit: int = 20000) -> None:

    results = {}
    for schema_id in schema_list:
        print(f"Querying for resources constrained by {schema_id}")
        rows = []
        failed = []
        query = CONSTRAINED_QUERY.format(schema_id)
        resources = forge.sparql(query, limit=limit)

        print(f"Found {len(resources)} resources constrained by {schema_id}")
        for ires in resources:
            resource = forge.retrieve(ires.id)
            if resource:
                row = {
                    "id": resource.get_identifier(),
                    "name": resource.name if hasattr(resource, 'name') else None,
                }
                type_ = schema_to_type_mapping[schema_id]
                row, failed = _check_schema(forge, resource, schema_id, type_, row, failed, use_forge)
                rows.append(row)
            else:
                bucket = forge._store.bucket
                org, proj = bucket.split('/')
                forge = allocate(org, proj, is_prod=True, token=forge._store.token)
                resource = forge.retrieve(ires.id)
                if resource:
                    row = {
                        "id": resource.get_identifier(),
                        "name": resource.name if hasattr(resource, 'name') else None,
                    }
                    type_ = schema_to_type_mapping[schema_id]
                    row, failed = _check_schema(forge, resource, schema_id, type_, row, failed, use_forge)
                    rows.append(row)
        results[schema_id] = {'failed': failed}

    return results


if __name__ == "__main__":
    # Given a filepath where the list of updated schemas, check if resources constrained with that schema are still valid

    parser = define_schemas_arguments(argparse.ArgumentParser())

    received_args, leftovers = parser.parse_known_args()
    org, project = received_args.bucket.split("/")
    output_dir = received_args.output_dir
    forge_validation = received_args.forge_validation
    changed_schemas_filepath = received_args.changed_schemas_filepath

    with open(changed_schemas_filepath, 'r') as finput:
        schema_list = json.load(finput)
    
    output_directory = os.path.join(os.getcwd(), output_dir)
    os.makedirs(output_directory, exist_ok=True)

    token, forge_bucket, forge =  initialize_objects(received_args.username, received_args.password, org, project, is_prod=True)

    mapping_source = forge.retrieve("https://bbp.epfl.ch/nexus/v1/resources/neurosciencegraph/datamodels/_/schema_to_type_mapping", cross_bucket=True)
    schema_to_type_mapping = forge.as_json(mapping_source.value)

    results = check_changed_schemas_in_project(forge_bucket, schema_list, schema_to_type_mapping, use_forge=forge_validation)
    
    if results:
        with open(os.path.join(output_directory, f"schema_validation_{org}_{project}.json"), "w") as f:
            json.dump(results, f, indent=4)
