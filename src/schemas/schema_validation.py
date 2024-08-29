
from typing import List, Dict

from kgforge.core import KnowledgeGraphForge, Resource
from src.helpers import run_trial_request

UNCONSTRAINED_SCHEMA = "https://bluebrain.github.io/nexus/schemas/unconstrained.json"


def _check_schema(forge: KnowledgeGraphForge,
                  resource: Resource,
                  schema_id: str,
                  type_: str,
                  row: dict,
                  failed: List,
                  use_forge: bool)-> None:
    if use_forge:
        return _validate_schema_forge(resource, forge, type_, row, failed)
    else:
        return _validate_schema_delta(resource, forge, schema_id, row, failed)


def check_schema(resources: List[Resource], forge: KnowledgeGraphForge, schema_to_type_mapping_value: Dict,
                 use_forge: bool = True):
    rows = []
    failed = []

    for resource in resources:

        row = {
            "id": resource.get_identifier(),
            "name": resource.name,
        }

        schema = resource._store_metadata._constrainedBy
        has_schema = schema != UNCONSTRAINED_SCHEMA
        row["Is Constrained"] = has_schema

        if has_schema:
            type_ = schema_to_type_mapping_value[schema]
            row, failed = _check_schema(forge, resource, schema, type_, row, failed, use_forge)
        else:
            row["Passes Validation"] = "-"

        rows.append(row)

    return rows, failed


def _validate_schema_forge(resource: Resource, forge: KnowledgeGraphForge, type_: str, row: Dict, failed: list):
    try:
        forge.validate(resource, type_=type_, inference=None)
        conforms = resource._validated
        report = resource._last_action.message
    except Exception as exc:
        failed.append({**row, "exception": str(exc)})
        row["Exception"] = str(exc)
    else:
        row["Passes Validation"] = conforms

        if not conforms:
            failed.append({**row, "report": report})
    return row, failed


def _validate_schema_delta(resource: Resource, forge: KnowledgeGraphForge, schema: str, row: Dict, failed: list):

    query_body = {"schema": schema,
                  "resource": forge.as_jsonld(resource)}

    try:
        endpoint = forge._store.endpoint
        bucket = forge._store.bucket
        token = forge._store.token
        response_json, _ = run_trial_request(query_body, endpoint, bucket, token)
        if 'result' in response_json:
            conforms = True
        elif 'error' in response_json:
            conforms = False
            report = response_json['error']['details']['result']
    except Exception as exc:
        failed.append({**row, "exception": str(exc)})
        row["Exception"] = str(exc)
    else:
        row["Passes Validation"] = conforms

        if not conforms:
            failed.append({**row, "report": report})
    return row, failed
