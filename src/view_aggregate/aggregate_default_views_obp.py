import argparse
import requests
import json
from typing import Dict, List, Tuple

from urllib.parse import quote_plus as url_encode

from src.helpers import authenticate, Deployment, get_token, DEFAULT_SPARQL_VIEW, DEFAULT_ES_VIEW
from src.logger import logger

from src.get_projects import get_obp_projects
from src.view_aggregate.common import DeltaUtils


def _make_view_base_endpoint(endpoint: str, org: str, project: str, es_view_id: str) -> str:
    return f"{endpoint}/views/{org}/{project}/{url_encode(es_view_id)}"


def get_es_view(
        endpoint: str, org: str, project: str, token: str, es_view_id: str,
        with_metadata: bool = True
) -> Dict:

    url = f"{_make_view_base_endpoint(endpoint, org, project, es_view_id)}" \
          f"{'/source' if not with_metadata else ''}"

    return DeltaUtils.check_response(
        requests.get(url=url, headers=DeltaUtils.make_header(token))
    )


def update_aggregated_org_project_list(
        endpoint: str, org: str, project: str, token: str, es_view_id: str,
        projects_to_aggregate: List[Tuple[str, str]],
        is_sparql: bool, with_metadata: bool = True
):

    aggregated_view = get_es_view(endpoint, org, project, token, es_view_id, with_metadata)

    view_id = DEFAULT_SPARQL_VIEW if is_sparql else DEFAULT_ES_VIEW

    assert all(i["viewId"] == view_id for i in aggregated_view["views"]) # TODO is that always the case?

    existing_list = set(tuple(i["project"].split("/")) for i in aggregated_view["views"])
    projects_to_aggregate_set = set(tuple(i) for i in projects_to_aggregate)

    a = existing_list.difference(projects_to_aggregate_set)
    b = projects_to_aggregate_set.difference(existing_list)

    if len(a) > 0:
        logger.warning(f"Project already aggregated is no longer in obp suite {a}")

    if len(b) > 0:
        logger.warning(f"New project in obp suite since last update {b}")

    if existing_list == projects_to_aggregate_set:
        logger.info("No update in obp suite since last time, no updates will be made")
        return aggregated_view

    aggregated_view["views"] = [
        {
            "project": f"{org}/{project}",
            "viewId": view_id
        }
        for org, project in projects_to_aggregate
    ]

    return update_aggregated_view(
        endpoint, org, project, token, es_view_id,
        es_view_rev=aggregated_view["_rev"], view_body=aggregated_view
    )


def update_aggregated_view(
        endpoint: str, org: str, project: str, token: str, es_view_id: str,
        view_body: Dict, es_view_rev: int
) -> Dict:
    url = f"{_make_view_base_endpoint(endpoint, org, project, es_view_id)}?rev={es_view_rev}"

    original_payload_keys = ["@id", "@type", "views"]
    view_body = dict((k, view_body[k]) for k in original_payload_keys if k in view_body)

    return DeltaUtils.check_response(
        requests.put(url=url, headers=DeltaUtils.make_header(token), json=view_body)
    )


if __name__ == "__main__":
    token = get_token(is_prod=True)
    org, project = "bbp", "atlas"

    projects_to_aggregate = get_obp_projects(token=token, is_prod=True)

    views_to_update = [
        ("https://bbp.epfl.ch/neurosciencegraph/data/views/aggreg-es/sbo", False),
        ("https://bbp.epfl.ch/neurosciencegraph/data/views/aggreg-sp/sbo", True)
    ]

    for aggregated_view_id, is_sparql in views_to_update:

        res = update_aggregated_org_project_list(
            endpoint=Deployment.PRODUCTION.value,
            org=org,
            project=project,
            token=token,
            es_view_id=aggregated_view_id,
            projects_to_aggregate=projects_to_aggregate,
            is_sparql=is_sparql
        )

        print(json.dumps(res, indent=4))
