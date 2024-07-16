import requests

from typing import Dict, Tuple, List, Optional
from urllib.parse import quote_plus as url_encode, quote_plus
import json

from src.helpers import get_token, Deployment
from src.utils.get_projects import get_all_projects
from src.view_aggregate.common import DeltaUtils


class DeltaException(Exception):
    body: Dict
    status_code: int

    def __init__(self, body: Dict, status_code: int):
        self.body = body
        self.status_code = status_code


MAX_NB_VIEWS_AGGREGATED = 20


def update_aggregated_view(
        token: str,
        endpoint: str,
        dest_org: str,
        dest_proj: str,
        aggregated_view_id: str,
        payload: Dict,
        latest_rev: str
):

    url = f"{endpoint}/views/{url_encode(dest_org)}/{url_encode(dest_proj)}/{url_encode(aggregated_view_id)}?rev={latest_rev}"

    response = requests.put(url=url, headers=DeltaUtils.make_header(token), json=payload)

    if response.status_code not in range(200, 229):
        raise DeltaException(body=json.loads(response.text), status_code=response.status_code)
    return json.loads(response.text)


def create_aggregated_view(
        token: str,
        endpoint: str,
        dest_org: str,
        dest_proj: str,
        projects_to_aggregate: List[Tuple[Tuple[str, str], str]],
        aggregated_view_id: str,
        is_sparql: bool
):

    url = f"{endpoint}/views/{url_encode(dest_org)}/{url_encode(dest_proj)}"

    payload = {
        "@id": aggregated_view_id,
        "@type": "AggregateElasticSearchView" if not is_sparql else "AggregateSparqlView",
        "views": [
            {
                "project": f"{org}/{proj}",
                "viewId": sub_view_id
            }
            for (org, proj), sub_view_id, in projects_to_aggregate
        ]
    }

    response = requests.post(url=url, headers=DeltaUtils.make_header(token), json=payload)

    if response.status_code not in range(200, 229):
        raise DeltaException(body=json.loads(response.text), status_code=response.status_code)
    return json.loads(response.text)


def aggregate_check(aggregated_view_id_base: str) -> List[Dict]:
    existing_views = []

    exists = True
    j = 0
    while exists:
        url = f"{endpoint}/views/{dest_org}/{dest_proj}"

        aggregated_view_id = f"{aggregated_view_id_base}_{j}"
        response = requests.get(url=f"{url}/{quote_plus(aggregated_view_id)}", headers=headers)
        response_json = response.json()
        exists = response_json["@type"] != 'ResourceNotFound'
        if exists:
            j += 1
            existing_views.append(response_json)

    return existing_views


def get_view(
        token: str,
        endpoint: str,
        org: str,
        project: str,
        view_id: str
):
    url = f"{endpoint}/views/{url_encode(org)}/{url_encode(project)}/{url_encode(view_id)}"
    response = requests.get(
        url=url,
        headers={
            "mode": "cors",
            "Content-Type": "application/json",
            "Accept": "application/ld+json, application/json",
            "Authorization": "Bearer " + token
        },
    )

    if response.status_code not in range(200, 229):
        raise DeltaException(body=json.loads(response.text), status_code=response.status_code)
    return json.loads(response.text)


def batch(iterable, n=MAX_NB_VIEWS_AGGREGATED):
    l = len(iterable)
    for ndx in range(0, l, n):
        yield iterable[ndx:min(ndx + n, l)]


def create_from_scratch(
        org_proj_list: List[Tuple[str, str]], token: str, endpoint: str, dest_org: str,
        dest_proj: str, aggregated_view_id_base: str, sub_view: str, is_sparql: bool, offset: Optional[int] = 0  # should be equal to len(existing ones)
) -> List[Dict]:

    created_all = []

    for i, x in enumerate(batch(org_proj_list)):

        projects_to_aggregate = [(xi, sub_view) for xi in x]
        aggregated_view_id = f"{aggregated_view_id_base}_{i+offset}"

        try:
            created_agg = create_aggregated_view(
                token=token, endpoint=endpoint,
                dest_org=dest_org, dest_proj=dest_proj,
                projects_to_aggregate=projects_to_aggregate,
                aggregated_view_id=aggregated_view_id,
                is_sparql=is_sparql
            )
            created_all.append(created_agg)
        except DeltaException as e:
            print(e.body)

    return created_all


def do(
        sub_view: str, aggregated_view_id_base: str, is_sparql: bool, org_proj_list: List[Tuple[str, str]], token: str, dest_org: str, dest_proj: str, endpoint: str
) -> Tuple[List[Dict], List[Dict]]:

    existing_views = aggregate_check(aggregated_view_id_base)

    if len(existing_views) > 0:
        print(f"{len(existing_views)} already exist")

        assert all(el_i["viewId"] == sub_view for el in existing_views for el_i in el["views"])
        existing_projects = [tuple(el_i["project"].split("/")) for el in existing_views for el_i in el["views"]]

        aggregated_but_not_meant_to_me = set(existing_projects).difference(set(org_proj_list))

        assert len(aggregated_but_not_meant_to_me) == 0, \
            f"Projects {aggregated_but_not_meant_to_me} were aggregated but shouldn't be. TODO implement removing from existing views"  # TODO implement removing it

        not_yet_aggregated = set(org_proj_list).difference(set(existing_projects))

        if len(not_yet_aggregated) > 0:

            space_left_in_latest_view = MAX_NB_VIEWS_AGGREGATED - len(existing_views[-1]["views"])

            if space_left_in_latest_view > 0:
                not_yet_aggregated_update = not_yet_aggregated[:space_left_in_latest_view] if len(not_yet_aggregated) > space_left_in_latest_view else not_yet_aggregated
                not_yet_aggregated_create = not_yet_aggregated[space_left_in_latest_view:] if len(not_yet_aggregated) > space_left_in_latest_view else []
                print(f"Will update latest view for {len(not_yet_aggregated_update)} projects: {not_yet_aggregated_update}")

                to_update = existing_views[-1]
                latest_rev = to_update["_rev"]
                to_update_id = to_update["@id"]

                payload = {key: to_update[key] for key in ["@id", "@type", "views"]}

                payload["views"] += [
                    {
                        "project": f"{org}/{proj}",
                        "viewId": sub_view
                    }
                    for org, proj in not_yet_aggregated_update
                ]

                res = update_aggregated_view(
                    token=token, dest_org=dest_org, dest_proj=dest_proj, payload=payload, aggregated_view_id=to_update_id, endpoint=endpoint, latest_rev=latest_rev
                )
                updated = [res]
            else:
                not_yet_aggregated_create = not_yet_aggregated
                updated = []

            if len(not_yet_aggregated_create) > 0:
                print(f"Will create additional aggregated views for {len(not_yet_aggregated_create)} projects left: {not_yet_aggregated_create}")
                created = create_from_scratch(
                    endpoint=endpoint, dest_org=dest_org, dest_proj=dest_proj, org_proj_list=list(not_yet_aggregated_create),
                    aggregated_view_id_base=aggregated_view_id_base, is_sparql=is_sparql, sub_view=sub_view, token=token, offset=len(existing_views)
                )
            else:
                print(f"All {len(not_yet_aggregated)} views left to aggregate were added to the last existing view {existing_views[-1]['@id']}")
                created = []
        else:
            print("Existing aggregated views are up to date already.")
            created, updated = [], []
    else:
        print(f"No views exist, new aggregation")

        updated = []
        created = create_from_scratch(
            endpoint=endpoint, dest_org=dest_org, dest_proj=dest_proj, org_proj_list=org_proj_list,
            aggregated_view_id_base=aggregated_view_id_base, is_sparql=is_sparql, sub_view=sub_view, token=token
        )

    print(f"{len(updated)} views updated, {len(created)} views created")
    return created, updated


if __name__ == "__main__":
    token = get_token(is_prod=True)
    endpoint = Deployment.PRODUCTION.value
    dest_org, dest_proj = "bbp", "atlas"

    org_proj_list = get_all_projects(token)

    not_aggregate_list = [("bbp", "allresources"), ("bbp", "resources")]

    org_proj_list = list(set(org_proj_list) - set(not_aggregate_list))

    headers = DeltaUtils.make_header(token)

    for sub_view, aggregated_view_id_base, is_sparql in [
            ("https://bbp.epfl.ch/neurosciencegraph/data/views/es/contentUrl", "https://bbp.epfl.ch/data/bbp/atlas/all_projects_es_content_url_aggregated_view", False),
            ("https://bluebrain.github.io/nexus/vocabulary/defaultElasticSearchIndex", "https://bbp.epfl.ch/data/bbp/atlas/all_projects_es_aggregated_view", False),
            ("https://bluebrain.github.io/nexus/vocabulary/defaultSparqlIndex",  "https://bbp.epfl.ch/data/bbp/atlas/all_projects_sp_aggregated_view", True)
        ]:
        created_res, updated_res = do(
            sub_view=sub_view, aggregated_view_id_base=aggregated_view_id_base, is_sparql=is_sparql, endpoint=endpoint,
            token=token, dest_proj=dest_proj, dest_org=dest_org, org_proj_list=org_proj_list
        )
