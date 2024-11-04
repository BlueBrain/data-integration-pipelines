import argparse
import getpass
import json
from collections import defaultdict
from typing import List, Optional, Tuple, Dict
from urllib.parse import quote_plus
import requests

from src.arguments import define_arguments
from src.logger import logger
from src.helpers import Deployment, DEFAULT_SPARQL_VIEW, authenticate_from_parser_arguments
from src.get_projects import _get_obp_projects

from enum import Enum


class OBPType(Enum):
    # Experimental Data
    RECONSTRUCTED_NEURON_MORPHOLOGY = "https://neuroshapes.org/ReconstructedNeuronMorphology"
    EXPERIMENTAL_TRACE = "https://bbp.epfl.ch/ontologies/core/bmo/ExperimentalTrace"
    EXPERIMENTAL_NEURON_DENSITY = "https://bbp.epfl.ch/ontologies/core/bmo/ExperimentalNeuronDensity"
    EXPERIMENTAL_BOUTON_DENSITY = "https://bbp.epfl.ch/ontologies/core/bmo/ExperimentalBoutonDensity"
    EXPERIMENTAL_SYNAPSE_PER_CONNECTION = "https://bbp.epfl.ch/ontologies/core/bmo/ExperimentalSynapsesPerConnection"
    # Model Data
    E_MODEL = "https://neuroshapes.org/EModel"


TYPE_TO_CURATED = {
    OBPType.EXPERIMENTAL_TRACE: True,
    OBPType.RECONSTRUCTED_NEURON_MORPHOLOGY: True,
    OBPType.EXPERIMENTAL_SYNAPSE_PER_CONNECTION: False,
    OBPType.EXPERIMENTAL_BOUTON_DENSITY: False,
    OBPType.EXPERIMENTAL_NEURON_DENSITY: False,
    OBPType.E_MODEL: True
}

TYPE_TO_EXTRA_FILTER = {

    OBPType.EXPERIMENTAL_TRACE:  None,
    OBPType.RECONSTRUCTED_NEURON_MORPHOLOGY: None,
    OBPType.EXPERIMENTAL_SYNAPSE_PER_CONNECTION: None,
    OBPType.EXPERIMENTAL_BOUTON_DENSITY: None,
    OBPType.EXPERIMENTAL_NEURON_DENSITY: None,
    OBPType.E_MODEL: None
}


def _make_sp_query(
        brain_regions: Optional[List[str]], type_: str, curated_flag: bool, extra_q: Optional[str] = None
) -> str:
    q = """
        SELECT ?id
        WHERE {
          ?id a <%s> .
          ?id <https://bluebrain.github.io/nexus/vocabulary/deprecated> false .
          %s 
          %s
          %s
        }
        LIMIT 10000
    """

    curated_flag_part = """
        ?id <https://neuroshapes.org/annotation>/<https://neuroshapes.org/hasBody>/<http://www.w3.org/2000/01/rdf-schema#label>  "Curated" .
    """ if curated_flag else ""

    brain_region_query = """
          ?id <https://neuroshapes.org/brainLocation>/<https://neuroshapes.org/brainRegion> ?br .
          VALUES ?br { %s } .
    """ % " ".join(f"<{i}>" for i in brain_regions) if brain_regions is not None else ""

    extra_q_part = extra_q if extra_q is not None else ""

    return q % (type_, curated_flag_part, brain_region_query, extra_q_part)


def _make_es_query(
        brain_regions: Optional[List[str]], type_: str, curated_flag: bool
) -> Dict:
    # Unlike the sparql call, no extra_query. The goal of the extra query is to add the additional logic/filter
    # that was applied during the construct query and that led to the data present in the ES view being targeted
    # (so already including the extra logic/filtering of the extra_query)

    q = {
        "size": 10000,
        "from": 0,
        "query": {
            "bool": {
                "must": [
                    {"term": {"@type.keyword": type_}},
                    {"term": {"deprecated": False}}
                ]
            }
        }
    }

    if curated_flag:
        q["query"]["bool"]["must"].append(
            {"term": {"curated": True}}
        )

    if brain_regions is not None:
        q["query"]["bool"]["must"].append(
            {"terms": {"brainRegion.@id.keyword": brain_regions}}
        )

    return q


def compare(
        token: str, deployment: Deployment,
        org: str, project: str, brain_regions: Optional[List[str]],
        type_: OBPType, show_indexing_err_on_mismatch: bool
):

    endpoint = deployment.value
    t_str = type_.value.split("/")[-1]
    logger.info(f"Checking {t_str} in {org}/{project}")

    is_curated = TYPE_TO_CURATED[type_]

    def make_header(content_type: Optional[str] = None):
        default_header = {
            "Authorization": f"Bearer {token}"
        }
        return default_header if content_type is None else {**default_header, "Content-Type": content_type}

    def _es_view(endpoint_value):

        response_es = requests.post(
            url=endpoint_value,
            headers=make_header("application/json"),
            data=json.dumps(
                _make_es_query(
                    brain_regions=brain_regions, type_=type_.value,
                    curated_flag=is_curated
                )
            )
        )

        response_es.raise_for_status()
        return response_es.json()

    def _sp_view(endpoint_value):
        extra_sp_q = TYPE_TO_EXTRA_FILTER[type_]

        el_q = _make_sp_query(
                brain_regions=brain_regions, type_=type_.value,
                extra_q=extra_sp_q, curated_flag=is_curated
            )

        response_sparql = requests.post(
            url=endpoint_value,
            headers=make_header("application/sparql-query"),
            data=el_q
        )

        response_sparql.raise_for_status()
        return response_sparql.json()

    default_sp_view = quote_plus(DEFAULT_SPARQL_VIEW)
    composite_view_id = quote_plus("https://bluebrain.github.io/nexus/vocabulary/searchView")
    projection_id = quote_plus("https://bluebrain.github.io/nexus/vocabulary/searchProjection")

    response_sparql_default = _sp_view(f"{endpoint}/views/{org}/{project}/{default_sp_view}/sparql")
    response_sparql_default = response_sparql_default['results']['bindings']

    sp_default_count = len(response_sparql_default)

    if sp_default_count == 0:
        logger.info(f'Nothing in {org}/{project} based on default sparql index')
        return

    sp_default_ids = [i["id"]['value'] for i in response_sparql_default]
    set_sp_default_ids = set(sp_default_ids)
    set_sp_default_count = len(set_sp_default_ids)

    # This occurs in the case of some references leading to duplicate sparql results ; not important yet

    # if set_sp_default_count != sp_default_count:
    #     counts = defaultdict(int)
    #     for el in sp_default_ids:
    #         counts[el] += 1
    #
    #     duplicates = [k for k, v in counts.items() if v > 1]
    #
    #     logger.warning(f"Duplicate values found in default sparql index of {org}/{project} {set_sp_default_count} vs {sp_default_count}")
    #     logger.warning(f"Duplicates: {duplicates}")

    composite_response = _es_view(f"{endpoint}/views/{org}/{project}/{composite_view_id}/projections/{projection_id}/_search")

    sparql_intermediate_response = _sp_view(f"{endpoint}/views/{org}/{project}/{composite_view_id}/sparql")

    search_idx_response = _es_view(f'{endpoint}/search/query/suite/sbo')

    es_composite_count = composite_response["hits"]["total"]["value"]

    sparql_intermediate_count = len({i["id"]['value'] for i in sparql_intermediate_response['results']['bindings']})

    search_idx_filtered_proj = [i for i in search_idx_response["hits"]["hits"] if f"{org}/{project}" in i["_source"]["_self"]]
    search_idx_count = search_idx_response["hits"]["total"]["value"]

    search_idx_len = len(search_idx_filtered_proj)

    logger.info(f"From Sparql Default Index {set_sp_default_count}")
    logger.info(f"From ES Composite view {es_composite_count}")
    logger.info(f"From Composite Sparql intermediate space {sparql_intermediate_count}")
    logger.info(f"From OBP Search index in {org}/{project} {search_idx_len}/{search_idx_count}")

    if len({set_sp_default_count, es_composite_count, search_idx_len}) == 1:
        return

    logger.warning(f"Mismatch in {org}/{project}")

    es_stuff = set(i["_source"]["@id"] for i in search_idx_filtered_proj)

    logger.warning(
        f"ES set of va {len(es_stuff)} SP {set_sp_default_count} Intersection {len(set_sp_default_ids.intersection(es_stuff))}"
    )

    logger.warning("Difference between what was found in SP and ES: ")
    logger.warning(set_sp_default_ids.difference(es_stuff))

    if show_indexing_err_on_mismatch:
        indexing_failures_search_project = requests.get(
            f"{endpoint}/views/{org}/{project}/search/failures",
            headers=make_header()
        )

        indexing_failures_search_project.raise_for_status()
        logger.warning(f"Number of indexing failures from composite view: {indexing_failures_search_project.json()['_total']}")

        url_stats = f"{endpoint}/views/{org}/{project}/{composite_view_id}/projections/{projection_id}/statistics"
        response_stats = requests.get(url_stats, headers=make_header())
        response_stats.raise_for_status()

        logger.warning(
            json.dumps(response_stats.json(), indent=4)
        )


def compare_for_all_projects(token: str, deployment: Deployment, type_value: OBPType, show_indexing_err_on_mismatch: bool = True):

    proj_list = _get_obp_projects(token=token, deployment=deployment)

    for org, proj in proj_list:
        compare(
            token=token, deployment=deployment, org=org, project=proj, brain_regions=None, type_=type_value,
            show_indexing_err_on_mismatch=show_indexing_err_on_mismatch
        )


if __name__ == "__main__":

    parser = define_arguments(argparse.ArgumentParser())

    parser.add_argument(
        "--data_type", type=str, required=True, choices=OBPType._member_names_
    )

    received_args, leftovers = parser.parse_known_args()

    type_v = OBPType[received_args.data_type]

    deployment, auth_token = authenticate_from_parser_arguments(received_args)

    compare_for_all_projects(
        token=auth_token,
        deployment=deployment,
        type_value=type_v
    )
