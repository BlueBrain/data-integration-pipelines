"""
Queries for all ExperimentalTrace-s that are not also of type SingleCellExperimentalTrace.
Iterates over all stimulus types found in the .nwb attachement of ExperimentalTrace-s.
If any stimulus type is a subclass of SingleCellProtocolStimulus according to the stimulus type ontology,
the ExperimentalTrace will be typed as SingleCellExperimentalTrace as well.
"""

import json
import os
from multiprocessing import Pool
from typing import Dict, Tuple, Optional, List
from kgforge.core import Resource, KnowledgeGraphForge

from src.helpers import _as_list, allocate_with_default_views, authenticate_from_parser_arguments

from src.logger import logger

from src.trace.fix.check_nwb_stimulus_match import _stimulus_type_extraction
from src.trace.query.query import query_traces
from src.trace.find_valid_data import (
    has_distribution, distribution_extension_from_name, retrieve_wrapper
)
from src.forge_extension import download_file
from src.trace.get_command_line_args import trace_command_line_args
from src.trace.stimulus_type_ontology_querying import stimulus_type_ontology

NEW_TYPE = "SingleCellExperimentalTrace"


# Not used

# from src.trace.add_curation_annotation import check_image_stimulus

# def add_single_cell_type_based_on_image(resource: Resource, forge: KnowledgeGraphForge, single_cell_stimulus_type_id_to_label_dict: Dict) -> Resource:
#     flags = check_image_stimulus(resource, forge, single_cell_stimulus_type_id_to_label_dict)
#
#     stimulus_type_ids = flags["image_stimulus_type_ids"]
#
#     if stimulus_type_ids != flags["stimulus_stimulus_type_ids"]:  # Re-check these are equal
#         logger.error(f"Run fix stimulus field first on {resource.get_identifier()}")
#         return resource
#
#     return _add_single_cell_type(resource, forge, stimulus_type_ids, single_cell_stimulus_type_id_to_label_dict)


def add_single_cell_type_based_on_nwb(
        resource: Resource, forge: KnowledgeGraphForge,
        single_cell_stimulus_type_id_to_label_dict: Dict, path: Optional[str]
) -> Tuple[Resource, Optional[str]]:

    has, _, _, content_url = distribution_extension_from_name(resource, "nwb") \
        if has_distribution(resource) else (False, False, False, None)

    if not has:
        err = f"Skipping {resource.get_identifier()}, no nwb distribution"
        logger.error(err)
        return resource, err

    try:
        nwb_path: str = download_file(content_url=content_url, forge=forge, path=path)
        in_nwb = _stimulus_type_extraction(nwb_path)
    except Exception as e:
        err = f"Could not proceed with {resource.get_identifier()} : {str(e)}"
        logger.error(err)
        return resource, err

    return _add_single_cell_type(resource, forge, list(in_nwb), single_cell_stimulus_type_id_to_label_dict), None


def _add_single_cell_type(resource: Resource, forge: KnowledgeGraphForge, stimulus_list_considered: List, single_cell_stimulus_type_id_to_label_dict: Dict) -> Resource:

    single_cell_stimulus_found = [
        i for i in stimulus_list_considered
        if f"http://bbp.epfl.ch/neurosciencegraph/ontologies/stimulustypes/{i}" in single_cell_stimulus_type_id_to_label_dict
    ]

    if len(single_cell_stimulus_found) > 0:
        existing_types = _as_list(resource.get_type())

        if NEW_TYPE not in existing_types:
            existing_types.append(NEW_TYPE)
            resource.type = existing_types
            forge.update(resource)
        else:
            logger.warning(f"{resource.get_identifier()} is already a {NEW_TYPE}")
    else:
        logger.info(f"Didn't find any single cell stimulus types in {resource.get_identifier()}")

    return resource


if __name__ == "__main__":

    parser = trace_command_line_args()

    received_args, leftovers = parser.parse_known_args()

    deployment, auth_token = authenticate_from_parser_arguments(received_args)

    write_directory = received_args.output_dir
    os.makedirs(write_directory, exist_ok=True)

    download_directory = f"{write_directory}/temp_traces"
    os.makedirs(download_directory, exist_ok=True)

    single_cell_stimulus_type_id_to_label, stimulus_type_id_to_label = stimulus_type_ontology(
        deployment_str=deployment.value, token=auth_token
    )

    errors = {}

    projects_to_query = [
        ("public", "sscx"),
        ("bbp", "lnmce"),
        ("public", "thalamus"),
        ("public", "hippocampus"),
    ]

    for org, project in projects_to_query:

        forge_instance = allocate_with_default_views(org, project, deployment=deployment, token=auth_token)

        trace_ids_all = query_traces(forge_instance)

        trace_ids = query_traces(forge_instance, extra_q="""
           FILTER NOT EXISTS { ?id a %s } .
        """ % NEW_TYPE)

        traces = Pool().starmap(retrieve_wrapper, [(t_id, forge_instance, 'Retrieve', False) for t_id in trace_ids])

        traces = [t for t in traces if not t._store_metadata._deprecated]

        logger.info(f"Found {len(trace_ids)} ExperimentalTrace ids and {len(traces)} "
                    f"resources in {org}/{project} that are not {NEW_TYPE}. {len(trace_ids_all)} total")

        res = Pool().starmap(
            add_single_cell_type_based_on_nwb,
            [(trace, forge_instance, single_cell_stimulus_type_id_to_label, download_directory) for trace in traces]
        )

        # TODO have really-update mechanism going on here

        os.rmdir(download_directory)

        errors[f"{org}/{project}"] = [
            {"id": res_i.get_identifier(), "err": err_i}
            for res_i, err_i in res if err_i is not None
        ]

    with open(os.path.join(write_directory, "nwb_stimulus_err.json"), "w") as f:
        json.dump(errors, indent=4, fp=f)


# 2 out of public/hippocampus for not having a nwb -> cannot check it is a singlecell experimental trace
# 2 out of public/thalamus. No stimulus types are single cell
# No issue in public/sscx
