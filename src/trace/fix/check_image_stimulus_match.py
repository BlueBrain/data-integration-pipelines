"""
Queries for all ExperimentalTrace in a bucket.
Checks the presence of the stimulus and image property of an ExperimentalTrace
Gathers the stimulus types that can be found in the stimulus property of an ExperimentalTrace
Gathers the stimulus types that can be found in the image property of an ExperimentalTrace
Make sure both sets of stimulus types are equal.
Checks that all stimulus types in the image property belong to the stimulus type ontology.
Outputs into one .json file all errors encountered (indexed by bucket, then resource id)
Outputs into another .json file a flattened set of all stimulus types encountered that are not present in the ontology (to facilitate completing the ontology)

+ Fix: if one of the issues encountered concerns the stimulus property (missing stimulus property, mismatch between stimulus type sets),
the fix_stimulus_field function (re)-creates the stimulus property based on all stimulus types found in the image property.
"""
import json
from multiprocessing import Pool
from typing import Dict, Tuple
import os

from kgforge.core import KnowledgeGraphForge, Resource

from src.helpers import (
    _as_list, allocate_with_default_views, authenticate_from_parser_arguments
)
from src.logger import logger
from src.trace.query.query import query_traces
from src.trace.find_valid_data import retrieve_wrapper
from src.trace.get_command_line_args import trace_command_line_args

from src.trace.stimulus_type_ontology_querying import stimulus_type_ontology


def success(check_im_stim_output) -> bool:
    return (
            check_im_stim_output["matching_sets"] and
            check_im_stim_output["has_stimulus"] and
            check_im_stim_output["has_image"] and
            len(check_im_stim_output["not_in_ontology"]) == 0
    )


def check_image_stimulus(resource: Resource, forge: KnowledgeGraphForge, stimulus_type_id_to_label: Dict[str, str]):
    id_ = resource.get_identifier()

    has_stimulus = "stimulus" in resource.__dict__
    has_image = "image" in resource.__dict__

    all_good = {"id": id_, "has_stimulus": has_stimulus, "has_image": has_image}

    if has_stimulus:
        # if not isinstance(res_i.stimulus, list):
        #     logger.warning(f"Stimulus field is not a list for {org}/{project} - {res_i.get_identifier()}")

        stimulus_json = forge.as_json(_as_list(resource.stimulus))

        if not all(len(i.keys()) == 2 for i in stimulus_json):
            logger.warning(f"More than 2 keys for stimulus of {resource.get_identifier()}")

        stimulus_stimulus_type_id = [entry["stimulusType"]["id"] for entry in stimulus_json]
    else:
        logger.warning(f"Stimulus field not found in {resource.get_identifier()}")
        stimulus_stimulus_type_id = []

    if has_image:
        # if not isinstance(res_i.image, list):
        #     logger.warning(f"Image field is not a list for {org}/{project} - {res_i.get_identifier()}")

        image_json = forge.as_json(_as_list(resource.image))
        image_stimulus_type_ids = [entry["stimulusType"]["id"] for entry in image_json]

    else:
        logger.error(f"Image field not found in {resource.get_identifier()}")
        image_stimulus_type_ids = []

    matching_sets = set(stimulus_stimulus_type_id) == set(image_stimulus_type_ids)

    all_good["not_in_ontology"] = [
        stim_type_i for stim_type_i in image_stimulus_type_ids
        if stim_type_i not in stimulus_type_id_to_label
    ]

    all_good["matching_sets"] = matching_sets
    all_good["stimulus_stimulus_type_ids"] = list(set(stimulus_stimulus_type_id))
    all_good["image_stimulus_type_ids"] = list(set(image_stimulus_type_ids))

    if not matching_sets:
        logger.error(f"Mismatch between image and stimulus field for {resource.get_identifier()}")

    return all_good


def fix_stimulus_field(resource: Resource, forge: KnowledgeGraphForge, stimulus_type_id_to_label: Dict[str, str]) -> Tuple[Dict, Resource]:
    all_good = check_image_stimulus(resource, forge, stimulus_type_id_to_label)

    stimulus_stimulus_type_id_set = all_good["stimulus_stimulus_type_ids"]
    image_stimulus_type_id_set = all_good["image_stimulus_type_ids"]
    not_in_ontology = all_good["not_in_ontology"]
    matching_sets = all_good["matching_sets"]

    if not matching_sets and len(set(stimulus_stimulus_type_id_set)) == 0 and len(set(image_stimulus_type_id_set)) > 0:

        logger.info("Missing stimulus field, will be filled by images' stimulus types")

        if len(not_in_ontology) != 0:
            logger.info("Some image stimulus types were not found in the ontology")
            print(not_in_ontology)
        else:
            # logger.info("All image stimulus types were found in the ontology")

            new_stim = [
                {
                    "@type": "Stimulus",
                    "stimulusType": {
                        "@id": stim_type_id,
                        "label": stimulus_type_id_to_label[stim_type_id]
                    }
                } for stim_type_id in image_stimulus_type_id_set
            ]
            resource.stimulus = forge.from_json(new_stim)
            # forge.update(resource)

    return all_good, resource


if __name__ == "__main__":

    projects_to_query = [
        ("public", "sscx"),
        ("bbp", "lnmce"),
        ("public", "thalamus"),
        ("public", "hippocampus"),
    ]

    parser = trace_command_line_args()

    received_args, leftovers = parser.parse_known_args()

    deployment, auth_token = authenticate_from_parser_arguments(received_args)

    single_cell_stimulus_type_id_to_label, stim_type_id_to_label = stimulus_type_ontology(
        deployment_str=deployment.value, token=auth_token
    )

    write_directory = received_args.output_dir
    os.makedirs(write_directory, exist_ok=True)

    errors = {}
    missing_from_ontology = set()

    for org, project in projects_to_query:

        forge_instance = allocate_with_default_views(org, project, deployment=deployment, token=auth_token)

        trace_ids = query_traces(forge_instance, raise_if_empty=True)

        traces = Pool().starmap(retrieve_wrapper, [(t_id, forge_instance, 'Retrieve', False) for t_id in trace_ids])

        traces = [t for t in traces if not t._store_metadata._deprecated]

        logger.info(f"Found {len(trace_ids)} ExperimentalTrace ids and {len(traces)} resources in {org}/{project}")

        res = Pool().starmap(check_image_stimulus, [(trace, forge_instance, stim_type_id_to_label) for trace in traces])

        # res = Pool().starmap(fix_stimulus_field, [(trace, forge_instance, stim_type_id_to_label) for trace in traces])

        missing_from_ontology.update(
            set(ii for flags in res for ii in flags["not_in_ontology"])
        )

        res_bad = [flags for flags in res if not success(flags)]

        errors[f"{org}/{project}"] = res_bad

    # print(json.dumps(errors, indent=4))
    with open(os.path.join(write_directory, "stimulus_mismatch.json"), "w") as f:
        json.dump(errors, indent=4, fp=f)

    with open(os.path.join(write_directory, "missing_from_ontology.json"), "w") as f:
        json.dump(list(missing_from_ontology), indent=4, fp=f)
