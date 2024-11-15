"""
Queries all ExperimentalTrace-s in a bucket. For one ExperimentalTrace:
- gathers all stimulus types inside the distribution of extension .nwb.
- gathers all stimulus types from the metadata's image property.
Checks that both sets of stimulus types are equal.
Saves into a .json file all errors encountered (difference of sets)
"""
import json
import os
from multiprocessing import Pool
from typing import Dict, Tuple, Optional, Union, Set, Callable, List
import h5py
from kgforge.core import Resource, KnowledgeGraphForge

from src.helpers import (
    _as_list, allocate_with_default_views, authenticate_from_parser_arguments
)
from src.logger import logger
from src.forge_extension import download_file

from src.trace.fix.check_image_stimulus_match import check_image_stimulus

from src.trace.query.query import query_traces
from src.trace.validation import (
    distribution_extension_from_name, has_distribution, retrieve_wrapper
)
from src.trace.get_command_line_args import trace_command_line_args
from src.trace.stimulus_type_ontology_querying import stimulus_type_ontology


def _stimulus_type_extraction(nwb_stuff: Union[str, bytes]) -> Set[str]:

    with h5py.File(nwb_stuff, 'r') as h5file:
        stimulus_type_data = h5file['/general/intracellular_ephys/sequential_recordings/stimulus_type'][:]
        stimuli = list(set(stimulus_type_data))

    return {stimulus.decode() if isinstance(stimulus, bytes) else stimulus for stimulus in stimuli}


def check_nwb_stimulus_match(resource: Resource, forge: KnowledgeGraphForge, stimulus_type_id_to_label_dict: Dict, download_dir: Optional[str] = None):

    dict_v = {
        "id": resource.get_identifier(),
        "err": None
    }

    has, _, _, content_url = distribution_extension_from_name(resource, "nwb") if has_distribution(resource) else (False, False, False, False)

    if not has:
        dict_v["err"] = "Distribution problem"
        return dict_v

    try:
        in_nwb = _stimulus_type_extraction(
            download_file(content_url=content_url, forge=forge, path=download_dir)
        )
    except Exception as e:
        dict_v["err"] = str(e)
        return dict_v

    in_metadata = check_image_stimulus(resource, forge, stimulus_type_id_to_label_dict)

    in_metadata_image = {
        i.replace("http://bbp.epfl.ch/neurosciencegraph/ontologies/stimulustypes/", "")
        for i in in_metadata["image_stimulus_type_ids"]
    }

    match = in_nwb == in_metadata_image

    dict_v["stimulus_metadata_nwb_match"] = match

    if match:
        return dict_v

    dict_v["err"] = "Stimulus metadata & nwb do not match"

    a = in_nwb.difference(in_metadata_image)

    if len(a) > 0:
        logger.error(f"{resource.get_identifier()} Stimulus found in nwb but not metadata: {a}")
        dict_v["nwb_not_metadata"] = list(a)

    b = in_metadata_image.difference(in_nwb)

    if len(b) > 0:
        logger.error(f"{resource.get_identifier()} Stimulus found in metadata but not nwb: {b}")
        dict_v["metadata_not_nwb"] = list(b)

    return dict_v


if __name__ == "__main__":

    parser = trace_command_line_args()

    received_args, leftovers = parser.parse_known_args()

    deployment, auth_token = authenticate_from_parser_arguments(received_args)

    projects_to_query = [
        ("public", "sscx"),
        ("bbp", "lnmce"),
        ("public", "thalamus"),
        ("public", "hippocampus")
    ]

    write_directory = received_args.output_dir
    os.makedirs(write_directory, exist_ok=True)

    download_directory = os.path.join(write_directory, "temp_trace")
    os.makedirs(download_directory, exist_ok=True)

    single_cell_stimulus_type_id_to_label, stimulus_type_id_to_label = stimulus_type_ontology(
        deployment_str=deployment.value, token=auth_token
    )

    errors = {}

    for org, project in projects_to_query:

        forge_instance = allocate_with_default_views(org, project, deployment=deployment, token=auth_token)

        trace_ids = query_traces(forge_instance, raise_if_empty=True)

        traces = Pool().starmap(retrieve_wrapper, [(t_id, forge_instance, 'Retrieve', False) for t_id in trace_ids])

        traces = [t for t in traces if not t._store_metadata._deprecated]

        logger.info(f"Found {len(trace_ids)} ExperimentalTrace ids and {len(traces)} resources in {org}/{project}.")

        res_2 = Pool().starmap(
            check_nwb_stimulus_match,
            [(trace, forge_instance, stimulus_type_id_to_label, download_directory) for trace in traces]
        )

        errs = [i for i in res_2 if i["err"] is not None]
        successes = [i for i in res_2 if i["err"] is None]

        logger.info(f"Success {len(successes)}")
        logger.info(f"Failure {len(errs)}")

        errors[f"{org}/{project}"] = errs

    with open(os.path.join(write_directory, "stimulus_mismatch_nwb.json"), "w") as f:
        json.dump(errors, indent=4, fp=f)

