"""
Updates the curated and un-assessed annotation of ExperimentalTrace-s based on all bool-valued checks returned by find_valid_data.single_resource.
If any value is False, marked as un-assessed with the note indicating which checks failed. If all values are True, mark the ExperimentalTrace as Curated.
Optionally update the Resource-s in Nexus.
Outputs all failed checks in a .json
"""

import json
from multiprocessing import Pool
from typing import Tuple, List
import os

from kgforge.core import KnowledgeGraphForge, Resource

from src.curation_annotations import make_curated, make_unassessed
from src.helpers import _as_list, allocate_with_default_views, authenticate_from_parser_arguments

from src.logger import logger

from src.trace.query.query import query_traces
from src.trace.find_valid_data import retrieve_wrapper, single_resource
from src.trace.get_command_line_args import trace_command_line_args
from src.trace.stimulus_type_ontology_querying import stimulus_type_ontology


def add_rm_curated_annotation(
        resource: Resource, forge: KnowledgeGraphForge, forge_datamodels: KnowledgeGraphForge
) -> Tuple[Resource, bool, List]:

    checks = single_resource(resource=resource, forge=forge, forge_datamodels=forge_datamodels, bool_only=True)
    values = list(checks.values())
    assert all(isinstance(i, bool) for i in values), [i for i in values if not isinstance(i, bool)]

    curated = all(values)

    new_annotation = _as_list(resource.annotation) if "annotation" in resource.__dict__ else []

    existing_curated = next(
        (idx for idx, ann in enumerate(new_annotation)
         if ann.hasBody.label == "Curated"), None
    )
    existing_unassessed = next(
        (idx for idx, ann in enumerate(new_annotation)
         if ann.hasBody.label == "Unassessed"), None
    )

    if existing_curated is not None and existing_unassessed is not None:
        logger.error(f"Weirdo labelled as both curated and unassessed {resource.get_identifier()}, skipped")
        return resource  # TODO handle this, hopefully it never happens

    if curated:
        reasons = []
        logger.info(f"{resource.get_identifier()}, Curated")

        if not existing_curated:
            new_annotation.append(forge.from_json(make_curated(note=None)))
        if existing_unassessed:
            del new_annotation[existing_unassessed]
    else:
        reasons = [k for k, v in checks.items() if not v]
        note = f"This ExperimentalTrace could not be marked as curated " \
               f"due to the following failed check(s): {reasons}"

        logger.warning(f"{resource.get_identifier()}, {note}")

        if not existing_unassessed:
            new_annotation.append(forge.from_json(make_unassessed(note=note)))
        if existing_curated:
            del new_annotation[existing_curated]
            # TODO could be unsafe if one record happened to have both annotations but that shouldn't be

    resource.annotation = new_annotation

    return resource, curated, reasons


if __name__ == "__main__":

    parser = trace_command_line_args(with_really_update=True, with_bucket=True)

    received_args, leftovers = parser.parse_known_args()

    org, project = received_args.bucket.split("/")

    deployment, auth_token = authenticate_from_parser_arguments(received_args)

    write_directory = received_args.output_dir
    os.makedirs(write_directory, exist_ok=True)

    logger.info(f"Curated annotations will be updated: {received_args.really_update}")

    forge_neurosciencegraph_datamodels = allocate_with_default_views(
        "neurosciencegraph", "datamodels", deployment=deployment, token=auth_token,
    )

    single_cell_stimulus_type_id_to_label, stim_type_id_to_label = stimulus_type_ontology(
        deployment_str=deployment.value, token=auth_token
    )

    forge_instance = allocate_with_default_views(org, project, deployment=deployment, token=token)

    trace_ids = query_traces(forge_instance, raise_if_empty=True)

    traces = Pool().starmap(retrieve_wrapper, [(t_id, forge_instance, 'Retrieve', False) for t_id in trace_ids])

    traces = [t for t in traces if not t._store_metadata._deprecated]

    logger.info(f"Found {len(trace_ids)} ExperimentalTrace ids and {len(traces)} resources in {org}/{project}")

    res = Pool().starmap(
        add_rm_curated_annotation,
        [(trace, forge_instance, forge_neurosciencegraph_datamodels) for trace in traces]
    )

    to_update = [res_i for res_i, curated, reasons in res if not res_i._synchronized]

    if received_args.really_update == "yes":
        forge_instance.update(to_update)

    errs = dict((res_i.get_identifier(), reasons) for res_i, curated, reasons in res if not curated)
    successes = [res_i.get_identifier() for res_i, curated, _ in res if curated]

    logger.info(f"Curated {len(successes)}")
    logger.info(f"Not curated {len(errs)}")

    with open(os.path.join(write_directory, f"curated_{org}_{project}.json"), "w") as f:
        json.dump(errs, indent=4, fp=f)
