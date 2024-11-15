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

from src.curation_annotations import CurationStatus, create_update_curated_annotation, bool_to_curation_status
from src.helpers import allocate_with_default_views, authenticate_from_parser_arguments

from src.logger import logger

from src.trace.query.query import query_traces
from src.trace.validation.validation import retrieve_wrapper, trace_quality_check
from src.trace.get_command_line_args import trace_command_line_args
from src.trace.stimulus_type_ontology_querying import stimulus_type_ontology


def create_update_curated_annotation_on_trace(
        resource: Resource, forge: KnowledgeGraphForge, forge_datamodels: KnowledgeGraphForge
) -> Tuple[Resource, bool, List, CurationStatus]:
    """
    Updates a Trace Resource's curation annotation.
    If the checks find that the Trace is curated, looks for a curation-related annotation.
    If it's marked as Unassessed, replaces it by Curated. If it's marked as Curated, leaves it as is.
    If the checks find that the Trace is not curated, looks for a curation-related annotation.
    If it's marked as Curated, replaces it by Unassessed. If it's marked as Unassessed, leaves it as is.
    When adding an Unassessed annotation, the reasons why are added to the annotation under the property "note".

    :param resource: the resource to curate
    :type resource: Resource
    :param forge: an instance of forge tied to the bucket where the resource belongs
    :type forge: KnowledgeGraphForge
    :param forge_datamodels: an instance of forge tied to neurosciencegraph/datamodels to check the stimulus type ontology
    :type forge_datamodels: KnowledgeGraphForge
    :return: the resource with its payload updated with the appropriate curation annotation,
     the curation status as a bool, and the reasons why the resource is not curated, if any.
    :rtype: Tuple[Resource, bool, List]
    """
    checks = trace_quality_check(resource=resource, forge=forge, forge_datamodels=forge_datamodels, bool_only=True)
    values = list(checks.values())
    assert all(isinstance(i, bool) for i in values), [i for i in values if not isinstance(i, bool)]
    curated = all(values)

    if curated:
        reasons = []
        note = None
        new_curated_status = CurationStatus.CURATED
    else:
        reasons = [k for k, v in checks.items() if not v]
        note = f"This ExperimentalTrace could not be marked as curated " \
               f"due to the following failed check(s): {reasons}"
        new_curated_status = CurationStatus.UNASSESSED

    resource, previous_curation_status = create_update_curated_annotation(resource, forge, new_curated_status, note)

    return resource, curated, reasons, previous_curation_status


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

    forge_instance = allocate_with_default_views(org, project, deployment=deployment, token=auth_token)

    trace_ids = query_traces(forge_instance, raise_if_empty=True)

    traces = Pool().starmap(retrieve_wrapper, [(t_id, forge_instance, 'Retrieve', False) for t_id in trace_ids])

    traces = [t for t in traces if not t._store_metadata._deprecated]

    logger.info(f"Found {len(trace_ids)} ExperimentalTrace ids and {len(traces)} resources in {org}/{project}")

    res = Pool().starmap(
        create_update_curated_annotation_on_trace,
        [(trace, forge_instance, forge_neurosciencegraph_datamodels) for trace in traces]
    )

    evolving_annotation = dict(
        (res_i.get_identifier(), (previous_curation_status, bool_to_curation_status(curated)))
        for res_i, curated, _, previous_curation_status in res
        if previous_curation_status != bool_to_curation_status(curated)
    )

    plus = [
        res_i_identifier for res_i_identifier, (from_, to_) in evolving_annotation.items()
        if (from_ == CurationStatus.UNASSESSED or from_ == CurationStatus.NOTHING) and to_ == CurationStatus.CURATED
    ]

    minus = [
        res_i_identifier for res_i_identifier, (from_, to_) in evolving_annotation.items()
        if from_ == CurationStatus.CURATED and to_ == CurationStatus.UNASSESSED
    ]

    logger.info(f"Number of traces that were curated and will be marked as incomplete: {len(minus)}")
    logger.info(f"Number of traces that were marked as incomplete/not yet curated and will now be marked as curated: {len(plus)}")

    evolving_annotation_save = dict(
        (res_i_identifier, f"From {from_.value} to {to_.value}")
        for res_i_identifier, (from_, to_) in evolving_annotation.items()
    )

    if received_args.really_update == "yes":
        to_update = [res_i for res_i, _, _, _ in res if not res_i._synchronized]
        forge_instance.update(to_update)

    errs = dict((res_i.get_identifier(), reasons) for res_i, curated, reasons, _ in res if not curated)
    successes = [res_i.get_identifier() for res_i, curated, _, _ in res if curated]

    logger.info(f"Curated {len(successes)}")
    logger.info(f"Not curated {len(errs)}")

    with open(os.path.join(write_directory, f"curated_{org}_{project}.json"), "w") as f:
        json.dump(errs, indent=4, fp=f)

    with open(os.path.join(write_directory, f"evolving_annotation_{org}_{project}.json"), "w") as f:
        json.dump(evolving_annotation_save, indent=4, fp=f)
