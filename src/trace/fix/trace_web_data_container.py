"""

1. assign_trace_has_part_field

TraceWebDataContainer have their isPartOf field point to a Trace (ExperimentalTrace, SimulationTrace).
Creates the symmetric property inside the ExperimentalTrace (hasPart).

Looks for all ExperimentalTrace-s that do not have a hasPart property.
Queries all TraceWebDataContainer in a bucket, and build a trace_id to trace web data container  dictionary.
Looks for all ExperimentalTrace found in the first query that can have a TraceWebDataContainer assigned to it.

If display additional information, displays whether:
- Some ExperimentalTrace-s without a TraceWebDataContainer can not have any assigned to it
- Any ExperimentalTrace in the bucket has multiple candidate TraceWebDataContainer
- Any TraceWebDataContainer has in its isPartOf something that isn't an ExperimentalTrace


2. set_is_part_of_to_right_type

TraceWebDataContainer point to a Trace in their isPartOf property.
Make sure that the reference has the right type (= the type of the Trace Resource).
Priority is given to more specific types (ExperimentalTrace, SimulationTrace).
If neither is found, the generic Trace type is kept.
"""

from collections import defaultdict
from typing import Optional

from kgforge.core import Resource, KnowledgeGraphForge

from src.logger import logger

from src.helpers import _as_list, allocate_with_default_views, authenticate_from_parser_arguments

from src.trace.query.query import query_traces, batch, query_trace_web_data_container
from src.trace.get_command_line_args import trace_command_line_args


def set_is_part_of_to_right_type(
        forge_instance: KnowledgeGraphForge, really_update: bool,
        display_additional_information: Optional[bool] = False
):

    trace_ids = query_traces(forge_instance, raise_if_empty=True)

    logger.info(f"Found {len(trace_ids)} ExperimentalTrace in {org}/{project}")

    trace_web_data_container_ids = query_trace_web_data_container(forge_instance)
    logger.info(f"Found {len(trace_web_data_container_ids)} TraceWebDataContainer in {org}/{project}")

    isPartOf_to_web_data = defaultdict(list)

    for el in trace_web_data_container_ids:
        isPartOf_to_web_data[el.isPartOf].append(el.id)

    can_be_assigned = set(isPartOf_to_web_data.keys()).intersection(trace_ids)
    logger.info(f"{len(can_be_assigned)} traces can be assigned a TraceWebDataContainer as a hasPart")

    if display_additional_information:
        lengths = set(len(value) for value in isPartOf_to_web_data.values())
        trace_without_assignment = set(trace_ids).difference(isPartOf_to_web_data.keys())

        isPartOf_not_trace = set(isPartOf_to_web_data.keys()).difference(trace_ids)

        logger.info(f"Set of lengths of hasPart: {lengths} -> count per ExperimentalTrace of TraceWebDataContainer-s that can be in 'hasPart' (Expected value = 1?)")
        logger.info(f"Missing traces with no hasPart assigned: {len(trace_without_assignment)}")

        append = f"Example: {next(iter(isPartOf_not_trace))}" if len(isPartOf_not_trace) > 0 else ''
        logger.info(f"TraceWebDataContainer pointing to something that isn't an un-deprecated ExperimentalTrace: {len(isPartOf_not_trace)} {append}\n")

    for i, batch_i in enumerate(batch(list(isPartOf_to_web_data.items()), n=500)):
        try:
            to_update = []

            for (trace_id, trace_web_id) in batch_i:
                res_trace = forge_instance.retrieve(trace_id)

                if res_trace._store_metadata._deprecated:
                    logger.warning(f"Skipping {res_trace.get_identifier()}, deprecated")
                    continue

                res_trace_web = forge_instance.retrieve(trace_web_id)

                types = [
                    forge_instance._model.context().expand(i)
                    for i in _as_list(res_trace.get_type())
                ]

                if "https://neuroshapes.org/SimulationTrace" in types:
                    res_trace_web.isPartOf.type = "https://neuroshapes.org/SimulationTrace"
                elif "https://bbp.epfl.ch/ontologies/core/bmo/ExperimentalTrace" in types:
                    res_trace_web.isPartOf.type = "https://bbp.epfl.ch/ontologies/core/bmo/ExperimentalTrace"
                else:
                    logger.warning(f"Not simulation not experimental, who are you? {res_trace.get_identifier()}")
                    res_trace_web.isPartOf.type = "https://neuroshapes.org/Trace"

                to_update.append(res_trace_web)

            if len(to_update) > 0 and really_update:
                forge_instance.update(to_update)

        except Exception as e:
            logger.error(f"Batch {i} failed: {e}")


def assign_trace_has_part_field(forge_instance: KnowledgeGraphForge, really_update: bool):

    trace_ids = query_traces(forge_instance, extra_q="""
        FILTER NOT EXISTS { ?id hasPart ?thing }
    """)

    logger.info(f"Found {len(trace_ids)} ExperimentalTrace in {org}/{project} without hasPart assigned")

    trace_web_data_container_ids = query_trace_web_data_container(forge_instance)
    logger.info(f"Found {len(trace_web_data_container_ids)} TraceWebDataContainer in {org}/{project}")

    isPartOf_to_web_data = defaultdict(list)

    for el in trace_web_data_container_ids:
        isPartOf_to_web_data[el.isPartOf].append(el.id)

    can_be_assigned = set(isPartOf_to_web_data.keys()).intersection(trace_ids)
    logger.info(f"{len(can_be_assigned)} traces can be assigned a TraceWebDataContainer as a hasPart")

    display_stuff = False

    if display_stuff:
        lengths = set(len(value) for value in isPartOf_to_web_data.values())
        trace_without_assignment = set(trace_ids).difference(isPartOf_to_web_data.keys())

        isPartOf_not_trace = set(isPartOf_to_web_data.keys()).difference(trace_ids)

        logger.info(f"Set of lengths of hasPart: {lengths} -> count per ExperimentalTrace of TraceWebDataContainer-s that can be in 'hasPart' (Expected value = 1?)")
        logger.info(f"Missing traces with no hasPart assigned: {len(trace_without_assignment)}")

        append = f"Example: {next(iter(isPartOf_not_trace))}" if len(isPartOf_not_trace) > 0 else ''
        logger.info(f"TraceWebDataContainer pointing to something that isn't an un-deprecated ExperimentalTrace: {len(isPartOf_not_trace)} {append}\n")

    for i, batch_i in enumerate(batch(list(can_be_assigned), n=500)):
        try:
            to_update = []

            for trace_id in batch_i:
                res = forge_instance.retrieve(trace_id)
                if "hasPart" in res.__dict__:
                    continue

                res.hasPart = forge_instance.from_json(
                    [{"id": isPartOf_to_web_data[trace_id][0], "type": "TraceWebDataContainer"}]
                )

                to_update.append(res)

            if len(to_update) > 0 and really_update:
                forge_instance.update(to_update)

        except Exception as e:
            logger.error(f"Batch {i} failed")


if __name__ == "__main__":
    parser = trace_command_line_args(with_really_update=True)

    received_args, leftovers = parser.parse_known_args()

    deployment, auth_token = authenticate_from_parser_arguments(received_args)

    really_update = received_args.really_update == "yes"

    projects_to_query = [
        # ("bbp", "ionchannel"),  # DONE
        ("bbp", "lnmce"),  # DONE
        ("public", "sscx"),  # DONE
        ("public", "thalamus"),  # DONE
        # ("bbp-external", "modelling-mouse-hippocampus"),  # DONE
        ("public", "hippocampus"),
        # DONE - except for 2 that don't seem to have a matching TraceWebDataContainer -
        # but also it's got this weird .smr extension
    ]

    for org, project in projects_to_query:
        forge = allocate_with_default_views(org, project, deployment=deployment, token=auth_token)

        assign_trace_has_part_field(forge, really_update=really_update)
        # set_is_part_of_to_right_type(forge, really_update=really_update)
