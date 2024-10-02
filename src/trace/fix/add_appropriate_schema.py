"""
Queries for ExperimentalTrace that are not constrained by the right schema.
Attempts to update schema to the right value.
Queries for TraceWebDataContainer that have their isPartOf as one of the ExperimentalTrace found in the bucket, and are not constrained by the right schema.
Attempts to update schema to the right value.
"""
from typing import List

from kgforge.core import Resource, KnowledgeGraphForge

# from src.get_projects import _get_all_projects
from src.logger import logger
from src.helpers import _as_list, allocate_with_default_views, Deployment, authenticate
from src.trace.query.query import query_traces, batch, query_trace_web_data_container

from src.trace.get_command_line_args import trace_command_line_args

from src.trace.types_and_schemas import TRACE_WEB_DATA_CONTAINER_SCHEMA, EXPERIMENTAL_TRACE_SCHEMA


def add_schema_to_traces(forge: KnowledgeGraphForge, schema_id=EXPERIMENTAL_TRACE_SCHEMA) -> None:
    """
    Queries all ExperimentalTrace-s in a bucket (find ids then retrieve them)
    that are not constrained by schema_id, and adds that schema to it

    :param forge:
    :type forge:
    :param schema_id:
    :type schema_id:
    :return:
    :rtype:
    """
    org, project = forge._store.bucket.split("/")[-2:]

    trace_ids_not_constrained = query_traces(
        forge,
        extra_q="FILTER NOT EXISTS {?id _constrainedBy <%s>} . \n" % schema_id
    )

    logger.info(
        f"Found {len(trace_ids_not_constrained)} ExperimentalTrace in {org}/{project} "
        f"that are not constrained by {EXPERIMENTAL_TRACE_SCHEMA}"
    )

    for batch_i in batch(list(trace_ids_not_constrained), n=500):

        traces_list = _as_list(forge.retrieve(batch_i))
        forge._store.update_schema(traces_list, schema_id=schema_id)


def add_schema_to_trace_web_data_container(
        forge: KnowledgeGraphForge, schema_id=TRACE_WEB_DATA_CONTAINER_SCHEMA
) -> None:
    """

    Queries all TraceWebDataContainer-s in a bucket (find id-s and schema_id-s only),
    Queries all ExperimentalTrace in a bucket (find id-s),
    filter to keep only the TraceWebDataContainer not constrained by schema_id and whose isPartOf
    is one of the ExperimentalTrace-s found. Finally, adds appropriate schema to TraceWebDataContainer-s

    :param forge:
    :type forge:
    :param schema_id:
    :type schema_id:
    :return:
    :rtype:
    """

    org, project = forge._store.bucket.split("/")[-2:]

    trace_web_data_container_ids: List[Resource] = query_trace_web_data_container(forge)

    logger.info(f"Found {len(trace_web_data_container_ids)} TraceWebDataContainer in {org}/{project}")

    trace_ids = query_traces(forge_instance)
    logger.info(f"Found {len(trace_ids)} ExperimentalTrace in {org}/{project}")

    filtered_trace_web_data_container_ids: List[str] = [
        t.get_identifier() for t in trace_web_data_container_ids
        if t.isPartOf in trace_ids and t.schema != schema_id
    ]

    logger.info(
        f"Found {len(filtered_trace_web_data_container_ids)} TraceWebDataContainer in {org}/{project} "
        f"with isPartOf an ExperimentalTrace and not constrained by {TRACE_WEB_DATA_CONTAINER_SCHEMA}"
    )

    for batch_i in batch(list(filtered_trace_web_data_container_ids), n=500):

        trace_web_data_containers: List[Resource] = _as_list(forge.retrieve(batch_i))
        forge._store.update_schema(trace_web_data_containers, schema_id=schema_id)

        # for el in trace_web_data_containers:
        #     el.isPartOf = {
        #         "id": el.isPartOf.get_identifier(),
        #         "type": "ExperimentalTrace"
        #     }
        #
        # forge.update(trace_web_data_containers, schema_id=schema_id)


if __name__ == "__main__":

    parser = trace_command_line_args()

    received_args, leftovers = parser.parse_known_args()

    token = authenticate(username=received_args.username, password=received_args.password)

    deployment = Deployment[received_args.deployment]

    # projects_to_query = _get_all_projects(token=token, deployment=deployment)

    projects_to_query = [
        # ("bbp", "ionchannel"),  # 58809, 749/58809 Right schema, 58809/58809 TraceWebDataContainer Relevant, 0/58809 TraceWebDataContainer Right Schema

        ("bbp", "lnmce"),  # 1732 Traces, 926/1579 Right Schema, 1732/1732 TraceWebDataContainer Relevant, 1732 TraceWebDataContainer Right Schema
        #  806 Failures on Trace Schema

        # ("public", "sscx"),  # 404 Traces, 404 Right Schema, 404/8721 TraceWebDataContainer Relevant, 404 TraceWebDataContainer Right Schema

        # ("public", "thalamus"),  # 57 Traces, 56 Right Schema, 57/108 TraceWebDataContainer Relevant, 57 TraceWebDataContainer Right Schema
        #       Failure for : 'https://bbp.epfl.ch/neurosciencegraph/data/traces/45962a20-9f6b-4bca-9b4b-abdac1dbb3d5'
        #       Reason: One stimulus type is not in the ontology <http://bbp.epfl.ch/neurosciencegraph/ontologies/stimulustypes/H10S8>

        # ("bbp-external", "modelling-mouse-hippocampus"),  # 21 Traces, 21 Traces Right Schema 21/30 TraceWebDataContainer Relevant, 21 TraceWebDataContainer Right Schema

        # ("public", "hippocampus"),  # 166 Traces, 164 Right schema, 164/194 TraceWebDataContainer Relevant, 164 TraceWebDataContainer Right schema -
        #       ISSUE WITH THE 2 encountered in trace_web_data_container script, they don't have a matching TraceWebDataContainer =

        # 165 have curated
    ]

    for org, project in projects_to_query:
        forge_instance = allocate_with_default_views(
            org, project, deployment=deployment, token=token
        )

        add_schema_to_traces(forge_instance)
        add_schema_to_trace_web_data_container(forge_instance)
