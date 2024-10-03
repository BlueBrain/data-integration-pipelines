"""
Queries for all Trace-s in a bucket that are not ExperimentalTrace-s
"""
from src.helpers import allocate_with_default_views, authenticate_from_parser_arguments

from src.logger import logger
from src.get_projects import _get_all_projects

from src.trace.query.query import query_traces
from src.trace.arguments import trace_command_line_args
from src.trace.types_and_schemas import TRACE_TYPE

if __name__ == "__main__":

    parser = trace_command_line_args()

    received_args, leftovers = parser.parse_known_args()

    deployment, auth_token = authenticate_from_parser_arguments(received_args)

    projects_to_query = _get_all_projects(token=auth_token, deployment=deployment)

    for org, project in projects_to_query:

        forge_instance = allocate_with_default_views(org, project, deployment=deployment, token=auth_token)

        trace_ids = query_traces(forge_instance, type_=TRACE_TYPE, extra_q="""
            FILTER NOT EXISTS {?id a ExperimentalTrace }
        """)

        if trace_ids is not None and len(trace_ids) > 0:
            logger.info(f"Found {len(trace_ids)} in {org}/{project}")
