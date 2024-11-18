"""
Query by curation: yes, no, both
and
Query by e-type presence: yes, no, both

Save list of ids in a .json file
"""
import json
import os
from typing import List

from src.logger import logger
from src.helpers import _as_list, Deployment, allocate_with_default_views, authenticate_from_parser_arguments

from src.trace.query.query import query_traces
from src.trace.arguments import trace_command_line_args


def query_by_curation_and_e_type(
        organisation: str, project: str, deployment: Deployment, token: str,
        curated: str, e_type: str
) -> List[str]:

    extra_q = ""

    if curated != "both":
        extra_q += """
          FILTER %s EXISTS {
               ?id nsg:annotation / nsg:hasBody <https://neuroshapes.org/Curated> .
          }
          """ % ("NOT" if curated == "no" else "")

    if e_type != "both":

        extra_q += """
         FILTER %s EXISTS {
               ?id nsg:annotation / nsg:hasBody / a <https://neuroshapes.org/EType> .
          }
        """ % ("NOT" if e_type == "no" else "")

    forge_instance = allocate_with_default_views(
        organisation, project, deployment=deployment, token=token
    )

    trace_ids = query_traces(forge_instance, extra_q=extra_q)

    logger.info(
        f"Found {len(trace_ids)} ExperimentalTrace in {organisation}/{project}, "
        f"Curated: {curated}, With E-Type: {e_type}")

    return trace_ids


if __name__ == "__main__":
    parser = trace_command_line_args(with_bucket=True, with_curated=True, with_e_type=True)

    received_args, leftovers = parser.parse_known_args()

    write_directory = received_args.output_dir
    os.makedirs(write_directory, exist_ok=True)

    curated_str, e_type_str = received_args.curated, received_args.e_type

    deployment, auth_token = authenticate_from_parser_arguments(received_args)

    org, proj = received_args.bucket.split("/")

    trace_ids_res = query_by_curation_and_e_type(
        organisation=org, project=proj, deployment=deployment,
        token=auth_token, curated=curated_str, e_type=e_type_str
    )

    filename = f"{org}_{proj}_curated_{curated_str}_e_type_{e_type_str}.json"

    with open(os.path.join(write_directory, filename), "w") as f:
        json.dump(trace_ids_res, f, indent=4)
