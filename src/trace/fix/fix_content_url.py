"""
Queries for all ExperimentalTrace-s that have a distribution with a content url not starting in {endpoint}/files/{org}/{project}
(can be used to download the file/retrieve its metadata as a GET endpoint)
If it is a file id instead of its self, attempts to build a self out of it and to make a GET call from it.
If it is successful, the value that was built is used to fix the faulty value.
"""
from multiprocessing import Pool
from typing import Optional, List, Tuple, Dict, Union, Any
from kgforge.core import Resource, KnowledgeGraphForge

from src.logger import logger

from src.helpers import _as_list, authenticate, Deployment, allocate_with_default_views
# from src.get_projects import _get_all_projects

from src.forge_extension import _retrieve_file_metadata

from src.trace.query.query import query_traces
from src.trace.get_command_line_args import trace_command_line_args


def make_get_file_endpoint(content_url: Any, resource: Resource, forge: KnowledgeGraphForge) -> Optional[str]:
    forge_store = forge._store

    if isinstance(content_url, Resource):  # is a resource when the file @id is provided
        logger.warning(
            f"{resource.get_identifier()}'s distribution/contentUrl must be updated:"
            f" current value is {content_url.get_identifier()}"
        )

        return forge_store.service.add_resource_id_to_endpoint(
            endpoint=forge_store.service.url_files, resource_id=content_url.get_identifier()
        )

    if isinstance(content_url, str):
        if forge_store.service.url_files in content_url:
            logger.info(f'Nothing wrong with {resource.get_identifier()}')
            return None

        ret_attempt = _retrieve_file_metadata(file_id=content_url, forge=forge)

        if not isinstance(ret_attempt, Resource):
            raise ValueError(
                f"Err with resource.distribution.contentUrl of {resource.get_identifier()}: "
                f"value '{content_url}' could not be retrieved"
            )

        # TODO could it be that it was an id and manifested as a str still?
        logger.warning(f"{resource.get_identifier()}'s must be updated: current value is {content_url}")
        return forge_store.service.add_resource_id_to_endpoint(
            endpoint=forge_store.service.url_files, resource_id=content_url
        )

    raise ValueError(
        f"Err with resource.distribution.contentUrl of {resource.get_identifier()}: "
        f"type = {type(content_url)}"
    )


def fix_content_url(id_: str, forge: KnowledgeGraphForge) -> None:

    resource = forge.retrieve(id_)

    if isinstance(resource.distribution, Resource):
        v = make_get_file_endpoint(resource.distribution.contentUrl, resource, forge)
        if v is not None:  # should be updated
            resource.distribution.contentUrl = v

    elif isinstance(resource.distribution, list):
        for i, d in enumerate(resource.distribution):
            v = make_get_file_endpoint(d.contentUrl, resource, forge)
            if v is not None:
                resource.distribution[i].contentUrl = v

    if not resource._synchronized:
        forge.update(resource)


if __name__ == "__main__":

    parser = trace_command_line_args()
    received_args, leftovers = parser.parse_known_args()

    token = authenticate(username=received_args.username, password=received_args.password)
    deployment = Deployment[received_args.deployment]

    # projects_to_query = _get_all_projects(token, deployment)

    projects_to_query = [
        ("bbp", "ionchannel"),
        # ("public", "sscx"),
        # ("bbp", "lnmce"),
        # ("public", "thalamus"),
        # ("bbp-external", "modelling-mouse-hippocampus"),
        # ("public", "hippocampus"),
    ]

    for org, project in projects_to_query:

        forge_instance = allocate_with_default_views(org, project, deployment=deployment, token=token)

        extra_query = """
                ?id distribution/contentUrl ?contentUrl .
                FILTER (!strStarts(?contentUrl, "%s"))
            """ % forge_instance._store.service.url_files

        trace_ids = query_traces(forge=forge_instance, extra_q=extra_query, raise_if_empty=True)

        logger.info(f"Found {len(trace_ids)} ExperimentalTrace in {org}/{project}")

        res = Pool().starmap(fix_content_url, [(t_id, forge_instance) for t_id in trace_ids])
