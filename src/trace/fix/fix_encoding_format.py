"""
Looks for all ExperimentalTrace-s in a bucket.
Looks for the distribution with name ending in .nwb.
If the encodingFormat is different from application/nwb, retrieve the file metadata
 and gets the encodingFormat for it.
If the wrong encoding format in the distribution is due to the file having the wrong encoding format,
 ignores.
If the wrong encoding format in the distribution doesn't match with a file encoding format that was actually correct,
 updates the distribution encoding format.

"""
from multiprocessing import Pool
from kgforge.core import Resource, KnowledgeGraphForge
from contextlib import redirect_stdout
import io


from src.logger import logger

from src.helpers import _as_list, allocate_with_default_views, authenticate_from_parser_arguments

from src.forge_extension import _retrieve_file_metadata, _exists

from src.trace.query.query import query_traces

from src.trace.arguments import trace_command_line_args


def fix_encoding_format(id_: str, forge: KnowledgeGraphForge) -> Resource:

    new_encoding_format = "application/nwb"
    resource = forge.retrieve(id_)

    dist_list = _as_list(resource.distribution)
    idx = next((idx for idx, d in enumerate(dist_list) if d.name.split(".")[-1] == "nwb"), None)

    if idx is None:
        logger.warning(f"Missing distribution with name holding .nwb for {resource.get_identifier()}, ignoring")
        return

    right_encoding_format = dist_list[idx].encodingFormat.split('/')[-1] == "nwb"

    if right_encoding_format:
        return

    id_to_ret = dist_list[idx].contentUrl
    file_metadata = _retrieve_file_metadata(file_id=id_to_ret, forge=forge, is_content_url=True)

    if file_metadata is None:
        logger.warning(f"For resource {id_} - Couldn't retrieve file by contentUrl, skipping")
        return

    original_file_encoding_format = file_metadata._mediaType

    if original_file_encoding_format != new_encoding_format:
        logger.warning(
            f"For resource {id_} - Encoding format was wrong from the file: {original_file_encoding_format}, "
            f"skipping"
        )
        return

    logger.info(
        f"For resource {id_} - Encoding format was correct in the file: {original_file_encoding_format},"
        f"fixing encoding format of in distribution"
    )

    if idx == 0 and isinstance(resource.distribution, Resource):
        resource.distribution.encodingFormat = new_encoding_format
    else:
        resource.distribution[idx].encodingFormat = new_encoding_format

    return resource


if __name__ == "__main__":
    parser = trace_command_line_args(with_really_update=True)
    received_args, leftovers = parser.parse_known_args()

    deployment, auth_token = authenticate_from_parser_arguments(received_args)

    projects_to_query = [
        ("bbp", "ionchannel"),
        # ("public", "sscx"),
        ("bbp", "lnmce"),
        # ("public", "thalamus"),
        # ("bbp-external", "modelling-mouse-hippocampus"),
        # ("public", "hippocampus"),
    ]

    for org, project in projects_to_query:

        forge_instance = allocate_with_default_views(org, project, deployment=deployment, token=auth_token)

        trace_ids = query_traces(forge_instance, raise_if_empty=True)

        logger.info(f"Found {len(trace_ids)} ExperimentalTrace in {org}/{project}")

        res = Pool().starmap(fix_encoding_format, [(t_id, forge_instance) for t_id in trace_ids])

        to_update = [i for i in res if not res._synchronized]

        logger.info(f"{len(to_update)} ExperimentalTrace-s to update. Will update: {received_args.really_update}")

        if received_args.really_update == "yes" and len(to_update) > 0:
             forge_instance.update(to_update)
