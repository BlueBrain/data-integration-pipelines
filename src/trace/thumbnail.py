"""
Checks a thumbnail can be generated for all ExperimentalTrace-s in a bucket.
Saves all failures into a .json file.
"""
import io
import json
import os
from typing import Any, Union, List, Optional, Dict

import h5py
import numpy as np
import requests
from kgforge.core import KnowledgeGraphForge

# thumbnail-generation-api
from api.utils.trace_img import get_conversion, select_element, select_protocol, select_response, get_unit, get_rate
from api.models.enums import MetaType

from src.helpers import allocate_with_default_views, authenticate_from_parser_arguments
from src.logger import logger
from src.trace.get_command_line_args import trace_command_line_args
from src.trace.query.query import query_traces


def mimic_thumbnail_api_logic(h5_handle: h5py.File) -> np.array:
    """
    Source: https://github.com/BlueBrain/thumbnail-generation-api/blob/main/api/services/trace_img.py#L75
    :param h5_handle:
    :type h5_handle:
    :return:
    :rtype:
    """
    h5_handle = h5_handle["data_organization"]
    h5_handle = h5_handle[select_element(list(h5_handle.keys()), n=0)]
    h5_handle = h5_handle[select_protocol(list(h5_handle.keys()))]
    h5_handle = h5_handle[select_element(list(h5_handle.keys()), n=0, meta=MetaType.REPETITION)]
    h5_handle = h5_handle[select_element(list(h5_handle.keys()), n=-3, meta=MetaType.SWEEP)]
    h5_handle = h5_handle[select_response(list(h5_handle.keys()))]

    _ = get_unit(h5_handle)
    _ = get_rate(h5_handle)
    conversion = get_conversion(h5_handle)

    data = np.array(h5_handle["data"][:]) * conversion

    return data


def data_from_content_url(content_url: str, token: str) -> Optional[np.array]:
    response = requests.get(content_url, headers={"authorization": f"Bearer {token}"}, timeout=15)

    response.raise_for_status()

    file_content = io.BytesIO(response.content)

    h5_handle = h5py.File(file_content, "r")

    res = mimic_thumbnail_api_logic(h5_handle)

    h5_handle.close()

    return res


def check_all_traces_in_bucket_can_be_thumbnail_ed(forge: KnowledgeGraphForge) -> Dict:

    token = forge._store.token

    extra_q = """
        ?id distribution ?d .
        ?d name ?dist_name .
        ?d contentUrl ?contentUrl .
        FILTER(contains(?dist_name, '.nwb'))
    """

    other_fields = "?contentUrl"

    res = query_traces(other_fields=other_fields, extra_q=extra_q, forge=forge, raise_if_empty=True)

    logger.info(f"Found {len(res)} ExperimentalTrace in {org}/{project} with .nwb encoding format")

    errors = {}

    for res_i in res:

        identifier = res_i['id']

        try:
            data = data_from_content_url(res_i["contentUrl"], token)
            if data is None:
                err_msg = "Data from content url returned None"
                logger.error(f"{identifier} : {err_msg}")
                errors[identifier] = err_msg

        except Exception as e:

            err_msg = str(e)
            logger.error(f"{identifier} : {err_msg}")
            errors[identifier] = err_msg

    logger.info(f"TOTAL - Success: {len(res) - len(errors)} ; Error: {len(errors)}")

    return errors


if __name__ == "__main__":

    parser = trace_command_line_args(with_bucket=True)

    received_args, leftovers = parser.parse_known_args()

    deployment, auth_token = authenticate_from_parser_arguments(received_args)

    org, project = received_args.bucket.split("/")

    forge_instance = allocate_with_default_views(org, project, deployment=deployment, token=auth_token)

    errs = check_all_traces_in_bucket_can_be_thumbnail_ed(forge_instance)

    write_directory = received_args.output_dir
    os.makedirs(write_directory, exist_ok=True)

    with open(os.path.join(write_directory, "thumbnail_generation_errors.json"), "w") as f:
        json.dump(errs, indent=4, fp=f)


