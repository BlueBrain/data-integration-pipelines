from enum import Enum
import getpass
import os
import json
from typing import Union

import numpy as np
from kgforge.core import KnowledgeGraphForge, Resource

from src.logger import logger

PROD_CONFIG_URL = "https://raw.githubusercontent.com/BlueBrain/nexus-forge/master/examples/notebooks/use-cases/prod-forge-nexus.yml"

ASSETS_DIRECTORY = os.path.join(os.getcwd(), "./assets")


class Deployment(Enum):
    PRODUCTION = "https://bbp.epfl.ch/nexus/v1"
    STAGING = "https://staging.nise.bbp.epfl.ch/nexus/v1"
    # SANDBOX = "https://sandbox.bluebrainnexus.io/v1"


def write_obj(filepath, obj):
    class NumpyTypeEncoder(json.JSONEncoder):
        def default(self, obj_):
            if isinstance(obj_, np.generic):
                return obj_.item()
            elif isinstance(obj_, np.ndarray):
                return obj_.tolist()
            return json.JSONEncoder.default(self, obj_)

    with open(filepath, "w") as f:
        content = json.dumps(obj, indent=2, cls=NumpyTypeEncoder, ensure_ascii=False)
        f.write(content)


def allocate(org, project, is_prod, token):

    endpoint = Deployment.STAGING.value if not is_prod else Deployment.PRODUCTION.value

    bucket = f"{org}/{project}"

    return KnowledgeGraphForge(
        PROD_CONFIG_URL,
        bucket=bucket,
        token=token,
        endpoint=endpoint
    )


def open_file(filename):
    e = open(filename)
    f = e.read()
    e.close()
    return f


def get_token(is_prod=True, prompt=False, token_file_path=None):
    """
    Helper to input an authentication token
    If prompt, the user is prompted to paste the token in a textbox
    If prompt is False, the user can specify a file path where the token will be located
    If prompt is False and no file path is provided, some default file path to an internal library
    file will be loaded (for development mode only)
    """
    if prompt:
        prompt = "Staging Token" if not is_prod else "Production Token"
        return getpass.getpass(prompt=prompt)

    if token_file_path is not None:
        file_path = token_file_path
    else:
        file_path = "../tokens/token_prod.txt" if is_prod else "../tokens/token_staging.txt"
        file_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), file_path)

    return open_file(file_path)


def get_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), "..", path)


class CustomEx(Exception):
    ...


def _as_list(obj):
    return obj if isinstance(obj, list) else ([obj] if obj is not None else [])


def _download_from(  # TODO better name and doc
        forge: KnowledgeGraphForge, link: Union[str, Resource], label: str,
        format_of_interest: str, download_dir: str, rename=None
) -> str:

    if isinstance(link, str):
        logger.info(f"Retrieving {label}")
        link_resource = forge.retrieve(link)

        if link_resource is None:
            err_msg = f"Failed to retrieve {label} {link}"
            # logger.error(err_msg)
            raise Exception(err_msg)
    else:
        if not isinstance(link, Resource):
            raise Exception("_download_from link should be str or Resource")
        else:
            link_resource = link

    logger.info(f"Attempting to download distribution of type {format_of_interest} "
                f"from {link_resource.get_identifier()}")

    d = next(
        (d for d in _as_list(link_resource.distribution)
         if d.encodingFormat == format_of_interest),
        None
    )
    if d is None:
        err_msg = f"Couldn't find distribution of encoding format {format_of_interest} in {label}"
        # logger.error(err_msg)
        raise Exception(err_msg)

    forge.download(d, path=download_dir, follow="contentUrl")

    filename, _ = forge._store._retrieve_filename(d.contentUrl)

    if filename is None:
        raise Exception(f"Couldn't get filename from {label}")

    if rename is not None:
        os.rename(os.path.join(download_dir, filename), os.path.join(download_dir, rename))

    return os.path.join(download_dir, (filename if rename is None else rename))