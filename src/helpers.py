import argparse
import base64
from enum import Enum
import getpass
import os
import json
import requests
from typing import Union, Dict, Tuple

from kgforge.core.commons import files
from kgforge.specializations.stores import bluebrain_nexus
import numpy as np
from kgforge.core import KnowledgeGraphForge, Resource


from src.logger import logger

PROD_CONFIG_URL = "https://raw.githubusercontent.com/BlueBrain/nexus-forge/master/examples/notebooks/use-cases/prod-forge-nexus.yml"

ORG_OF_INTEREST = ["bbp", "bbp-external", "public"]

DELTA_METADATA_KEYS = [
    "_constrainedBy", "_createdAt", "_createdBy", "_deprecated", "_incoming", "_outgoing",
    "_project", "_rev", "_schemaProject", "_self", "_updatedAt", "_updatedBy"
]

SEARCH_QUERY_URL = "https://bbp.epfl.ch/nexus/v1/search/query/suite/sbo"
DEFAULT_ES_VIEW = "https://bluebrain.github.io/nexus/vocabulary/defaultElasticSearchIndex"
DEFAULT_SPARQL_VIEW = "https://bluebrain.github.io/nexus/vocabulary/defaultSparqlIndex"
DATASET_ES_VIEW = "https://bbp.epfl.ch/neurosciencegraph/data/views/es/dataset"
ES_SIZE_LIMIT = 2000


class Deployment(Enum):
    PRODUCTION = "https://bbp.epfl.ch/nexus/v1"
    STAGING = "https://staging.nise.bbp.epfl.ch/nexus/v1"
    AWS = "https://openbluebrain.com/api/nexus/v1"
    # SANDBOX = "https://sandbox.bluebrainnexus.io/v1"


# def delta_get(relative_url: str, token: str, is_prod: bool, debug: bool = False):
#     deployment = Deployment.PRODUCTION if is_prod else Deployment.STAGING
#     return _delta_get(relative_url, token, deployment, debug)

def _make_header(token: str) -> Dict:
    return {
        "mode": "cors",
        "Accept": "application/ld+json, application/json",
        "Authorization": f"Bearer {token}"
    }


def _delta_get(relative_url: str, token: str, deployment: Deployment = Deployment.PRODUCTION, debug: bool = False):

    url = f'{deployment.value}{relative_url}'
    if debug:
        logger.info(f"Querying {url}")

    return requests.get(url, headers=_make_header(token))


def _post_delta(body, token, url=SEARCH_QUERY_URL):
    req = requests.post(
        url,
        headers=_make_header(token),
        data=json.dumps(body),
    )
    if not req.status_code == 200:
        error = req.reason
    else:
        error = None
    req.raise_for_status()
    return req.json(), error


def run_index_query(query_body, token):
    req_json, error = _post_delta(query_body, token)
    return req_json['hits']['hits'], error


def run_trial_request(body, endpoint, bucket, token):
    url = f"{endpoint}/trial/resources/{bucket}"
    return _post_delta(body, token, url)


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


def allocate_with_default_views(org: str, project: str, deployment: Deployment, token: str):
    return allocate_by_deployment(
        org=org, project=project, deployment=deployment, token=token,
        es_view=DEFAULT_ES_VIEW, sparql_view=DEFAULT_SPARQL_VIEW
    )


def allocate_by_deployment(
        org: str, project: str, deployment: Deployment, token: str, es_view=None, sparql_view=None
):

    bucket = f"{org}/{project}"

    files.REQUEST_TIMEOUT = 300
    bluebrain_nexus.REQUEST_TIMEOUT = 300

    args = dict(
        configuration=PROD_CONFIG_URL,
        endpoint=deployment.value,
        token=token,
        bucket=bucket,
        debug=False
    )

    search_endpoints = {}

    if es_view is not None:
        search_endpoints["elastic"] = {"endpoint": es_view}

    if sparql_view is not None:
        search_endpoints["sparql"] = {"endpoint": sparql_view}

    if len(search_endpoints) > 0:
        args["searchendpoints"] = search_endpoints

    logger.info(f"Allocating forge session tied to bucket {bucket}")
    return KnowledgeGraphForge(**args)


def open_file(filename):
    e = open(filename)
    f = e.read()
    e.close()
    return f


# def get_token(is_prod=True, prompt=False, token_file_path=None):
#     """
#     Helper to input an authentication token
#     If prompt, the user is prompted to paste the token in a textbox
#     If prompt is False, the user can specify a file path where the token will be located
#     If prompt is False and no file path is provided, some default file path to an internal library
#     file will be loaded (for development mode only)
#     """
#     if prompt:
#         prompt = "Staging Token" if not is_prod else "Production Token"
#         return getpass.getpass(prompt=prompt)
#
#     if token_file_path is not None:
#         file_path = token_file_path
#     else:
#         file_path = "../tokens/token_prod.txt" if is_prod else "../tokens/token_staging.txt"
#         file_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), file_path)
#
#     return open_file(file_path)


def get_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), "..", path)


class CustomEx(Exception):
    ...


def _as_list(obj):
    return obj if isinstance(obj, list) else ([obj] if obj is not None else [])


def _download_from(  # TODO better name and doc
        forge: KnowledgeGraphForge, link: Union[str, Resource], label: str,
        format_of_interest: str, download_dir: str, rename=None, tag=None
) -> str:

    if isinstance(link, str):
        logger.info(f"Retrieving {label}")
        link_resource = forge.retrieve(link, version=tag)

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
                f"from {link_resource.get_identifier()} at tag {tag}")

    d = next(
        (d for d in _as_list(link_resource.distribution)
         if d.encodingFormat == format_of_interest),
        None
    )
    if d is None:
        err_msg = f"Couldn't find distribution of encoding format {format_of_interest} in {label}"
        # logger.error(err_msg)
        raise Exception(err_msg)

    orig_filename, _ = forge._store._retrieve_filename(d.contentUrl)
    if orig_filename is None:
        raise Exception(f"Couldn't get filename from {label}")

    filename = orig_filename if not rename else rename
    filepath = os.path.join(download_dir, filename)
    if os.path.isfile(filepath):  # If already present, no need to download
        return filepath

    forge.download(d, path=download_dir, follow="contentUrl")
    if rename:
        os.rename(os.path.join(download_dir, orig_filename), os.path.join(download_dir, filename))
    return os.path.join(download_dir, filename)


def _format_boolean(bool_value: bool, sparse: bool):
    return str(bool_value) if not sparse else ("" if bool_value else str(bool_value))


def authenticate_from_parser_arguments(received_args) -> Tuple[Deployment, str]:
    deployment = Deployment[received_args.deployment]
    auth_token = authenticate(
        username=received_args.username,
        password=received_args.password,
        deployment=deployment,
        is_service_account=received_args.is_service_account == "yes"
    )
    return deployment, auth_token


def authenticate(username, password, is_service_account: bool, deployment: Deployment):

    is_aws = deployment == Deployment.AWS

    realm, server_url = ("SBO", "https://openbluebrain.com/auth") \
        if is_aws else ("BBP", "https://bbpauth.epfl.ch/auth")

    res = _auth(
        username, password,
        realm=realm,
        server_url=server_url,
        is_service=is_service_account
    )

    res.raise_for_status()

    return res.json()["access_token"]


def _auth(username, password, realm, server_url, is_service=True) -> requests.Response:
    def basic_auth():
        token = base64.b64encode(f"{username}:{password}".encode('utf-8')).decode("ascii")
        return f'Basic {token}'

    url = f"{server_url}/realms/{realm}/protocol/openid-connect/token"

    body = {
        'grant_type': ["password"],
        'scope': "openid",
        'client_id': "bbp-atlas-pipeline",
        'username': username,
        'password': password
    } \
        if not is_service else {
        'grant_type': "client_credentials",
        'scope': "openid"
    }

    return requests.post(
        url=url,
        headers={
            'Content-Type': "application/x-www-form-urlencoded",
            'Authorization': basic_auth()
        },
        data=body
    )

ASSETS_DIRECTORY = get_path("./assets")

