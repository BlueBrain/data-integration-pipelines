import base64
from enum import Enum
import getpass
import os
import re
import json
import requests
from typing import Union, List, Tuple

from kgforge.core.commons import files
from kgforge.specializations.stores import bluebrain_nexus
import numpy as np
from kgforge.core import KnowledgeGraphForge, Resource


from src.logger import logger

PROD_CONFIG_URL = "https://raw.githubusercontent.com/BlueBrain/nexus-forge/master/examples/notebooks/use-cases/prod-forge-nexus.yml"

ASSETS_DIRECTORY = os.path.join(os.getcwd(), "./assets")

ORG_OF_INTEREST = ["bbp", "bbp-external", "public"]

DELTA_METADATA_KEYS = [ "_constrainedBy", "_createdAt", "_createdBy", "_deprecated", "_incoming", "_outgoing",
        "_project", "_rev", "_schemaProject", "_self", "_updatedAt", "_updatedBy"
]

SEARCH_QUERY_URL = "https://bbp.epfl.ch/nexus/v1/search/query/suite/sbo"
DEFAULT_ES_VIEW = "https://bluebrain.github.io/nexus/vocabulary/defaultElasticSearchIndex"
DATASET_ES_VIEW = "https://bbp.epfl.ch/neurosciencegraph/data/views/es/dataset"
ES_SIZE_LIMIT = 2000


class Deployment(Enum):
    PRODUCTION = "https://bbp.epfl.ch/nexus/v1"
    STAGING = "https://staging.nise.bbp.epfl.ch/nexus/v1"
    AWS = "https://openbluebrain.com/api/nexus/v1"
    # SANDBOX = "https://sandbox.bluebrainnexus.io/v1"


def delta_get(relative_url, token, is_prod: bool):
    endpoint_prod = Deployment.PRODUCTION.value if is_prod else Deployment.STAGING.value

    headers = {
        "mode": "cors",
        "Content-Type": "application/json",
        "Accept": "application/ld+json, application/json",
        "Authorization": "Bearer " + token
    }

    return requests.get(f'{endpoint_prod}{relative_url}', headers=headers)


def _post_delta(body, token, url=SEARCH_QUERY_URL):
    req = requests.post(
                url,
                headers={
                    "Authorization": f"Bearer {token}",
                    "Content-Type": "application/json",
                },
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


def allocate(org, project, is_prod, token, es_view=None, sparql_view=None):

    endpoint = Deployment.STAGING.value if not is_prod else Deployment.PRODUCTION.value

    bucket = f"{org}/{project}"

    files.REQUEST_TIMEOUT = 300
    bluebrain_nexus.REQUEST_TIMEOUT = 300

    args = dict(
        configuration=PROD_CONFIG_URL,
        endpoint=endpoint,
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

    return KnowledgeGraphForge(**args)


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


def get_obp_projects(token: str, is_prod: bool = True) -> List[Tuple[str, str]]:
    res = delta_get(
        "/search/suites/sbo",
        is_prod=is_prod, token=token
    ).json()["projects"]

    return [tuple(org_project.split("/")) for org_project in res]


def get_all_projects(token: str, is_prod: bool = True, organisation_of_interest=ORG_OF_INTEREST) -> List[Tuple[str, str]]:
    res = delta_get(
        "/projects?size=1000&deprecated=false",
        is_prod=is_prod, token=token
    ).json()["_results"]

    def get_org_project(string, is_prod: bool = True) -> Tuple[str, str]:
        endpoint = Deployment.STAGING.value if not is_prod else Deployment.PRODUCTION.value 
        m = re.match(fr'{endpoint}/projects/(.*)/(.*)', string)
        return m.group(1), m.group(2)

    res_formatted = [get_org_project(project_entry["@id"], is_prod) for project_entry in res]

    if not organisation_of_interest:
        return res_formatted

    return [
        (org, project) for org, project in res_formatted
        if org in organisation_of_interest
    ]


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


def _format_boolean(bool_value: bool, sparse: bool):
    return str(bool_value) if not sparse else ("" if bool_value else str(bool_value))


def authenticate(username, password, is_service: bool = True, is_aws: bool = False):

    realm, server_url = ("SBO", "https://openbluebrain.com/auth") \
        if is_aws else ("https://bbpauth.epfl.ch/auth/", "BBP")

    res = _auth(
        username, password,
        realm=realm, server_url=server_url,
        is_service=is_service
    )

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
