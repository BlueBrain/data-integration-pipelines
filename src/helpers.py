from enum import Enum
import getpass
import os
import json
import numpy as np
from kgforge.core import KnowledgeGraphForge

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
