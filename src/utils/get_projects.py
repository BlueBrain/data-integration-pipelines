import re
from typing import List, Tuple
import requests

from src.helpers import Deployment
from src.logger import logger

ORG_OF_INTEREST = ["bbp", "bbp-external", "public"]


def delta_get(relative_url: str, token: str, is_prod: bool, debug: bool = False):
    endpoint_prod = Deployment.PRODUCTION.value if is_prod else Deployment.STAGING.value

    headers = {
        "mode": "cors",
        "Content-Type": "application/json",
        "Accept": "application/ld+json, application/json",
        "Authorization": "Bearer " + token
    }

    url = f'{endpoint_prod}{relative_url}'
    if debug:
        logger.info(f"Querying {url}")

    return requests.get(url, headers=headers)


def get_obp_projects(token: str, is_prod: bool = True) -> List[Tuple[str, str]]:
    res = delta_get(
        "/search/suites/sbo",
        is_prod=is_prod, token=token
    ).json()["projects"]

    return [org_project.split("/") for org_project in res]


def get_all_projects(token: str, is_prod: bool = True, organisation_of_interest=ORG_OF_INTEREST) -> List[Tuple[str, str]]:
    res = delta_get(
        "/projects?size=1000&deprecated=false",
        is_prod=is_prod, token=token
    ).json()["_results"]

    def get_org_project(string) -> Tuple[str, str]:
        m = re.match(r'https://bbp.epfl.ch/nexus/v1/projects/(.*)/(.*)', string)  # TODO for staging?
        return m.group(1), m.group(2)

    res_formatted = [get_org_project(project_entry["@id"]) for project_entry in res]

    if not organisation_of_interest:
        return res_formatted

    return [
        (org, project) for org, project in res_formatted
        if org in organisation_of_interest
    ]
