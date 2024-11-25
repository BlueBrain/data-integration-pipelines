import getpass
import json
import re
from typing import List, Tuple

from src.helpers import Deployment, _delta_get

ORG_OF_INTEREST = ["bbp", "bbp-external", "public"]


# def get_obp_projects(token: str, is_prod: bool = True) -> List[Tuple[str, str]]:
#     deployment = Deployment.PRODUCTION if is_prod else Deployment.STAGING
#     return _get_obp_projects(token, deployment)
#
#
# def get_all_projects(token: str, is_prod: bool = True, organisation_of_interest=ORG_OF_INTEREST) -> List[Tuple[str, str]]:
#     deployment = Deployment.PRODUCTION if is_prod else Deployment.STAGING
#     return _get_all_projects(token, deployment, organisation_of_interest)


def _get_obp_projects(token: str, deployment: Deployment = Deployment.PRODUCTION) -> List[Tuple[str, str]]:
    res = _delta_get(
        "/search/suites/sbo",
        deployment=deployment, token=token
    )

    res.raise_for_status()

    res = res.json()["projects"]

    return [org_project.split("/") for org_project in res]


def _get_all_projects(
        token: str, deployment: Deployment = Deployment.PRODUCTION, organisation_of_interest=ORG_OF_INTEREST
) -> List[Tuple[str, str]]:

    res = _delta_get(
        "/projects?size=1000&deprecated=false",
        deployment=deployment, token=token
    )

    res.raise_for_status()

    res = res .json()["_results"]

    def get_org_project(string) -> Tuple[str, str]:
        m = re.match(rf'{deployment.value}/projects/(.*)/(.*)', string)
        return m.group(1), m.group(2)

    res_formatted = [get_org_project(project_entry["@id"]) for project_entry in res]

    if not organisation_of_interest:
        return res_formatted

    return [
        (org, project) for org, project in res_formatted
        if org in organisation_of_interest
    ]


if __name__ == "__main__":
    deployment = Deployment.PRODUCTION
    token = getpass.getpass()
    res = _get_obp_projects(token=token, deployment=deployment)
    print(json.dumps(res, indent=4))
