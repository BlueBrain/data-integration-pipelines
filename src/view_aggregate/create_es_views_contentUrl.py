from urllib.parse import quote_plus

import requests

from src.helpers import get_token, Deployment
from src.get_projects import get_all_projects
from src.view_aggregate.common import DeltaUtils


def create_update_es_view(endpoint: str, org: str, project: str, token: str, central_field: str):
    url = f"{endpoint}/views/{org}/{project}"
    headers = DeltaUtils.make_header(token)
    view_id = f"https://bbp.epfl.ch/neurosciencegraph/data/views/es/{central_field}"

    get_url = f"{url}/{quote_plus(view_id)}"
    response = requests.get(url=get_url, headers=headers)
    response_json = response.json()
    exists = response_json["@type"] != 'ResourceNotFound'

    url = url if not exists else f"{get_url}?rev={response_json['_rev']}"

    payload = {
        "@id": view_id,
        "@type": "ElasticSearchView",
        "includeDeprecated": True,
        "includeMetadata": True,
        "mapping": {
            "dynamic": False,
            "properties": {
                "@id": {
                    "type": "keyword"
                },
                "@type": {
                    "type": "keyword"
                },
                "_deprecated": {
                    "type": "boolean"
                },
                "distribution": {
                    "properties": {
                        "contentSize": {
                            "type": "nested"
                        },
                        central_field: {
                            "type": "keyword"
                        },
                        "digest": {
                            "properties": {
                                "value": {
                                    "type": "keyword"
                                }
                            },
                            "type": "nested"
                        },
                        "encodingFormat": {
                            "type": "keyword"
                        }
                    },
                    "type": "nested"
                }
            }
        },
        "sourceAsText": False
    }

    fc = requests.post if not exists else requests.put
    print("Updating" if exists else "Creating", f"view {view_id} in {org}/{project}")

    response = fc(url=url, headers=headers, json=payload)

    if response.status_code not in range(200, 229):
        print(f"FAILURE - {org}/{project}")
        print(response.text)
    else:
        print(f"SUCCESS - {org}/{project}")

    return response


if __name__ == "__main__":
    token = get_token()

    for org, project in get_all_projects(token):
        response = create_update_es_view(org=org, project=project, endpoint=Deployment.PRODUCTION.value, central_field="contentUrl", token=token)

    response = create_update_es_view(org="bbp", project="allresources", endpoint=Deployment.PRODUCTION.value, central_field="url", token=token)


