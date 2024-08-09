import json
from typing import List, Dict
from urllib.parse import quote_plus

from kgforge.core import KnowledgeGraphForge, Resource
from kgforge.core.conversions.rdf import _from_jsonld_one
from kgforge.core.commons.context import Context

from src.logger import logger
from src.helpers import (DEFAULT_ES_VIEW,
                         ES_SIZE_LIMIT,
                         DELTA_METADATA_KEYS,
                         _post_delta
)


def _forge_elastic(forge, es_query, view, debug=False, limit=2000, offset=0):
    
    error = None
    try:
        resources = forge.elastic(json.dumps(es_query), view=view, debug=debug, limit=limit, offset=offset)
    except Exception as exc:
        error = f'Failed while using forge.elastic with error: {str(exc)}'
    return resources, error


def _payload_to_resource(data_context: Context, payload: dict,
                         store_metadata: dict = None, metadata_terms: List[str] = DELTA_METADATA_KEYS):
    metadata = {}
    data = {}
    for k, v in payload.items():
        if k in metadata_terms:
            metadata[k] = v
        else:
            data[k] = v
    # Complement store metadata
    if store_metadata:
        for k, v in store_metadata.items():
            if k in metadata_terms:
                metadata[k] = v
    data['@context'] = data_context.document["@context"]
    resource = _from_jsonld_one(data)
    resource.context = "https://bbp.neuroshapes.org"
    resource._store_metadata = Resource.from_json(metadata)
    return resource


def _delta_es(forge: KnowledgeGraphForge, es_query: dict, view: str, limit: int, offset: int = 0) -> List[Resource]:
    endpoint = forge._store.endpoint
    bucket = forge._store.bucket
    token = forge._store.token
    data_context = forge.get_model_context()

    url = f"{endpoint}/views/{bucket}/{quote_plus(view)}/_search"
    es_query['size'] = limit
    es_query['from'] = offset

    response_json, error = _post_delta(es_query, token, url)

    results = response_json['hits']['hits']
    
    resources = []
    for r in results:
        resource_json = r['_source']
        if '_original_source' in resource_json:
            resource = _payload_to_resource(data_context,
                                            json.loads(resource_json['_original_source']),
                                            store_metadata=resource_json)
        else:
            resource = _payload_to_resource(data_context, resource_json)
        resources.append(resource)
    return resources, error


def get_resources_by_type_es(forge: KnowledgeGraphForge,
                             type_: str,
                             limit: int = 10000,
                             view: str = DEFAULT_ES_VIEW) -> List[Resource]:

    logger.info(f"Retrieving resources of type {type_} from project")

    es_query = {
        "query": {
            "bool": {
                "must": [
                    {
                        "term": {
                            "@type": type_
                        }
                    },
                    {
                        "term": {
                            "_deprecated": False
                        }
                    }
                ]
            }
        }
    }
    resources = []
    size = ES_SIZE_LIMIT
    if limit > size:
        start = 0
        count = 0
        upper = limit - count
        while count < limit:
            size = ES_SIZE_LIMIT if ES_SIZE_LIMIT < upper else upper
            results, error = _delta_es(forge, es_query, view, limit=size, offset=start)
            # results, error = _forge_elastic(forge, es_query, view, limit=size, offset=start)
            if results:
                resources += results
                count = len(resources)
                start = count
                upper = limit - count
            else:
                logger.info(f"Failed to get more resources. Submited query:\n {json.dumps(es_query, indent=2)}"
                            f"This is the start:{start}, count: {count}, and the limit: {limit}")
                break
        # loop until limit is reached
    else:
        results, error = _delta_es(forge, es_query, view, limit=limit)
        if results:
            resources += results
        
    logger.info(f"Found {len(resources)} resources")

    return resources, error


def get_resources_by_type_search(forge: KnowledgeGraphForge,
                                 type_: str,
                                 limit: int = 10000,
                                 debug: bool = False):
  
    error = None

    try:
        results = forge.search({'type': type_, '_deprecated': False}, limit=limit, debug=debug)
    except Exception as exc:
        error = str(exc)

    logger.info(f"Found {len(results)} of type {type_}")

    return results, error
