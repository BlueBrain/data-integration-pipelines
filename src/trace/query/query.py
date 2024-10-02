"""
Helper file for querying
"""
from typing import Optional, List, Union, Dict

from kgforge.core import KnowledgeGraphForge, Resource

from src.logger import logger
from src.trace.types_and_schemas import EXPERIMENTAL_TRACE_TYPE


def batch(iterable, n=1):
    l = len(iterable)
    for ndx in range(0, l, n):
        yield iterable[ndx:min(ndx + n, l)]


def query_traces(
        forge: KnowledgeGraphForge,
        limit: Optional[int] = None,
        extra_q: Optional[str] = None,
        debug: Optional[bool] = False,
        type_: Optional[str] = EXPERIMENTAL_TRACE_TYPE,
        other_fields: Optional[str] = None,
        raise_if_empty: Optional[bool] = False
) -> List[Union[str, Dict]]:

    batch_size = 10000 if limit is None else min(limit, 10000)
    offset = 0
    last_count = batch_size if limit is None else 0

    trace_ids = []

    while (last_count == batch_size and limit is None) or (limit is not None and last_count < limit):
        size = min(batch_size, limit - last_count) if limit else batch_size

        q = """
           SELECT ?id %s WHERE {
               ?id a <%s> .
               ?id _deprecated false .
               %s
           }
            ORDER BY ASC(?id)
            LIMIT %s OFFSET %s 
       """ % (other_fields or '', type_, extra_q or '', size, offset)

        traces = forge.sparql(q, debug=debug)

        ret_val = [t.id for t in traces] if other_fields is None else forge.as_json(traces)
        trace_ids.extend(ret_val)

        last_count = len(traces)
        offset = len(trace_ids)

    if raise_if_empty and len(trace_ids) == 0:
        org, project = forge._store.bucket.split("/")[-2:]
        raise Exception(f"No traces found in {org}/{project}")

    return trace_ids


def query_trace_web_data_container(forge: KnowledgeGraphForge, limit: Optional[int] = None) -> List[Resource]:
    batch_size = 10000 if limit is None else min(limit, 10000)
    offset = 0
    last_count = batch_size if limit is None else 0

    trace_web_data_container_ids = []

    while (last_count == batch_size and limit is None) or (limit is not None and last_count < limit):
        size = min(batch_size, limit - last_count) if limit else batch_size

        q = """
           SELECT ?id ?isPartOf ?schema WHERE {
               ?id a TraceWebDataContainer .
               ?id _deprecated false .
               ?id _constrainedBy ?schema . 
               OPTIONAL { ?id isPartOf ?isPartOf }
           }
            ORDER BY ASC(?id)
           LIMIT %s OFFSET %s
       """ % (size, offset)

        trace_web_data_container = forge.sparql(q)
        trace_web_data_container_ids.extend(trace_web_data_container)
        last_count = len(trace_web_data_container)
        offset = len(trace_web_data_container_ids)

    without = [
        el.id for i, el in enumerate(trace_web_data_container_ids)
        if "isPartOf" not in el.__dict__
    ]

    if len(without) > 0:
        logger.warning(f"{without} doesn't have an isPartOf")

    with_ = [el for el in trace_web_data_container_ids if "isPartOf" in el.__dict__]

    return with_
