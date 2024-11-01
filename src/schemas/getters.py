from collections import defaultdict

from kgforge.core import KnowledgeGraphForge

import json
import os
import xlsxwriter

from typing import Callable, Dict, Set, Tuple, List

from src.helpers import allocate_by_deployment, _delta_get, Deployment
from src.schemas.schema_validation import UNCONSTRAINED_SCHEMA


class SchemaGetter:

    def get_schema_from_type_forge(self, type_str):
        try:
            return self.forge_atlas._model.schema_id(
                self.forge_atlas._model.context().to_symbol(type_str))
        except Exception:
            return None

    def get_schema_from_type_nd(self, type_str):
        return self.type_schema.get(type_str, None)

    def __init__(self, token: str, deployment: Deployment):

        self.forge_atlas: KnowledgeGraphForge = allocate_by_deployment("bbp", "atlas", deployment=deployment, token=token)

        forge_datamodels = allocate_by_deployment("neurosciencegraph", "datamodels", deployment=deployment, token=token)

        get_schemas_q = """

        SELECT ?id ?tc WHERE {
                GRAPH ?g {
                    ?id a Schema ;
                        _deprecated false ;
                        nxv:shapes/sh:targetClass ?tc
                }
            }
        """
        res = forge_datamodels.sparql(get_schemas_q)
        self.type_schema = dict((el.tc, el.id) for el in res)


class TypeGetter:

    def __init__(self, token: str, deployment: Deployment):
        self.token = token
        self.deployment = deployment

    @staticmethod
    def _filter_types(types: Dict) -> Tuple[Dict, Set]:
        ignore_prefix = [
            'https://bluebrain.github.io', 'https://bluebrainnexus.io'
        ]
        def special_key(key): return any(key.startswith(e) for e in ignore_prefix)

        types_kept = dict((a, b) for (a, b) in types.items() if not special_key(a))
        types_filtered = set(a for (a, b) in types.items() if special_key(a))
        return types_kept, types_filtered

    def get_types_delta(self, org: str, project: str):
        relative_url = f'/resources/{org}/{project}/_/?aggregations=true&deprecated=false'
        response = _delta_get(relative_url, token=self.token, deployment=self.deployment)
        try:
            response.raise_for_status()
        except Exception as e:
            print(org, project, e)
            return None
        json_response = response.json()
        bucket_types = json_response['aggregations']['types']['buckets']
        types = dict((e["key"], (e["doc_count"], None)) for e in bucket_types)

        return self._filter_types(types)

    def get_types_sparql(self, org, project):

        forge = allocate_by_deployment(org, project, deployment=self.deployment, token=self.token)
        q = """
            SELECT  ?type (COUNT(?id) AS ?count)
            (GROUP_CONCAT(DISTINCT ?cs; SEPARATOR=",") AS ?schemas) WHERE {
                GRAPH ?g {
                    ?id a ?type ;
                    _deprecated false ;
                    _constrainedBy ?cs
                }
            } GROUP BY ?type
        """
        t = forge.as_json(forge.sparql(q))

        types = dict((el["type"], (el["count"], el["schemas"])) for el in t)
        return self._filter_types(types)

    def get_unconstrained_types(self, org, project):
        forge = allocate_by_deployment(org, project, deployment=self.deployment, token=self.token)
        q = f"""
            SELECT  ?type (COUNT(?id) AS ?count)
             WHERE {{
                GRAPH ?g {{
                    ?id a ?type ;
                    _deprecated false ;
                    _constrainedBy <{UNCONSTRAINED_SCHEMA}>;
                    _project <{forge._store.endpoint}/projects/{forge._store.bucket}> .
                }}
            }} GROUP BY ?type
        """
        t = forge.as_json(forge.sparql(q))

        types = dict((el["type"], el["count"]) for el in t)
        return self._filter_types(types)


def bucket_to_type_schema_dict(
        bucket_to_type, schema_getter: Callable[[str], str],
        dir_path: str,
        write_into_file=False,
        filename_per_bucket=None,
        filename_flattened=None
):

    t = dict(
        (bucket, dict(
            (type_i, (schema_getter(type_i), count_i, schemas_used_i))

            for (type_i, (count_i, schemas_used_i)) in type_to_count_dict.items()
        ))
        for bucket, type_to_count_dict in bucket_to_type.items()
    )

    flattened_across_buckets = flatten_across_buckets(t)

    if write_into_file and filename_per_bucket and filename_flattened:
        with open(os.path.join(dir_path, f"{filename_per_bucket}.json"), "w") as f:
            f.write(json.dumps(t, indent=4))
        with open(os.path.join(dir_path, f"{filename_flattened}.json"), "w") as f:
            f.write(json.dumps(flattened_across_buckets, indent=4))

    return t, flattened_across_buckets


def flatten_across_buckets(bucket_to_type_schema_dict):

    all_counts = defaultdict(int)
    all_schemas = defaultdict(list)
    all_schemas_from_type = dict()

    for _, mapping in bucket_to_type_schema_dict.items():
        for i, (type_, (schema_from_type, count, schemas)) in enumerate(mapping.items()):
            all_schemas_from_type[type_] = schema_from_type
            all_counts[type_] += count
            all_schemas[type_].extend(schemas.split(",") if schemas else "")

    flattened = dict(
        (
            type_,
            (
                all_schemas_from_type.get(type_, None),
                all_counts[type_],
                ", ".join(set(e for e in list_schemas if e is not None))
            )
        )
        for type_, list_schemas in all_schemas.items()
    )

    return flattened


def get_org_project_to_types(
        org_project_list: List[Tuple[str, str]],
        getter: Callable[[str, str], Tuple[Dict, Set]],
        dir_path: str,
        write_into_file=False, filename_per_bucket=None, filename_flattened=None
):
    types = dict(
        (f"{org}/{project}", getter(org, project)[0])
        for org, project in org_project_list
    )

    types_flattened = set.union(*[set(i.keys()) for i in types.values()])

    if write_into_file and filename_per_bucket and filename_flattened:
        with open(os.path.join(dir_path, f"{filename_per_bucket}.json"), "w") as f:
            f.write(json.dumps(types, indent=4))

        with open(os.path.join(dir_path, f"{filename_flattened}.json"), "w") as f:
            f.write(json.dumps(list(types_flattened), indent=4))

    return types, types_flattened


def write_into_xls(filename, data, data_flattened, dir_path):

    def do(name, mapping_):
        worksheet = workbook.add_worksheet(name)

        worksheet.write('A1', 'Type', bold)
        worksheet.write('B1', 'Schema used in resources', bold)
        worksheet.write('C1', 'Schema intended', bold)
        worksheet.write('D1', 'Count of resources', bold)

        for i, (type_, (schema_from_type, count, schemas)) in enumerate(mapping_.items()):
            worksheet.write_string(i + 1, 0, type_)
            worksheet.write_string(i + 1, 1, schemas or "")
            worksheet.write_string(i + 1, 2, schema_from_type or "")
            worksheet.write_number(i + 1, 3, count)

        worksheet.autofit()

    workbook = xlsxwriter.Workbook(os.path.join(dir_path, f'{filename}.xlsx'))
    bold = workbook.add_format({'bold': 1})

    do("All Org-Projects", data_flattened)

    for bucket, mapping in data.items():
        worksheet_name = bucket.replace("/", " ")
        if len(worksheet_name) > 31:
            worksheet_name = worksheet_name[:27] + "..."

        do(worksheet_name, mapping)

    workbook.close()


def compare_sparql_and_delta_and_get_sparql(org_project_list, type_getter: TypeGetter, dir_path):

    types_delta, types_delta_flattened = get_org_project_to_types(
        org_project_list=org_project_list, getter=type_getter.get_types_delta,
        write_into_file=False, dir_path=dir_path
    )

    types_sparql, types_sparql_flattened = get_org_project_to_types(
        org_project_list=org_project_list, getter=type_getter.get_types_sparql,
        write_into_file=False, dir_path=dir_path
    )

    # Compare getting types with delta endpoint, and sparql query
    for org, project in org_project_list:
        k = f"{org}/{project}"
        a, b = types_sparql[k], types_delta[k]
        assert len(a) == len(b)
        assert len(set(a.keys()).difference(b.keys())) == 0

        for type_ in a.keys():
            assert a[type_][0] == b[type_][0]  # counts
    #       if not a[type_][0] == b[type_][0]:
    #           print(org, project, type_, a[type_][0], b[type_][0])

    assert types_delta_flattened == types_sparql_flattened

    return types_sparql, types_sparql_flattened


def get_missing_schemas(
        schema_getter: SchemaGetter, types_sparql_flattened, dir_path, filename,
        write_into_file: bool
):

    schema_dict_flattened_forge = dict(
        (e, schema_getter.get_schema_from_type_forge(e)) for e in types_sparql_flattened
    )

    schema_dict_flattened_nd = dict(
        (e, schema_getter.get_schema_from_type_nd(e)) for e in types_sparql_flattened
    )

    # Compare both methods of retrieving a schema for a type:

    fg = set(type_ for type_, schema in schema_dict_flattened_forge.items() if schema is None)
    # types without a schema according to forge
    nd = set(type_ for type_, schema in schema_dict_flattened_nd.items() if schema is not None)
    # types with a schema according to nd
    intersection_no_schema_schema = nd.intersection(fg)
    # Schemas that exist according to neurosciencegraph/datamodels but not according to forge _model

    if intersection_no_schema_schema:
        print("Difference between both schema retrieval techniques", intersection_no_schema_schema)
    # It does have a schema that exists,
    # but because there are no entries of ModelBuildingConfig in the knowledge graph,
    # forge has not loaded the schema

    with_schema_count = len(list(e for e in schema_dict_flattened_nd.values() if e is not None))
    without_schema = list(k for k, e in schema_dict_flattened_nd.items() if e is None)
    without_schema_count = len(without_schema)

    type_count = len(schema_dict_flattened_nd.values())

    print(
        "With schema", with_schema_count, "Without schema", without_schema_count, "All", type_count
    )

    if write_into_file and filename:
        with open(os.path.join(dir_path, f"{filename}.json"), "w") as f:
            f.write(json.dumps(without_schema, indent=4))

    return without_schema
