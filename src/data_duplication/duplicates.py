from collections import defaultdict

from src.helpers import get_token, allocate, DEFAULT_ES_VIEW, DEFAULT_SPARQL_VIEW
from src.logger import logger
from src.trace.find_valid_data import _query_traces
from src.get_projects import get_obp_projects

if __name__ == "__main__":
    token = get_token()
    is_prod = True
    curated = True

    # projects_to_query = get_obp_projects(token, is_prod)

    projects_to_query = [
        ("public", "sscx"),  # OBP
        ("bbp", "lnmce"),  # OBP
        ("public", "thalamus"),  # OBP
        ("public", "hippocampus"),  # OBP
    ]

    def make_query_extra(org, project, is_curated, extra_params):

        # is_curated = is_curated and not (org == "bbp" and project == "lnmce")
        # is_curated = False

        other_fields = "?file_name ?file_checksum" if extra_params else None

        filter_curated = """
          FILTER NOT EXISTS {
            ?id nsg:annotation / nsg:hasBody <https://neuroshapes.org/Curated> .
          }
        """ if is_curated else ""

        extra_query = filter_curated + """
            OPTIONAL {
                ?id <http://schema.org/distribution> ?dist .
                ?dist <http://schema.org/name> ?file_name .
                ?dist <https://neuroshapes.org/digest>/<http://schema.org/value> ?file_checksum
            }
        """

        return other_fields, extra_query

    traces_all = defaultdict(list)

    for org, project in projects_to_query:

        forge_instance = allocate(
            org, project, is_prod=is_prod, token=token,
            es_view=DEFAULT_ES_VIEW, sparql_view=DEFAULT_SPARQL_VIEW
        )

        other_fields, extra_query = make_query_extra(org, project, is_curated=True, extra_params=True)

        trace_ids_extra_fields = _query_traces(
            forge_instance, extra_q=extra_query, other_fields=other_fields
        )

        if trace_ids_extra_fields is not None:
            id_set = set(t["id"] for t in trace_ids_extra_fields)

            # other_fields_2, extra_query_2 = make_query_extra(org, project, is_curated=True, extra_params=False)
            # trace_ids = _query_traces(forge_instance, extra_q=extra_query_2, other_fields=other_fields_2)
            # assert len(id_set) == len(trace_ids), f"{len(id_set)}, {len(trace_ids)}"

            if len(id_set) > 0:
                logger.info(f"Found {len(id_set)} in {org}/{project}")
                if project == "hippocampus":
                    print(trace_ids_extra_fields)
                    exit()

        for t in trace_ids_extra_fields:

            if "file_name" not in t or ".nwb" in t["file_name"]:
                # Consider traces with no distrib, and if distrib, only .nwb ones

                traces_all[t["id"]].append({
                    "id": t["id"],
                    "bucket": f"{org}/{project}",
                    "checksum": t.get("file_checksum"),
                    "filename": t.get("file_name")
                })

    duplicates = []

    for trace_id, entries in traces_all.items():
        if len(entries) > 1:
            duplicates.append({
                "id": trace_id,
                "same_checksum": len(set(entry["checksum"] for entry in entries)) == 1,
                "same_filename": len(set(entry["filename"] for entry in entries)) == 1,
                "buckets": tuple(sorted([entry["bucket"] for entry in entries]))
            })

    # per_bucket_id_duplication = defaultdict(list)
    # for duplicate in duplicates:
    #     per_bucket_id_duplication[duplicate["buckets"]].append(duplicate["id"])
    #
    # print(per_bucket_id_duplication[('bbp/lnmce', 'bbp/lnmce')])

    print("Number of duplicates", len(duplicates))
    print("Same checksum", len([el for el in duplicates if el["same_checksum"]]))
    print("Same filename", len([el for el in duplicates if el["same_filename"]]))

    bucket_count = defaultdict(int)
    for duplicate in duplicates:
        bucket_count[duplicate["buckets"]] += 1

    print(bucket_count)
