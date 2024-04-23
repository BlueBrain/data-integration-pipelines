import argparse
from typing import List

from kgforge.core import KnowledgeGraphForge, Resource
import cachetools
import os
import pandas as pd

from src.helpers import allocate, get_token, _as_list, _download_from, _format_boolean, authenticate
from src.logger import logger
from src.neuron_morphology.arguments import define_arguments
from src.neuron_morphology.query_data import get_neuron_morphologies


@cachetools.cached(cache=cachetools.LRUCache(maxsize=100))
def cacheretrieve(id_, forge):
    id_expanded = forge._model.context().expand(id_)
    if id_expanded != id_:
        logger.info(f"Expanded id {id_} into {id_expanded} for retrieval")  # TODO this doesn't seem to happen ever

    if 'ncbitaxon' in id_:
        id_expanded = id_.replace('ncbitaxon:', 'http://purl.obolibrary.org/obo/NCBITaxon_')

    return forge.retrieve(id_expanded, cross_bucket=True)


def check(resources: List[Resource], forge: KnowledgeGraphForge, sparse=True):
    rows = []

    for resource in resources:
        row = {}

        r_df = forge.as_dataframe(resource)
        columns_with_id = [i for i in r_df.columns if "id" in i and i != "id"]
        columns_with_id_rm_and_label = [i for i in columns_with_id if i.replace(".id", ".label") in r_df.columns]

        for k in columns_with_id:
            id_ = r_df[k].values[0]

            logger.info(f"Retrieving link {k} for resource of id {id_}")
            link_resource = cacheretrieve(id_, forge)

            not_none = link_resource is not None
            row[f"{k} can be retrieved"] = not_none
            if not not_none:
                row[f"Value of {k} that couldn't be retrieved"] = id_

            if not not_none:
                logger.warning(f"For morphology {resource.get_identifier()}, couldn't retrieve {k} {id_}")
            else:
                if k in columns_with_id_rm_and_label:
                    k_label = k.replace(".id", ".label")
                    if "label" in link_resource.__dict__:
                        same = link_resource.label == r_df[k_label].values[0]
                    else:
                        logger.warning(
                            f"Morphology {resource.get_identifier()} has an id paired with a label,"
                            f" when the original resource doesn't have a label, see field {k_label}"
                        )
                        same = False
                    row[f"{k} label is the same"] = same

                    if not same:
                        logger.warning(f"For morphology {resource.get_identifier()}, {k_label} is inaccurate")

        row = {
            "id": resource.get_identifier(),
            "name": resource.name,
            **dict((k, _format_boolean(v, sparse)) for k, v in row.items())
        }
        rows.append(row)

    return rows


# def check(resources: List[Resource], forge: KnowledgeGraphForge, sparse=True):
#     rows = []
#
#     for resource in resources:
#         row = {"id": resource.get_identifier(), "name": resource.name}
#
#         brain_location = resource.brainLocation
#
#         for i, br in enumerate(_as_list(brain_location.brainRegion)):
#             br_resource = cacheretrieve(br.get_identifier(), forge)
#             if br_resource is None:
#                 logger.warning(f"For morphology {resource.get_identifier()}, "
#                                f"couldn't retrieve brain region {br.get_identifier()}")
#
#             row[f"Brain Region id {i} can be retrieved"] = br_resource is not None
#             row[f"Brain Region label {i} is the same"] = br_resource.label == br.label
#
#             if br_resource.label != br.label:
#                 logger.warning(f"For morphology {resource.get_identifier()}, "
#                                f"brain region label {br.label} is inaccurate")
#
#         if "layer" in brain_location.__dict__:
#             for i, l in enumerate(_as_list(brain_location.layer)):
#                 l_resource = cacheretrieve(l.get_identifier(), forge)
#
#                 if l_resource is None:
#                     logger.warning(f"For morphology {resource.get_identifier()}, "
#                                    f"couldn't retrieve layer {l.get_identifier()}")
#
#                 row[f"Layer id {i} can be retrieved"] = l_resource is not None
#                 row[f"Layer label {i} is the same"] = l_resource.label == l.label
#
#                 if l_resource.label != l.label:
#                     logger.warning(f"For morphology {resource.get_identifier()}, "
#                                    f"layer label {l.label} is inaccurate")
#
#         row = dict((k, _format_boolean(v, sparse)) for k, v in row.items())
#         rows.append(row)
#
#     return rows


if __name__ == "__main__":
    parser = define_arguments(argparse.ArgumentParser())

    received_args, leftovers = parser.parse_known_args()
    org, project = received_args.bucket.split("/")
    output_dir = received_args.output_dir
    token = authenticate(username=received_args.username, password=received_args.password)
    is_prod = True

    working_directory = os.path.join(os.getcwd(), output_dir)
    os.makedirs(working_directory, exist_ok=True)

    logger.info(f"Working directory {working_directory}")

    logger.info(f"Querying for morphologies in {org}/{project}")

    forge_bucket = allocate(org, project, is_prod=is_prod, token=token)
    forge = allocate("bbp", "atlas", is_prod=is_prod, token=token)

    resources = get_neuron_morphologies(forge=forge_bucket, curated=received_args.curated)

    logger.info(f"Found {len(resources)} morphologies in {org}/{project}")

    rows = check(resources, forge)
    df = pd.DataFrame(rows)
    df.to_csv(os.path.join(working_directory, 'check_links.csv'))
