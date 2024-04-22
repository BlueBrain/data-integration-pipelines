import argparse
import json
import shutil

from kgforge.core import KnowledgeGraphForge, Resource
from voxcell import RegionMap, VoxelData
from voxcell.nexus.voxelbrain import Atlas
import cachetools
import morphio
import os
import pandas as pd

from src.get_atlas import _get_atlas_dir_ready
from src.helpers import allocate, get_token, _as_list, _download_from, _format_boolean
from src.logger import logger
from src.neuron_morphology.arguments import define_arguments
from src.neuron_morphology.query_data import get_neuron_morphologies


def get_region(morph_path: str, brain_region_map: RegionMap, voxel_data: VoxelData) -> str:
    morph = morphio.Morphology(morph_path)
    soma_x, soma_y, soma_z = voxel_data.positions_to_indices(morph.soma.center)
    region_id = voxel_data.raw[soma_x, soma_y, soma_z]
    return brain_region_map.get(region_id, 'name')


# TODO stick to neurosciencegraph/datamodels
def is_indirectly_in(a, b, forge):
    return b in [r.id for r in forge.search(
        {'hasPart*': {'id': a}}, cross_bucket=True, debug=False, search_endpoint='sparql', limit=1000
    )]


@cachetools.cached(cache=cachetools.LRUCache(maxsize=100))
def cacheresolve(text, forge, scope='ontology', strategy='EXACT_MATCH'):
    return forge.resolve(text=text, scope=scope, strategy=strategy)


def do(
        search_results: str,
        morphology_dir: str,
        forge: KnowledgeGraphForge,
        brain_region_map: RegionMap,
        voxel_data: VoxelData,
        sparse: bool = True
) -> pd.DataFrame:

    rows = []
    for n, morph in enumerate(search_results):
        row = {'name': morph.name}

        swc_path = _download_from(
            forge, link=morph, label=f"morphology {n}",
            format_of_interest='application/swc', download_dir=morphology_dir, rename=None
        )

        declared = _as_list(morph.brainLocation.brainRegion)  # could be a list
        declared_label = ",".join([i.label for i in declared])
        row['declared_region'] = declared_label

        try:
            observed_label = get_region(swc_path, brain_region_map, voxel_data)
        except Exception as exc:
            logger.error(f"Exception raised when retrieving brain region where soma is "
                         f"located for {morph.name}: {str(exc)}")

            observed_label = None

        row['observed_region'] = observed_label

        if observed_label is None:
            logger.error(
                f"Couldn't figure out the brain region where {morph.name}'s soma is located "
                f"inside the parcellation volume"
            )
        else:
            observed_id = cacheresolve(observed_label, forge).id
            in_in_each_other = lambda a, b: is_indirectly_in(a, b, forge) or is_indirectly_in(b, a, forge)
            agreement = any(in_in_each_other(i.id, observed_id) for i in declared)
            row['agreement'] = _format_boolean(agreement, sparse)

            msg = f"{morph.name} - Observed region {observed_label} and declared region(s) " \
                  f"{declared_label} are {'' if agreement else 'not '}within each other"

            log_fc = logger.info if agreement else logger.warning
            log_fc(msg)

        rows.append(row)

    return pd.DataFrame(rows)


if __name__ == "__main__":

    parser = define_arguments(argparse.ArgumentParser())

    received_args, leftovers = parser.parse_known_args()
    org, project = received_args.bucket.split("/")
    output_dir = received_args.output_dir
    token = received_args.token
    is_prod = True
    query_limit = 10000

    working_directory = os.path.join(os.getcwd(), output_dir)
    os.makedirs(working_directory, exist_ok=True)

    logger.info(f"Working directory {working_directory}")

    logger.info("Downloading atlas")

    # atlas_dir = os.path.join(os.getcwd(), "output/atlas")
    atlas_dir = os.path.join(working_directory, "atlas")
    atlas_id = "https://bbp.epfl.ch/neurosciencegraph/data/4906ab85-694f-469d-962f-c0174e901885"
    _get_atlas_dir_ready(
        atlas_dir=atlas_dir, atlas_id=atlas_id,
        is_prod=is_prod, token=token,
        atlas_version=None  # TODO version
    )

    #  TODO if ran against multiple buckets, do not re-run this everytime?
    #   different job and then propagate artifacts

    logger.info("Loading atlas")

    atlas = Atlas.open(atlas_dir)
    br_map: RegionMap = atlas.load_region_map()
    voxel_d: VoxelData = atlas.load_data('brain_regions')

    logger.info(f"Querying for morphologies in {org}/{project}")

    forge_bucket = allocate(org, project, is_prod=True, token=token)

    resources = get_neuron_morphologies(forge=forge_bucket, curated=received_args.curated, limit=query_limit)

    logger.info(f"Found {len(resources)} morphologies in {org}/{project}")

    morphologies_dir = os.path.join(working_directory, "morphologies")

    df = do(
        search_results=resources, morphology_dir=morphologies_dir, forge=forge_bucket,
        brain_region_map=br_map, voxel_data=voxel_d
    )

    df.sort_values(by=['agreement'], inplace=True)

    shutil.rmtree(morphologies_dir)
    shutil.rmtree(atlas_dir)

    df.to_csv(os.path.join(working_directory, 'region_comparison.csv'))
