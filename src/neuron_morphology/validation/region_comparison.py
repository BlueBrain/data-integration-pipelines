import argparse
import json
import shutil
from typing import Union, List

from kgforge.core import KnowledgeGraphForge, Resource
from voxcell import RegionMap, VoxelData
from voxcell.nexus.voxelbrain import Atlas
import cachetools
import morphio
import os
import pandas as pd

from src.helpers import allocate, get_token, _as_list
from src.logger import logger
from src.neuron_morphology.arguments import define_arguments


def get_region(morph_path: str, brain_region_map: RegionMap, voxel_data: VoxelData) -> str:
    morph = morphio.Morphology(morph_path)
    soma_x, soma_y, soma_z = voxel_data.positions_to_indices(morph.soma.center)
    region_id = voxel_data.raw[soma_x, soma_y, soma_z]
    return brain_region_map.get(region_id, 'name')


def get_tree(a, forge, debug=False):
    return [
        r.id
        for r in forge.search(
            {'hasPart*': {'id': a}}, cross_bucket=True, debug=debug, search_endpoint='sparql', limit=1000
        )
    ]


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
        voxel_data: VoxelData
) -> pd.DataFrame:

    rows = []
    for n, morph in enumerate(search_results):
        row = {'name': morph.name}

        swc_path = _download_from(
            forge, link=morph, label=f"morphology {n}",
            format_of_interest='application/swc', download_dir=morphology_dir, rename=None
        )

        declared = _as_list(morph.brainLocation.brainRegion)  # could be a list
        row['declared_region'] = ",".join([i.label for i in declared])

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
            row['agreement'] = agreement

            msg = f"{morph.name} - Observed region {observed_label} and declared region(s) " \
                  f"{observed_label} are {'' if agreement else 'not '}within each other"

            log_fc = logger.info if agreement else logger.warning
            log_fc(msg)

        rows.append(row)

    return pd.DataFrame(rows)


def _download_from(
        forge: KnowledgeGraphForge, link: Union[str, Resource], label: str,
        format_of_interest: str, download_dir: str, rename=None
) -> str:

    if isinstance(link, str):
        logger.info(f"Retrieving {label}")
        link_resource = forge.retrieve(link)

        if link_resource is None:
            err_msg = f"Failed to retrieve {label} {link}"
            # logger.error(err_msg)
            raise Exception(err_msg)
    else:
        if not isinstance(link, Resource):
            raise Exception("_download_from link should be str or Resource")
        else:
            link_resource = link

    logger.info(f"Attempting to download distribution of type {format_of_interest} "
                f"from {link_resource.get_identifier()}")

    d = next(
        (d for d in _as_list(link_resource.distribution)
         if d.encodingFormat == format_of_interest),
        None
    )
    if d is None:
        err_msg = f"Couldn't find distribution of encoding format {format_of_interest} in {label}"
        # logger.error(err_msg)
        raise Exception(err_msg)

    forge.download(d, path=download_dir, follow="contentUrl")

    filename, _ = forge._store._retrieve_filename(d.contentUrl)

    if filename is None:
        raise Exception(f"Couldn't get filename from {label}")

    if rename is not None:
        os.rename(os.path.join(download_dir, filename), os.path.join(download_dir, rename))

    return os.path.join(download_dir, (filename if rename is None else rename))


def _get_atlas_dir_ready(atlas_dir, atlas_id, atlas_version):

    os.makedirs(atlas_dir, exist_ok=True)

    logger.info(f"Allocating forge session tied to bucket bbp/atlas")
    forge_atlas = allocate("bbp", "atlas", is_prod=is_prod, token=token)

    atlas_resource = forge_atlas.retrieve(atlas_id)

    if atlas_resource is None:
        logger.error(f"Failed to retrieve atlas {atlas_id}")
        exit()

    download_list = {
        "parcellation volume": (atlas_resource.parcellationVolume.id, "application/nrrd", "brain_regions.nrrd"),
        "parcellation ontology": (atlas_resource.parcellationOntology.id, "application/json", "hierarchy.json"),
        "placement hints data catalog": (atlas_resource.placementHintsDataCatalog.id, "application/json", None),
        "cell orientation field": (atlas_resource.cellOrientationField.id, "application/nrrd", None),
        "direction vector": (atlas_resource.directionVector.id, "application/nrrd", None),
        "hemisphere volume": (atlas_resource.hemisphereVolume.id, "application/nrrd", None)
    }

    download_output_paths = dict(
        (
            label,
            _download_from(
                forge_atlas, link=link_id, format_of_interest=format_of_interest,
                rename=rename, download_dir=atlas_dir, label=label
            )
        )
            for label, (link_id, format_of_interest, rename) in download_list.items()
    )

    with open(download_output_paths["placement hints data catalog"], "r") as f:
        placement_hints_data_catalog = json.loads(f.read())

    to_download_2 = dict()
    for key in ["placementHints", "voxelDistanceToRegionBottom"]:
        for i, el in enumerate(_as_list(placement_hints_data_catalog[key])):
            to_download_2[f"{key}_{i}"] = forge_atlas.from_json(el)

    for name, res in to_download_2.items():
        _download_from(
            forge_atlas, link=res.get_identifier(), format_of_interest="application/nrrd",
            rename=None, download_dir=atlas_dir, label=name
        )


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
    _get_atlas_dir_ready(atlas_dir, atlas_id, atlas_version=None)  # TODO version
    # # TODO if ran against multiple buckets, do not re-run this everytime?

    logger.info("Loading atlas")

    atlas = Atlas.open(atlas_dir)
    br_map: RegionMap = atlas.load_region_map()
    voxel_d: VoxelData = atlas.load_data('brain_regions')

    logger.info(f"Querying for morphologies in {org}/{project}")

    forge_bucket = allocate(org, project, is_prod=True, token=token)

    resources = forge_bucket.search({
        "type": "ReconstructedNeuronMorphology",
        "annotation": {
            "hasBody": {
                "label": "Curated"
            }
        }
    }, limit=query_limit)

    logger.info(f"Found {len(resources)} morphologies in {org}/{project}")

    morphologies_dir = os.path.join(working_directory, "morphologies")

    df = do(
        search_results=resources, morphology_dir=morphologies_dir, forge=forge_bucket,
        brain_region_map=br_map, voxel_data=voxel_d
    )

    shutil.rmtree(morphologies_dir)
    df.to_csv(os.path.join(working_directory, 'region_comparison.csv'))
