import copy
import argparse
import shutil
from typing import List, Dict, Optional, Tuple

from kgforge.core import KnowledgeGraphForge, Resource
from voxcell import RegionMap, VoxelData
from voxcell.nexus.voxelbrain import Atlas
import cachetools
import morphio
import os
import pandas as pd
import math

from src.get_atlas import _get_atlas_dir_ready
from src.helpers import ASSETS_DIRECTORY, allocate, _as_list, _download_from, _format_boolean, authenticate
from src.logger import logger
from src.neuron_morphology.arguments import define_morphology_arguments
from src.neuron_morphology.query_data import get_neuron_morphologies

# From /gpfs/bbp.cscs.ch/data/project/proj162/Experimental_Data/Reconstructed_morphologies/Categorized/Neurons/Mouse/
BRAIN_AREAS = ["Cerebellum", "Isocortex", "Hippocampal region", "Olfactory areas",
               "Thalamus", "Claustrum", "Endopiriform nucleus", "Main olfactory bulb", "Striatum"]
REGION_ATTRIBUTE = 'name'

SEU_METADATA_FILEPATH = os.path.join(ASSETS_DIRECTORY, "Neuronal_morphologies_metadata_400_20240418_v5.xlsx")
SEU_METADATA_COLUMNS = [
    "Manually corrected soma region （1 or 0)",
    "Region from original brain", "Layer\n(1,2, 3, etc.).1",
    "Region in Allen CCFv3 atlas", "Layer\n(1,2, 3, etc.).2"
]

REGION_NAME_COLUMN = 'declared_region_name (brainLocation.brainRegion.label)'
REGION_ACRONYM_COLUMN = 'declared_region_acronym'
REGION_AREA_COLUMN = 'brain_area'
AGREEMENT_CRITERIA = "ancestor or descendant (or sibling for 'barrel field' or 'layer 2/3')"
COORD_SWC_COLUMN = 'coordinates_swc'
COORD_METADATA_COLUMN = 'coordinates_metadata (brainLocation.coordinatesInBrainAtlas)'

ATLAS_TAG = "v1.1.0"
ALLEN_ANNOT_LABEL = "Allen CCFv3 2017"
ADDITIONAL_ANNOTATION_VOLUME = {
    ALLEN_ANNOT_LABEL: os.path.join(ASSETS_DIRECTORY, "annotation_25_Allen_CCFv3_2017")
}


def get_agreement_col_name(criteria, coord_type, ref):
    return f'{criteria}: {coord_type} vs {ref}'


def get_relation_col_name(coord_type, ref):
    return f'relationship ({coord_type} w.r.t. {ref}) or first common ancestor'


def get_neigh_col_name(coord_type):
    return f"neighbour_regions_{coord_type} (within a 25um radius)"


def get_soma_center(morph_path: str) -> Optional[List]:
    morph = morphio.Morphology(morph_path)
    return [float(i) for i in morph.soma.center]


def get_value(coordinate):
    # Different morphology revisions store coordinates with or without an extra dict
    if not isinstance(coordinate, dict):
        return coordinate
    else:
        return coordinate["@value"]


def get_morphology_coordinates(morphology: Resource, forge: KnowledgeGraphForge) -> Optional[List]:
    brain_location = forge.as_json(morphology.brainLocation)

    if 'coordinatesInBrainAtlas' in brain_location:
        return [get_value(brain_location['coordinatesInBrainAtlas'][f"value{i}"]) for i in ["X", "Y", "Z"]]

    return None


def get_region(position, brain_region_map: RegionMap, voxel_data: VoxelData, region_attribute=REGION_ATTRIBUTE, with_neighbours=False) -> Tuple[str, List]:

    soma_x, soma_y, soma_z = voxel_data.positions_to_indices(position)
    region_id = int(voxel_data.raw[soma_x, soma_y, soma_z])

    if not with_neighbours:
        return brain_region_map.get(region_id, region_attribute), []

    neigh_ids = set()

    # Find first neighbour regions
    reg_indices = [soma_x, soma_y, soma_z]
    for i_idx in range(len(reg_indices)):
        neigh_indices = copy.deepcopy(reg_indices)
        for step in [-1, 0, +1]:
            neigh_indices[i_idx] = reg_indices[i_idx] + step
            neigh_id = int(voxel_data.raw[tuple(neigh_indices)])
            neigh_ids.add(neigh_id)

    if region_id not in neigh_ids:
        raise Exception(f"region_id {region_id} not found in neigh_ids: {neigh_ids}")
    neigh_ids.remove(region_id)

    return brain_region_map.get(region_id, region_attribute), \
        [brain_region_map.get(neigh_id, region_attribute) for neigh_id in neigh_ids]


def is_descendant_of_forge(a, b, forge):
    return b in [r.id for r in forge.search(
        {'hasPart*': {'id': a}}, cross_bucket=True, debug=False, search_endpoint='sparql', limit=1000
    )]


def is_descendant_of_region_map(a, b, reg_map):
    a_ancestors = reg_map.get(a, "id", with_ascendants=True)
    return b in a_ancestors


def are_siblings(a, b, forge):
    # print("\nId a:", a)
    a_res = forge.retrieve(a)
    # print("\nResource a:\n", a_res)
    # print("\nId b:", b)
    b_res = forge.retrieve(b)
    # print("\nResource b:\n", b_res)
    return a_res.isPartOf == b_res.isPartOf


@cachetools.cached(cache=cachetools.LRUCache(maxsize=100))
def cacheresolve(text, forge, scope='ontology', target='BrainRegion', strategy='EXACT_MATCH'):
    return forge.resolve(text=text, scope=scope, target=target, strategy=strategy)


def add_external_info(row, ext_info):
    if ext_info.empty:
        return

    for i_info, info in enumerate(SEU_METADATA_COLUMNS):

        if "Layer" in info:
            continue

        info_value = ext_info[info].values[0]
        if info.startswith("Region "):
            if type(info_value) != str:
                continue
            info_final = info.replace("Allen CCFv3", ALLEN_ANNOT_LABEL)
            row[info_final] = info_value
            layer_info = ext_info[SEU_METADATA_COLUMNS[i_info + 1]].values[0]
            if (type(layer_info) == float) and (math.isnan(layer_info)):
                continue
            else:
                row[info_final] = row[info_final] + str(layer_info)
        else:
            row[info] = info_value if i_info != 0 else int(info_value)


def create_brain_region_comparison(
        search_results: List[Resource],
        morphology_dir: str,
        forge: KnowledgeGraphForge,
        brain_region_map: RegionMap,
        voxel_data: VoxelData,
        ext_metadata: Optional[pd.DataFrame],
        sparse: bool = True,
        float_coordinates_check=False
) -> Tuple[List[Dict], str]:

    descend_or_ancest_forge = lambda a, b: (
            is_descendant_of_forge(a, b, forge)
            or is_descendant_of_forge(b, a, forge)
    )
    #descend_or_ancest_reg_map = lambda a, b: (is_descendant_of_region_map(a, b, brain_region_map)
    #    or is_descendant_of_region_map(b, a, brain_region_map))

    default_region = "declared"
    neigh_col_label = "neighbours"
    def get_coord_col_names(coord_label):
        col_names = [f'observed_region_{coord_label}']
        for ref in [default_region, 'original brain', ALLEN_ANNOT_LABEL]:
            col_names.append(get_agreement_col_name(AGREEMENT_CRITERIA, coord_label, ref))
            col_names.append(get_relation_col_name(coord_label, ref))
        # Neighbours
        col_names.append(get_neigh_col_name(coord_label))
        col_names.append(get_agreement_col_name(AGREEMENT_CRITERIA, f'{coord_label}_{neigh_col_label}', default_region))
        col_names.append(get_relation_col_name(f"{coord_label}_{neigh_col_label}", default_region))

        return col_names


    # This is the order with which keys will be inserted in the final dict, which should be
    column_order = [
        'morphology_id', 'morphology_name', REGION_NAME_COLUMN, REGION_AREA_COLUMN,
        REGION_ACRONYM_COLUMN, SEU_METADATA_COLUMNS[0], SEU_METADATA_COLUMNS[1], SEU_METADATA_COLUMNS[3].replace("Allen CCFv3", ALLEN_ANNOT_LABEL)]
    # swc coordinates
    column_order.extend(get_coord_col_names('swc'))
    # Coordinates comparison
    column_order.extend([COORD_SWC_COLUMN, COORD_METADATA_COLUMN, 'coordinates_equal'])
    # metadata coordinates
    column_order.extend(get_coord_col_names('metadata'))

    default_coordinates = 'swc'
    seu_regions_ref = {REGION_ACRONYM_COLUMN: default_region,
                       SEU_METADATA_COLUMNS[1]: "original brain",
                       SEU_METADATA_COLUMNS[3].replace("Allen CCFv3", ALLEN_ANNOT_LABEL): ALLEN_ANNOT_LABEL}
    #def_sort_column = get_agreement_col_name(AGREEMENT_CRITERIA, default_coordinates, default_region)
    def_sort_column = 'morphology_name'

    brain_areas_descendants = {}
    for brain_area in BRAIN_AREAS:
        brain_areas_descendants[brain_area] = brain_region_map.find(
            brain_area, REGION_ATTRIBUTE, ignore_case=False, with_descendants=True
        )

    tot_morphs = len(search_results)
    rows = []
    for n, morph in enumerate(search_results):
        morphology_name = morph.name
        logger.info("------------------------------------")
        logger.info(f"Morphology {n + 1} of {tot_morphs}: {morphology_name}")

        row = dict((k,  "") for k in column_order)
        row['morphology_id'] = morph.get_identifier()
        row['morphology_name'] = morphology_name

        swc_path = _download_from(
            forge, link=morph, label=f"morphology {n}",
            format_of_interest='application/swc', download_dir=morphology_dir, rename=None
        )

        declared = _as_list(morph.brainLocation.brainRegion)  # could be a list
        declared_id = declared[0].id

        declared_int = int(declared_id.split("/")[-1])
        declared_label = ",".join([region.label for region in declared])
        row[REGION_NAME_COLUMN] = declared_label
        row[REGION_AREA_COLUMN] = declared[0].label
        for brain_area, brain_area_descendants in brain_areas_descendants.items():
            if declared_int in brain_area_descendants:
                row['brain_area'] = brain_area
        declared_acronym = brain_region_map.get(declared_int, "acronym")
        row[REGION_ACRONYM_COLUMN] = declared_acronym

        if ext_metadata is not None:
            add_external_info(row, ext_metadata.loc[ext_metadata["Cell Name (Cell ID)"] == morphology_name])

        swc_coordinates = get_soma_center(swc_path)
        metadata_coordinates_orig = get_morphology_coordinates(morph, forge)
        metadata_coordinates = [float(coord) for coord in metadata_coordinates_orig]

        def do(is_swc_coordinates: bool, coordinates: Optional[List]):

            coord_type = 'metadata' if not is_swc_coordinates else default_coordinates
            coord_neigh_label = f"{coord_type}_{neigh_col_label}"
            neigh_agr, neigh_rel = None, None
            try:
                observed_label, neighbour_labels = get_region(coordinates, brain_region_map, voxel_data, "acronym", with_neighbours=True) \
                    if coordinates is not None \
                    else (None, None)

            except Exception as exc:
                logger.error(
                    f"Exception raised when retrieving brain region where "
                    f"{morphology_name} is using {coord_type} coordinates: '{str(exc)}'")
                observed_label, neighbour_labels = None, None

            row[f"observed_region_{coord_type}"] = observed_label
            if observed_label is None:
                logger.error(
                    f"Couldn't figure out the brain region where {morphology_name} is located"
                    f" inside the parcellation volume using {coord_type} coordinates")
                return
            observed_res = cacheresolve(observed_label, forge)
            observed_id = observed_res.id


            def check_agreement(obs_id, ref_id):
                # Match observed region with declared region
                regions_match = False
                sibling_regions = False
                if ref_id == obs_id:
                    regions_match = True
                if regions_match:
                    agr = True
                else:
                    agr = descend_or_ancest_forge(ref_id, obs_id)
                if not agr:
                    if ("barrel field" in declared_label) or ("layer 2/3" in declared_label):
                        sibling_regions = are_siblings(obs_id, ref_id, forge)
                        agr = sibling_regions

                # Add relationship
                ref_int = int(ref_id.split("/")[-1])
                obs_int = int(obs_id.split("/")[-1])
                obs_ancestors = brain_region_map.get(obs_int, "id", with_ascendants=True)

                if agr:
                    if regions_match:
                        rel = "same region"
                    elif is_descendant_of_region_map(ref_int, obs_int, brain_region_map):
                        rel = "ancestor"
                    elif is_descendant_of_region_map(obs_int, ref_int, brain_region_map):
                        rel = "descendant"
                    elif sibling_regions:
                        rel = "sibling"
                    else:
                        raise Exception("Agreement error")
                    rel_string = f'relationship: {rel}'
                else:
                    ref_ancestors = brain_region_map.get(ref_int, "id", with_ascendants=True)
                    logger.info(f"observed_ancestors: {obs_ancestors}", )
                    logger.info(f"ref_ancestors: {ref_ancestors}")

                    common_ancestors: List[str] = [anc for anc in obs_ancestors if anc in ref_ancestors]
                    if not common_ancestors:
                        raise Exception("No common ancestor!")
                    first_common_ancestor = forge.retrieve(
                        "http://api.brain-map.org/api/v2/data/Structure/" + str(common_ancestors[0])
                        ).notation
                    rel_string = f'first common ancestor: {first_common_ancestor}'

                return agr, rel_string


            for seu_label, ref in seu_regions_ref.items():
                logger.info(f"{coord_type} - {seu_label}")
                seu_region = row[seu_label]
                if not seu_region:
                    continue
                logger.info(f"seu_region: {seu_region}")
                if ((type(seu_region) is float) and math.isnan(seu_region)) or \
                        ((type(seu_region) is str) and seu_region == "unknown"):
                    continue

                agreement_column = get_agreement_col_name(AGREEMENT_CRITERIA, coord_type, ref)
                seu_res = cacheresolve(seu_region, forge)
                if not seu_res:
                    row[agreement_column] = "region not resolved"
                    continue

                seu_id = seu_res.id

                agreement, relationship = check_agreement(observed_id, seu_id)
                row[agreement_column] = agreement
                row[get_relation_col_name(coord_type, ref)] = relationship

                msg = f"{morphology_name} - Observed region '{observed_label}' in {coord_type} and {ref} region '{seu_region}'" \
                      f" are {'' if agreement else 'not '}within each other."
                log_fc = logger.info if agreement else logger.warning
                log_fc(msg)

                if ref == default_region:
                    for neigh_label in neighbour_labels:
                        neigh_res = cacheresolve(neigh_label, forge)
                        if not neigh_res:
                            continue
                        neigh_agr, neigh_rel = check_agreement(neigh_res.id, seu_id)
                        neigh_rel = f"{neigh_label} {neigh_rel}"
                        if neigh_agr:
                            # one agreement is enough
                            break

            row[get_neigh_col_name(coord_type)] = neighbour_labels
            if neighbour_labels:
                row[get_agreement_col_name(AGREEMENT_CRITERIA, coord_neigh_label, default_region)] = neigh_agr
                row[get_relation_col_name(coord_neigh_label, default_region)] = neigh_rel

        do(True, swc_coordinates)
        row[COORD_SWC_COLUMN] = swc_coordinates
        row[COORD_METADATA_COLUMN] = metadata_coordinates

        if swc_coordinates is not None and metadata_coordinates is not None:
            temp = [(round(swc_coordinates[i], 3), round(metadata_coordinates[i], 3)) for i in range(3)]
            coordinates_equal = False if metadata_coordinates is None else all(a == b for a, b in temp)
            row['coordinates_equal'] = coordinates_equal
        else:
            row['coordinates_equal'] = "Error"

        do(False, metadata_coordinates)

        if float_coordinates_check:
            if 'coordinatesInBrainAtlas' in morph.brainLocation.__dict__:
                row['float coordinates'] = _format_boolean(all(
                    isinstance(coord, float) for coord in metadata_coordinates_orig
                ), sparse)

        rows.append(row)

    return rows, def_sort_column


def get_atlas(working_dir: str, is_prod: bool, token: str, tag: str = None, add_annot: str = None) -> Tuple[RegionMap, VoxelData, Optional[VoxelData]]:
    logger.info(f"Downloading atlas at tag {tag}")

    atlas_dir = os.path.join(working_dir, "atlas")
    atlas_id = "https://bbp.epfl.ch/neurosciencegraph/data/4906ab85-694f-469d-962f-c0174e901885"
    _get_atlas_dir_ready(atlas_dir, atlas_id, is_prod, token, tag)

    logger.info("Loading atlas")
    atlas = Atlas.open(atlas_dir)
    brain_region_map: RegionMap = atlas.load_region_map()
    voxel_data: VoxelData = atlas.load_data('brain_regions')
    shutil.rmtree(atlas_dir)

    add_voxel_data: VoxelData = atlas.load_data(add_annot) if add_annot else None
    return brain_region_map, voxel_data, add_voxel_data

if __name__ == "__main__":

    parser = define_morphology_arguments(argparse.ArgumentParser())

    received_args, leftovers = parser.parse_known_args()
    org, project = received_args.bucket.split("/")
    output_dir = received_args.output_dir
    nexus_token = authenticate(username=received_args.username, password=received_args.password)
    is_prod_env = True
    query_limit = received_args.limit

    forge_bucket = allocate(org, project, is_prod=True, token=nexus_token)

    working_directory = os.path.join(os.getcwd(), output_dir)
    os.makedirs(working_directory, exist_ok=True)

    logger.info(f"Working directory {working_directory}")

    br_map, voxel_d, add_voxel_d = get_atlas(
        working_dir=working_directory,
        is_prod=is_prod_env, token=nexus_token,
        tag=ATLAS_TAG, add_annot=list(ADDITIONAL_ANNOTATION_VOLUME.values())[0]
    )

    #  TODO if ran against multiple buckets, do not re-run this everytime?
    #   different job and then propagate artifacts

    result_version = {f'BBP {ATLAS_TAG}': voxel_d}
    for version in ADDITIONAL_ANNOTATION_VOLUME:
        result_version[version] = add_voxel_d

    forge_datamodels = allocate("neurosciencegraph", "datamodels", is_prod=True, token=nexus_token)

    resources = get_neuron_morphologies(forge=forge_bucket, curated=received_args.curated, limit=query_limit)
    #resources = [
    #    forge_bucket.retrieve("https://bbp.epfl.ch/neurosciencegraph/data/neuronmorphologies/ed3bfb7b-bf43-4e92-abed-e2ca1170c654"),
    #    forge_bucket.retrieve("https://bbp.epfl.ch/data/bbp-external/seu/0f9021f0-83b2-4ff7-a11c-c7b91fd6d9be"),
    #    forge_bucket.retrieve("https://bbp.epfl.ch/data/bbp-external/seu/ed3aa595-d7eb-4fc4-a080-6894e544ad31"),
    #    forge_bucket.retrieve("https://bbp.epfl.ch/data/bbp-external/seu/e429ecc8-ed1e-4920-9846-e51c4cc14b4b"),
    #    forge_bucket.retrieve("https://bbp.epfl.ch/neurosciencegraph/data/neuronmorphologies/b08710aa-53ec-403e-8c30-51b626659e63"),
    #    forge_bucket.retrieve("https://bbp.epfl.ch/data/bbp-external/seu/2902f601-1dc0-4d7e-93da-698f4fa5c64c"),
    #    forge_bucket.retrieve("https://bbp.epfl.ch/data/bbp-external/seu/52230e08-9f86-40e4-b7ab-201c482e7445")
    #]

    morphologies_dir = os.path.join(working_directory, "morphologies")

    external_metadata = pd.read_excel(SEU_METADATA_FILEPATH, skiprows=1, na_values=' ') if org == "bbp-external" and project == "seu" else None

    for version, annotation in result_version.items():
        logger.info(f"Performing comparison in atlas {version}")
        comparison, sort_column = create_brain_region_comparison(
            search_results=resources, morphology_dir=morphologies_dir, forge=forge_datamodels,
            brain_region_map=br_map, voxel_data=annotation, ext_metadata=external_metadata,
            float_coordinates_check=False
        )

        df = pd.DataFrame(comparison)
        #df[SEU_METADATA_COLUMNS[0]] = df[SEU_METADATA_COLUMNS[0]].astype('Int64')

        logger.info("\nColumns list:\n", df.columns.tolist())

        output_filename = f"region_comparison_for_atlas_{version.replace(' ', '_')}.csv"
        #df.to_csv(os.path.join(working_directory, output_filename.replace('region_comparison_', 'region_comparison_unsorted'))
        df.sort_values(by=[sort_column, REGION_NAME_COLUMN], inplace=True)
        df.to_csv(os.path.join(working_directory, output_filename))

    shutil.rmtree(morphologies_dir)
