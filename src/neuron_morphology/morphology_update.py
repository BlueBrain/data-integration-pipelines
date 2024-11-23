import glob
import json
import logging
import shutil
import zipfile
from typing import List, Tuple

import numpy as np
import pandas as pd
import os

from kgforge.core import KnowledgeGraphForge, Resource
from voxcell import RegionMap, VoxelData

from src.arguments import default_output_dir
from src.helpers import allocate_by_deployment, Deployment, ASSETS_DIRECTORY, get_path
from src.logger import logger
from src.neuron_morphology.morphology_registration import load_excel_file, do, convert_swcs, extract_zip, ENTITY_SCHEMA, MORPHOLOGY_SCHEMA, make_catalog_resource, to_excel, \
    zip_output
from src.neuron_morphology.validation.region_comparison import get_atlas, ATLAS_TAG, get_soma_center, get_region

logger.setLevel(logging.WARNING)


def extract_zip_for_check(zip_file_path: str, dst_dir: str):

    with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
        zip_ref.extractall(dst_dir)

    filename, _ = os.path.splitext(os.path.basename(zip_file_path))
    zip_files_per_brain = os.path.join(dst_dir, filename)
    brains = list(glob.iglob(os.path.join(zip_files_per_brain, "*.zip")))

    logger.info(f"Extracting {len(brains)} archives (per mouse id)")

    for f_path in brains:
        with zipfile.ZipFile(f_path, 'r') as zip_ref:

            logger.info(f"Extracting archive {f_path}")
            zip_ref.extractall(zip_files_per_brain)

            fpath_2 = f_path.replace('.zip', '')
            for file_name in os.listdir(fpath_2):
                shutil.move(os.path.join(fpath_2, file_name), zip_files_per_brain)

            os.removedirs(fpath_2)
            os.remove(f_path)

    directories = [x[0] for x in os.walk(zip_files_per_brain)]
    return zip_files_per_brain, directories


def deal(
        zip_path: str, forge_instance: KnowledgeGraphForge,
        cell_name_column: str, brain_region_map: RegionMap, voxel_data: VoxelData,
        processed_metadata_file_path: str, log_file_path: str, enable_changes: bool,
        processed_zip_path: str
) -> List[Resource]:

    dst_root_folder, dst_folders = extract_zip(zip_file_path=zip_path, dst_dir=dst_folder, re_extract=False)

    name_to_file = convert_swcs(dst_folders, re_convert=False)

    metadata = load_excel_file(dst_root_folder)

    swc_list = set(name_to_file.keys())
    metadata_list = set(metadata[cell_name_column])
    assert swc_list == metadata_list, f'Incomplete correspondence between data (swc) and metadata (rows in excel),' \
                                      f' {swc_list.symmetric_difference(metadata_list)}'

    nrows, incomplete, not_done = do(
        metadata, name_to_file, forge_instance,
        cell_name_column=cell_name_column, brain_region_map=brain_region_map, voxel_data=voxel_data
    )

    with open(log_file_path, 'w') as f:
        json.dump({
            'not registered': not_done,
            'incomplete': incomplete
        }, f, indent=2)

    logger.info(f'Written log of incomplete data and not registered morphologies in {log_file_path}')

    df = pd.DataFrame(nrows)
    df = df.reindex(columns=columns_ordered)  # to always have them in the same order, for readability

    to_excel(processed_metadata_file_path, df)

    resources = forge_instance.from_dataframe(df, na=np.nan, nesting=".")

    for resource in resources:

        schema = ENTITY_SCHEMA if "brainRegion" not in resource.brainLocation.__dict__ else MORPHOLOGY_SCHEMA

        resource.distribution = [forge_instance.attach(x, content_type=f"application/{x.split('.')[-1]}") for x in resource.distribution]

        sr = forge_instance.search({'type': 'NeuronMorphology', 'name': resource.name})

        if len(sr) != 1:
            update = False
        else:
            update = True
            old = sr[0]
            resource.id = old.get_identifier()
            resource._store_metadata = old._store_metadata

        if enable_changes:
            if update:
                forge.update(resource, schema_id=schema)
            else:
                forge.register(resource, schema_id=schema)

        resources.append(resource)

    zip_output(working_directory, dst_root_folder, processed_metadata_file_path, log_file_path, zip_path=processed_zip_path)

    return resources


if __name__ == "__main__":

    enabled = False

    working_directory = "/Users/mouffok/Desktop/4th_delivery_SEU_11182024"
    f1 = os.path.join(working_directory, "2020&2021.zip")
    f2 = os.path.join(working_directory, "2024.zip")

    with open(os.path.join(ASSETS_DIRECTORY, "ordered_columns.json"), "r") as f:
        columns_ordered = json.load(f)

    # dst_folder = default_output_dir()
    dst_folder = get_path("output/seuing")

    auth_token = ""
    forge = allocate_by_deployment("bbp-external", "seu", deployment=Deployment.PRODUCTION, token=auth_token)

    # 1. Initial data catalog

    datacatalog = make_catalog_resource(
        name="4th delivery of SEU neuronal morphologies",
        description="Holds revised versions of 400 morphologies shared in 2020/2021 and 400 morphologies shared in 2024",
        zip_file_path=[f1, f2], forge=forge
    )

    if enabled:
        forge.register(datacatalog)
        logger.info(f"Your datacatalog has the id: {datacatalog.get_identifier()}")
    # 1. Initial data catalog

    # 2. Morphologies
    br_map, voxel_d, _ = get_atlas(working_dir=working_directory, deployment=Deployment.PRODUCTION, token=auth_token, tag=ATLAS_TAG)

    logger.info("2020")

    processed_zip_path_2020_2021 = os.path.join(working_directory, 'processed_zip_2020_2021')

    resources_2020_2021 = deal(
        f1, forge_instance=forge, cell_name_column='New Cell Name (Cell ID)',
        brain_region_map=br_map, voxel_data=voxel_d,
        processed_metadata_file_path=os.path.join(working_directory, 'processed_metadata_2020_2021.xlsx'),
        log_file_path=os.path.join(working_directory, 'log_2020_2021.json'),
        enable_changes=enabled,
        processed_zip_path=processed_zip_path_2020_2021
    )

    logger.info("2024")

    processed_zip_path_2024 = os.path.join(working_directory, 'processed_zip_2024')

    resources_2024 = deal(
        f2, forge_instance=forge, cell_name_column='Cell Name (Cell ID)',
        brain_region_map=br_map, voxel_data=voxel_d,
        processed_metadata_file_path=os.path.join(working_directory, 'processed_metadata_2024.xlsx'),
        log_file_path=os.path.join(working_directory, 'log_2024.json'),
        enable_changes=enabled,
        processed_zip_path=processed_zip_path_2024
    )

    # TODO make curation also a function of the presence of the brainRegion field & dendrite attached to axon

    # 2. Morphologies

    # 3. Processed data catalog

    datacatalog.description = 'Processed metadata for the revised version of 400 morphologies shared in 2020/2021 and 400 morphologies shared in 2024'
    datacatalog.distribution = [
        forge.attach(zip_path, content_type='application/zip')
        for zip_path in [processed_zip_path_2020_2021, processed_zip_path_2024]
    ]  # this can fail for large datasets

    datacatalog.hasPart = [
        {"@id": res.get_identifier(), "@type": "ReconstructedNeuronMorphology"}
        for res in resources_2020_2021 + resources_2024
    ]

    if enabled:
        forge.update(datacatalog, schema_id="https://bbp.epfl.ch/shapes/dash/datacatalog")

    # 3. Processed data catalog
