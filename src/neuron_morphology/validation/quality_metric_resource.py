import argparse
import shutil
from typing import Tuple, Dict, List, Optional

import pandas as pd
from kgforge.core import KnowledgeGraphForge, Resource
from kgforge.specializations.mappings import DictionaryMapping
from kgforge.specializations.mappers import DictionaryMapper
from voxcell import RegionMap, VoxelData

from src.logger import logger
from src.helpers import allocate, ASSETS_DIRECTORY, authenticate, _format_boolean, DEFAULT_ES_VIEW, DEFAULT_SPARQL_VIEW
from src.neuron_morphology.arguments import define_morphology_arguments
from src.neuron_morphology.query_data import get_neuron_morphologies, get_ext_path
from src.neuron_morphology.validation.quality_metric import (
    SOLO_TYPE, BATCH_TYPE, save_batch_quality_measurement_annotation_report, QUALITY_SCHEMA, BATCH_QUALITY_SCHEMA
)
from src.neuron_morphology.creation_helpers import get_contribution, get_generation
import os
import json

from src.neuron_morphology.validation.region_comparison import get_atlas, create_brain_region_comparison, SEU_METADATA_FILEPATH, ALLEN_ANNOT_LABEL, ADDITIONAL_ANNOTATION_VOLUME, \
    ATLAS_TAG
from src.neuron_morphology.validation.validator import validation_report_checks
# from src.neuron_morphology.validation.workflow_usage import run_workflow_on_path


def quality_measurement_report_to_resource(
        morphology_resources_swc_path_and_report: List[Tuple[Resource, str, Dict]],
        forge: KnowledgeGraphForge,
        contribution: Dict,
        generation: Dict,
        batch_report_name: str,
        batch_report_dir: str,
        mapping_batch_validation_report: DictionaryMapping
) -> Tuple[Resource, List[Resource], List[Resource]]:

    logger.info(f"Creating a {BATCH_TYPE} for {len(morphology_resources_swc_path_and_report)} resources and returning the existing {SOLO_TYPE} to update, or new ones to create")

    reports_as_resources = []

    for resource, swc_path, report in morphology_resources_swc_path_and_report:
        body = [
            {
                "type": [
                    "QualityMeasurement",
                    "AnnotationBody"
                ],
                "isMeasurementOf": {
                    "id": validation_report_checks[k][k_2].id_,
                    "type": "Metric",
                    "label": validation_report_checks[k][k_2].label
                },
                "value": v_2
            }
            for k, v in report.items() for k_2, v_2 in v.items()
        ]

        name = swc_path.split("/")[-1].split(".")[0]

        dict_for_res = {
            "distribution": [
                forge.attach(f"{batch_report_dir}/json/{name}.json", content_type="application/json"),
                forge.attach(f"{batch_report_dir}/tsv/{name}.tsv", content_type="application/tsv")
            ],
            "name": f"Quality Measurement Annotation of {resource.name}",
            "description": f"This resources contains quality measurement annotations of the neuron morphology {resource.name}",
            "type": [
                "Annotation",
                "QualityMeasurementAnnotation"
            ],
            "hasTarget": {
                "type": "AnnotationTarget",
                "hasSource": {
                    "id": resource.id,
                    "type": "NeuronMorphology",
                    "_rev": resource._store_metadata._rev
                }
            },
            "hasBody": body
        }

        report_resource = forge.from_json(dict_for_res)
        reports_as_resources.append(report_resource)

    batch_report = {
        "morphologies": [i[0] for i in morphology_resources_swc_path_and_report],
        "name": batch_report_name,
        "filepath": os.path.join(batch_report_dir, batch_report_name)
    }

    batch_report_resource = forge.map(batch_report, mapping_batch_validation_report, DictionaryMapper)
    batch_report_resource.contribution = contribution
    batch_report_resource.generation = generation

    to_upd, to_register = [], []
    for n, report in enumerate(reports_as_resources):
        # forge._debug = True

        search_results = forge.search({
            'type': SOLO_TYPE,
            'hasTarget': {'hasSource': {'@id': report.hasTarget.hasSource.id}}
        })

        if len(search_results) == 0:
            to_register.append(report)
        else:
            if len(search_results) == 1:
                old = search_results[0]
            else:
                times = [r._store_metadata._createdAt for r in search_results]
                imax = times.index(max(times))
                old = search_results[imax]
                forge.deprecate([i for n, i in enumerate(search_results) if n != imax])
            report._store_metadata = old._store_metadata
            report.id = old.id

            to_upd.append(report)

    logger.info(f"For {len(morphology_resources_swc_path_and_report)}, {len(to_upd)} existing {SOLO_TYPE} to update, {len(to_register)} {SOLO_TYPE} to register")

    return batch_report_resource, to_upd, to_register


def asc_has_no_nan(asc_path) -> bool:
    with open(asc_path, "r") as asc_file:
        try:
            content = asc_file.read()
        except Exception:
            return False

    return "nan" not in content


def save_batch_quality_measurement_annotation_report_on_resources(
        resources: List[Resource],
        swc_download_folder: str,
        asc_download_folder: str,
        forge: KnowledgeGraphForge,
        forge_datamodels: KnowledgeGraphForge,
        report_dir_path: str,
        report_name: str,
        individual_reports: bool,
        br_map: RegionMap,
        voxel_d: VoxelData,
        external_metadata: Optional[pd.DataFrame],
        with_br_check: bool = True,
        with_asc_check: bool = True
) -> Tuple[List[Tuple[Resource, str, Dict]], List[Tuple[Resource, str, Exception]]]:

    n_resources = len(resources)

    if with_br_check:
        brain_region_comp, sort_column = create_brain_region_comparison(
            search_results=resources, morphology_dir=swc_download_folder, forge=forge_datamodels,
            brain_region_map=br_map, voxel_data=voxel_d, float_coordinates_check=False, ext_metadata=external_metadata
        )
        brain_region_comp_dict = dict(
            (i["morphology_id"], i) for i in brain_region_comp
        )

    if with_asc_check:
        logger.info(f"Performing asc check on {n_resources} Resources:")
        resource_id_to_asc_path = dict(
            (resource.get_identifier(), get_ext_path(resource, ext_download_folder=asc_download_folder,
                forge=forge, i_res=i_res, n_resources=n_resources, ext="asc"))
            for i_res, resource in enumerate(resources)
        )

    def resource_added_content(resource: Resource) -> Dict:
        temp = {
            "morphology_name": resource.name,
            "morphology_id": resource.get_identifier(),
            "rev": resource._store_metadata._rev,
        }

        if with_asc_check:
            temp.update({"ASC has no nan": _format_boolean(asc_has_no_nan(resource_id_to_asc_path[resource.get_identifier()]), sparse=True)})

        if with_br_check:
            temp.update(brain_region_comp_dict[resource.get_identifier()])

        return temp

    added_list = [resource_added_content(resource) for resource in resources]

    swc_path_to_resource = dict(
        (get_ext_path(resource, ext_download_folder=swc_download_folder, forge=forge,
            ext="swc", i_res=i_res, n_resources=n_resources), resource)
        for i_res, resource in enumerate(resources)
    )

    swc_path_to_report, swc_path_to_error = save_batch_quality_measurement_annotation_report(
        swc_paths=list(swc_path_to_resource.keys()), report_dir_path=report_dir_path,
        morphologies=None, report_name=report_name,
        added_list=added_list, individual_reports=individual_reports
    )

    reports = [(swc_path_to_resource[swc_path], swc_path, report) for swc_path, report in swc_path_to_report.items()]
    errors = [(swc_path_to_resource[swc_path], swc_path, error) for swc_path, error in swc_path_to_error.items()]

    return reports, errors


if __name__ == "__main__":
    parser = define_morphology_arguments(argparse.ArgumentParser())

    parser.add_argument(
        "--default_annotation", help="Whether to use the default atlas annotation volume or not",
        type=str, choices=["yes", "no"], required=True
    )

    received_args, leftovers = parser.parse_known_args()
    org, project = received_args.bucket.split("/")
    output_dir = received_args.output_dir
    token = authenticate(username=received_args.username, password=received_args.password)
    is_prod = True

    # Would push into a test project in staging a subset of the quality metrics
    # Else would push them in the same bucket as the neuron morphology's, for all of them
    morphology_tag = received_args.morphology_tag if received_args.morphology_tag != "-" else None
    limit = received_args.limit
    really_update = received_args.really_update == "yes"
    push_to_staging = received_args.push_to_staging == "yes"
    constrain = True
    is_default_annotation = received_args.default_annotation == "yes"

    logger.info(f"Neuron morphology quality annotations will be created/updated: {str(really_update)}")

    working_directory = os.path.join(os.getcwd(), output_dir)

    logger.info(f"Working directory {working_directory}")
    os.makedirs(working_directory, exist_ok=True)

    forge = allocate(org, project, is_prod=is_prod, token=token)
    forge_datamodels = allocate("neurosciencegraph", "datamodels", is_prod=True, token=token)

    # with open(os.path.join(os.getcwd(), "src/neuron_morphology/axon_on_dendrite/res_to_len.json"), "r") as f:
    #     res_to_len = list(json.loads(f.read()).keys())
    #
    # resources = forge.retrieve(res_to_len)

    # resources = [
    #    forge.retrieve("https://bbp.epfl.ch/neurosciencegraph/data/neuronmorphologies/ed3bfb7b-bf43-4e92-abed-e2ca1170c654"),
    #    forge.retrieve("https://bbp.epfl.ch/data/bbp-external/seu/0f9021f0-83b2-4ff7-a11c-c7b91fd6d9be"),
    #    forge.retrieve("https://bbp.epfl.ch/data/bbp-external/seu/ed3aa595-d7eb-4fc4-a080-6894e544ad31"),
    #    forge.retrieve("https://bbp.epfl.ch/data/bbp-external/seu/e429ecc8-ed1e-4920-9846-e51c4cc14b4b"),
    #    forge.retrieve("https://bbp.epfl.ch/neurosciencegraph/data/neuronmorphologies/b08710aa-53ec-403e-8c30-51b626659e63"),
    #    forge.retrieve("https://bbp.epfl.ch/data/bbp-external/seu/2902f601-1dc0-4d7e-93da-698f4fa5c64c"),
    #    forge.retrieve("https://bbp.epfl.ch/data/bbp-external/seu/52230e08-9f86-40e4-b7ab-201c482e7445")
    # ]

    resources = get_neuron_morphologies(curated=received_args.curated, forge=forge, tag=morphology_tag, limit=limit)

    swc_download_folder = os.path.join(working_directory, "swcs")
    asc_download_folder = os.path.join(working_directory, "ascs")
    os.makedirs(swc_download_folder, exist_ok=True)
    os.makedirs(asc_download_folder, exist_ok=True)

    report_dir_path = os.path.join(working_directory, f'{org}_{project}')

    logger.info(f"Saving reports to directory {report_dir_path}")

    v_string = f"BBP {ATLAS_TAG}" if is_default_annotation else ALLEN_ANNOT_LABEL
    report_name = f"batch_report_for_atlas_{v_string.replace(' ', '_')}.tsv"
    if morphology_tag:
        report_name = report_name.replace("batch_report", f"batch_report_{morphology_tag}")

    # morphologies_to_update = []
    # issues = dict()
    # for resource in resources:
    #     try:
    #         to_update = check_swc_on_resource(resource, swc_download_folder=swc_download_folder, forge=forge)
    #         if to_update:
    #             morphologies_to_update.append(resource)
    #
    #     except CustomEx as ex:
    #         issues[resource.id] = (resource, ex)
    #
    # forge.update(morphologies_to_update, NEURON_MORPHOLOGY_SCHEMA)
    #
    # resources = [r for r in resources if r.id not in issues]

    br_map, voxel_d, add_voxel_d = get_atlas(
        working_dir=working_directory,
        is_prod=is_prod, token=token,
        tag=ATLAS_TAG, add_annot=list(ADDITIONAL_ANNOTATION_VOLUME.values())[0]
    )

    used_voxel_data = voxel_d if is_default_annotation else add_voxel_d

    external_metadata_seu = pd.read_excel(SEU_METADATA_FILEPATH, skiprows=1, na_values=' ') if org == "bbp-external" and project == "seu" else None

    reports, errors = save_batch_quality_measurement_annotation_report_on_resources(
        resources=resources,
        swc_download_folder=swc_download_folder,
        asc_download_folder=asc_download_folder,
        report_dir_path=report_dir_path,
        forge=forge,
        forge_datamodels=forge_datamodels,
        report_name=report_name,
        individual_reports=False,
        br_map=br_map,
        voxel_d=used_voxel_data,
        external_metadata=external_metadata_seu,
        with_asc_check=True,
        with_br_check=True,
    )

    # for resource in resources:
    #     path = Path(get_swc_path(resource, swc_download_folder, forge))
    #     dst_dir = Path(os.path.join(working_directory, "workflow_output"))
    #     result = run_workflow_on_path(path, dst_dir)

    shutil.rmtree(swc_download_folder)
    shutil.rmtree(asc_download_folder)

    logger.info("Turning quality measurements into QualityMeasurementAnnotation Resources")

    mapping_batch_validation_report = DictionaryMapping.load(os.path.join(ASSETS_DIRECTORY, 'BatchQualityMeasurementAnnotation.hjson'))

    generation = get_generation()

    if push_to_staging:
        forge_push = allocate(
            "dke", "kgforge", is_prod=False, token=token,
            es_view=DEFAULT_ES_VIEW,
            sparql_view=DEFAULT_SPARQL_VIEW
        )
        contribution = get_contribution(token=token, production=False)
    else:
        forge_push = forge
        contribution = get_contribution(token=token, production=is_prod)

    batch_quality_to_register, quality_to_update, quality_to_register = quality_measurement_report_to_resource(
        morphology_resources_swc_path_and_report=reports, forge=forge_push,
        contribution=contribution, generation=generation,
        batch_report_name=report_name, batch_report_dir=report_dir_path,
        mapping_batch_validation_report=mapping_batch_validation_report,
    )

    if really_update:
        logger.info("Updating data has been enabled")

        existing = forge_push.retrieve(batch_quality_to_register.get_identifier(), cross_bucket=False)

        if existing is not None:
            batch_quality_to_register._store_metadata = existing._store_metadata
            forge_push.update(batch_quality_to_register, schema_id=BATCH_QUALITY_SCHEMA if constrain else None)
        else:
            forge_push.register(batch_quality_to_register, schema_id=BATCH_QUALITY_SCHEMA if constrain else None)

        forge_push.register(quality_to_register, schema_id=QUALITY_SCHEMA if constrain else None)
        forge_push.update(quality_to_update, schema_id=QUALITY_SCHEMA if constrain else None)
    else:
        logger.info("Updating data has been disabled")

    with open(os.path.join(report_dir_path, f"batch_resource_{org}_{project}.json"), "w") as batch_file:
        json.dump(forge_push.as_json(batch_quality_to_register), batch_file, indent=4)
