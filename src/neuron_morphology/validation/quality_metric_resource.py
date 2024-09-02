import argparse
import shutil
from typing import Tuple, Dict, List

from kgforge.core import KnowledgeGraphForge, Resource
from kgforge.specializations.mappings import DictionaryMapping
from kgforge.specializations.mappers import DictionaryMapper
from voxcell import RegionMap, VoxelData

from src.logger import logger
from src.helpers import allocate, ASSETS_DIRECTORY, authenticate, _format_boolean
from src.neuron_morphology.arguments import define_morphology_arguments
from src.neuron_morphology.query_data import get_neuron_morphologies, get_swc_path, get_asc_path
from src.neuron_morphology.validation.quality_metric import (
    SOLO_TYPE, BATCH_TYPE, save_batch_quality_measurement_annotation_report, QUALITY_SCHEMA, BATCH_QUALITY_SCHEMA
)
from src.neuron_morphology.creation_helpers import get_contribution, get_generation
import os
import json

from src.neuron_morphology.validation.region_comparison import get_atlas, create_brain_region_comparison
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
    with open(asc_path, "r") as f:
        try:
            content = f.read()
        except Exception as e:
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
        with_br_check: bool = True,
        with_asc_check: bool = True
) -> Tuple[List[Tuple[Resource, str, Dict]], List[Tuple[Resource, str, Exception]]]:

    if with_br_check:
        brain_region_comp = create_brain_region_comparison(
            search_results=resources, morphology_dir=swc_download_folder, forge=forge_datamodels,
            brain_region_map=br_map, voxel_data=voxel_d, float_coordinates_check=False
        )

        brain_region_comp_dict = dict((i["id"], i) for i in brain_region_comp)

    if with_asc_check:
        resource_id_to_asc_path = dict(
            (resource.get_identifier(), get_asc_path(resource, asc_download_folder=asc_download_folder, forge=forge))
            for resource in resources
        )

    def resource_added_content(resource: Resource) -> Dict:
        temp = {
            "name": resource.name,
            "id": resource.get_identifier(),
            "rev": resource._store_metadata._rev,
        }

        if with_asc_check:
            temp.update({"ASC has no nan": _format_boolean(asc_has_no_nan(resource_id_to_asc_path[resource.get_identifier()]), sparse=True)})

        if with_br_check:
            temp.update(brain_region_comp_dict[resource.get_identifier()])

        return temp

    added_list = [resource_added_content(resource) for resource in resources]
    added_dict = {resource.id: resource_added_content(resource) for resource in resources}

    swc_path_to_resource = dict(
        (get_swc_path(resource, swc_download_folder=swc_download_folder, forge=forge), resource)
        for resource in resources
    )
    resource_to_swc_path = dict(
        (resource.id, get_swc_path(resource, swc_download_folder=swc_download_folder, forge=forge))
        for resource in resources
    )
    miss_resources = list(set(added_dict.keys()) - set(resource_to_swc_path.keys()))
    
    if len(miss_resources) > 1:
        raise ValueError(f"Missmatch between added content and swc paths: {miss_resources}")

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
    received_args, leftovers = parser.parse_known_args()
    org, project = received_args.bucket.split("/")
    output_dir = received_args.output_dir
    token = authenticate(username=received_args.username, password=received_args.password)
    is_prod = True

    # Would push into a test project in staging a subset of the quality metrics
    # Else would push them in the same bucket as the neuron morphology's, for all of them
    limit = received_args.limit
    really_update = received_args.really_update == "yes"
    push_to_staging = received_args.push_to_staging == "yes"
    constrain = True

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
    resources = get_neuron_morphologies(curated=received_args.curated, forge=forge, limit=limit)

    swc_download_folder = os.path.join(working_directory, "swcs")
    asc_download_folder = os.path.join(working_directory, "ascs")
    os.makedirs(swc_download_folder, exist_ok=True)
    os.makedirs(asc_download_folder, exist_ok=True)

    report_dir_path = os.path.join(working_directory, f'{org}_{project}')

    logger.info(f"Saving reports to directory {report_dir_path}")
    report_name = "batch_report.tsv"

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

    br_map, voxel_d = get_atlas(working_directory=working_directory, is_prod=is_prod, token=token)
    # br_map, voxel_d = None, None

    reports, errors = save_batch_quality_measurement_annotation_report_on_resources(
        resources=resources,
        swc_download_folder=swc_download_folder,
        asc_download_folder=asc_download_folder,
        report_dir_path=report_dir_path,
        forge=forge,
        forge_datamodels=forge_datamodels,
        report_name=report_name,
        individual_reports=True,
        br_map=br_map,
        voxel_d=voxel_d,
        with_asc_check=True,
        with_br_check=True
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
            es_view="https://bluebrain.github.io/nexus/vocabulary/defaultElasticSearchIndex",
            sparql_view="https://bluebrain.github.io/nexus/vocabulary/defaultSparqlIndex"
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

    with open(os.path.join(report_dir_path, f"batch_resource_{org}_{project}.json"), "w") as f:
        json.dump(forge_push.as_json(batch_quality_to_register), f, indent=4)

