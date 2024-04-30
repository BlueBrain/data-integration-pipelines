import argparse
import shutil
from pathlib import Path
from typing import Tuple, Dict, List

from kgforge.core import KnowledgeGraphForge, Resource
from kgforge.specializations.mappings import DictionaryMapping
from kgforge.specializations.mappers import DictionaryMapper

from src.logger import logger
from src.helpers import allocate, ASSETS_DIRECTORY, authenticate
from src.neuron_morphology.arguments import define_arguments
from src.neuron_morphology.query_data import get_neuron_morphologies, get_swc_path
from src.neuron_morphology.validation.quality_metric import (
    SOLO_TYPE, BATCH_TYPE, save_batch_quality_measurement_annotation_report, QUALITY_SCHEMA, BATCH_QUALITY_SCHEMA
)
from src.neuron_morphology.creation_helpers import get_contribution, get_generation
import os
import json

from src.neuron_morphology.validation.validator import validation_report_checks
from src.neuron_morphology.validation.workflow_usage import run_workflow_on_path


def quality_measurement_report_to_resource(
        morphology_resources_and_report: List[Tuple[Resource, Dict]],
        forge: KnowledgeGraphForge,
        contribution: Dict,
        generation: Dict,
        batch_report_name: str,
        batch_report_dir: str,
        mapping_batch_validation_report: DictionaryMapping
) -> Tuple[Resource, List[Resource], List[Resource]]:

    logger.info(f"Creating a {BATCH_TYPE} for {len(morphology_resources_and_report)} resources and returning the existing {SOLO_TYPE} to update, or new ones to create")

    reports_as_resources = []

    for resource, report in morphology_resources_and_report:
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

        dict_for_res = {
            "distribution": [
                forge.attach(f"{batch_report_dir}/json/{resource.name}.json", content_type="application/json"),
                forge.attach(f"{batch_report_dir}/tsv/{resource.name}.tsv", content_type="application/tsv")
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
        "morphologies": [i[0] for i in morphology_resources_and_report],
        "name": batch_report_name,
        "filepath": batch_report_dir
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

    logger.info(f"For {len(morphology_resources_and_report)}, {len(to_upd)} existing {SOLO_TYPE} to update, {len(to_register)} {SOLO_TYPE} to register")

    return batch_report_resource, to_upd, to_register


def save_batch_quality_measurement_annotation_report_on_resources(
        resources: List[Resource],
        swc_download_folder: str,
        forge: KnowledgeGraphForge,
        report_dir_path: str,
        report_name: str,
        individual_reports: bool
) -> Tuple[List[Tuple[Resource, Dict]], List[Tuple[Resource, Exception]]]:

    added_list = [
        {
            "name": resource.name,
            "neuron_morphology_id": resource.id,
            "neuron_morphology_rev": resource._store_metadata._rev
        }
        for resource in resources
    ]

    path_to_resource = dict(
        (get_swc_path(resource, swc_download_folder=swc_download_folder, forge=forge), resource)
        for resource in resources
    )

    swc_path_to_report, swc_path_to_error = save_batch_quality_measurement_annotation_report(
        swc_paths=list(path_to_resource.keys()), report_dir_path=report_dir_path,
        morphologies=None, report_name=report_name,
        added_list=added_list, individual_reports=individual_reports
    )

    reports = [(path_to_resource[swc_path], report) for swc_path, report in swc_path_to_report.items()]
    errors = [(path_to_resource[swc_path], error) for swc_path, error in swc_path_to_error.items()]

    return reports, errors


if __name__ == "__main__":
    parser = define_arguments(argparse.ArgumentParser())
    received_args, leftovers = parser.parse_known_args()
    org, project = received_args.bucket.split("/")
    output_dir = received_args.output_dir
    token = authenticate(username=received_args.username, password=received_args.password)
    is_prod = True

    # Would push into a test project in staging a subset of the quality metrics
    # Else would push them in the same bucket as the neuron morphology's, for all of them
    limit = received_args.limit
    really_update = received_args.really_update
    push_to_staging = received_args.push_to_staging
    constrain = True

    logger.info(f"Neuron morphology quality annotations will be created/updated: {str(really_update)}")

    working_directory = os.path.join(os.getcwd(), output_dir)

    logger.info(f"Working directory {working_directory}")
    os.makedirs(working_directory, exist_ok=True)

    forge = allocate(org, project, is_prod=is_prod, token=token)
    resources = get_neuron_morphologies(curated=received_args.curated, forge=forge, limit=limit)

    swc_download_folder = os.path.join(working_directory, "swcs")
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

    reports, errors = save_batch_quality_measurement_annotation_report_on_resources(
        resources=resources,
        swc_download_folder=swc_download_folder,
        report_dir_path=report_dir_path,
        forge=forge,
        report_name=report_name,
        individual_reports=True
    )

    for resource in resources:
        path = Path(get_swc_path(resource, swc_download_folder, forge))
        dst_dir = Path(os.path.join(working_directory, "workflow_output"))
        result = run_workflow_on_path(path, dst_dir)

    shutil.rmtree(swc_download_folder)

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
        morphology_resources_and_report=reports, forge=forge_push,
        contribution=contribution, generation=generation,
        batch_report_name=report_name, batch_report_dir=report_dir_path,
        mapping_batch_validation_report=mapping_batch_validation_report,
    )

    with open(os.path.join(report_dir_path, f"batch_resource_{org}_{project}.json"), "w") as f:
        json.dump(forge_push.as_json(batch_quality_to_register), f, indent=4)

    if really_update:
        logger.info("Updating data has been enabled")
        # TODO: more programmatic way of dealing with multiple pre-existing Batch reports
        forge_push.register(batch_quality_to_register, schema_id=BATCH_QUALITY_SCHEMA if constrain else None)
        forge_push.register(quality_to_register, schema_id=QUALITY_SCHEMA if constrain else None)
        forge_push.update(quality_to_update, schema_id=QUALITY_SCHEMA if constrain else None)
    else:
        logger.info("Updating data has been disabled")
