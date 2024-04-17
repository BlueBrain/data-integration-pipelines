from typing import Tuple, Dict, List, Optional

from kgforge.core import KnowledgeGraphForge, Resource
from kgforge.specializations.mappings import DictionaryMapping
from kgforge.specializations.mappers import DictionaryMapper

from src.logger import logger
from src.helpers import allocate, get_token, CustomEx, ASSETS_DIRECTORY
from src.neuron_morphology.validation.check_swc_on_resource import check_swc_on_resource, get_swc_path
from src.neuron_morphology.validation.quality_metric import (
    SOLO_TYPE, BATCH_TYPE, save_batch_quality_measurement_annotation_report
)
from src.neuron_morphology.feature_annotations.creation_helpers import get_contribution, get_generation
import os
import json


def quality_measurement_report_to_resource(
        reports_and_morphology_resources: List[Tuple[Dict, Resource]],
        forge: KnowledgeGraphForge,
        token: str,
        is_prod: bool,
        batch_report_name: str,
        batch_report_dir: str,
        mapping_batch_validation_report: DictionaryMapping
) -> Tuple[Resource, List[Resource], List[Resource]]:

    logger.info(f"Creating a {BATCH_TYPE} for {len(reports_and_morphology_resources)} resources and returning the existing {SOLO_TYPE} to update, or new ones to create")

    contribution = get_contribution(token=token, production=is_prod)
    generation = get_generation()

    reports_as_resources = []

    for report, _ in reports_and_morphology_resources:
        # resource.contribution = contribution
        # resource.generation = generation

        dict_for_res = {
            "distribution": [
                forge.attach(f"{batch_report_dir}/{report['name']}.json", content_type="application/json"),
                forge.attach(f"{batch_report_dir}/{report['name']}.tsv", content_type="application/tsv")
            ],
            "name": f"Quality Measurement Annotation of {report['name']}",
            "description": f"This resources contains quality measurement annotations of the neuron morphology {x['name']}",
            "type": [
                "Annotation",
                "QualityMeasurementAnnotation"
            ],
            "hasTarget": {
                "type": "AnnotationTarget",
                "hasSource": {
                    "id": report["neuron_morphology_id"],
                    "type": "NeuronMorphology",
                    "_rev": report['neuron_morphology_rev']
                }
            },
            "hasBody": []
        }

        report_resource = forge.from_json(dict_for_res)
        reports_as_resources.append(report_resource)

    batch_report = {"morphologies": resources, "name": batch_report_name, 'filepath': batch_report_dir}
    batch_report_resource = forge.map(batch_report, mapping_batch_validation_report, DictionaryMapper)

    batch_report_resource.contribution = contribution
    batch_report_resource.generation = generation

    to_upd, to_register = [], []
    for n, report in enumerate(reports_as_resources):
        search_results = forge.search({'name': report.name})
        if not search_results:
            search_results = forge.search({'type': SOLO_TYPE, 'hasTarget': {'hasSource': {'@id': report.hasTarget.hasSource.id}}})

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

    logger.info(f"For {len(reports_and_morphology_resources)}, {len(to_upd)} existing {SOLO_TYPE} to update, {len(to_register)} {SOLO_TYPE} to register")

    return batch_report_resource, to_upd, to_register


def save_batch_quality_measurement_annotation_report_on_resources(
        resources: List[Resource],
        swc_download_folder: str,
        forge: KnowledgeGraphForge,
        report_dir_path: str,
        report_name: str
):
    added_list = [
        {
            "name": resource.name,
            "neuron_morphology_id": resource.id,
            "neuron_morphology_rev": resource._store_metadata._rev
        }
        for resource in resources
    ]

    paths = [
        get_swc_path(resource, swc_download_folder=swc_download_folder, forge=forge)
        for resource in resources
    ]

    return save_batch_quality_measurement_annotation_report(
        swc_paths=paths, report_dir_path=report_dir_path,
        morphologies=None, report_name=report_name,
        added_list=added_list
    )


if __name__ == "__main__":

    is_prod = True
    token = get_token(is_prod=is_prod, prompt=False)
    forge = allocate("bbp-external", "seu", is_prod=is_prod, token=token)

    working_directory = os.path.join(os.path.dirname(os.path.realpath(__file__)), "../../../draft/test2")
    os.makedirs(working_directory, exist_ok=True)

    resources = forge.search({"type": "ReconstructedNeuronMorphology"}, limit=10)

    morphologies_to_update = []
    issues = dict()

    swc_download_folder = os.path.join(working_directory, "swcs")
    report_dir_path = os.path.join(working_directory, 'validation_report')
    report_name = "batch_report.tsv"

    # for resource in resources:
    #     try:
    #         to_update = check_swc_on_resource(resource, swc_download_folder=swc_download_folder, forge=forge)
    #         if to_update:
    #             morphologies_to_update.append(resource)
    #
    #     except CustomEx as ex:
    #         issues[resource.id] = (resource, ex)

    # forge.update(morphologies_to_update, NEURON_MORPHOLOGY_SCHEMA)

    reports, errors = save_batch_quality_measurement_annotation_report_on_resources(
        resources=[r for r in resources if r.id not in issues],
        swc_download_folder=swc_download_folder,
        report_dir_path=report_dir_path,
        forge=forge,
        report_name=report_name
    )

    print(json.dumps(reports, indent=4))

    # mapping_validation_report = DictionaryMapping.load(os.path.join(ASSETS_DIRECTORY, 'QualityMeasurementAnnotation.hjson'))

    # mapping_validation_report.rules["distribution"] = [
    #     entry.replace("../../data/raw/morpho/validation_report", report_dir_path)
    #     for entry in mapping_validation_report.rules["distribution"]
    # ]

    # mapping_batch_validation_report = DictionaryMapping.load(os.path.join(ASSETS_DIRECTORY, 'BatchQualityMeasurementAnnotation.hjson'))
    #
    # batch_quality_to_register, quality_to_update, quality_to_register = quality_measurement_report_to_resource(
    #     reports_and_morphology_resources=reports, forge=forge, token=token, is_prod=is_prod,
    #     batch_report_name=report_name, batch_report_dir=report_dir_path,
    #     mapping_batch_validation_report=mapping_batch_validation_report,
    #     # mapping_validation_report=mapping_validation_report
    # )

    # TODO: more programmatic way of dealing with multiple pre-existing Batch reports
    # forge.update(batch_quality_to_register, BATCH_QUALITY_SCHEMA)
    #
    # forge.register(quality_to_register, QUALITY_SCHEMA)
    # forge.update(quality_to_update, QUALITY_SCHEMA)
