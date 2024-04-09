import json
from typing import Tuple, Dict, List

from kgforge.core import KnowledgeGraphForge, Resource
from kgforge.specializations.mappings import DictionaryMapping
from kgforge.specializations.mappers import DictionaryMapper

from src.logger import logger
from src.helpers import allocate, get_token, CustomEx, ASSETS_DIRECTORY
from src.neuron_morphology.validation.check_swc_on_resource import check_swc_on_resource, get_swc_path
from src.neuron_morphology.validation.validator import validation_report, validation_report_complete, get_tsv_report_line, get_tsv_header_columns
from src.neuron_morphology.feature_annotations.creation_helpers import get_contribution, get_generation
from neurom import load_morphology
import os


BATCH_TYPE = "BatchQualityMeasurementAnnotation"
SOLO_TYPE = "QualityMeasurementAnnotation"

NEURON_MORPHOLOGY_SCHEMA = "datashapes:neuronmorphology"
BATCH_QUALITY_SCHEMA = "datashapes:batchqualitymeasurementannotation"
QUALITY_SCHEMA = "datashapes:qualitymeasurementannotation"


def get_validation_report_line(resource: Resource, swc_download_folder: str, forge: KnowledgeGraphForge) -> Tuple[str, Dict]:
    swc_path = get_swc_path(resource, swc_download_folder=swc_download_folder, forge=forge)

    logger.info(f"Creating validation report for resource {resource.name} at path {swc_path}")
    morphology = load_morphology(swc_path)

    report_output = validation_report(morphology)
    json_data = validation_report_complete(report_output, is_report=True)

    quality_measurement_annotation_line = get_tsv_report_line(swc_path, report=report_output) + "\n"

    return quality_measurement_annotation_line, json_data


def save_quality_measurement_annotation_report(resource: Resource, swc_download_folder: str, forge: KnowledgeGraphForge, report_dir_path: str) -> Tuple[str, Dict]:
    os.makedirs(report_dir_path, exist_ok=True)
    quality_measurement_annotation_line, json_data = get_validation_report_line(resource, swc_download_folder=swc_download_folder, forge=forge)
    name = resource.name

    json_data["name"] = name
    json_data["neuron_morphology_id"] = resource.id
    json_data["neuron_morphology_rev"] = resource._store_metadata._rev

    tsv_header = "\t".join(get_tsv_header_columns())
    tsv_content = "# " + tsv_header + "\n" + quality_measurement_annotation_line  # Quality Metrics

    json_report = json.dumps(json_data, indent=2)

    with open(os.path.join(report_dir_path, f'{name}.json'), "w") as f:
        f.write(json_report)

    with open(os.path.join(report_dir_path, f'{name}.tsv'), "w") as f:
        f.write(tsv_content)

    return quality_measurement_annotation_line, json_data


def save_batch_quality_measurement_annotation_report(
        resources: List[Resource], swc_download_folder: str, forge: KnowledgeGraphForge, report_dir_path: str, report_name: str
):
    os.makedirs(report_dir_path, exist_ok=True)

    reports: List[Tuple[Dict, Resource]] = []
    errors: List[Tuple[Exception, Resource]] = []

    tsv_header = "\t".join(get_tsv_header_columns())
    batch_quality_measurement_annotation_tsv = "# " + tsv_header + "\n"

    for resource in resources:
        try:
            morphology_report_line_entry, report = save_quality_measurement_annotation_report(
                resource, swc_download_folder=swc_download_folder, forge=forge, report_dir_path=report_dir_path
            )
            batch_quality_measurement_annotation_tsv += morphology_report_line_entry  # Batch Quality metrics
            reports.append((report, resource))
        except Exception as e:
            logger.error(f"Error creating validation report for resource {resource.name}: {str(e)}")
            errors.append((e, resource))

    with open(os.path.join(report_dir_path, report_name), "w") as f:
        f.write(batch_quality_measurement_annotation_tsv)

    return reports, errors


def quality_measurement_resources(
        reports_and_morphology_resources: List[Tuple[Dict, Resource]],
        forge: KnowledgeGraphForge, token: str, is_prod: bool,
        batch_report_name: str, batch_report_dir: str,
        mapping_validation_report: DictionaryMapping,
        mapping_batch_validation_report: DictionaryMapping
) -> Tuple[Resource, List[Resource], List[Resource]]:

    logger.info(f"Creating a {BATCH_TYPE} for {len(reports_and_morphology_resources)} resources and returning the existing {SOLO_TYPE} to update, or new ones to create")

    resources = [i[1] for i in reports_and_morphology_resources]
    reports = [i[0] for i in reports_and_morphology_resources]

    contribution = get_contribution(token=token, production=is_prod)
    generation = get_generation()
    reports_resources = forge.map(reports, mapping_validation_report, DictionaryMapper)

    for resource in reports_resources:
        resource.contribution = contribution
        resource.generation = generation

    batch_report = {"morphologies": resources, "name": batch_report_name, 'filepath': batch_report_dir}
    batch_report_resource = forge.map(batch_report, mapping_batch_validation_report, DictionaryMapper)
    batch_report_resource.contribution = contribution
    batch_report_resource.generation = generation

    to_upd, to_register = [], []
    for n, report in enumerate(reports_resources):
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


if __name__ == "__main__":

    is_prod = True
    token = get_token(is_prod=is_prod, prompt=False)
    forge = allocate("bbp-external", "seu", is_prod=is_prod, token=token)

    working_directory = os.path.join(os.path.dirname(os.path.realpath(__file__)), "../../draft/test1")
    os.makedirs(working_directory, exist_ok=True)

    resources = forge.search({"type": "ReconstructedNeuronMorphology"}, limit=10)

    morphologies_to_update = []
    issues = dict()

    swc_download_folder = os.path.join(working_directory, "swcs")
    report_dir_path = os.path.join(working_directory, 'validation_report')
    report_name = "batch_report.tsv"

    for resource in resources:
        try:
            to_update = check_swc_on_resource(resource, swc_download_folder=swc_download_folder, forge=forge)
            if to_update:
                morphologies_to_update.append(resource)

        except CustomEx as ex:
            issues[resource.id] = (resource, ex)

    # forge.update(morphologies_to_update, NEURON_MORPHOLOGY_SCHEMA)

    reports, errors = save_batch_quality_measurement_annotation_report(
        resources=[r for r in resources if r.id not in issues],
        swc_download_folder=swc_download_folder,
        report_dir_path=report_dir_path,
        forge=forge,
        report_name=report_name
    )

    mapping_validation_report = DictionaryMapping.load(os.path.join(ASSETS_DIRECTORY, 'QualityMeasurementAnnotation.hjson'))

    mapping_validation_report.rules["distribution"] = [
        entry.replace("../../data/raw/morpho/validation_report", report_dir_path)
        for entry in mapping_validation_report.rules["distribution"]
    ]

    mapping_batch_validation_report = DictionaryMapping.load(os.path.join(ASSETS_DIRECTORY, 'BatchQualityMeasurementAnnotation.hjson'))

    batch_quality_to_register, quality_to_update, quality_to_register = quality_measurement_resources(
        reports_and_morphology_resources=reports, forge=forge, token=token, is_prod=is_prod,
        batch_report_name=report_name, batch_report_dir=report_dir_path, mapping_batch_validation_report=mapping_batch_validation_report,
        mapping_validation_report=mapping_validation_report
    )

    # TODO: more programmatic way of dealing with multiple pre-existing Batch reports
    # forge.update(batch_quality_to_register, BATCH_QUALITY_SCHEMA)
    #
    # forge.register(quality_to_register, QUALITY_SCHEMA)
    # forge.update(quality_to_update, QUALITY_SCHEMA)
