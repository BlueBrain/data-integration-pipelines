import argparse
import json
import shutil
from typing import Tuple, Dict, List, Optional, Union

from src.helpers import _as_list
from src.logger import logger
from src.neuron_morphology.arguments import define_arguments
from src.neuron_morphology.validation.load_test_data import get_random_test_data #, get_neurom_test_data
from src.neuron_morphology.validation.validator import (
    get_validation_report_as_tsv_line,
    get_validation_report_as_json,
    get_tsv_header_columns, get_report
)
import os
from neurom.core.morphology import Morphology

BATCH_TYPE = "BatchQualityMeasurementAnnotation"
SOLO_TYPE = "QualityMeasurementAnnotation"

NEURON_MORPHOLOGY_SCHEMA = "https://neuroshapes.org/dash/neuronmorphology"
BATCH_QUALITY_SCHEMA = "https://neuroshapes.org/dash/batchqualitymeasurementannotation"
QUALITY_SCHEMA = "https://neuroshapes.org/dash/qualitymeasurementannotation"


def save_quality_measurement_annotation_report(
        swc_path: str,
        report_dir_path: str,
        individual_reports: bool,
        name: Optional[str] = None,
        added: Optional[Dict] = None,
        morphology: Optional[Morphology] = None,
) -> Tuple[List[str], Dict]:

    os.makedirs(report_dir_path, exist_ok=True)
    json_path = os.path.join(report_dir_path, "json")
    tsv_path = os.path.join(report_dir_path, "tsv")
    os.makedirs(json_path, exist_ok=True)
    os.makedirs(tsv_path, exist_ok=True)

    logger.info(f"Processing {swc_path}")

    report = get_report(swc_path, morphology, None)
    json_data = get_validation_report_as_json(swc_path, morphology=morphology, report=report)
    quality_measurement_annotation_line = get_validation_report_as_tsv_line(swc_path, morphology=morphology, report=report, added=added)

    if individual_reports:
        if name is None:
            name = swc_path.split("/")[-1].split(".")[0]

        json_report = dict(**json_data, **added) if added else dict(**json_data)
        json_path = os.path.join(json_path, f'{name}.json')

        logger.info(f"Saving to {json_path}")

        with open(json_path, "w") as f:
            json.dump(json_report, f, indent=4)

        columns = _get_headers_and_more(added)
        tsv_header = "\t".join(columns)
        tsv_content = "# " + tsv_header + "\n" + '\t'.join(quality_measurement_annotation_line) + "\n"
        tsv_path = os.path.join(tsv_path, f'{name}.tsv')

        logger.info(f"Saving to {tsv_path}")
        with open(tsv_path, "w") as f:
            f.write(tsv_content)

    return quality_measurement_annotation_line, json_data


def _get_headers_and_more(added: Union[List[Dict], Dict]):
    added_list = _as_list(added)
    columns = get_tsv_header_columns()
    if added_list is not None and len(added_list) > 0:
        columns += list(added_list[0].keys())
    return columns


def save_batch_quality_measurement_annotation_report(
        swc_paths: List[str],
        report_dir_path: str,
        report_name: str,
        individual_reports: bool,
        morphologies: Optional[List[Morphology]] = None,
        added_list: Optional[List[Dict]] = None,
) -> Tuple[Dict[str, Dict], Dict[str, Exception]]:
    os.makedirs(report_dir_path, exist_ok=True)

    if morphologies is None:
        morphologies = [None] * len(swc_paths)
    elif len(morphologies) != len(swc_paths):
        raise Exception("Provided morphology list should be the same length as swc paths")

    if added_list is None:
        added_list = [None] * len(swc_paths)
    elif len(added_list) != len(swc_paths):
        raise Exception("Provided list of data to add to json report should be the same length as swc paths")

    reports: Dict[str, Dict] = dict()
    errors: Dict[str, Exception] = dict()

    columns = _get_headers_and_more(added_list)
    tsv_header = "\t".join(columns)
    batch_quality_measurement_annotation_tsv = "# " + tsv_header + "\n"

    n_paths = len(swc_paths)
    for i_path, (swc_path, morphology, added) in enumerate(zip(swc_paths, morphologies, added_list)):
        logger.info(f"Processing swc path {i_path +1} of {n_paths}")
        # try:
        report_as_tsv_line, report_as_json = save_quality_measurement_annotation_report(
            swc_path=swc_path, report_dir_path=report_dir_path, morphology=morphology, added=added,
            individual_reports=individual_reports
        )
        # except Exception as e:
        #     logger.error(f"Error creating validation report for path {swc_path}: {str(e)}")
        #     errors[swc_path] = e
        # else:
        batch_quality_measurement_annotation_tsv += '\t'.join(report_as_tsv_line) + "\n"
        reports[swc_path] = report_as_json

    with open(os.path.join(report_dir_path, report_name), "w") as f:
        f.write(batch_quality_measurement_annotation_tsv)

    return reports, errors


if __name__ == "__main__":

    parser = define_arguments(argparse.ArgumentParser())
    received_args, leftovers = parser.parse_known_args()

    org, project = received_args.bucket.split("/")
    is_prod = True

    working_directory = os.path.join(os.getcwd(), received_args.output_dir)
    os.makedirs(working_directory, exist_ok=True)

    # path_to_value = get_neurom_test_data()
    path_to_value = get_random_test_data()

    paths = list(path_to_value.keys())
    morphologies = list(path_to_value.values())

    report_dir_path = os.path.join(working_directory, 'validation_report')
    report_name = "batch_report.tsv"

    if os.path.exists(report_dir_path):
        shutil.rmtree(report_dir_path)

    reports, errors = save_batch_quality_measurement_annotation_report(
        swc_paths=paths,
        morphologies=morphologies,
        report_dir_path=report_dir_path,
        report_name=report_name,
        individual_reports=True
    )

    print(json.dumps(reports, indent=4))
