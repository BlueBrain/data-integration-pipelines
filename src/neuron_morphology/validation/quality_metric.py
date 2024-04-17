import json
import shutil
from typing import Tuple, Dict, List, Optional

from src.logger import logger
from src.neuron_morphology.validation.load_test_data import get_neurom_test_data, get_random_test_data
from src.neuron_morphology.validation.validator import (
    get_validation_report_as_tsv_line,
    get_validation_report_as_json,
    get_tsv_header_columns, get_report
)
import os
from neurom.core.morphology import Morphology


BATCH_TYPE = "BatchQualityMeasurementAnnotation"
SOLO_TYPE = "QualityMeasurementAnnotation"

NEURON_MORPHOLOGY_SCHEMA = "datashapes:neuronmorphology"
BATCH_QUALITY_SCHEMA = "datashapes:batchqualitymeasurementannotation"
QUALITY_SCHEMA = "datashapes:qualitymeasurementannotation"


def save_quality_measurement_annotation_report(
        swc_path: str, report_dir_path: str, name: Optional[str] = None,
        added: Optional[Dict] = None, morphology: Optional[Morphology] = None
) -> Tuple[str, Dict]:

    os.makedirs(report_dir_path, exist_ok=True)
    json_path = os.path.join(report_dir_path, "json")
    tsv_path = os.path.join(report_dir_path, "tsv")
    os.makedirs(json_path, exist_ok=True)
    os.makedirs(tsv_path, exist_ok=True)

    logger.info(f"Processing {swc_path}")

    report = get_report(swc_path, morphology, None)
    json_data = get_validation_report_as_json(swc_path, morphology=morphology, report=report)
    quality_measurement_annotation_line = get_validation_report_as_tsv_line(swc_path, morphology=morphology, report=report)

    if name is None:
        name = swc_path.split("/")[-1].split(".")[0]

    tsv_header = "\t".join(get_tsv_header_columns())
    tsv_content = "# " + tsv_header + "\n" + quality_measurement_annotation_line + "\n"

    json_report = dict(**json_data, **added) if added else dict(**json_data)

    with open(os.path.join(json_path, f'{name}.json'), "w") as f:
        json.dump(json_report, f)

    with open(os.path.join(tsv_path, f'{name}.tsv'), "w") as f:
        f.write(tsv_content)

    return quality_measurement_annotation_line, json_data


def save_batch_quality_measurement_annotation_report(
        swc_paths: List[str],
        report_dir_path: str,
        report_name: str,
        morphologies: Optional[List[Morphology]] = None,
        added_list: Optional[List[Dict]] = None
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

    tsv_header = "\t".join(get_tsv_header_columns())
    batch_quality_measurement_annotation_tsv = "# " + tsv_header + "\n"

    for swc_path, morphology, added in zip(swc_paths, morphologies, added_list):
        try:
            report_as_tsv_line, report_as_json = save_quality_measurement_annotation_report(
                swc_path=swc_path, report_dir_path=report_dir_path, morphology=morphology, added=added
            )
        except Exception as e:
            logger.error(f"Error creating validation report for path {swc_path}: {str(e)}")
            errors[swc_path] = e
        else:
            batch_quality_measurement_annotation_tsv += report_as_tsv_line + "\n"
            reports[swc_path] = report_as_json

    with open(os.path.join(report_dir_path, report_name), "w") as f:
        f.write(batch_quality_measurement_annotation_tsv)

    return reports, errors


if __name__ == "__main__":

    working_directory = os.path.join(os.path.dirname(os.path.realpath(__file__)), "../../../draft/test_1")
    os.makedirs(working_directory, exist_ok=True)

    path_to_value = get_neurom_test_data()
    # path_to_value = get_random_test_data()

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
        report_name=report_name
    )

    print(json.dumps(reports, indent=4))



