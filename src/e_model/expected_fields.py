import argparse
import json
import os
from typing import Dict, Tuple, Optional, Any, List

import pandas
from kgforge.core import KnowledgeGraphForge, Resource

from src.e_model.querying import get_e_models_and_categorisation
from src.helpers import _as_list, authenticate, Deployment, allocate_by_deployment
from src.logger import logger
import requests

# generates_members = ["FitnessCalculatorConfiguration", "EModelScript", "EModel"]
# has_part_members = ["ExtractionTargetsConfiguration", "EModelPipelineSettings", "EModelConfiguration"]


def get_file(content_url: str, token: str, metadata_only: bool, write_path: Optional[str] = None) -> Optional[Dict]:

    if not metadata_only and write_path is None:
        raise Exception("write_path needs to be set if metadata_only is False")

    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/ld+json" if metadata_only else "*/*"
    }

    response = requests.get(content_url, headers=headers, timeout=300)

    if not metadata_only:
        with open(write_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=4096):
                f.write(chunk)
        return None

    else:
        metadata = response.json()
        return metadata


def _get_info_from_e_model_configuration(
        emodel_configuration_resource: Resource, emodel_configuration_file_content: Dict, forge: KnowledgeGraphForge
) -> Tuple[int, int, int, List[Dict], str]:

    find_in_params = lambda k: next(
        (i['value'] for i in emodel_configuration_file_content["parameters"] if i["name"] == k), None
    )

    temperature = find_in_params("celsius")
    ra = find_in_params('Ra')
    initial_voltage = find_in_params('v_init')

    mechanisms = [
        {"name": el["name"], "location": el["location"]}
        for el in emodel_configuration_file_content['mechanisms']
    ]

    exemplar_morphology = next(
        i.get_identifier() for i in _as_list(emodel_configuration_resource.uses)
        if forge._model.context().expand(i.get_type()) == "https://neuroshapes.org/NeuronMorphology"
    )

    assert emodel_configuration_file_content["morphology"]["id"] == exemplar_morphology

    return temperature, initial_voltage, ra, mechanisms, exemplar_morphology


def _get_info_from_e_model_pipeline_settings(
        emodel_pipeline_resource: Resource, emodel_pipeline_file_content: Dict
) -> int:
    return emodel_pipeline_file_content['max_ngen']


def _get_info_from_extraction_targets_configuration(
        extraction_targets_configuration_resource: Resource, extraction_targets_configuration_file_content: Dict, forge: KnowledgeGraphForge
) -> Tuple[Any, Any]:

    temp = extraction_targets_configuration_file_content["files"][0]["ecodes"]
    ljp = temp[list(temp.keys())[0]]["ljp"]

    traces = [
        i.get_identifier()
        for i in _as_list(extraction_targets_configuration_resource.uses)
        if forge._model.context().expand(i.get_type()) == "https://neuroshapes.org/Trace"
    ]

    return ljp, traces


# Expected to be there for collab: EModelConfiguration
# Expected to be missing for collab: FitnessCalculatorConfiguration, ExtractionTargetsConfiguration, EModelPipelineSettings
def get_sub_parts(
        download_directory: str, e_model: Resource, forge: KnowledgeGraphForge, is_complete: bool
) -> Dict[str, str]:

    workflow_id = e_model.generation.activity.followedWorkflow.get_identifier()
    workflow_resource = forge.retrieve(workflow_id)

    def find_by_type_in_field_and_download(field, t) -> Tuple[Optional[Resource], Optional[Dict]]:
        res = next(
            (forge.retrieve(i.get_identifier()) for i in _as_list(workflow_resource.__dict__[field]) if i.get_type() == t),
            None
        )
        if res is None:
            if not is_complete:
                # logger.info(f"Expectedly didn't find {t} in {field} of {workflow_id} from {e_model.get_identifier()}")
                return None, None
            else:
                raise Exception(f"Unexpectedly didn't find {t} in {field} of {workflow_id} from {e_model.get_identifier()}")

        try:
            encoding_format = "application/json"

            dist = next((d for d in _as_list(res.distribution) if d.encodingFormat == encoding_format), None)

            if dist is None:
                raise Exception(
                    f"Couldn't find distribution of encodingFormat {encoding_format} in {res.get_identifier()}"
                )

            filename, _ = forge._store._retrieve_filename(dist.contentUrl)
            filepath = os.path.join(subdir, filename)
            if not os.path.isfile(filepath):
                get_file(
                    content_url=dist.contentUrl, write_path=filepath,
                    metadata_only=False, token=forge._store.token
                )

            # forge.download(res, path=subdir, content_type=encoding_format)

            with open(os.path.join(subdir, filename), "r") as f:
                file_content = json.loads(f.read())

            return res, file_content
        except Exception as e:
            logger.error(f"Could not download/load distribution.contentUrl from {res.get_identifier()} : {e}")
            return res, None

    subdir = os.path.join(download_directory, e_model.name)
    os.makedirs(subdir, exist_ok=True)

    # What should be everywhere
    emodel_configuration_resource, emodel_configuration_file_content = \
        find_by_type_in_field_and_download("hasPart", "EModelConfiguration")

    if emodel_configuration_file_content is None:
        raise Exception(f"EModelConfiguration is never expected to be None but is for {e_model.get_identifier()}")

    temperature, initial_voltage, ra, mechanisms, exemplar_morphology = _get_info_from_e_model_configuration(
        emodel_configuration_resource, emodel_configuration_file_content, forge
    )

    # What should be only in some places
    emodel_pipeline_settings_resource, emodel_pipeline_settings_file_content = \
        find_by_type_in_field_and_download("hasPart", "EModelPipelineSettings")

    if emodel_pipeline_settings_file_content is not None:
        max_opt_gen = _get_info_from_e_model_pipeline_settings(
            emodel_pipeline_settings_resource, emodel_pipeline_settings_file_content
        )
    else:
        max_opt_gen = None

    extraction_targets_configuration_resource, extraction_targets_configuration_file_content = \
        find_by_type_in_field_and_download("hasPart", "ExtractionTargetsConfiguration")

    if extraction_targets_configuration_file_content is not None:
        ljp, traces = _get_info_from_extraction_targets_configuration(
            extraction_targets_configuration_resource, extraction_targets_configuration_file_content, forge
        )
    else:
        ljp, traces = None, None

    # fitness_resource, fitness_file_content = \
    #     find_by_type_in_field_and_download("generates", "FitnessCalculatorConfiguration")

    optimisation_target = "?"  # TODO when info is available

    return {
        "Temperature": temperature,
        "Initial Voltage": initial_voltage,
        "Ra": ra,
        "Max optimisation generation": max_opt_gen,
        "LJP": ljp,
        "Exemplar Morphology": exemplar_morphology,
        "Traces": traces,
        "Optimisation Target": optimisation_target,
        "Mechanisms": mechanisms
    }


def to_excel(dst_path: str, dataframe: pandas.DataFrame):
    writer = pandas.ExcelWriter(dst_path, engine='xlsxwriter')
    dataframe.to_excel(writer, index=False, sheet_name='Sheet1')
    worksheet = writer.sheets['Sheet1']
    for i, col in enumerate(dataframe.columns):
        if_too_large = min(dataframe[col].apply(lambda x: len(str(x))).max(), 30)
        width = max(if_too_large, len(col))
        worksheet.set_column(i, i, width)
    writer.close()


if __name__ == "__main__":

    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--username", help="Service account username", type=str, required=True
    )
    parser.add_argument(
        "--password", help="Service account password", type=str, required=True
    )
    parser.add_argument(
        "--output_dir", help="Output Directory", type=str, required=True
    )
    parser.add_argument(
        "--is_aws", help="AWS Deployment (yes) or Kubernetes Deployment (no)", type=str, required=True, choices=["yes", "no"]
    )

    received_args, leftovers = parser.parse_known_args()

    is_aws = received_args.is_aws == "yes"
    deployment = Deployment.AWS if is_aws else Deployment.PRODUCTION

    token = authenticate(
        username=received_args.username,
        password=received_args.password,
        is_aws=is_aws,
        is_service=True
    )

    org, project = "bbp", "mmb-point-neuron-framework-model"

    forge_instance = allocate_by_deployment(org, project, deployment=deployment, token=token)

    download_dir = received_args.output_dir

    os.makedirs(download_dir, exist_ok=True)

    e_model_id_to_completeness = get_e_models_and_categorisation(forge_instance)

    mandatory_fields = ["Mechanisms", "Exemplar Morphology", "Ra", "Temperature", "Initial Voltage"]
    mandatory_if_complete_fields = ["Max optimisation generation", "LJP", "Traces"]

    # TODO when provided, Optimisation targets - classify in either category

    list_res = []

    for e_model_id, (e_model, completeness_flag) in e_model_id_to_completeness.items():
        try:

            extracted_values = get_sub_parts(
                e_model=e_model, forge=forge_instance, download_directory=download_dir, is_complete=completeness_flag
            )

            def _check_fields_arr(fields_arr):

                for k in fields_arr:
                    if extracted_values[k] is None:
                        logger.error(
                            f"{k} is None in {e_model_id} (brain region: {e_model.brainLocation.brainRegion.label})"
                        )
                        extracted_values[k] = "! MISSING !"

            _check_fields_arr(mandatory_fields)

            if completeness_flag:
                _check_fields_arr(mandatory_if_complete_fields)

            list_res.append(extracted_values)

        except Exception as e:
            logger.error(f"Error with {e_model_id}, {e}")

    df = pandas.DataFrame(list_res)
    to_excel(os.path.join(download_dir, "e_model_fields.xlsx"), df)

