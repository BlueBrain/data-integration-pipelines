import argparse
import json
from typing import List

from kgforge.core import KnowledgeGraphForge, Resource
import os
import pandas as pd

from src.curation_annotations import create_update_curated_annotation, CurationStatus
from src.helpers import allocate_by_deployment, authenticate_from_parser_arguments
from src.logger import logger
from src.neuron_morphology.arguments import define_morphology_arguments
from src.neuron_morphology.query_data import get_neuron_morphologies

CAN_BE_LOADED_QUERY = """
SELECT DISTINCT ?canBeLoaded
WHERE {{
    
 <{0}>  a  nsg:NeuronMorphology ;
    _deprecated false ;
    ^nsg:hasSource / ^nsg:hasTarget ?qualityMeasurementAnnotation .

    ?qualityMeasurementAnnotation a nsg:QualityMeasurementAnnotation  .

    ?qualityMeasurementAnnotation nsg:hasBody ?canBeLoadedBody.
        ?canBeLoadedBody nsg:isMeasurementOf bmo:CanBeLoadedWithMorphioMetric ;
                         schema:value ?canBeLoaded .
}}
"""


def check_minds(resource):
    if not hasattr(resource, 'contribution'):
        return False
    if not hasattr(resource, 'brainLocation'):
        return False
    if not hasattr(resource, 'subject'):
        return False
    if not hasattr(resource, 'distribution'):
        return False
    return True


def check_update_curated(resources: List[Resource], forge: KnowledgeGraphForge):
    "Loop over curated morphologies and verify if the metric CanBeLoaded is True if not change the annotation"

    rows = []
    failed = []

    for resource in resources:
        row = {
            "id": resource.get_identifier(),
            "name": resource.name,
        }
        query = CAN_BE_LOADED_QUERY.format(resource.get_identifier())
        try:
            result = forge.sparql(query)
            if not result:
                raise ValueError("Error querying for the annotation information. The annotation may not exist yet.")
            elif len(result) > 1:
                logger.warning(f"More than one annotation was found for {resource.name}: {resource.get_identifier()}")
            result = result[0]

            if str(result.canBeLoaded) == 'True':
                new_curated_status = CurationStatus.CURATED
                note = "NeuronMorphology dataset contains complete minimal metadata and it can be loaded using Morphio without warnings."
            else:
                new_curated_status = CurationStatus.UNASSESSED
                note = str(result.canBeLoaded)

            minds = check_minds(resource)
            if not minds:
                raise ValueError("Resource doesn't have complete with MINDS")

            resource, previous_curation_status = create_update_curated_annotation(resource, forge, new_curated_status, note)
            forge.update(resource)

            if not resource._last_action.succeeded:
                raise ValueError(f"Error updating: {resource._last_action.message}")
        except Exception as e:
            row['Error'] = str(e)
            failed.append(row)
        row['Succeeded'] = resource._last_action.succeeded
        rows.append(row)
    return rows, failed


if __name__ == "__main__":
    parser = define_morphology_arguments(argparse.ArgumentParser())

    received_args, leftovers = parser.parse_known_args()
    org, project = received_args.bucket.split("/")
    output_dir = received_args.output_dir

    deployment, auth_token = authenticate_from_parser_arguments(received_args)

    working_directory = os.path.join(os.getcwd(), output_dir)
    os.makedirs(working_directory, exist_ok=True)

    logger.info(f"Working directory {working_directory}")

    forge_bucket = allocate_by_deployment(org, project, deployment=deployment, token=auth_token)

    resources = get_neuron_morphologies(forge=forge_bucket, curated="both")

    rows, failed = check_update_curated(resources, forge_bucket)
    df = pd.DataFrame(rows)

    with open(os.path.join(working_directory, "error_reports.json"), "w") as f:
        json.dump(failed, f, indent=4)

    df.to_csv(os.path.join(working_directory, 'check_update_curated.csv'))
