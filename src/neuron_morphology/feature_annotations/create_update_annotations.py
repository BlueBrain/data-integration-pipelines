import argparse
import shutil
from typing import List, Union, Optional, Tuple, Dict

import os
import json

from kgforge.core import Resource, KnowledgeGraphForge

from src.helpers import allocate, authenticate
from src.logger import logger
from src.neuron_morphology.arguments import define_arguments

from src.neuron_morphology.creation_helpers import get_generation, get_contribution
from src.neuron_morphology.feature_annotations.data_classes.AnnotationTarget import AnnotationTarget
from src.neuron_morphology.feature_annotations.morph_metrics import compute_metrics_default_atlas
from src.neuron_morphology.feature_annotations.morph_metrics_neurom import compute_metrics_neurom, \
    compute_metrics_neurom_raw

import pandas as pd
import re

from src.neuron_morphology.query_data import get_neuron_morphologies, get_swc_path

ANNOTATION_SCHEMA = "https://neuroshapes.org/dash/annotation"


def escape_ansi(line: str) -> str:
    """
    Removes any ansi escape code (e.g. coloured text) turning it to standard text

    Parameters
    ----------
    line : str
        the text that might contain ansi escape codes for coloured text

    Returns
    -------
    str
        the same string without ansi escape codes.

    """
    escape_ansi = re.compile(r'(?:\x1B[@-_]|[\x80-\x9F])[0-?]*[ -/]*[@-~]')
    return escape_ansi.sub('', line)


# Only for neurom features, used for comparing features available across neuron morphologies for
# neurite feature embedding investigation
def _create_raw(morphology: Resource, download_directory: str, forge: KnowledgeGraphForge) -> Tuple[Dict, str]:

    morph_path = get_swc_path(morphology, swc_download_folder=download_directory, forge=forge)
    temp, warnings = compute_metrics_neurom_raw(morph_path)

    def floatify(dict_instance):
        return dict(
            (key, float(value) if value is not None else None)
            for (key, value) in dict_instance.items()
        )

    temp = dict((key, floatify(value)) for key, value in temp.items())
    return temp, warnings


def add_additional_info(
        annotation: Dict, forge_annotations: KnowledgeGraphForge,
        generation, contribution, morphology: Resource
) -> Resource:
    resource = forge_annotations.from_json(annotation)
    resource.brainLocation = morphology.brainLocation
    resource.hasTarget = AnnotationTarget.obj_to_dict(
        AnnotationTarget(
            id_=morphology.id, type_=morphology.type,
            rev=morphology._store_metadata._rev
        )
    )
    resource.generation = generation
    resource.contribution = contribution
    return resource


def update_create_one(
        morphology: Resource,
        existing_annotations,
        generation, contribution,
        download_directory: str,
        atlas_directory: str,
        forge_data: KnowledgeGraphForge,
        forge_atlas: KnowledgeGraphForge
) -> Tuple[List[Resource], List[Resource], Union[str, pd.DataFrame]]:

    morph_path = get_swc_path(morphology, swc_download_folder=download_directory, forge=forge_data)

    with_location = "coordinatesInBrainAtlas" in morphology.brainLocation.__dict__

    if with_location:
        annotations, warnings = compute_metrics_default_atlas(
            morphology_path=morph_path,
            atlas_download_directory=atlas_directory,
            forge_atlas=forge_atlas
        )
    else:
        annotations, warnings = compute_metrics_neurom(morphology_filepath=morph_path)
        annotations = list(annotations.values())

    computed_annotations = forge_data.from_json(annotations)

    # Annotations were re-computed, update existing resources with
    # by matching created annotations/existing annotations by compartment

    computed = dict((el.compartment, el) for el in computed_annotations)
    existing = dict((el.compartment, el) for el in existing_annotations)

    updated_annotations = []
    created_annotations = []

    for compartment_key in computed.keys():

        existing_for_compartment = existing.get(compartment_key, None)
        if not existing_for_compartment:
            created = add_additional_info(
                annotation=computed[compartment_key], forge_annotations=forge_data,
                generation=generation, contribution=contribution, morphology=morphology
            )
            created_annotations.append(created)
        else:
            # Update hasBody of annotations only
            existing_for_compartment.hasBody = computed[compartment_key].hasBody
            updated_annotations.append(existing_for_compartment)

    return updated_annotations, created_annotations, warnings


def create_update_annotations(
        forge_data: KnowledgeGraphForge,
        forge_atlas: KnowledgeGraphForge,
        morphologies: List[Resource],
        is_prod: bool,
        token: str,
        atlas_directory: str,
        download_directory: str
) -> Tuple[
    List[Resource],
    List[Resource],
    Dict[str, Dict],
    Dict[str, List[Union[Resource, Dict]]],
    Dict[str, str]
]:
    logger.info("Retrieving neuron morphology feature annotations")

    annotations = dict(
        (
            r.get_identifier(),
            forge_data.search({
                "type": "NeuronMorphologyFeatureAnnotation",
                "hasTarget": {"hasSource": {"id": r.id}}})
        )
        for r in morphologies
    )

    generation = get_generation()
    contribution = get_contribution(token=token, production=is_prod)

    annotations_update, annotations_create, log_dict = [], [], {}

    features_dict: Dict[str, Dict] = dict()
    annotations_dict: Dict[str, List[Union[Resource, Dict]]] = dict()

    logger.info("Building neuron morphology feature annotations")

    for i, morphology in enumerate(morphologies):
        if (i + 1) % 20 == 0:
            logger.info(f"{i + 1}/{len(morphologies)}")

        m_id = morphology.get_identifier()

        try:
            updated_annotations, created_annotations, warnings = update_create_one(
                morphology=morphology,
                existing_annotations=annotations[m_id],
                download_directory=download_directory,
                forge_data=forge_data,
                forge_atlas=forge_atlas,
                atlas_directory=atlas_directory,
                generation=generation,
                contribution=contribution,
            )
            annotations_update.extend(updated_annotations)
            annotations_create.extend(created_annotations)

            neurom_output, _ = _create_raw(
                morphology=morphology, download_directory=download_directory, forge=forge_data
            )

            annotations_dict[m_id] = forge_data.as_json(updated_annotations + created_annotations)

            features_dict[m_id] = neurom_output

            if warnings.strip():  # do not add to warning_dicts empty warnings
                log_dict[m_id] = escape_ansi(warnings)
            assert (all(not e._synchronized for e in updated_annotations))
        except Exception as e:
            logger.error(f"Error with morphology {m_id}: {e}")
            log_dict[m_id] = e.args[0]

    logger.info("Validating")
    forge_data.validate(data=annotations_update, type_="Annotation")
    forge_data.validate(data=annotations_create, type_="Annotation")

    return annotations_update, annotations_create, features_dict, annotations_dict, log_dict


if __name__ == '__main__':
    parser = define_arguments(argparse.ArgumentParser())
    received_args, leftovers = parser.parse_known_args()
    org, project = received_args.bucket.split("/")
    output_dir = received_args.output_dir
    token = authenticate(username=received_args.username, password=received_args.password)
    is_prod = True

    limit = received_args.limit
    really_update = received_args.really_update == "yes"
    constrain = False  # TODO change

    logger.info(f"Neuron morphology feature annotations will be created/updated: {str(really_update)}")

    download_dir = os.path.join(output_dir, f"./files_{org}_{project}")
    dst_dir = os.path.join(output_dir, f"./{org}_{project}")
    atlas_dir = os.path.join(output_dir, "./atlas")

    os.makedirs(output_dir, exist_ok=True)
    os.makedirs(dst_dir, exist_ok=True)
    os.makedirs(atlas_dir, exist_ok=True)

    forge_data = allocate(org, project, is_prod, token)
    forge_atlas = allocate("bbp", "atlas", is_prod, token)

    morphologies = get_neuron_morphologies(curated=received_args.curated, forge=forge_data, limit=limit)

    annotations_to_update, annotations_to_create, features_dict, annotations_dict, log_dict = create_update_annotations(
        is_prod=is_prod,
        token=token,
        forge_data=forge_data,
        morphologies=morphologies,
        forge_atlas=forge_atlas,
        atlas_directory=atlas_dir,
        download_directory=download_dir
    )

    if really_update:
        logger.info("Updating data has been enabled")
        forge_data.update(annotations_to_update, schema_id=ANNOTATION_SCHEMA if constrain else None)
        forge_data.register(annotations_to_create, schema_id=ANNOTATION_SCHEMA if constrain else None)

        logger.info(
            f"{len(annotations_to_create)} annotation created, "
            f"{len(annotations_to_update)} annotations updated"
        )
    else:
        logger.info("Updating data has been disabled")

    dicts = {
        'annotations': annotations_dict,
        'features': features_dict,
        'log': log_dict
    }

    for dict_label, dict_ in dicts.items():
        with open(os.path.join(dst_dir, f"{dict_label}_{org}_{project}.json"), "w") as f:
            json.dump(dict_, f, indent=4)

    shutil.rmtree(download_dir)
    shutil.rmtree(atlas_dir)
