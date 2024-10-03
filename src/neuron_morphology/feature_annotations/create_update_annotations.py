import argparse
import shutil
#from multiprocessing.pool import ThreadPool, Pool
from typing import List, Union, Optional, Tuple, Dict

import os
import json

from kgforge.core import Resource, KnowledgeGraphForge

from src.helpers import allocate_by_deployment, DEFAULT_ES_VIEW, DEFAULT_SPARQL_VIEW, Deployment, authenticate_from_parser_arguments
from src.logger import logger
from src.neuron_morphology.arguments import define_morphology_arguments

from src.neuron_morphology.creation_helpers import get_generation, get_contribution
from src.neuron_morphology.feature_annotations.data_classes.AnnotationTarget import AnnotationTarget
from src.neuron_morphology.feature_annotations.morph_metrics import compute_metrics_default_atlas
from src.neuron_morphology.feature_annotations.morph_metrics_neurom import compute_metrics_neurom, \
    compute_metrics_neurom_raw

import pandas as pd
import re
import traceback

from src.neuron_morphology.query_data import get_neuron_morphologies
from src.helpers import get_ext_path


ANNOTATION_SCHEMA = "https://neuroshapes.org/dash/annotation"

BATCH_SIZE = 50


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

    morph_path = get_ext_path(morphology, ext_download_folder=download_directory, forge=forge, ext="swc")
    temp, warnings = compute_metrics_neurom_raw(morph_path)

    def floatify(dict_instance):
        return dict(
            (key, float(value) if value is not None else None)
            for (key, value) in dict_instance.items()
        )

    temp = dict((key, floatify(value)) for key, value in temp.items())
    return temp, warnings


def add_additional_info(
        resource: Resource, generation, contribution, morphology: Resource
) -> Resource:
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
        generation: Dict,
        contribution: Dict,
        download_directory: str,
        atlas_directory: str,
        forge_data: KnowledgeGraphForge,
        forge_atlas: KnowledgeGraphForge
) -> Tuple[List[Resource], List[Resource], Union[str, pd.DataFrame]]:

    morph_path = get_ext_path(morphology, ext_download_folder=download_directory, forge=forge_data, ext="swc")

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

            annotation = computed[compartment_key]
            resource = forge_data.from_json(annotation)
            created = add_additional_info(
                resource=resource,
                generation=generation, contribution=contribution, morphology=morphology
            )
            created_annotations.append(created)
        else:
            # Update hasBody of annotations only
            existing_for_compartment.hasBody = computed[compartment_key].hasBody
            add_additional_info(
                resource=existing_for_compartment, generation=generation,
                contribution=contribution, morphology=morphology
            )
            updated_annotations.append(existing_for_compartment)

    return updated_annotations, created_annotations, warnings


def batch(iterable, n=BATCH_SIZE):
    l = len(iterable)
    for ndx in range(0, l, n):
        yield iterable[ndx:min(ndx + n, l)]


def m_1(morphology: Resource, annotations, atlas_directory, download_directory, forge_atlas, contribution, generation) -> Tuple[str, Optional[Tuple[List[Resource], List[Resource], Dict, Dict, Optional[str]]], Optional[Exception]]:

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

        neurom_output, _ = _create_raw(
            morphology=morphology, download_directory=download_directory, forge=forge_data
        )

        annotation_dict_i = forge_data.as_json(updated_annotations + created_annotations)

        warnings = escape_ansi(warnings) if warnings.strip() else None  # do not add to warning_dicts empty warnings

        return m_id, (updated_annotations, created_annotations, neurom_output, annotation_dict_i, warnings), None

    except Exception as e:
        traceback.print_exc()
        return m_id, None, e


def create_update_annotations(
        forge_data: KnowledgeGraphForge,
        forge_atlas: KnowledgeGraphForge,
        morphologies: List[Resource],
        contribution: Dict,
        generation: Dict,
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

    annotations_update, annotations_create, log_dict = [], [], {}

    features_dict: Dict[str, Dict] = dict()
    annotations_dict: Dict[str, List[Union[Resource, Dict]]] = dict()

    logger.info("Building neuron morphology feature annotations")

    for i, morphology_batch in enumerate(batch(morphologies)):

        logger.info(f"{i*BATCH_SIZE}/{len(morphologies)}")

        # res = Pool().starmap(m_1, [(m_i, annotations, atlas_directory, download_directory, forge_atlas, contribution, generation) for m_i in morphology_batch])

        res = [m_1(m_i, annotations, atlas_directory, download_directory, forge_atlas, contribution, generation) for m_i in morphology_batch]

        for (m_id, a, ex) in res:

            if ex is not None:
                logger.error(f"Error with morphology {m_id}: {ex}")
                log_dict[m_id] = ex.args[0]
            else:
                updated_annotations, created_annotations, neurom_output, annotation_dict_i, warnings = a
                if warnings is not None:
                    log_dict[m_id] = warnings

                annotations_update.extend(updated_annotations)
                annotations_create.extend(created_annotations)

                annotations_dict[m_id] = annotation_dict_i
                features_dict[m_id] = neurom_output
                assert (all(not e._synchronized for e in updated_annotations))

    # logger.info("Validating")
    # forge_data.validate(data=annotations_update, type_="Annotation")
    # forge_data.validate(data=annotations_create, type_="Annotation")

    return annotations_update, annotations_create, features_dict, annotations_dict, log_dict


if __name__ == '__main__':
    parser = define_morphology_arguments(argparse.ArgumentParser())
    received_args, leftovers = parser.parse_known_args()
    org, project = received_args.bucket.split("/")
    output_dir = received_args.output_dir

    deployment, auth_token = authenticate_from_parser_arguments(received_args)

    limit = received_args.limit
    really_update = received_args.really_update == "yes"
    push_to_staging = received_args.push_to_staging == "yes"
    constrain = True

    logger.info(f"Neuron morphology feature annotations will be created/updated: {str(really_update)}")

    download_dir = os.path.join(output_dir, f"./files_{org}_{project}")
    dst_dir = os.path.join(output_dir, f"./{org}_{project}")
    atlas_dir = os.path.join(output_dir, "./atlas")

    os.makedirs(output_dir, exist_ok=True)
    os.makedirs(dst_dir, exist_ok=True)
    os.makedirs(atlas_dir, exist_ok=True)

    forge_data = allocate_by_deployment(org, project, token=auth_token, deployment=deployment)
    forge_atlas = allocate_by_deployment("bbp", "atlas", token=auth_token, deployment=deployment)

    morphologies = get_neuron_morphologies(curated=received_args.curated, forge=forge_data, limit=limit)

    generation = get_generation()

    if push_to_staging:
        forge_push = allocate_by_deployment(
            "dke", "kgforge", deployment=Deployment.STAGING, token=auth_token,
            es_view=DEFAULT_ES_VIEW,
            sparql_view=DEFAULT_SPARQL_VIEW
        )
        contribution = get_contribution(token=auth_token, deployment=Deployment.STAGING)
    else:
        forge_push = forge_data
        contribution = get_contribution(token=auth_token, deployment=deployment)

    annotations_to_update, annotations_to_create, features_dict, annotations_dict, log_dict = create_update_annotations(
        forge_data=forge_data,
        morphologies=morphologies,
        forge_atlas=forge_atlas,
        atlas_directory=atlas_dir,
        download_directory=download_dir,
        contribution=contribution,
        generation=generation
    )

    if really_update:
        logger.info("Updating data has been enabled")
        forge_data.update(annotations_to_update, schema_id=ANNOTATION_SCHEMA if constrain else None)
        forge_data.register(annotations_to_create, schema_id=ANNOTATION_SCHEMA if constrain else None)

        logger.info(
            f"{len(annotations_to_create)} annotations created, "
            f"{len(annotations_to_update)} annotations updated"
        )
    else:
        logger.info("Updating data has been disabled, only validating (if constrained)")

        if constrain:
            forge_data.validate(annotations_to_create, type_="Annotation")
            forge_data.validate(annotations_to_update, type_="Annotation")

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
