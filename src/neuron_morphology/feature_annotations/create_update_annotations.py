from typing import List, Union, Optional, Tuple, Dict

from datetime import datetime
import os
import json

from kgforge.core import Resource, KnowledgeGraphForge

from src.helpers import allocate, get_token, get_path

from src.neuron_morphology.feature_annotations import common
from src.neuron_morphology.creation_helpers import get_generation, get_contribution
from src.neuron_morphology.feature_annotations.data_classes.AnnotationTarget import AnnotationTarget
from src.neuron_morphology.feature_annotations.common import _get_morph_path, download_morphology_file, \
    _get_neuron_morphologies
from src.neuron_morphology.feature_annotations.morph_metrics import compute_metrics_default_atlas
from src.neuron_morphology.feature_annotations.morph_metrics_neurom import compute_metrics_neurom, \
    compute_metrics_neurom_raw

import pandas as pd
import re


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
def _create_raw(morphology: Resource, download_dir: str) -> Tuple[Dict, str]:
    morph_path = _get_morph_path(morphology, download_dir)
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
        forge_data: KnowledgeGraphForge,
        with_location: bool,
        atlas_dir: str,
        forge_atlas: KnowledgeGraphForge,
) -> Tuple[List[Resource], List[Resource], Union[str, pd.DataFrame]]:
    download_morphology_file(
        morphology=morphology, download_dir=download_directory, forge_data=forge_data
    )

    morph_path = _get_morph_path(morphology, download_directory)

    if with_location:
        annotations, warnings = compute_metrics_default_atlas(
            morphology_path=morph_path,
            atlas_download_directory=atlas_dir,
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
        org: str, project: str, with_location: bool,
        is_prod: bool, token: str, data_dir: str,
        update: bool, nm_id_list: Optional[List] = None,
) -> Tuple[
    List[Resource],
    List[Resource],
    Dict[str, Dict],
    Dict[str, List[Union[Resource, Dict]]],
    Dict[str, str]
]:
    forge_atlas = allocate("bbp", "atlas", is_prod, token)

    download_dir = os.path.join(data_dir, f"./files_{org}_{project}")
    atlas_dir = os.path.join(data_dir, "./raw/atlas")

    morphologies, forge_data = _get_neuron_morphologies(org, project, is_prod, token, nm_id_list)

    print("Retrieving neuron morphology feature annotations")

    annotations = dict(
        (
            r.id,
            #         encode_id_rev(r.id, r.rev),
            forge_data.search({
                "type": "NeuronMorphologyFeatureAnnotation",
                "hasTarget": {"hasSource": {"id": r.id}}})
        )
        for r in morphologies
    )

    generation = get_generation()
    contribution = get_contribution(token=token, production=is_prod)

    def get_annotations(morphology):
        # key = encode_id_rev(morphology.id, morphology._store_metadata._rev)
        return annotations[morphology.id]

    annotations_update, annotations_create, log_dict = [], [], {}

    features_dict: Dict[str, Dict] = dict()
    annotations_dict: Dict[str, List[Union[Resource, Dict]]] = dict()

    print("Building neuron morphology feature annotations")

    for i, morphology in enumerate(morphologies):
        if (i + 1) % 20 == 0:
            print(f"{i + 1}/{len(morphologies)}")

        try:
            updated_annotations, created_annotations, warnings = update_create_one(
                morphology=morphology,
                existing_annotations=get_annotations(morphology),
                download_directory=download_dir,
                forge_data=forge_data,
                forge_atlas=forge_atlas,
                with_location=with_location,
                atlas_dir=atlas_dir,
                generation=generation,
                contribution=contribution,
            )
            annotations_update.extend(updated_annotations)
            annotations_create.extend(created_annotations)

            neurom_output, _ = _create_raw(
                morphology=morphology, download_dir=download_dir
            )

            annotations_dict[morphology.id] = forge_data.as_json(
                updated_annotations + created_annotations
            )

            features_dict[morphology.id] = neurom_output
            if warnings.strip():  # do not add to warning_dicts empty warnings
                log_dict[morphology.id] = escape_ansi(warnings)
            assert (all(not e._synchronized for e in updated_annotations))
        except Exception as e:
            print(f"Error with morphology {morphology.id}: {e}")
            log_dict[morphology.id] = e.args[0]

    print("Validating")
    forge_data.validate(data=annotations_update, type_="Annotation")
    forge_data.validate(data=annotations_create, type_="Annotation")

    if update:
        print("Updating and creating")
        forge_data.update(annotations_update)
        forge_data.register(annotations_create)

    print(
        f"{len(annotations_create)} annotation created, "
        f"{len(annotations_update)} annotations updated"
    )

    return annotations_update, annotations_create, features_dict, annotations_dict, log_dict


if __name__ == '__main__':

    common.NEURON_MORPHOLOGY_RETRIEVAL_LIMIT = 10
    really_update = False  # Important if just testing

    checklist = [
        # ("public", "hippocampus", False),
        # ("public", "thalamus", False),
        ("bbp-external", "seu", True),
        # ("bbp", "mouselight", True),
        # ("public", "sscx", False)
    ]
    is_prod = True
    token = get_token(is_prod=is_prod, prompt=True)
    data_dir = get_path("./examples/data")
    dst_dir = get_path("./examples/attempts2")

    os.makedirs(data_dir, exist_ok=True)
    os.makedirs(dst_dir, exist_ok=True)

    for org, project, with_location_, in checklist:
        annotations_updated, annotations_created, features_dict, annotations_dict, log_dict = \
            create_update_annotations(
                org=org,
                project=project,
                with_location=with_location_,
                is_prod=is_prod,
                token=token,
                data_dir=data_dir,
                update=really_update,
            )

        timestamp = datetime.today().strftime('%Y%m%d_%Hh%M')
        fnames = [f"{dict_name}_{org}_{project}_{timestamp}.json" for dict_name in ['annotations', 'features', 'log']]
        for fname, dict_ in zip(fnames, [annotations_dict, features_dict, log_dict]):
            with open(os.path.join(dst_dir, fname), "w") as f:
                json.dump(dict_, f, indent=2)
