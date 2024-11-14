import os
from typing import Dict, List, Tuple
import numpy as np
from kgforge.core import KnowledgeGraphForge

from nrrd import NRRDHeader, read
from nptyping.ndarray import NDArray

from src.arguments import default_output_dir
from src.neuron_morphology.feature_annotations.data_classes.Annotation import Annotation
from src.neuron_morphology.feature_annotations.morph_metrics_dke import (
    compute_metrics_dke,
    compute_world_to_vox_mat,
    index_brain_region_labels,
    get_parcellation_volume_and_ontology
)
from src.neuron_morphology.feature_annotations.morph_metrics_neurom import compute_metrics_neurom
from src.helpers import write_obj, get_path


def _compute_metrics(
        volume_data: NDArray,
        world_to_vox_mat: np.matrix,
        morphology_path: str,
        brain_region_index: Dict[str, str]
) -> Tuple[List[Dict], str]:
    """
    Compute neurom and dke metrics using an atlas' volume data,
    its world to vox matrix (extracted from its volume metadata),
    brain region index (keys are brain region IDs and the values are brain region labels),
    and the path to the morphology's .swc file. These annotations are returned as an array
    """
    neuro_m_metrics_per_compartment, warnings = compute_metrics_neurom(morphology_path)

    neuro_m_metrics_per_compartment_res = dict(
        (key, Annotation.dict_to_obj(e)) for key, e in neuro_m_metrics_per_compartment.items()
    )

    dke_metrics, warnings = compute_metrics_dke(
        volume_data=volume_data,
        world_to_vox_mat=world_to_vox_mat,
        morphology_path=morphology_path,
        brain_region_index=brain_region_index,
        as_annotation_body=True
    )

    # Now we are merging those two sets of metrics into a single payload

    neuro_m_metrics_per_compartment_res["Soma"].add_annotation_body(
        dke_metrics["somaNumberOfPoints"]
    )

    for neurite_type, (section_regions, leaf_regions) in dke_metrics["neuriteFeature"].items():
        neuro_m_metrics_per_compartment_res[neurite_type].add_annotation_body(section_regions)
        neuro_m_metrics_per_compartment_res[neurite_type].add_annotation_body(leaf_regions)

    return (
        [Annotation.obj_to_dict(e) for e in list(neuro_m_metrics_per_compartment_res.values())],
        warnings
    )


def compute_metrics(
        volume_data: NDArray,
        volume_metadata: NRRDHeader,
        morphology_path: str,
        brain_region_index: Dict[str, str]
) -> Tuple[List[Dict], str]:

    """
    Compute neurom and dke metrics using an atlas' volume data, volume metadata,
    brain region index (keys are brain region IDs and the values are brain region labels),
    and the path to the morphology's .swc file. These annotations are returned as an array
    """
    world_to_vox_mat = compute_world_to_vox_mat(volume_metadata)

    return _compute_metrics(
        volume_data=volume_data, world_to_vox_mat=world_to_vox_mat,
        morphology_path=morphology_path, brain_region_index=brain_region_index
    )


def compute_metrics_default_atlas(
        morphology_path: str,
        forge_atlas: KnowledgeGraphForge,
        atlas_download_directory: str
) -> Tuple[List[Dict], str]:
    brain_region_index, volume_data, world_to_vox_mat = get_parcellation_volume_and_ontology(
        forge_atlas=forge_atlas, download_directory=atlas_download_directory
    )

    return _compute_metrics(
        volume_data=volume_data, world_to_vox_mat=world_to_vox_mat,
        morphology_path=morphology_path, brain_region_index=brain_region_index
    )


if __name__ == "__main__":

    morphology_filename = "17302_00023.swc"
    label, _ = os.path.splitext(morphology_filename)

    data_dir = get_path("./data")
    dst_dir = default_output_dir()
    os.makedirs(dst_dir, exist_ok=True)

    volume_path = os.path.join(data_dir, "atlas/annotation_25_ccf2017.nrrd")
    brain_region_onto_path = os.path.join(data_dir, "atlas/1.json")
    morph_path = os.path.join(data_dir, f"swcs/{morphology_filename}")

    v_data, v_metadata = read(volume_path)
    br_index = index_brain_region_labels(brain_region_onto_path)

    annotations = compute_metrics(
        volume_data=v_data, volume_metadata=v_metadata,
        morphology_path=morph_path, brain_region_index=br_index
    )

    os.makedirs(dst_dir, exist_ok=True)
    write_obj(os.path.join(dst_dir, f"{label}_metrics_all.json"), annotations)
