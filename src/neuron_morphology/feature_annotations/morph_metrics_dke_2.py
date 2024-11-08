"""
This scripts opens a neuron morphology file (SWC) as well as a brain annotation file (NRRD)
to look up the brain regions being traversed by the morphology.
The result is saved in a JSON file at the very end of the main() function.
If we were to make this script run along with Nexus Forge to automatically extract data and
push them to Nexus, it is possible to fetch the annotation volume because it is linked to the
'atlasRelease' of each morphology.
Then, if we were to process multiple morphologies using the same volume, it would be optimal
to load the NRRD file only once and reuse its data and metadata for each morph
(the NRRD opening takes a few seconds and the morph metrics takes a few ms)

The library 'pyswcparser' is in this same repo, in the folder 'other_tools'. It should be installed
with the command 'pip install .' or 'pip install -e .'
"""
from collections import defaultdict
from typing import List, Dict, Tuple, Any, Union

import numpy
import numpy as np
import json
import os
import nrrd
import cProfile
import pstats

from IPython.utils import io
from kgforge.core import KnowledgeGraphForge
from nptyping.ndarray import NDArray
from nrrd import NRRDHeader

from src.helpers import write_obj, get_path

from neurom import NeuriteType
from neurom.core.morphology import Morphology as NeuromMorphology, Section

from src.logger import logger
from src.neuron_morphology.feature_annotations.data_classes.AnnotationBody import AnnotationBody
from src.neuron_morphology.morphology_loading import load_morphology_with_neurom
from src.neuron_morphology.section_type_labels import neurite_type_to_ontology_term, neurite_type_to_name

ATLAS_RELEASE_ID = "https://bbp.epfl.ch/neurosciencegraph/data/4906ab85-694f-469d-962f-c0174e901885"


def compute_metrics_dke(
        volume_data: NDArray,
        world_to_vox_mat: np.matrix,
        morphology_path: str,
        brain_region_index: Dict[str, str],
        as_annotation_body=False
) -> Tuple[Dict[str, Union[Any, AnnotationBody]], str]:
    """
    Compute DKE metrics. Returned as a dictionary.
    The metrics used in NeuronMorphologyFeatureAnnotation are indexed by "somaNumberOfPoints"
    and "neuriteFeature". They can be formatted to be of type AnnotationBody if as_annotation_body
    is set to True.
    """

    morph: NeuromMorphology = load_morphology_with_neurom(morphology_path)

    with io.capture_output() as captured:
        general_metrics = {
            "somaNumberOfPoints": compute_soma_number_of_points(morphology=morph),
            "neuriteFeature": _compute_section_leaf_regions(
                morphology=morph, volume_data=volume_data,
                world_to_voxel_mat=world_to_vox_mat,
                brain_region_index=brain_region_index
            )
        }

        if as_annotation_body:
            general_metrics = {
                "somaNumberOfPoints": output_soma_number_of_point_as_annotation_body(
                    general_metrics["somaNumberOfPoints"]
                ),
                "neuriteFeature": output_section_leaf_regions_as_annotation_body(
                    general_metrics["neuriteFeature"]
                )
            }

    return general_metrics, captured.stderr


def initialize_object_per_section_type(section_type: NeuriteType) -> Dict[str, Any]:
    ontology_term_for_type = neurite_type_to_ontology_term(section_type)
    name_for_type = f"{neurite_type_to_name(section_type)} features"

    return {
        "type": ontology_term_for_type,
        "name": name_for_type,
        # Name is required due to the collection schema which is
        # imported by the neuron morphology schema
        "traversedBrainRegion": defaultdict(int),
        "projectionBrainRegion": defaultdict(int),
        "cumulatedLength": {
            "value": 0,
            "unitCode": "μm"
        },
        "longestBranchLength": {
            "value": 0,
            "unitCode": "μm"
        },
        "longestBranchNumberOfNodes": 0,
        "numberOfProjections": 0,
    }


def warning_unknown_brain_region(volume_value, p_world_v4: np.ndarray, p_voxel_v4: np.ndarray):
    return f"💥 Unknown brain region {volume_value} at world position {p_world_v4} " \
           f"(voxel position {p_voxel_v4})",


def warning_outside_bounds(p_voxel_v4: np.ndarray, volume_data: NDArray):
    return f"Voxel position {p_voxel_v4}) outside of boundingbox with shape {volume_data.shape}"


def p_inside_volume(volume_data: NDArray, p_voxel_v4: np.ndarray):
    return volume_data.shape[0] > int(p_voxel_v4[0]) and \
        volume_data.shape[1] > int(p_voxel_v4[1]) and \
        volume_data.shape[2] > int(p_voxel_v4[2])


def point_to_world_voxel_position(
        p: numpy.ndarray, world_to_voxel_mat: np.matrix
) -> Tuple[np.ndarray, np.ndarray]:
    # the position as a list [x, y, z]
    # the position as a homogeneous-coordinate compliant vector, in Numpy format
    p_world = np.array([p[0], p[1], p[2], 1])
    # converted into voxel space with each element rounded to make sure we address
    # integer voxels ( this is given as a 2D matrix, this explains the ending .tolist()[0] )
    p_voxel = np.round(p_world * world_to_voxel_mat).tolist()[0]
    return p_world, p_voxel


def compute_world_to_vox_mat(volume_metadata: NRRDHeader):
    # preparing the metadata to look up the volume using world positions
    space_origin = volume_metadata["space origin"]
    space_direction = volume_metadata["space directions"]

    # Using homogeneous coordinates to leverage some Numpy help
    voxel_to_world_mat = np.matrix([
        [space_direction[0][0], space_direction[0][1], space_direction[0][2], space_origin[0]],
        [space_direction[1][0], space_direction[1][1], space_direction[1][2], space_origin[1]],
        [space_direction[2][0], space_direction[2][1], space_direction[2][2], space_origin[2]],
        [0, 0, 0, 1]
    ])

    # This affine transformation is the one we'll be using to convert the world-coordinate points
    # from the morphology into the voxel coordinates usable in the nrrd file
    world_to_voxel_mat = voxel_to_world_mat.I

    return world_to_voxel_mat


def compute_section_leaf_regions(
        morph: NeuromMorphology,
        volume_data: NDArray,
        volume_metadata: NRRDHeader,
        brain_region_index: Dict[str, str]
):
    world_to_voxel_mat = compute_world_to_vox_mat(volume_metadata)
    return _compute_section_leaf_regions(morph, brain_region_index, world_to_voxel_mat, volume_data)


def _compute_section_leaf_regions(
        morphology: NeuromMorphology,
        brain_region_index: Dict[str, str],
        world_to_voxel_mat: np.matrix,
        volume_data: NDArray
):
    def get_volume_data(p_voxel):
        return int(volume_data[int(p_voxel[0]), int(p_voxel[1]), int(p_voxel[2])])

    # we compute the contribution of each section to the final metrics
    sections: List[Section] = morphology.sections

    # a structure to gather neurite information
    morph_metrics_per_neurite_types: Dict[NeuriteType, Dict] = dict(
        (section_type, initialize_object_per_section_type(section_type))
        for section_type in set(s.type for s in sections)
    )

    unique_node_control = set()

    for section in sections:

        section_points: numpy.ndarray = section._morphio_section.points
        # each element in also a ndarray of len 3 -> coordinates of points

        metrics = morph_metrics_per_neurite_types[section.type]

        # adding the size of the current section
        metrics["cumulatedLength"]["value"] = metrics["cumulatedLength"]["value"] + section.length

        # Lookup every point of this section in the annotation volume
        for node in section_points:
            if tuple(node) in unique_node_control:
                continue

            unique_node_control.add(tuple(node))

            p_world_v4, p_voxel_v4 = point_to_world_voxel_position(node, world_to_voxel_mat)

            if p_inside_volume(volume_data, p_voxel_v4):

                volume_value = get_volume_data(p_voxel_v4)

                if volume_value in brain_region_index:
                    metrics["traversedBrainRegion"][volume_value] += 1
                else:
                    logger.warning(warning_unknown_brain_region(volume_value, p_world_v4, p_voxel_v4))
            else:
                logger.warning(warning_outside_bounds(p_voxel_v4, volume_data))

        s_is_projection = len(section.children) == 0

        if s_is_projection:
            p_world_v4, p_voxel_v4 = point_to_world_voxel_position(section_points[-1], world_to_voxel_mat)

            if p_inside_volume(volume_data, p_voxel_v4):
                volume_value = get_volume_data(p_voxel_v4)

                if volume_value in brain_region_index:
                    metrics["projectionBrainRegion"][volume_value] += 1
                else:
                    logger.warning(warning_unknown_brain_region(volume_value, p_world_v4, p_voxel_v4))
            else:
                logger.warning(warning_outside_bounds(p_voxel_v4, volume_data))

    def reformat(metrics_i):

        for key in ["projectionBrainRegion", "traversedBrainRegion"]:
            metrics_i[key] = list(
                {
                    "brainRegion": {
                        "@id": f"http://api.brain-map.org/api/v2/data/Structure/{str(reg_id)}"
                    },
                    "count": count
                }
                for reg_id, count in metrics_i[key].items()
            )

        return metrics_i

    # This is going to be in the final schema, per neurite type (except for soma
    # for what comes next, we don't deal with soma metrics
    neurite_feature = [
        reformat(metrics)
        for neurite_type, metrics in morph_metrics_per_neurite_types.items()
        if neurite_type != NeuriteType.soma
    ]

    # Disabled so far because unused and time-consuming

    # Compute the number of projections per non-soma type
    # for node in morph.get_ending_nodes():
    #     morph_metrics_per_neurite_types[node.get_type()]["numberOfProjections"] += 1
    #
    # # Compute the longest continuous branch per type
    # branches_per_type = morph.get_longest_branches_per_type()
    #
    # for branch_type in branches_per_type:
    #     branch_metrics = branches_per_type[branch_type]
    #     # nsg_type = swc_node_types_to_nsg[branch_type]
    #     # print(morph_metrics_per_neurite_types)
    #     morph_metrics_per_neurite_types[branch_type]["longestBranchLength"]["value"] = \
    #         branch_metrics["size"]
    #     morph_metrics_per_neurite_types[branch_type]["longestBranchNumberOfNodes"] = \
    #         len(branch_metrics["nodes"])

    return neurite_feature


def output_section_leaf_regions_as_annotation_body(
        section_leaf_regions: List[Dict]
) -> Dict[str, Tuple[AnnotationBody, AnnotationBody]]:
    def _to_annotation_body(nf):
        compartment = nf["type"].split(":").pop()

        section_regions = AnnotationBody(
            is_measurement_of="Section Regions",
            series=nf["traversedBrainRegion"]
        )

        leaf_regions = AnnotationBody(
            is_measurement_of="Leaf Regions",
            series=nf["projectionBrainRegion"]
        )

        return compartment, (section_regions, leaf_regions)

    return dict(_to_annotation_body(nf) for nf in section_leaf_regions)


def compute_soma_number_of_points(morphology: NeuromMorphology) -> int:
    return len(morphology.soma.points)


def output_soma_number_of_point_as_annotation_body(nb: int) -> AnnotationBody:
    return AnnotationBody(
        is_measurement_of="Soma Number Of Points",
        series=[
            {
                "statistic": "N",
                "unitCode": "dimensionless",
                "value": float(nb)
            }
        ]
    )


def index_brain_region_labels(br_ontology: str) -> Dict[str, str]:
    """
  This function is in charge of making a flat index of brain regions
  in a key-value form (dictionary) where the keys are brain region IDs
  and the values are brain region labels
  """
    # onto_str = open(br_ontology).read()
    with open(br_ontology, "r") as f:
        node_stack = json.loads(f.read())["msg"]

    index = {}

    while len(node_stack):
        node = node_stack.pop()
        node_stack = node_stack + node["children"]
        index[node["id"]] = node["name"]

    return index


# TODO use the same function as quality metrics: src/get_atlas.py
def get_parcellation_volume_and_ontology(
        download_directory: str,
        forge_atlas: KnowledgeGraphForge
) -> Tuple[Dict[str, str], NDArray, np.matrix]:
    atlas_release = forge_atlas.retrieve(ATLAS_RELEASE_ID)

    if atlas_release is None:
        raise Exception("Atlas release not found")

    parcellation_volume = forge_atlas.retrieve(atlas_release.parcellationVolume.id)
    if parcellation_volume is None:
        raise Exception("Parcellation volume not found")

    forge_atlas.download(
        parcellation_volume, "distribution.contentUrl", download_directory, overwrite=True
    )

    volume_path = f"{download_directory}/{parcellation_volume.distribution.name}"
    volume_data, volume_metadata = nrrd.read(volume_path)

    world_to_vox_mat = compute_world_to_vox_mat(volume_metadata)

    ontology = forge_atlas.retrieve(atlas_release.parcellationOntology.id)

    if ontology is None:
        raise Exception("Ontology not found")

    ontology_distribution = next(
        d for d in ontology.distribution if d.encodingFormat == "application/json"
    )

    forge_atlas.download(ontology_distribution, "contentUrl", download_directory, overwrite=True)

    brain_region_onto_path = f"{download_directory}/{ontology_distribution.name}"

    brain_region_index = index_brain_region_labels(brain_region_onto_path)

    return brain_region_index, volume_data, world_to_vox_mat


if __name__ == "__main__":
    with cProfile.Profile() as pr:
        data_dir = get_path("./data/atlas")
        dst_dir = get_path("./examples/attempts")

        volume_path = os.path.join(data_dir, "annotation_25_ccf2017.nrrd")
        brain_region_onto_path = os.path.join(data_dir, "1.json")

        morph_path = os.path.join("swcs", "17302_00023.swc")

        v_data, v_metadata = nrrd.read(volume_path)

        br_index = index_brain_region_labels(brain_region_onto_path)

        annotations, warnings = compute_metrics_dke(
            volume_data=v_data, world_to_vox_mat=compute_world_to_vox_mat(v_metadata),
            morphology_path=morph_path, brain_region_index=br_index,
            as_annotation_body=False
        )
        write_obj(os.path.join(dst_dir, "17302_00023_metrics_dke.json"), annotations)

        pstats.Stats(pr).sort_stats(pstats.SortKey.CUMULATIVE).print_stats(10)
