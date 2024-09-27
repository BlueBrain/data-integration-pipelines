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
from voxcell import RegionMap

from src.helpers import write_obj, get_path
from src.pyswcparser.Morphology import Morphology
from src.pyswcparser.Parser import parse
from src.pyswcparser.Node import Node
from src.pyswcparser.SWC_NODE_TYPES import SWC_NODE_TYPES_BY_NAME

from src.neuron_morphology.feature_annotations.data_classes.AnnotationBody import AnnotationBody


ATLAS_RELEASE_ID = "https://bbp.epfl.ch/neurosciencegraph/data/4906ab85-694f-469d-962f-c0174e901885"


def compute_metrics_dke(
        volume_data: NDArray,
        world_to_vox_mat: np.matrix,
        morphology_path: str,
        brain_region_map: RegionMap,
        as_annotation_body=False
) -> Tuple[Dict[str, Union[Any, AnnotationBody]], str]:
    """
    Compute DKE metrics. Returned as a dictionary.
    The metrics used in NeuronMorphologyFeatureAnnotation are indexed by "somaNumberOfPoints"
    and "neuriteFeature". They can be formatted to be of type AnnotationBody if as_annotation_body
    is set to True.
    """

    with open(morphology_path, "r") as f:
        morph = parse(f.read())

    with io.capture_output() as captured:
        general_metrics = {
            "somaNumberOfPoints": compute_soma_number_of_points(morphology=morph),
            "neuriteFeature": _compute_section_leaf_regions(
                morphology=morph, volume_data=volume_data,
                world_to_voxel_mat=world_to_vox_mat,
                brain_region_map=brain_region_map
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


# In the final schemas, the types "nsg:xxx" are going to be used
swc_node_types_to_nsg = {
    SWC_NODE_TYPES_BY_NAME.UNDEFINED: "nsg:UndefinedNeurite",
    # just for compliance with SWC spec, not going to be used here
    SWC_NODE_TYPES_BY_NAME.SOMA: "nsg:Soma",
    SWC_NODE_TYPES_BY_NAME.AXON: "nsg:Axon",
    SWC_NODE_TYPES_BY_NAME.BASAL_DENDRITE: "nsg:BasalDendrite",
    SWC_NODE_TYPES_BY_NAME.APICAL_DENDRITE: "nsg:ApicalDendrite",
    SWC_NODE_TYPES_BY_NAME.CUSTOM: "nsg:CustomNeurite",
    # just for compliance with SWC spec, not going to be used here
}


def init_thing(s_type: str, s_type_name: str) -> Dict[str, Any]:
    return {
        "type": swc_node_types_to_nsg[s_type],
        "name": f"{s_type_name} features",
        # Name is required due to the collection schema which is
        # imported by the neuron morphology schema
        "traversedBrainRegion": defaultdict(int),
        "projectionBrainRegion": defaultdict(int),
        "outsideBrain": False,
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
    return f"Voxel position {p_voxel_v4}) outside of boundingBox with shape {volume_data.shape}"


def p_inside_volume(volume_data: NDArray, p_voxel_v4: np.ndarray):
    return volume_data.shape[0] > int(p_voxel_v4[0]) and \
        volume_data.shape[1] > int(p_voxel_v4[1]) and \
        volume_data.shape[2] > int(p_voxel_v4[2])


def node_to_world_voxel_position(
        node: Node, world_to_voxel_mat: np.matrix
) -> Tuple[np.ndarray, np.ndarray]:
    # the position as a list [x, y, z]
    p = node.get_position()
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


def increment_metric(s_node, metric, world_to_voxel_mat, volume_data, brain_region_map):
    def get_volume_data(p_voxel):
        return int(volume_data[int(p_voxel[0]), int(p_voxel[1]), int(p_voxel[2])])

    outside = False

    p_world_v4, p_voxel_v4 = node_to_world_voxel_position(s_node, world_to_voxel_mat)
    if p_inside_volume(volume_data, p_voxel_v4):
        volume_value = get_volume_data(p_voxel_v4)
        if brain_region_map.find(volume_value, attr="id"):
            metric[volume_value] = metric.get(volume_value, 0) + 1
        else:
            outside = True
            print(warning_unknown_brain_region(volume_value, p_world_v4, p_voxel_v4))
    else:
        outside = True
        print(warning_outside_bounds(p_voxel_v4, volume_data))

    return outside


def compute_section_leaf_regions(
        morph: Morphology,
        volume_data: NDArray,
        volume_metadata: NRRDHeader,
        brain_region_map: RegionMap
):
    world_to_voxel_mat = compute_world_to_vox_mat(volume_metadata)
    return _compute_section_leaf_regions(morph, brain_region_map, world_to_voxel_mat, volume_data)


def _compute_section_leaf_regions(
        morphology: Morphology,
        brain_region_map: RegionMap,
        world_to_voxel_mat: np.matrix,
        volume_data: NDArray
):
    unique_node_control = set()

    # we compute the contribution of each section to the final metrics
    sections = morphology.get_sections()

    section_types = dict((section.get_type(), section.get_type_name()) for section in sections)

    # a structure to gather neurite information
    morph_metrics_per_neurite_types = dict(
        (s_type, init_thing(s_type, s_type_name))
        for s_type, s_type_name in section_types.items()
    )

    for section in sections:
        s_type = section.get_type()
        s_size = section.get_size()
        s_nodes = section.get_nodes()
        s_is_projection = len(section.get_children()) == 0

        metrics = morph_metrics_per_neurite_types[s_type]

        # adding the size of the current section
        metrics["cumulatedLength"]["value"] = metrics["cumulatedLength"]["value"] + s_size

        # Lookup every point of this section in the annotation volume
        for node in s_nodes:
            if node in unique_node_control:
                continue
            unique_node_control.add(node)
            increment_metric(node, metrics["traversedBrainRegion"],
                world_to_voxel_mat, volume_data, brain_region_map)

        if s_is_projection:
            outside_brain = increment_metric(s_nodes[-1], metrics["projectionBrainRegion"],
                world_to_voxel_mat, volume_data, brain_region_map)
            if outside_brain:
                metrics["outsideBrain"] = outside_brain

    def reformat(metrics_i):
        for key in ["projectionBrainRegion", "traversedBrainRegion"]:
            metrics_i[key] = list(
                {"brainRegion": {
                    "@id": f"http://api.brain-map.org/api/v2/data/Structure/{str(reg_id)}"},
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
        if neurite_type != SWC_NODE_TYPES_BY_NAME.SOMA
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


def compute_soma_number_of_points(morphology: Morphology) -> int:
    # Computing the number of points for the soma
    # For this, we need to check inside all the 'soma sections' and add soma nodes to a Set.
    # (A single node can be part of multiple sections, so a Set guarantees uniqueness)

    unique_soma_nodes = set()
    soma_sections = morphology.get_sections_by_type(SWC_NODE_TYPES_BY_NAME.SOMA)
    for section in soma_sections:
        for node in section.get_nodes():
            unique_soma_nodes.add(node)

    return len(unique_soma_nodes)


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
) -> Tuple[RegionMap, NDArray, np.matrix]:

    atlas_release = forge_atlas.retrieve(ATLAS_RELEASE_ID)

    if atlas_release is None:
        raise Exception("Atlas release not found")

    parcellation_volume = forge_atlas.retrieve(atlas_release.parcellationVolume.id)
    if parcellation_volume is None:
        raise Exception("Parcellation volume not found")

    forge_atlas.download(
        parcellation_volume, "distribution.contentUrl", download_directory, overwrite=True
    )

    volume_p = f"{download_directory}/{parcellation_volume.distribution.name}"
    volume_data, volume_metadata = nrrd.read(volume_p)

    world_to_vox_mat = compute_world_to_vox_mat(volume_metadata)

    ontology = forge_atlas.retrieve(atlas_release.parcellationOntology.id)

    if ontology is None:
        raise Exception("Ontology not found")

    ontology_distribution = next(
        d for d in ontology.distribution if d.encodingFormat == "application/json"
    )

    forge_atlas.download(ontology_distribution, "contentUrl", download_directory, overwrite=True)

    brain_region_onto = f"{download_directory}/{ontology_distribution.name}"

    brain_region_map = RegionMap.load_json(brain_region_onto)

    return brain_region_map, volume_data, world_to_vox_mat


if __name__ == "__main__":
    with cProfile.Profile() as pr:

        data_dir = get_path("./examples/data/src")
        dst_dir = get_path("./examples/attempts")

        volume_path = os.path.join(data_dir, "ccfv3_annotation_25.nrrd")
        brain_region_onto_path = os.path.join(data_dir, "1.json")
        morph_path = os.path.join(data_dir, "17302_00023.swc")

        v_data, v_metadata = nrrd.read(volume_path)

        br_map = RegionMap.load_json(brain_region_onto_path)

        annotations, warnings = compute_metrics_dke(
            volume_data=v_data, world_to_vox_mat=compute_world_to_vox_mat(v_metadata),
            morphology_path=morph_path, brain_region_map=br_map,
            as_annotation_body=False
        )
        write_obj(os.path.join(dst_dir, "17302_00023_metrics_dke.json"), annotations)

        pstats.Stats(pr).sort_stats(pstats.SortKey.CUMULATIVE).print_stats(10)
