import os
import neurom as nm

from neurom.apps import morph_stats
from typing import Dict, List, Tuple

from neurom.features import NameSpace

from src.arguments import default_output_dir
from src.neuron_morphology.feature_annotations.data_classes.Annotation import Annotation
from src.neuron_morphology.feature_annotations.data_classes.AnnotationBody import AnnotationBody
from src.helpers import write_obj, get_path

from src.neuron_morphology.morphology_loading import load_morphology_with_neurom

dictionary_map = {
    NameSpace.NEURITE: nm.features._NEURITE_FEATURES,
    NameSpace.NEURON: nm.features._MORPHOLOGY_FEATURES,
    NameSpace.POPULATION: nm.features._POPULATION_FEATURES
}


def is_distribution(feature_name: str, feature_compartment: NameSpace):
    return bool(dictionary_map[feature_compartment][feature_name].shape)


def get_statistics_to_extract(feature_name: str, feature_compartment: NameSpace):
    return ["raw"] \
        if not is_distribution(feature_name, feature_compartment) \
        else ['min', 'max', 'median', 'mean', 'std']


METRIC_CONFIG = {
    'neurite': [
        'max_radial_distance',
        'number_of_sections',
        'number_of_bifurcations',
        'number_of_leaves',
        'total_length',
        'total_area',
        'total_volume',
        'section_lengths',
        'section_term_lengths',
        'section_bif_lengths',
        'section_branch_orders',
        'section_bif_branch_orders',
        'section_term_branch_orders',
        'section_path_distances',
        'section_taper_rates',
        'local_bifurcation_angles',
        'remote_bifurcation_angles',
        'partition_asymmetry',
        'partition_asymmetry_length',
        'sibling_ratios',
        'diameter_power_relations',
        'section_radial_distances',
        'section_term_radial_distances',
        'section_bif_radial_distances',
        'terminal_path_lengths',
        'section_volumes',
        'section_areas',
        'section_tortuosity',
        'section_strahler_orders'
    ],
    'morphology': [
        'soma_surface_area',
        'soma_radius',
        'max_radial_distance',
        'number_of_sections_per_neurite',
        'total_length_per_neurite',
        'total_area_per_neurite',
        'total_height',
        'total_width',
        'total_depth',
        'number_of_neurites'
    ],
    'neurite_type': ['AXON', 'BASAL_DENDRITE', 'APICAL_DENDRITE']
}

METRIC_CONFIG[NameSpace.NEURITE.value] = dict(
    (stat, get_statistics_to_extract(stat, NameSpace.NEURITE))
    for stat in METRIC_CONFIG[NameSpace.NEURITE.value]
)

METRIC_CONFIG[NameSpace.NEURON.value] = dict(
    (stat, get_statistics_to_extract(stat, NameSpace.NEURON))
    for stat in METRIC_CONFIG[NameSpace.NEURON.value]
)

neurite_types = list(map(lambda el: el.lower(), METRIC_CONFIG['neurite_type']))
morphology = "morphology"
soma = "soma"

neuroM_to_nexus_names = {
    "axon": "Axon",
    "basal_dendrite": "BasalDendrite",
    "apical_dendrite": "ApicalDendrite",
    "morphology": "NeuronMorphology",
    "soma": "Soma",
}

word_to_unit = {
    "distance": "μm",
    "length": "μm",
    "height": "μm",
    "width": "μm",
    "depth": "μm",
    "radius": "μm",
    "area": "μm²",
    "volume": "μm³",
    "angle": "radian",
    "radii": "μm",
    "extents": "μm",
    "azimuths": "radian",
    "elevations": "radian"
}

stat_to_full_name = {
    "mean": "mean",
    "median": "median",
    "min": "minimum",
    "max": "maximum",
    "std": "standard deviation",
    "raw": "raw"
}


# convert all the dictionary keys from underscore case to came case.
# this is for compatibility with JSON in general but also with property names in Nexus.

def u2c(s):
    temp = s.split('_')
    return temp[0] + ''.join(ele.title() for ele in temp[1:])


def u2l(s):
    return " ".join(ele.title() for ele in s.split('_'))


def get_unit(metric_name):
    return next(
        (unit for word, unit in word_to_unit.items() if word in metric_name.lower()),
        "dimensionless"
    )


def labelify(label_base, prepend_str):
    label = u2l(label_base)

    if label.lower() == "max radial distance":
        label = prepend_str + label

    return label


def _build_series(
        neurom_output: Dict[str, Dict],
        metric_name: str,
        compartment_metric_config: str,
        compartment_neurom_output: str
) -> List[Dict]:
    """
    @param compartment_metric_config: The name of the compartment in the extract_stats configuration
    @param compartment_neurom_output: The name of the compartment in the neurom output
    Both are the same in the case of morphology.
    They are different in the case of neurites. Ex: neurite is in the config,
    whereas "axon", "basal_dendrite", "apical_dendrite" is in the neurom output
    """
    def build_stat(stat_name) -> Dict:
        value = neurom_output[compartment_neurom_output][f"{stat_name}_{metric_name}"]

        return {
            "statistic": stat_to_full_name[stat_name],
            "unitCode": get_unit(metric_name),
            "value": float(value) if value is not None else None
        }

    return [
        build_stat(stat_name=stat_name)
        for stat_name in METRIC_CONFIG[compartment_metric_config][metric_name]
    ]

    # # Previous check, for single-valued metrics, would enforce std = 0 if min = max = mean..
    # if len(set([ei["value"] for ei in e])) == 1:
    #     f = [i for i, ei in enumerate(e) if ei["statistic"] == "standard deviation"]
    #
    #     if len(f) == 1:
    #         e[f[0]]["value"] = 0.0

    # return e


def _annotation_object(compartment_idx: str, body: List[AnnotationBody]) -> Annotation:
    return Annotation(
        type_=["Annotation", "NeuronMorphologyFeatureAnnotation"],
        compartment=neuroM_to_nexus_names[compartment_idx],
        has_body=body,
        name="Neuron Morphology Feature Annotation"
    )


def _neurite_feature_annotation(neurom_output: Dict[str, Dict], neurite_name: str) -> Annotation:
    annotation_body = [
        AnnotationBody(
            is_measurement_of=labelify(label_base=metric_name, prepend_str="Neurite "),
            series=_build_series(
                neurom_output=neurom_output,
                metric_name=metric_name,
                compartment_metric_config="neurite",
                compartment_neurom_output=neurite_name
            )
        )
        for metric_name in METRIC_CONFIG["neurite"]
    ]

    return _annotation_object(neurite_name, annotation_body)


def _soma_feature_annotation(neurom_output: Dict[str, Dict]) -> Annotation:
    soma_feature_annotation_body = [
        AnnotationBody(
            is_measurement_of=labelify(label_base=metric_name, prepend_str="Morphology "),
            series=_build_series(
                neurom_output=neurom_output,
                metric_name=metric_name,
                compartment_metric_config=morphology,
                compartment_neurom_output=morphology
            )
        )
        for metric_name in METRIC_CONFIG[morphology] if soma in metric_name.lower()
    ]

    return _annotation_object(soma, soma_feature_annotation_body)


def _morphology_feature_annotation(neurom_output: Dict[str, Dict]) -> Annotation:
    morph_feature_annotation_body = [
        AnnotationBody(
            is_measurement_of=labelify(label_base=metric_name, prepend_str="Morphology "),
            series=_build_series(
                neurom_output=neurom_output,
                metric_name=metric_name,
                compartment_metric_config=morphology,
                compartment_neurom_output=morphology
            )
        )
        for metric_name in METRIC_CONFIG[morphology] if soma not in metric_name.lower()
    ]

    return _annotation_object(morphology, morph_feature_annotation_body)


def annotations_per_compartment(neurom_output: Dict[str, Dict]) -> Dict[str, Dict]:
    ann_dict = dict(
        (
            neuroM_to_nexus_names[nt],
            Annotation.obj_to_dict(_neurite_feature_annotation(neurom_output, nt))
        )
        for nt in neurite_types
    )

    ann_dict[neuroM_to_nexus_names[morphology]] = Annotation.obj_to_dict(
        _morphology_feature_annotation(neurom_output)
    )
    ann_dict[neuroM_to_nexus_names[soma]] = Annotation.obj_to_dict(
        _soma_feature_annotation(neurom_output)
    )

    return ann_dict


def compute_metrics_neurom_raw(morphology_filepath: str) -> Tuple[Dict, str]:
    """
    Compute metrics of a neuron morphology. Returns the raw output of neurom
    """

    morph, captured = load_morphology_with_neurom(morphology_filepath, return_capture=True)
    stats = morph_stats.extract_stats(morph, METRIC_CONFIG)
    return stats, captured.stderr


def compute_metrics_neurom(morphology_filepath: str) -> Tuple[Dict[str, Dict], str]:
    """
    Compute metrics of a neuron morphology. Returns them as annotations.
    Annotations are returned indexed by the compartment they describe
    """
    metrics, warnings = compute_metrics_neurom_raw(morphology_filepath)
    return annotations_per_compartment(metrics), warnings


if __name__ == "__main__":

    # print(
    #     set(dictionary_map[NameSpace.NEURITE].keys()).symmetric_difference(
    #         METRIC_CONFIG[NameSpace.NEURITE.value].keys())
    # )
    # print(
    #     set(dictionary_map[NameSpace.NEURON].keys()).symmetric_difference(
    #         METRIC_CONFIG[NameSpace.NEURON.value].keys())
    # )

    # units = sorted(nm.features._NEURITE_FEATURES.keys()) + \
    #         sorted(nm.features._MORPHOLOGY_FEATURES.keys()) + \
    #         sorted(nm.features._POPULATION_FEATURES.keys())
    #
    # units_left = [e for e in units if all(i not in e for i in word_to_unit.keys())]
    # print(units_left)

    morphology_filename = "17302_00023.swc"
    label, _ = os.path.splitext(morphology_filename)

    data_dir = get_path("./data")
    dst_dir = default_output_dir()
    os.makedirs(dst_dir, exist_ok=True)

    morph_path = os.path.join(data_dir, f"swcs/{morphology_filename}")

    by_compartment, _ = compute_metrics_neurom(morph_path)

    write_obj(os.path.join(dst_dir, f"{label}_metric_neurom_compartment.json"), by_compartment)
