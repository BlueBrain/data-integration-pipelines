'''A backend that return a validation report for a given morphology. This script was originally shared here: https://bbpteam.epfl.ch/project/issues/browse/NSETM-1002'''

import os
from typing import Dict, Union, Any, List

import neurom as nm
from neurom import load_morphology
from neurom.check import morphology_checks
from src.neuron_morphology.validation import custom_validation
from neurom.core.morphology import Morphology

validation_report_callable = {
    'neurites': {
        'dangling_branch': lambda neuron: morphology_checks.has_no_dangling_branch(neuron).info,
        'root_node_jump': lambda neuron: morphology_checks.has_no_root_node_jumps(neuron).info,
        'z_jumps': lambda neuron: morphology_checks.has_no_jumps(neuron, axis='z').info,
        # has_no_radical_diameter_changes
        'narrow_start': lambda neuron: morphology_checks.has_no_narrow_start(neuron, frac=0.9).info,
        'fat_ends': lambda neuron: morphology_checks.has_no_fat_ends(neuron).info,
        'has_all_nonzero_segment_lengths': lambda neuron: morphology_checks.has_all_nonzero_segment_lengths(neuron).info,
        'narrow_neurite_section': lambda neuron: morphology_checks.has_no_narrow_neurite_section(neuron, neurite_filter=None).info,
    },

    'bifurcations': {
        'single_child': lambda neuron: custom_validation.has_no_single_child(neuron).info,
        'multifurcation': lambda neuron: custom_validation.has_no_multifurcation(neuron).info,
    },

    'dendrites': {
        'number_of_dendritic_trees_stemming_from_the_soma': lambda neuron: custom_validation.number_of_dendritic_trees_stemming_from_the_soma(neuron),
    },

    'axons': {
        'number_of_axons': lambda neuron: custom_validation.number_of_axons(neuron)
    },

    'additional_features': {
        'max_branch_order': lambda neuron: int(max(nm.features.get('section_branch_orders', neuron))),
        'total_section_length': lambda neuron: float(nm.features.get('total_length', neuron)),
        'max_section_length': lambda neuron: float(max(nm.features.get('section_lengths', neuron))),
    }
}


def _get_nested_check_names() -> List[str]:
    return [k_2 for _, v in validation_report_callable.items() for k_2, _ in v.items()]


def validation_report(neuron: Morphology) -> Dict[str, Dict[str, Any]]:
    '''Return the payload that will be sent back to the user'''
    return dict(
        (k, dict((check_name, to_call(neuron)) for check_name, to_call in sub_dictionary.items()))
        for k, sub_dictionary in validation_report_callable.items()
    )


def validation_report_complete(input_: Union[Dict, Morphology], is_report=False):
    '''Return the  payload that will be sent back to the user'''
    report: Dict[str, Dict[str, Any]] = input_ if is_report else validation_report(input_)
    for key, value in report.items():
        if key == 'neurites':
            for inkey, invalue in value.items():
                if invalue and not isinstance(invalue, int):
                    if inkey == 'has_all_nonzero_segment_lengths':
                        report[key][inkey] = list(map(lambda el: list(el), invalue))
                    else:
                        report[key][inkey] = list(map(lambda el: el[1][0].tolist(), invalue))
        elif key == 'bifurcations':
            for inkey, invalue in value.items():
                if invalue and not isinstance(invalue, int):
                    report[key][inkey] = list(map(lambda el: el[1].tolist(), invalue))
    return report

#         'dendrites': {
#             # Apical dendrite annotated for pyramidal cell = boolean (A)
#             'number_of_dendritic_trees_stemming_from_the_soma': [
#                 neurite for neurite in iter_neurites(neuron)
#                 if neurite.type in {NeuriteType.basal_dendrite, NeuriteType.apical_dendrite}],
#         },

#         'axons': {
#             'number_of_axons': [neurite for neurite in iter_neurites(neuron)
#                                     if neurite.type == NeuriteType.axon],
#             # Correct start point of the axon (or axons if there are 2) = boolean (A)
#         },

#         'additional_features': {
#             'max_branch_order': int(max(nm.features.get('section_branch_orders', neuron))),
#             'total_section_length': float(nm.features.get('total_length', neuron)[0]),
#             'max_section_length': float(max(nm.features.get('section_lengths', neuron))),
#         }


def get_tsv_header_columns():
    return ["filename"] + _get_nested_check_names()


def get_tsv_report_line(neuron_path, report=None):
    basename = os.path.basename(neuron_path)

    try:
        if report is None:
            morphology = load_morphology(neuron_path)
            report = validation_report(morphology)

        line_list = [basename] + [
            str(len(v_2)) if isinstance(v_2, list) else str(v_2)
            for k, v in report.items() for k_2, v_2 in v.items()
        ]

    except Exception as e:
        except_name = e.__class__.__name__
        line_list = [basename] + [except_name for _, v in report.items() for _, _ in v.items()]

    line_tsv = '\t'.join(line_list)
    return line_tsv
