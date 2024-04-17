'''A backend that return a validation report for a given morphology. This script was originally shared here: https://bbpteam.epfl.ch/project/issues/browse/NSETM-1002'''

import os
from typing import Dict, Union, Any, List, Callable, Optional, Tuple

import neurom as nm
from IPython.utils import io
from neurom import load_morphology
from neurom.check import morphology_checks, CheckResult
from src.neuron_morphology.validation import custom_validation
from neurom.core.morphology import Morphology
from morphio import ostream_redirect


class Check:
    def __init__(
            self,
            callable_: Callable,
            label: str, id_: str,
            value_in_json: Callable,
            value_in_tsv: Tuple[Callable, Optional[bool]],
            unit_code: Optional[str] = None,
            example_failure: Optional[List] = None,
    ):
        self.callable_ = callable_
        self.label = label
        self.id_ = id_
        self.value_in_json = value_in_json
        self.unit_code = unit_code
        self.example_failure = example_failure
        self.value_in_tsv = value_in_tsv

    def format_as_json_value(self, neuron_path, k, k_2, output_of_callable):

        if self.value_in_json is not None:
            return self.value_in_json(neuron_path, k, k_2, output_of_callable)

        if not isinstance(output_of_callable, CheckResult):
            return output_of_callable

        if output_of_callable.status is True:
            return True

        return output_of_callable.info if output_of_callable.info is not None else output_of_callable.status

    def format_as_tsv_value(self, neuron_path, k, k_2, output_of_callable):
        res = self.value_in_tsv[0](neuron_path, k, k_2, output_of_callable)
        expected_value = self.value_in_tsv[1]
        if isinstance(expected_value, bool):
            if str(expected_value) != res:
                print(
                    f"For check \"{validation_report_checks[k][k_2].label}\" expected value: {expected_value}, "
                    f"obtained {res} for {neuron_path}"
                )
        return res

    def run(self, neuron):
        with io.capture_output() as captured:
            with ostream_redirect(stdout=True, stderr=True):
                return self.callable_(neuron)

    @staticmethod
    def basic_tsv(neuron_path, k_1, k_2, x):
        if x.status is not None:
            return str(x.status)

        if x.info:
            return str(len(x.info))

        return str(x.status)

    @staticmethod
    def basic_json(neuron_path, k_1, k_2, x):

        if not isinstance(x, CheckResult):
            return x

        if x.status is True:
            return True

        return x.info if x.info is not None else x.status

    @staticmethod
    def basic_numeric(neuron_path, k_1, k_2, x):
        return str(x)


validation_report_checks = {
    'neurites': {
        'has_no_dangling_branch': Check(
            id_="https://neuroshapes.org/danglingBranchMetric",
            label="Has No Dangling Branch",
            callable_=lambda neuron: morphology_checks.has_no_dangling_branch(neuron),
            value_in_json=lambda neuron_path, k_1, k_2, x: x.status if x.status is True else  list({
                "root_node_id": root_node_id,
                "point": [float(i) for i in point[0]],
              } for root_node_id, point in x.info),
            example_failure=['dangling_axon.swc'],
            value_in_tsv=(Check.basic_tsv, True)

        ),
        'has_no_root_node_jump': Check(
            id_="https://neuroshapes.org/rootNodeJumpMetric",
            label="Has No Root Node Jump",
            value_in_json=lambda neuron_path, k_1, k_2, x: x.status if x.status is True else list({
                "root_node_id": root_node_id,
                "point": [float(i) for i in point[0]],
              } for root_node_id, point in x.info),
            callable_=lambda neuron: morphology_checks.has_no_root_node_jumps(neuron),
            example_failure=['root_node_jump.swc'],
            value_in_tsv=(Check.basic_tsv, True)

        ),
        'has_no_z_jumps': Check(
            id_="https://neuroshapes.org/zJumpMetric",
            label="Has No Z Jump",
            value_in_json=lambda neuron_path, k_1, k_2, x: x.status if x.status is True else list({
                "section_id": section_id,
                "from": list(float(i) for i in from_),
                "to": list(float(i) for i in to_)
              } for section_id, (from_, to_) in x.info),
            callable_=lambda neuron: morphology_checks.has_no_jumps(neuron, axis='z'),
            example_failure=['z_jump.swc'],
            value_in_tsv=(Check.basic_tsv, True)

        ),
        # has_no_radical_diameter_changes
        'has_no_narrow_start': Check(
            id_="https://neuroshapes.org/narrowStartMetric",
            label="Has No Narrow Start",
            value_in_json=lambda neuron_path, k_1, k_2, x: x.status if x.status is True else list({
                "root_node_id": root_node_id,
                "root_node_points": [[float(ip) for ip in p] for p in root_node_points],
              } for (root_node_id, root_node_points) in x.info),
            callable_=lambda neuron: morphology_checks.has_no_narrow_start(neuron, frac=0.9),
            example_failure=['narrow_start.swc'],
            value_in_tsv=(Check.basic_tsv, True)
        ),
        'has_no_fat_ends': Check(
            id_="https://neuroshapes.org/fatEndMetric",
            label="Has No Fat Ends",
            value_in_json=lambda neuron_path, k_1, k_2, x: list({
                "leaf_id": leaf_id,
                "leaf_points": [list(float(a) for a in el) for el in leaf_points],
              } for (leaf_id, leaf_points) in x.info),
            callable_=lambda neuron: morphology_checks.has_no_fat_ends(neuron),
            example_failure=['fat_end.swc'],
            value_in_tsv=(Check.basic_tsv, True)

        ),
        'has_all_nonzero_segment_lengths': Check(
            id_="https://neuroshapes.org/zeroLengthSegmentMetric",
            label="Has all nonzero segment lengths",
            value_in_json=lambda neuron_path, k_1, k_2, x: x.status if x.status is True else list({
                "sectionId": i[0],
                "segmentId": i[1]
              } for i in x.info),
            callable_=lambda neuron: morphology_checks.has_all_nonzero_segment_lengths(neuron),
            example_failure=[
                'Neuron_zero_length_segments.swc',
                'Single_apical.swc',
                'Single_basal.swc',
                'Single_axon.swc',
            ],
            value_in_tsv=(Check.basic_tsv, True)

        ),
        "has_all_nonzero_neurite_radii": Check(
            id_="todo",  # TODO
            label="Has all non zero neurite radii",
            value_in_json=None,
            callable_=lambda m: morphology_checks.has_all_nonzero_neurite_radii(m),
            example_failure=['Neuron_zero_radius.swc'],
            value_in_tsv=(Check.basic_tsv, True)
        ),
        "has_all_nonzero_section_lengths": Check(
            id_="todo",  # TODO
            label="Has all non-zero section lengths",
            value_in_json=None,
            callable_=lambda m: morphology_checks.has_all_nonzero_section_lengths(m),
            example_failure=['Neuron_zero_length_sections.swc'],
            value_in_tsv=(Check.basic_tsv, True)
        ),
        'has_no_narrow_neurite_section': Check(
            id_="https://neuroshapes.org/narrowNeuriteSectionMetric",
            label="Has no narrow Neurite Section",
            value_in_json=lambda neuron_path, k_1, k_2, x: x.status if x.status is True else list({
                "valueX": i[0],
                "valueY": i[1],
                "valueZ": i[2],
                "neuriteDiameter": {
                    "value": i[3],
                    "unitCode": "μm"
                },
                "type": "Vector3D"
              } for i in x.info),
            callable_=lambda neuron: morphology_checks.has_no_narrow_neurite_section(neuron, neurite_filter=None),
            example_failure=[],  # TODO
            value_in_tsv=(Check.basic_tsv, True)
        ),
        "has_no_flat_neurites": Check(
            id_="todo",   # TODO
            label="Has no flat neurites",
            value_in_json=Check.basic_json,
            callable_=lambda m: morphology_checks.has_no_flat_neurites(m, 1e-6, method="tolerance"),
            example_failure=['Neuron-flat.swc'],
            value_in_tsv=(Check.basic_tsv, True)
        )
    },
    'bifurcations': {
        'has_unifurcation': Check(
            id_="todo",  # TODO
            value_in_json=lambda neuron_path, k_1, k_2, x: x.status if x.status is True else len(x.info),
            label="Has Unifurcation",
            callable_=lambda neuron: morphology_checks.has_unifurcation(neuron),
            example_failure=["unifurcation.asc"],
            value_in_tsv=(Check.basic_tsv, True)
        ),
        'has_multifurcation': Check(
            id_="todo",  # TODO
            value_in_json=lambda neuron_path, k_1, k_2, x: x.status if x.status is True else len(x.info),
            label="Has Multifurcation",
            callable_=lambda neuron: morphology_checks.has_multifurcation(neuron),
            example_failure=["multifurcation.asc"],
            value_in_tsv=(Check.basic_tsv, True)
        ),
    },
    "soma": {
        "has_nonzero_soma_radius": Check(
            id_="todo",  # TODO
            label="Has nonzero soma radius",
            value_in_json=lambda neuron_path, k_1, k_2, x: x.status,
            callable_=lambda m: morphology_checks.has_nonzero_soma_radius(m),
            example_failure=['soma_zero_radius.swc'],
            value_in_tsv=(Check.basic_tsv, True)
        ),
    },
    'dendrites': {
        "has_apical_dendrite": Check(
            id_="todo",  # TODO
            label="Has apical dendrite",
            value_in_json=lambda neuron_path, k_1, k_2, x: x.status,
            callable_=lambda m: morphology_checks.has_apical_dendrite(m),
            example_failure=['Single_axon.swc', 'Single_basal.swc'],
            value_in_tsv=(Check.basic_tsv, True)
        ),
        "has_basal_dendrite": Check(
            id_="todo",  # TODO
            label="Has basal dendrite",
            value_in_json=lambda neuron_path, k_1, k_2, x: x.status,
            callable_=lambda m: morphology_checks.has_basal_dendrite(m),
            example_failure=['Single_axon.swc', 'Single_apical.swc'],
            value_in_tsv=(Check.basic_tsv, True)
        )
    },

    'axons': {
        "has_axon": Check(
            id_="todo",  # TODO
            label="Has axon",
            value_in_json=lambda neuron_path, k_1, k_2, x: x.status,
            callable_=lambda m: morphology_checks.has_axon(m),
            example_failure=['Single_apical.swc', 'Single_basal.swc'],
            value_in_tsv=(Check.basic_tsv, True)
        )
    },

    'custom': {
        'number_of_dendritic_trees_stemming_from_the_soma': Check(  # DONE
            id_="https://neuroshapes.org/dendriteStemmingFromSomaMetric",
            label="Number of Dendrites Stemming From Soma",
            value_in_json=Check.basic_numeric,
            callable_=lambda neuron: custom_validation.number_of_dendritic_trees_stemming_from_the_soma(neuron),
            example_failure=None,  # Not a check, a metric
            value_in_tsv=(Check.basic_numeric, None)
        ),
        'number_of_axons': Check(  # DONE
            id_="https://neuroshapes.org/axonMetric",
            label="Number of Axons",
            value_in_json=Check.basic_numeric,
            callable_=lambda neuron: custom_validation.number_of_axons(neuron),
            value_in_tsv=(Check.basic_numeric, None)
        ),
        # 'single_child': Check(  # TODO rm and keep neurom implementation?
        #     id_="https://neuroshapes.org/singleChildMetric",
        #     label="Single Child Metric",
        #     value_in_resource=lambda x: list({
        #          "valueX": i[0],
        #          "valueY": i[1],
        #          "valueZ": i[2],
        #          "type": "Vector3D"
        #     } for i in x),
        #     callable_=lambda neuron: custom_validation.has_no_single_child(neuron).info
        # ),
        # 'multifurcation': Check(  # TODO rm and keep neurom implementation?
        #     id_="https://neuroshapes.org/multifurcationMetric",
        #     label="Multifurcation Metric",
        #     value_in_resource=lambda x: list({
        #         "valueX": i[0],
        #         "valueY": i[1],
        #         "valueZ": i[2],
        #         "type": "Vector3D"
        #       } for i in x),
        #     callable_=lambda neuron: custom_validation.has_no_multifurcation(neuron).info
        # )
        'max_branch_order':  Check(  # DONE
            id_="https://neuroshapes.org/maximumBranchOrderMetric",
            label="Maximum Section Branch Order",
            value_in_json=Check.basic_numeric,
            callable_=lambda neuron: int(max(nm.features.get('section_branch_orders', neuron))),
            unit_code="μm",
            example_failure=None,
            value_in_tsv=(Check.basic_numeric, None)
        ),
        'total_section_length': Check(  # DONE
            id_="https://neuroshapes.org/totalSectionLengthMetric",
            label="Total Section Length",
            value_in_json=Check.basic_numeric,
            callable_=lambda neuron: float(nm.features.get('total_length', neuron)),
            unit_code="μm",
            example_failure=None,
            value_in_tsv=(Check.basic_numeric, None)
        ),
        'max_section_length': Check(  # DONE
            id_="https://neuroshapes.org/maximumSectionLengthMetric",
            label="Maximum Section Length",
            value_in_json=Check.basic_numeric,
            callable_=lambda neuron: float(max(nm.features.get('section_lengths', neuron))),
            unit_code="μm",
            example_failure=None,
            value_in_tsv=(Check.basic_numeric, None)
        )
    }
}


def _load_morph(swc_path: str):
    with io.capture_output() as captured:
        with ostream_redirect(stdout=True, stderr=True):
            return load_morphology(swc_path)


def _validation_report(neuron: Morphology, swc_path: str) -> Dict[str, Dict[str, Any]]:
    '''Return the payload that will be sent back to the user'''

    return dict(
        (check_top_key, dict(
            (check_sub_key, check.run(neuron))
            for check_sub_key, check in sub_dictionary.items()
        ))
        for check_top_key, sub_dictionary in validation_report_checks.items()
    )


# # TODO turn put this logic in value_in_resource field for each check
#
# def validation_report_complete(input_: Union[Dict, Morphology], swc_path: str, is_report=False):
#     '''Return the  payload that will be sent back to the user'''
#     report: Dict[str, Dict[str, Any]] = input_ if is_report else validation_report(input_, swc_path)
#
#     for key, value in report.items():
#         if key == 'neurites':
#             for inkey, invalue in value.items():
#                 if invalue and not isinstance(invalue, int):
#                     if inkey == 'has_all_nonzero_segment_lengths':
#                         report[key][inkey] = list(map(lambda el: list(el), invalue))
#                     else:
#                         report[key][inkey] = list(map(lambda el: el[1][0].tolist(), invalue))
#         elif key == 'bifurcations':
#             for inkey, invalue in value.items():
#                 if invalue and not isinstance(invalue, int):
#                     report[key][inkey] = list(map(lambda el: el[1].tolist(), invalue))
#     return report


def get_report(neuron_path: str, morphology: Optional[Morphology] = None, report: Optional[Dict] = None):
    if report is None:
        if morphology is None:
            morphology = _load_morph(neuron_path)
        report = _validation_report(morphology, neuron_path)

    return report



def get_tsv_header_columns():
    return ["filename"] + _get_nested_check_names()


def _get_nested_check_names() -> List[str]:
    return [v_2.label for _, v in validation_report_checks.items() for k_2, v_2 in v.items()]


def get_validation_report_as_json(
    neuron_path: str, morphology: Optional[Morphology] = None, report: Optional[Dict] = None
):
    report = get_report(neuron_path, morphology, report)

    return dict(
        (
            k,
            dict(
                (k_2, validation_report_checks[k][k_2].format_as_json_value(neuron_path, k, k_2, value))
                for k_2, value in v_dict.items()
            )
        )
        for k, v_dict in report.items()
    )


def get_validation_report_as_tsv_line(
        neuron_path: str, morphology: Optional[Morphology] = None, report: Optional[Dict] = None
) -> str:
    basename = os.path.basename(neuron_path)
    report = get_report(neuron_path, morphology, report)

    # try:
    line_list = [basename] + [
        validation_report_checks[k][k_2].format_as_tsv_value(neuron_path, k, k_2, value)
        for k, v_dict in report.items() for k_2, value in v_dict.items()
    ]
    # except Exception as e:
    #     except_name = e.__class__.__name__
    #     line_list = [basename] + [except_name for _, v in report.items() for _, _ in v.items()]

    return '\t'.join(line_list)
