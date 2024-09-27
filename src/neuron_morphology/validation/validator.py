'''A backend that return a validation report for a given morphology. This script was originally shared here: ≈2'''

import os
from typing import Dict, Any, List, Callable, Optional, Tuple #, Union

import neurom as nm
from IPython.utils import io
from neurom import load_morphology
from neurom.check import morphology_checks, CheckResult
from src.neuron_morphology.validation import custom_validation
from neurom.core.morphology import Morphology
import morphio
# from morphology_workflows import curation # TODO re-enable once morphology_workflows is compatible with neurom v4

morphio.set_maximum_warnings(-1)


class Check:
    def __init__(
            self,
            callable_: Callable,
            label: str,
            pref_label: str,
            id_: str,
            value_in_json: Callable,
            value_in_tsv: Tuple[Callable, Optional[bool]],
            unit_code: Optional[str] = None,
            example_failure: Optional[List] = None,
    ):
        self.callable_ = callable_
        self.label = label
        self.pref_label = pref_label
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

    def format_as_tsv_value(self, neuron_path, k, k_2, output_of_callable, stdout=False, sparse=False):
        res = self.value_in_tsv[0](neuron_path, k, k_2, output_of_callable)

        expected_value = self.value_in_tsv[1]

        if isinstance(expected_value, bool):
            if str(expected_value) != res:
                if stdout:
                    print(
                        f"For check \"{validation_report_checks[k][k_2].pref_label}\" expected value: {expected_value}, obtained {res} for {neuron_path}"
                    )
            else:
                if sparse:
                    return ""
        return res

    def run(self, neuron, swc_path, brain_region_hierarchy_map=None, volume_path=None):
        # TODO try catch and return CheckResult(false) if exception
        with io.capture_output() as captured:
            with morphio.ostream_redirect(stdout=True, stderr=True):
                try:
                    return self.callable_(neuron, swc_path, brain_region_hierarchy_map, volume_path)
                except Exception as e:
                    return CheckResult(status=False, info=e)

    @staticmethod
    def basic_tsv(neuron_path, k_1, k_2, x):
        if x.status is not None:
            if not x.status and isinstance(x.info, Exception):
                return str(x.info).replace("\n", "")

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

        if x.info is None:
            return x.status

        return x.info if not isinstance(x.info, Exception) else str(x.info)

    @staticmethod
    def basic_numeric(neuron_path, k_1, k_2, x):
        if isinstance(x, CheckResult):
            if not x.status:
                return str(x.info)
        return str(x)

    @staticmethod
    def json_wrapper(neuron_path, k_1, k_2, x, fc):
        return x.status if x.status is True else (str(x.info) if isinstance(x.info, Exception) else fc(neuron_path, k_1, k_2, x))


validation_report_checks = {
    'morphology': {
        'can_be_loaded_with_morphio': Check(
            id_="https://bbp.epfl.ch/ontologies/core/bmo/CanBeLoadedWithMorphioMetric",
            pref_label="Can be loaded with morphio",
            label="Can be loaded with Morphio Metric",
            callable_=lambda neuron, swc_path, brain_region_hierarchy_map, volume_path: CheckResult(
                status=not isinstance(_load_morph_morphio(swc_path, raise_=True), Exception),
                info=_load_morph_morphio(swc_path, raise_=True)
            ),
            value_in_json=lambda a, b, c, d: Check.json_wrapper(a, b, c, d, lambda neuron_path, k_1, k_2, x: None),
            example_failure=[],
            value_in_tsv=(Check.basic_tsv, True)
        ),
        # 'z_thickness_larger_than_50': Check(
        #     id_="https://bbp.epfl.ch/ontologies/core/bmo/ZThicknessMetric",  # TODO
        #     label="Z Thickness Metric",
        #     pref_label="Z thickness is larger than 50",
        #     callable_=lambda neuron, swc_path: curation.z_range(neuron),
        #     value_in_json=lambda a, b, c, d: Check.json_wrapper(a, b, c, d, lambda neuron_path, k_1, k_2, x: {
        #         "min": {
        #             "section_id": x.info[0][0],
        #             "point": [float(e) for e in x.info[0][1][0]]
        #         },
        #         "max": {
        #             "section_id": x.info[1][0],
        #             "point": [float(e) for e in x.info[1][1][0]]
        #         }
        #     }),
        #     example_failure=[],
        #     value_in_tsv=(Check.basic_tsv, True)
        # )
    },
    'neurites': {
        'has_different_diameters': Check(
            id_="https://bbp.epfl.ch/ontologies/core/bmo/NeuriteHasDifferentDiametersMetric",
            label="Neurite Has Different Diameters Metric",
            pref_label="Neurite Has Different Diameters",
            callable_=lambda neuron, swc_path, brain_region_hierarchy_map, volume_path: CheckResult(
                status=len(set(_load_morph_morphio(swc_path, raise_=False).diameters)) >= 2
            ),
            value_in_json=lambda neuron_path, k_1, k_2, x: x.status,
            example_failure=[],
            value_in_tsv=(Check.basic_tsv, True)
        ),
        'has_no_dangling_branch': Check(
            id_="https://neuroshapes.org/danglingBranchMetric",
            label="Dangling Branch Metric",
            pref_label="Neurite Has No Dangling Branch",
            callable_=lambda neuron, swc_path, brain_region_hierarchy_map, volume_path: morphology_checks.has_no_dangling_branch(neuron),
            value_in_json=lambda a, b, c, d: Check.json_wrapper(a, b, c, d, lambda neuron_path, k_1, k_2, x: list({
                "root_node_id": root_node_id,
                "point": [float(i) for i in point[0]],
              } for root_node_id, point in x.info)),
            example_failure=['dangling_axon.swc'],
            value_in_tsv=(Check.basic_tsv, True)
        ),
        'has_no_root_node_jump': Check(
            id_="https://neuroshapes.org/rootNodeJumpMetric",
            label="Root Node Jump Metric",
            pref_label="Neurite Has No Root Node Jump",
            value_in_json=lambda a, b, c, d: Check.json_wrapper(a, b, c, d, lambda neuron_path, k_1, k_2, x: list({
                "root_node_id": root_node_id,
                "point": [float(i) for i in point[0]],
              } for root_node_id, point in x.info)),
            callable_=lambda neuron, swc_path, brain_region_hierarchy_map, volume_path: morphology_checks.has_no_root_node_jumps(neuron),
            example_failure=['root_node_jump.swc'],
            value_in_tsv=(Check.basic_tsv, True)
        ),
        'has_no_z_jumps': Check(
            id_="https://neuroshapes.org/zJumpMetric",
            pref_label="Neurite Has No Z Jump",
            label="Z Jump Metric",
            value_in_json=lambda a, b, c, d: Check.json_wrapper(a, b, c, d, lambda neuron_path, k_1, k_2, x:  list({
                "section_id": section_id,
                "from": list(float(i) for i in from_),
                "to": list(float(i) for i in to_)
              } for section_id, (from_, to_) in x.info)),
            callable_=lambda neuron, swc_path, brain_region_hierarchy_map, volume_path: morphology_checks.has_no_jumps(neuron, axis='z'),
            example_failure=['z_jump.swc'],
            value_in_tsv=(Check.basic_tsv, True)
        ),
        # has_no_radical_diameter_changes
        'has_no_narrow_start': Check(
            id_="https://neuroshapes.org/narrowStartMetric",
            label="Narrow Start Metric",
            pref_label="Neurite Has No Narrow Start",
            value_in_json=lambda a, b, c, d: Check.json_wrapper(a, b, c, d, lambda neuron_path, k_1, k_2, x: list({
                "root_node_id": root_node_id,
                "root_node_points": [[float(ip) for ip in p] for p in root_node_points],
              } for (root_node_id, root_node_points) in x.info)),
            callable_=lambda neuron, swc_path, brain_region_hierarchy_map, volume_path: morphology_checks.has_no_narrow_start(neuron, frac=0.9),
            example_failure=['narrow_start.swc'],
            value_in_tsv=(Check.basic_tsv, True)
        ),
        'has_no_fat_ends': Check(
            id_="https://neuroshapes.org/fatEndMetric",
            pref_label="Neurite Has No Fat Ends",
            label="Fat End Metric",
            value_in_json=lambda a, b, c, d: Check.json_wrapper(a, b, c, d, lambda neuron_path, k_1, k_2, x: list({
                "leaf_id": leaf_id,
                "leaf_points": [list(float(a) for a in el) for el in leaf_points],
              } for (leaf_id, leaf_points) in x.info)),
            callable_=lambda neuron, swc_path, brain_region_hierarchy_map, volume_path: morphology_checks.has_no_fat_ends(neuron),
            example_failure=['fat_end.swc'],
            value_in_tsv=(Check.basic_tsv, True)

        ),
        'has_all_nonzero_segment_lengths': Check(
            id_="https://neuroshapes.org/zeroLengthSegmentMetric",
            pref_label="Neurite Has all nonzero segment lengths",
            label="Zero Length Segment Metric",
            value_in_json=lambda a, b, c, d: Check.json_wrapper(a, b, c, d, lambda neuron_path, k_1, k_2, x: list({
                "sectionId": i[0],
                "segmentId": i[1]
              } for i in x.info)),
            callable_=lambda neuron, swc_path, brain_region_hierarchy_map, volume_path: morphology_checks.has_all_nonzero_segment_lengths(neuron),
            example_failure=[
                'Neuron_zero_length_segments.swc',
                'Single_apical.swc',
                'Single_basal.swc',
                'Single_axon.swc',
            ],
            value_in_tsv=(Check.basic_tsv, True)

        ),
        "has_all_nonzero_neurite_radii": Check(
            id_="https://bbp.epfl.ch/ontologies/core/bmo/HasAllNonZeroNeuriteRadiiMetric",
            label="Has all non-zero neurite radii Metric",
            pref_label="Neurite Has all non zero neurite radii",
            value_in_json=None,
            callable_=lambda m, swc_path, brain_region_hierarchy_map, volume_path: morphology_checks.has_all_nonzero_neurite_radii(m),
            example_failure=['Neuron_zero_radius.swc'],
            value_in_tsv=(Check.basic_tsv, True)
        ),
        "has_all_nonzero_section_lengths": Check(
            id_="https://bbp.epfl.ch/ontologies/core/bmo/HasAllNonZeroSectionLengthsMetric",
            label="Has all non-zero section lengths Metric",
            pref_label="Neurite Has all non-zero section lengths",
            value_in_json=None,
            callable_=lambda m, swc_path, brain_region_hierarchy_map, volume_path: morphology_checks.has_all_nonzero_section_lengths(m),
            example_failure=['Neuron_zero_length_sections.swc'],
            value_in_tsv=(Check.basic_tsv, True)
        ),
        'has_no_narrow_neurite_section': Check(
            id_="https://neuroshapes.org/narrowNeuriteSectionMetric",
            pref_label="Has no narrow Neurite Section",
            label="Narrow Neurite Section Metric",
            value_in_json=lambda a, b, c, d: Check.json_wrapper(a, b, c, d, lambda neuron_path, k_1, k_2, x:  list({
                "section_id": section_id,
                "section_points": [[float(ip) for ip in p] for p in section_points],
              } for section_id, section_points in x.info)),
            callable_=lambda neuron, swc_path, brain_region_hierarchy_map, volume_path: morphology_checks.has_no_narrow_neurite_section(neuron, neurite_filter=None),
            example_failure=[],  # TODO
            value_in_tsv=(Check.basic_tsv, True)
        ),
        "has_no_flat_neurites": Check(
            id_="https://bbp.epfl.ch/ontologies/core/bmo/HasNoFlatNeuritesMetric",
            label="Has no flat neurites Metric",
            pref_label="Has no flat neurites",
            value_in_json=Check.basic_json,
            callable_=lambda m, swc_path, brain_region_hierarchy_map, volume_path: morphology_checks.has_no_flat_neurites(m, 1e-6, method="tolerance"),
            example_failure=['Neuron-flat.swc'],
            value_in_tsv=(Check.basic_tsv, True)
        )
    },
    'bifurcations': {
        'has_unifurcation': Check(
            id_="https://bbp.epfl.ch/ontologies/core/bmo/HasUnifurcationMetric",
            label="Has Unifurcation Metric",
            value_in_json=lambda a, b, c, d: Check.json_wrapper(a, b, c, d, lambda neuron_path, k_1, k_2, x: len(x.info)),
            pref_label="Has Unifurcation",
            callable_=lambda neuron, swc_path, brain_region_hierarchy_map, volume_path: morphology_checks.has_unifurcation(neuron),
            example_failure=["unifurcation.asc"],
            value_in_tsv=(Check.basic_tsv, True)
        ),
        'has_multifurcation': Check(
            id_="https://bbp.epfl.ch/ontologies/core/bmo/HasMultifurcationMetric",
            label="Has Multifurcation Metric",
            value_in_json=lambda a, b, c, d: Check.json_wrapper(a, b, c, d, lambda neuron_path, k_1, k_2, x: len(x.info)),
            pref_label="Has Multifurcation",
            callable_=lambda neuron, swc_path, brain_region_hierarchy_map, volume_path: morphology_checks.has_multifurcation(neuron),
            example_failure=["multifurcation.asc"],
            value_in_tsv=(Check.basic_tsv, True)
        ),
    },
    "soma": {
        "has_nonzero_soma_radius": Check(
            id_="https://bbp.epfl.ch/ontologies/core/bmo/HasNoZeroSomaRadiusMetric",
            label="Has non-zero soma radius Metric",
            pref_label="Has nonzero soma radius",
            value_in_json=lambda neuron_path, k_1, k_2, x: x.status,
            callable_=lambda m, swc_path, brain_region_hierarchy_map, volume_path: morphology_checks.has_nonzero_soma_radius(m),
            example_failure=['soma_zero_radius.swc'],
            value_in_tsv=(Check.basic_tsv, True)
        ),
    },
    'dendrites': {
        "has_apical_dendrite": Check(
            id_="https://bbp.epfl.ch/ontologies/core/bmo/HasApicalDendriteMetric",
            label="Has Apical Dendrite Metric",
            pref_label="Has apical dendrite",
            value_in_json=lambda neuron_path, k_1, k_2, x: x.status,
            callable_=lambda m, swc_path, brain_region_hierarchy_map, volume_path: morphology_checks.has_apical_dendrite(m),
            example_failure=['Single_axon.swc', 'Single_basal.swc'],
            value_in_tsv=(Check.basic_tsv, True)
        ),
        "has_basal_dendrite": Check(
            id_="https://bbp.epfl.ch/ontologies/core/bmo/HasBasalDendrite",
            label="Has Basal Dendrite Metric",
            pref_label="Has basal dendrite",
            value_in_json=lambda neuron_path, k_1, k_2, x: x.status,
            callable_=lambda m, swc_path, brain_region_hierarchy_map, volume_path: morphology_checks.has_basal_dendrite(m),
            example_failure=['Single_axon.swc', 'Single_apical.swc'],
            value_in_tsv=(Check.basic_tsv, True)
        )
    },
    'axons': {
        "has_axon": Check(
            id_="https://bbp.epfl.ch/ontologies/core/bmo/HasAxonMetric",
            label="Has Axon Metric",
            pref_label="Has axon",
            value_in_json=lambda neuron_path, k_1, k_2, x: x.status,
            callable_=lambda m, swc_path, brain_region_hierarchy_map, volume_path: morphology_checks.has_axon(m),
            example_failure=['Single_apical.swc', 'Single_basal.swc'],
            value_in_tsv=(Check.basic_tsv, True)
        )
    },
    'custom': {
        'has_no_heterogeneous_neurites': Check(
            id_="TODO",  # TODO
            label="Has no heterogeneous neurites",
            pref_label="Has no heterogeneous neurites",
            value_in_json=lambda neuron_path, k_1, k_2, x: x.status,
            callable_=lambda m, swc_path, brain_region_hierarchy_map, volume_path: custom_validation.has_no_heterogeneous_neurites(m),
            value_in_tsv=(Check.basic_tsv, True)
        ),
        'has_no_heterogeneous_neurites_near_soma': Check(
            id_="TODO",  # TODO
            label="Has no heterogeneous neurites near soma (40 μm)",
            pref_label="Has no heterogeneous neurites near soma (40 μm)",
            value_in_json=lambda neuron_path, k_1, k_2, x: x.status,
            callable_=lambda m, swc_path, brain_region_hierarchy_map, volume_path: custom_validation.has_no_heterogeneous_sections_close_to_soma(m, no_min=False, min_soma_distance=40),
            value_in_tsv=(lambda neuron_path, k_1, k_2, x: str(x.status) if x.status is True else (str(x.info) if isinstance(x.info, Exception) else ", ".join([str(i[1]) for i in x.info])), True)
            # output more than True/False in tsv
        ),
        'has_no_composite_subtree_type_starting_in_axon': Check(
            id_="TODO",  # TODO
            label="Has no composite subtree type starting in axon",
            pref_label="Has no composite subtree type starting in axon",
            value_in_json=lambda neuron_path, k_1, k_2, x: x.status,
            callable_=lambda m, swc_path, brain_region_hierarchy_map, volume_path: custom_validation.has_no_composite_subtree_type_starting_in_axon(m),
            value_in_tsv=(lambda neuron_path, k_1, k_2, x: str(x.status) if x.status is True else (str(x.info) if isinstance(x.info, Exception) else
                                                                                                   str(x.status)), True)
        ),
        'number_of_dendritic_trees_stemming_from_the_soma': Check(  # DONE
            id_="https://neuroshapes.org/dendriteStemmingFromSomaMetric",
            label="Dendrite Stemming From Soma Metric",
            pref_label="Number of Dendrites Stemming From Soma",
            value_in_json=Check.basic_numeric,
            callable_=lambda neuron, swc_path, brain_region_hierarchy_map, volume_path: custom_validation.number_of_dendritic_trees_stemming_from_the_soma(neuron),
            example_failure=None,  # Not a check, a metric
            value_in_tsv=(Check.basic_numeric, None)
        ),
        'number_of_axons': Check(  # DONE
            id_="https://neuroshapes.org/axonMetric",
            label="Axon Metric",
            pref_label="Number of Axons",
            value_in_json=Check.basic_numeric,
            callable_=lambda neuron, swc_path, brain_region_hierarchy_map, volume_path: custom_validation.number_of_axons(neuron),
            value_in_tsv=(Check.basic_numeric, None)
        ),
        # 'single_child': Check(  # TODO rm and keep neurom implementation?
        #     id_="https://neuroshapes.org/singleChildMetric",
        #     label="Single Child Metric",
        #     pref_label="Has no Single Child",
        #     value_in_json=lambda x: list({
        #          "valueX": i[0],
        #          "valueY": i[1],
        #          "valueZ": i[2],
        #          "type": "Vector3D"
        #     } for i in x),
        #     callable_=lambda neuron, swc_path: custom_validation.has_no_single_child(neuron).info,
        #     value_in_tsv=(Check.basic_tsv, True)
        # ),
        # 'multifurcation': Check(  # TODO rm and keep neurom implementation?
        #     id_="https://neuroshapes.org/multifurcationMetric",
        #     pref_label="Has no Multifurcation",
        #     label="Multifurcation Metric",
        #     value_in_json=lambda x: list({
        #         "valueX": i[0],
        #         "valueY": i[1],
        #         "valueZ": i[2],
        #         "type": "Vector3D"
        #       } for i in x),
        #     callable_=lambda neuron, swc_path: custom_validation.has_no_multifurcation(neuron).info,
        #     value_in_tsv=(Check.basic_tsv, True)
        # ),
        'max_branch_order':  Check(  # DONE
            id_="https://neuroshapes.org/maximumBranchOrderMetric",
            pref_label="Maximum Section Branch Order",
            label="Maximum Branch Order Metric",
            value_in_json=Check.basic_numeric,
            callable_=lambda neuron, swc_path, brain_region_hierarchy_map, volume_path: int(max(nm.features.get('section_branch_orders', neuron))),
            unit_code="μm",
            example_failure=None,
            value_in_tsv=(Check.basic_numeric, None)
        ),
        'total_section_length': Check(  # DONE
            id_="https://neuroshapes.org/totalSectionLengthMetric",
            pref_label="Total Section Length",
            label="Total Section Length Metric",
            value_in_json=Check.basic_numeric,
            callable_=lambda neuron, swc_path, brain_region_hierarchy_map, volume_path: float(nm.features.get('total_length', neuron)),
            unit_code="μm",
            example_failure=None,
            value_in_tsv=(Check.basic_numeric, None)
        ),
        'max_section_length': Check(  # DONE
            id_="https://neuroshapes.org/maximumSectionLengthMetric",
            pref_label="Maximum Section Length",
            label="Maximum Section Length Metric",
            value_in_json=Check.basic_numeric,
            callable_=lambda neuron, swc_path, brain_region_hierarchy_map, volume_path: float(max(nm.features.get('section_lengths', neuron))),
            unit_code="μm",
            example_failure=None,
            value_in_tsv=(Check.basic_numeric, None)
        ),
        'axon_outside_brain': Check(
            id_="TODO",
            pref_label="Axon outside brain",
            label="Axon outside brain metric",
            value_in_json=Check.basic_numeric,
            value_in_tsv=(Check.basic_numeric, None),
            callable_=lambda neuron, swc_path, brain_region_map, volume_path:
                custom_validation.axon_outside_brain(swc_path, brain_region_map, volume_path)
        )
    }
}


def _load_morph(swc_path: str):
    morphio.set_raise_warnings(False)

    with io.capture_output() as captured:
        with morphio.ostream_redirect(stdout=True, stderr=True):
            return load_morphology(swc_path, process_subtrees=True)


def _load_morph_morphio(swc_path: str, raise_: bool):
    morphio.set_raise_warnings(raise_)

    try:
        return morphio.Morphology(swc_path)
    except morphio._morphio.MorphioError as e:
        return e


def _validation_report(neuron: Morphology, swc_path: str, brain_region_map=None, volume_path=None) -> Dict[str, Dict[str, Any]]:
    '''Return the payload that will be sent back to the user'''

    return dict(
        (check_top_key, dict(
            (check_sub_key, check.run(neuron, swc_path, brain_region_map, volume_path))
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


def get_report(neuron_path: str, morphology: Optional[Morphology] = None, report: Optional[Dict] = None, brain_region_map=None, volume_path=None):
    if report is None:
        if morphology is None:
            morphology = _load_morph(neuron_path)
        report = _validation_report(morphology, neuron_path, brain_region_map, volume_path)

    return report


def get_tsv_header_columns():
    return ["filename"] + _get_nested_check_names()


def _get_nested_check_names() -> List[str]:
    return [v_2.pref_label for _, v in validation_report_checks.items() for k_2, v_2 in v.items()]


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
        neuron_path: str, morphology: Optional[Morphology] = None, report: Optional[Dict] = None, added: Optional[Dict] = None
) -> List[str]:
    basename = os.path.basename(neuron_path)
    report = get_report(neuron_path, morphology, report)

    # try:
    line_list = [basename] + [
        validation_report_checks[k][k_2].format_as_tsv_value(neuron_path, k, k_2, value, stdout=False, sparse=True)
        # if the expected value is equal to the obtained value, an empty cell will be in the tsv. If sparse is False, the expected value will be there
        for k, v_dict in report.items() for k_2, value in v_dict.items()
    ]

    if added:
        line_list += [str(i) for i in list(added.values())]
    # except Exception as e:
    #     except_name = e.__class__.__name__
    #     line_list = [basename] + [except_name for _, v in report.items() for _, _ in v.items()]

    return line_list
