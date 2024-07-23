import numpy as np
from typing import List, Tuple

from morphio._morphio import SectionType
from neurom import NeuriteType, iter_sections, iter_segments, iter_neurites
from neurom.check import CheckResult
from neurom.core import Morphology, Section
from neurom.core.dataformat import COLS


def has_no_heterogeneous_sections_close_to_soma(
        morph: Morphology, no_min: bool = False, min_soma_distance: int = 40
) -> CheckResult:

    soma_distance_check = lambda length: length <= min_soma_distance or no_min

    sections_to_move = []
    for neurite in morph.neurites:
        total_length = 0
        for sec in neurite.sections:
            total_length += sec.length
            if not sec.is_homogeneous_point():
                if soma_distance_check(total_length):
                    sections_to_move.append((sec.to_morphio(), total_length))

            if not soma_distance_check(total_length):
                break

    return CheckResult(len(sections_to_move) == 0, sections_to_move)


def has_no_heterogeneous_neurites(neuron: Morphology) -> CheckResult:
    """
    Check the neuron has no heterogeneous neurites
    https://bbpteam.epfl.ch/project/issues/browse/BBPP134-1449?focusedId=243997&page=com.atlassian.jira.plugin.system.issuetabpanels:comment-tabpanel#comment-243997
    """
    heterogeneous_neurites = [n for n in iter_neurites(neuron) if n.is_heterogeneous()]
    return CheckResult(len(heterogeneous_neurites) == 0, heterogeneous_neurites)


def has_no_radical_diameter_changes(neuron: Morphology, max_change=10) -> CheckResult:
    '''Check if the neuron is radical diameter changes
    Arguments:
        neuron(Neuron): The neuron object to test
        max_change(float): The maximum percentage of variation allowed per um.
            example: max_change=2 means the diameter cannot vary more than 2% per um

    Returns:
        CheckResult with result. result.info contains a list of (section Id, position)
        where radical diameter changes happen
    '''
    bad_ids = list()
    for section in iter_sections(neuron):
        for p0, p1 in iter_segments(section):  # pylint: disable=invalid-name
            length = np.linalg.norm(p0[COLS.XYZ] - p1[COLS.XYZ])
            relative_change = abs(p0[COLS.R] - p1[COLS.R]) / (p0[COLS.R] + p1[COLS.R])
            if relative_change / length > (max_change / 100.):
                bad_ids.append((section.id, p1))
    return CheckResult(len(bad_ids) == 0, bad_ids)


# TODO RM SIMILAR IMPLEMENTATION TO NEUROM -> has_unifurcation
def has_no_single_child(neuron):
    '''Check if the neuron has sections with only one child
    Arguments:
        neuron(Neuron): The neuron object to test

    Returns:
        CheckResult with result. result.info contains a list of (section Id, section end position)
        for each section with a single child
    '''

    bad_ids = list()
    for section in iter_sections(neuron):
        if len(section.children) == 1:
            bad_ids.append((section.id, section.points[-1, COLS.XYZ]))
    return CheckResult(len(bad_ids) == 0, bad_ids)


# TODO RM SIMILAR IMPLEMENTATION TO NEUROM -> has_multifurcation
def has_no_multifurcation(neuron):
    '''Check if the neuron has sections with more than 2 children
    Arguments:
        neuron(Neuron): The neuron object to test

    Returns:
        CheckResult with result. result.info contains a list of (section Id, section end position)
        for each section with more than 2 children
    '''

    bad_ids = list()
    for section in iter_sections(neuron):
        if len(section.children) > 2:
            bad_ids.append((section.id, section.points[-1, COLS.XYZ]))
    return CheckResult(len(bad_ids) == 0, bad_ids)


def number_of_dendritic_trees_stemming_from_the_soma(neuron) -> int:
    # Apical dendrite annotated for pyramidal cell = boolean (A)
    return len([
        neurite for neurite in iter_neurites(neuron)
        if neurite.type in {NeuriteType.basal_dendrite, NeuriteType.apical_dendrite}]
    )


def has_no_composite_subtree_type_starting_in_axon(neuron: Morphology):
    neurite_tree_types = [neurite.subtree_types for neurite in list(iter_neurites(neuron))]
    composite_subtree_type_starting_in_axon_flags = [
        isinstance(tree_types, list) and len(tree_types) == 2 and tree_types[0] == SectionType.axon
        for tree_types in neurite_tree_types
    ]
    return CheckResult(not any(composite_subtree_type_starting_in_axon_flags), info=neurite_tree_types)


def number_of_axons(neuron) -> int:
    # Correct start point of the axon (or axons if there are 2) = boolean (A)
    return len([neurite for neurite in iter_neurites(neuron) if neurite.type == NeuriteType.axon])


def has_zero_soma_radius(morph, threshold=0.0):
    """Check if soma radius not above threshold.

    Arguments:
        morph(Morphology): the morphology to test
        threshold: value under which the soma radius is considered to be zero

    Returns:
        CheckResult with result
    """
    return CheckResult(morph.soma.radius <= threshold)
