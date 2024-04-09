import numpy as np
from neurom import NeuriteType, iter_sections, iter_segments, iter_neurites
from neurom.check import CheckResult
from neurom.core.dataformat import COLS


def has_no_radical_diameter_changes(neuron, max_change=10):
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


def has_no_multifurcation(neuron):
    '''Check if the neuron has sections with only one child
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


def number_of_axons(neuron) -> int:
    # Correct start point of the axon (or axons if there are 2) = boolean (A)
    return len([neurite for neurite in iter_neurites(neuron) if neurite.type == NeuriteType.axon])
