from typing import Union, Optional, Tuple

from IPython.utils.capture import capture_output
from morphio import (
    Option, MorphioError, RawDataError,
    set_raise_warnings, set_maximum_warnings, ostream_redirect
)

from morphio import Morphology as MorphioMorphology
from neurom.core.morphology import Morphology as NeuromMorphology
from neurom import load_morphology
from IPython.utils import io


set_maximum_warnings(-1)


def load_morphology_with_morphio(swc_path: str, raise_: bool) -> Union[MorphioMorphology, Exception]:
    """
    Loads a morphology with the morphio library. Returns an Exception if raise=True,
    and there is a data quality problem with the morphology file.

    :param swc_path: the path where the morphology swc file is located
    :type swc_path: str
    :param raise_: whether to let Morphio raise an Exception in case of problem with the morphology.
    If True, if there is a problem with the morphology, an Exception instance will be returned.
    :type raise_: bool
    :return:
    :rtype: Union[Morphology, Exception]
    """
    set_raise_warnings(raise_)

    try:
        if not raise_:
            with ostream_redirect(stdout=True, stderr=True):
                return MorphioMorphology(swc_path, options=Option.allow_unifurcated_section_change)

        return MorphioMorphology(swc_path)

    except (MorphioError, RawDataError) as e:
        return e


def load_morphology_with_neurom(
        swc_path: str, return_capture: Optional[bool] = False
) -> Union[Tuple[NeuromMorphology, capture_output], NeuromMorphology]:
    with io.capture_output() as captured:

        e = load_morphology_with_morphio(swc_path, raise_=False)
        morphology = load_morphology(
            e,
            process_subtrees=True
        )

        if return_capture:
            return morphology, captured

        return morphology
