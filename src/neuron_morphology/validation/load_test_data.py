from io import StringIO
import os
from typing import Dict, Optional

from neurom import load_morphology
from neurom.core.morphology import Morphology

from src.neuron_morphology.validation.validator import validation_report_checks

data_dir = os.path.join(os.getcwd(), "data")


def get_random_test_data() -> Dict[str, Optional[Morphology]]:

    swc_download_folder = f"{data_dir}/swcs"
    paths = [os.path.join(swc_download_folder, e) for e in os.listdir(swc_download_folder)]
    morphologies = [None] * len(paths)

    return dict(zip(paths, morphologies))


def get_neurom_test_data() -> Dict[str, Morphology]:
    DATA_PATH = f'{data_dir}/test_data'
    SWC_PATH = os.path.join(DATA_PATH, 'swc')
    ASC_PATH = os.path.join(DATA_PATH, 'neurolucida')
    H5V1_PATH = os.path.join(DATA_PATH, 'h5/v1')

    paths = [SWC_PATH, ASC_PATH, H5V1_PATH]

    all_files = [
        os.path.join(root, name)
        for path in paths
        for root, dirs, files in os.walk(path)
        for name in files
    ]

    unifurcation = load_morphology(StringIO(u"""
    ((CellBody) (-1 0 0 2) (1 0 0 2))

     ((Dendrite)
      (0 0 0 2)
      (0 5 0 2)
      (
       (-5 5 0 3)
       (
        (-10 5 0 3)
       )
       |
       (6 5 0 3)
       )
      )
    """), reader='asc', process_subtrees=True)

    multifurcation = load_morphology(StringIO(u"""
    	((CellBody) (-1 0 0 2) (1 0 0 2))
    ( (Color Blue)
      (Axon)
      (0 5 0 2)
      (2 9 0 2)
      (0 13 0 2)
      (
        (0 13 0 2)
        (4 13 0 2)
        |
        (0 13 0 2)
        (4 13 0 2)
        |
        (0 13 0 2)
        (4 13 0 2)
        |
        (0 13 0 2)
        (4 13 0 2)
      )
    )
    """), reader='asc', process_subtrees=True)

    def load(i):
        try:
            return load_morphology(i, process_subtrees=True)
        except Exception as e:
            return e
    data = dict((i, load(i)) for i in all_files)
    data["str/unifurcation.asc"] = unifurcation
    data["str/multifurcation.asc"] = multifurcation

    all_tied_to_check = [
        t
        for key_a, dict_a in validation_report_checks.items()
        for key_b, check in dict_a.items() if check.example_failure
        for t in check.example_failure
    ]

    to_keep = lambda k: any(i in k for i in all_tied_to_check)

    data_success = dict(
        (i, j) for i, j in data.items()
        if not isinstance(j, Exception) and to_keep(i)
    )

    return data_success