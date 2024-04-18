from pathlib import Path

from morphology_workflows import curation
import os
import json

from src.neuron_morphology.validation.load_test_data import get_neurom_test_data


class DictWrapper:
    def __init__(self, dictionary):
        for k, v in dictionary.items():
            setattr(self, k, v)


def run_workflow_on_path(path: Path, dst_dir: Path):

    filename_no_ext = path.name.split(".")[0]
    dst_dir_with_filename = Path(os.path.join(str(dst_dir), filename_no_ext))

    row = DictWrapper({"name": path.name, "morph_path": path})
    os.makedirs(dst_dir_with_filename, exist_ok=True)

    res = curation.detect_errors(row, dst_dir_with_filename)
    res['error_annotated_path'] = str(res['error_annotated_path'])
    if res['error_marker_path'] is not None:
        res['error_marker_path'] = str(res['error_marker_path'])

    with open(f"{dst_dir_with_filename}/{filename_no_ext}.json", "w") as f:
        json.dump(res, f, indent=4)

    return res


if __name__ == "__main__":

    dst_dir = Path(os.path.join(os.getcwd(), "data/swcs_curated"))
    os.makedirs(dst_dir, exist_ok=True)

    neurom_test_data = get_neurom_test_data()

    paths = [
        path for path in list(neurom_test_data.keys())
        if path not in ["str/unifurcation.asc", "str/multifurcation.asc"]
    ]

    for path in paths:
        result = run_workflow_on_path(Path(path), dst_dir)
        print(json.dumps(result, indent=4))
