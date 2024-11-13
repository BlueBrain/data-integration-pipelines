import json
import os
from collections import defaultdict

import nrrd

from src.helpers import get_path, write_obj
from src.neuron_morphology.feature_annotations.morph_metrics_dke import index_brain_region_labels, compute_world_to_vox_mat, compute_metrics_dke
from replace_pyswcparser.morph_metrics_old import compute_metrics_dke as compute_metrics_dke_old

if __name__ == "__main__":

    morph_name = "17302_00023.swc"
    label, _ = os.path.splitext(morph_name)

    data_dir = get_path("./data")
    dst_dir = get_path("./replace_pyswcparser")

    volume_path = os.path.join(data_dir, "atlas/annotation_25_ccf2017.nrrd")
    brain_region_onto_path = os.path.join(data_dir, "atlas/1.json")

    morph_path = os.path.join(data_dir, f"swcs/{morph_name}")

    v_data, v_metadata = nrrd.read(volume_path)

    br_index = index_brain_region_labels(brain_region_onto_path)

    world_to_vox_mat = compute_world_to_vox_mat(v_metadata)

    annotations, warnings = compute_metrics_dke(
        volume_data=v_data, world_to_vox_mat=world_to_vox_mat,
        morphology_path=morph_path, brain_region_index=br_index,
        as_annotation_body=False
    )

    annotations_old, warnings_old = compute_metrics_dke_old(
        volume_data=v_data, world_to_vox_mat=world_to_vox_mat,
        morphology_path=morph_path, brain_region_index=br_index,
        as_annotation_body=False
    )

    write_obj(os.path.join(dst_dir, f"{label}_metrics_dke.json"), annotations)
    write_obj(os.path.join(dst_dir, f"{label}_metrics_dke_old.json"), annotations_old)


    def compare(v1, v2, key):

        sort_fc = lambda fc: sorted(fc, key=lambda el: el["brainRegion"]["@id"])

        v1 = sort_fc(v1[key])
        v2 = sort_fc(v2[key])

        if v1 != v2:
            print(f"Warning - Difference for {type_}'s {key}")

        if len(v1) != len(v2):
            print(f"Difference in count of brain regions being traversed")

        for i, (v1_i, v2_i) in enumerate(zip(v1, v2)):
            if v1_i["brainRegion"]["@id"] == v2_i["brainRegion"]["@id"]:
                if v1_i["count"] != v2_i["count"]:
                    print("Mismatch of counts for", v1_i["brainRegion"]["@id"], v1_i["count"], v2_i["count"])

            # print(json.dumps(v1, indent=4))
            # print(json.dumps(v2, indent=4))

    pairing = defaultdict(lambda: [None, None])

    for a, b in zip(annotations_old["neuriteFeature"], annotations["neuriteFeature"]):
        pairing[a["type"]][0] = a
        pairing[b["type"]][1] = b

    for type_, pair in pairing.items():
        old, new = pair
        compare(old, new, "traversedBrainRegion")
        compare(old, new, "projectionBrainRegion")

