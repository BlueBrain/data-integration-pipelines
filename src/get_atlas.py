import os
import json
from src.helpers import _as_list, allocate_by_deployment, _download_from, Deployment
from src.logger import logger


def _get_atlas_dir_ready(atlas_dir: str, atlas_id: str, deployment: Deployment, token: str, atlas_tag):

    os.makedirs(atlas_dir, exist_ok=True)

    forge_atlas = allocate_by_deployment("bbp", "atlas", deployment=deployment, token=token)

    atlas_resource = forge_atlas.retrieve(atlas_id, version=atlas_tag)

    if atlas_resource is None:
        logger.error(f"Failed to retrieve atlas {atlas_id}")
        exit()

    download_list = {
        "parcellation ontology": (atlas_resource.parcellationOntology.id, "application/json", "hierarchy.json"),
        "parcellation volume": (atlas_resource.parcellationVolume.id, "application/nrrd", "brain_regions.nrrd"),
        "placement hints data catalog": (atlas_resource.placementHintsDataCatalog.id, "application/json", None),
        "cell orientation field": (atlas_resource.cellOrientationField.id, "application/nrrd", None),
        "direction vector": (atlas_resource.directionVector.id, "application/nrrd", None),
        "hemisphere volume": (atlas_resource.hemisphereVolume.id, "application/nrrd", None)
    }

    to_download = ["parcellation ontology", "parcellation volume"]
    download_output_paths = dict(
        (
            label,
            _download_from(
                forge_atlas, link=link_id, format_of_interest=format_of_interest,
                rename=rename, download_dir=atlas_dir, label=label, tag=atlas_tag
            )
        )
            for label, (link_id, format_of_interest, rename) in download_list.items() if label in to_download
    )

    if "placement hints data catalog" in download_output_paths:
        with open(download_output_paths["placement hints data catalog"], "r") as f:
            placement_hints_data_catalog = json.loads(f.read())

        to_download_2 = dict()
        for key in ["placementHints", "voxelDistanceToRegionBottom"]:
            for i, el in enumerate(_as_list(placement_hints_data_catalog[key])):
                to_download_2[f"{key}_{i}"] = forge_atlas.from_json(el)

        for name, res in to_download_2.items():
            _download_from(
                forge_atlas, link=res.get_identifier(), format_of_interest="application/nrrd",
                rename=None, download_dir=atlas_dir, label=name
            )
