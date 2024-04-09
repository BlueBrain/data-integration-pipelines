from typing import Optional, List

from kgforge.core import Resource, KnowledgeGraphForge
from src.helpers import allocate

NEURON_MORPHOLOGY_RETRIEVAL_LIMIT = 1500


def _get_morph_path(morphology: Resource, download_dir: str) -> str:
    if not isinstance(morphology.distribution, list):
        if not isinstance(morphology.distribution, Resource):
            raise Exception("Invalid morphology distribution")
        return f"{download_dir}/{morphology.distribution.name}"

    return next(
        f"{download_dir}/{d.name}"
        for d in morphology.distribution
        if "swc" in d.name
    )


def download_morphology_file(
        morphology: Resource, download_dir: str, forge_data: KnowledgeGraphForge
) -> None:
    forge_data.download(
        morphology, path=download_dir, overwrite=True, follow="distribution.contentUrl"
    )


def _get_neuron_morphologies(org, project, is_prod, token, nm_id_list: Optional[List] = None):
    print("Retrieving neuron morphologies")

    forge_data = allocate(org, project, is_prod, token)

    if nm_id_list:
        morphologies = [forge_data.retrieve(i) for i in nm_id_list]
    else:
        morphologies = forge_data.search(
            {"type": "NeuronMorphology"},
            limit=NEURON_MORPHOLOGY_RETRIEVAL_LIMIT, debug=False
        )
    print(len(morphologies))
    return morphologies, forge_data
