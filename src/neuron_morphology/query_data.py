import os

from kgforge.core import KnowledgeGraphForge, Resource
from kgforge.core.wrappings import FilterOperator, Filter

from src.logger import logger


def get_neuron_morphologies(forge: KnowledgeGraphForge, reconstructed=True, curated="yes", limit=10000, debug=False):

    org, project = forge._store.bucket.split("/")[-2:]

    logger.info(
        f"Querying for morphologies in {org}/{project}, "
        f"Curated: {curated}, Reconstructed: {str(reconstructed)}, limit: {limit}"
    )

    type_ = "ReconstructedNeuronMorphology" if reconstructed else "NeuronMorphology"
    filters = [Filter(operator=FilterOperator.EQUAL, path=["type"], value=type_)]

    if curated in ["yes", "no"]:
        filters.append(
            Filter(
                operator=FilterOperator.EQUAL if curated == "yes" else FilterOperator.NOT_EQUAL,
                path=["annotation", "hasBody", "label"], value="Curated"
             )
        )
    elif curated == "both":
        pass
    else:
        raise Exception(f"Unknown curated flag when retrieving neuron morphologies {curated}")

    res = forge.search(*filters, limit=limit, debug=debug)
    logger.info(f"Found {len(res)} morphologies in {org}/{project}")

    return res


def get_swc_path(resource: Resource, swc_download_folder: str, forge: KnowledgeGraphForge) -> str:
    return get_ext_path(resource=resource, ext_download_folder=swc_download_folder, forge=forge, ext="swc")


def get_asc_path(resource: Resource, asc_download_folder: str, forge: KnowledgeGraphForge) -> str:
    return get_ext_path(resource=resource, ext_download_folder=asc_download_folder, forge=forge, ext="asc")


def get_ext_path(resource: Resource, ext_download_folder: str, forge: KnowledgeGraphForge, ext: str) -> str:
    distributions = resource.distribution if isinstance(resource.distribution, list) else [resource.distribution]
    distribution_name = next(d.name for d in distributions if d.encodingFormat.split('/')[-1] == ext)

    file_path = os.path.join(ext_download_folder, distribution_name)

    if not os.path.isfile(file_path):  # If already present, no need to download
        logger.info(f"Downloading {ext} file for morphology {resource.get_identifier()}")
        forge.download(resource, follow='distribution.contentUrl', content_type=f'application/{ext}', path=ext_download_folder)

    return file_path
