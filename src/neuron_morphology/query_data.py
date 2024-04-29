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
    distributions = resource.distribution if isinstance(resource.distribution, list) else [resource.distribution]
    distribution_name = next(d.name for d in distributions if d.encodingFormat.split('/')[-1] == "swc")

    swcfpath = os.path.join(swc_download_folder, distribution_name)
    if not os.path.isfile(swcfpath):  # If already present, no need to download
        logger.info(f"Downloading swc file for morphology {resource.get_identifier()}")
        forge.download(resource, follow='distribution.contentUrl', content_type='application/swc', path=swc_download_folder)

    return swcfpath