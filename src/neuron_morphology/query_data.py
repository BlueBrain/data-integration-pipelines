import os

from kgforge.core import KnowledgeGraphForge, Resource
from kgforge.core.wrappings import FilterOperator, Filter

from src.logger import logger


def filter_by_tag(all_resources: list, tag: str, forge: KnowledgeGraphForge):
    tagged_resources = []
    for count, res in enumerate(all_resources):
        logger.info(f"Retrieving Resource at tag '{tag}': {count +1} of {len(all_resources)}")
        retrieved_res = forge.retrieve(id=res.get_identifier(), version=tag)
        if retrieved_res is not None:
            tagged_resources.append(retrieved_res)
    return tagged_resources

def get_neuron_morphologies(forge: KnowledgeGraphForge, reconstructed=True, curated="yes", limit=10000, debug=False, tag=None):

    org, project = forge._store.bucket.split("/")[-2:]
    bucket = f"{org}/{project}"
    logger.info(
        f"Querying for morphologies in {bucket}, "
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

    ress = forge.search(*filters, limit=limit, debug=debug)
    logger.info(f"Found {len(ress)} morphologies in {bucket}")
    if tag:
        ress = filter_by_tag(ress, tag, forge)
        logger.info(f"Found {len(ress)} morphologies in {bucket} with tag {tag}")

    return ress


def get_ext_path(resource: Resource, ext_download_folder: str, forge: KnowledgeGraphForge, ext: str, i_res: int = None, n_resources: int = None) -> str:
    if i_res is not None and n_resources is not None:
        logger.info(f"Getting {ext} file for Resource {i_res +1} of {n_resources}")
    distributions = resource.distribution if isinstance(resource.distribution, list) else [resource.distribution]
    distribution_name = next(d.name for d in distributions if d.encodingFormat.split('/')[-1] == ext)

    file_path = os.path.join(ext_download_folder, distribution_name)

    if not os.path.isfile(file_path):  # If already present, no need to download
        logger.info(f"Downloading {ext} file for morphology {resource.get_identifier()}")
        forge.download(resource, follow='distribution.contentUrl', content_type=f'application/{ext}', path=ext_download_folder)

    return file_path
