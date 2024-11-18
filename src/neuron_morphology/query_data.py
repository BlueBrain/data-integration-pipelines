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


