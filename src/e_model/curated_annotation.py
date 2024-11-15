import getpass
from typing import Tuple, List

from kgforge.core import Resource

from src.curation_annotations import CurationStatus, create_update_curated_annotation
from src.e_model.querying import curated_e_models
from src.helpers import allocate_by_deployment, Deployment

if __name__ == "__main__":

    token = getpass.getpass()
    forge_bucket = allocate_by_deployment(
        "bbp", "mmb-point-neuron-framework-model", deployment=Deployment.PRODUCTION, token=token
    )

    resources = curated_e_models(forge_bucket)

    # TODO explicitly mark the others as un-assessed?
    # TODO note to confluence pages?

    updated_resources: List[Tuple[Resource, CurationStatus]] = [
        create_update_curated_annotation(resource, forge_bucket, CurationStatus.CURATED, None)
        for resource in resources
    ]

    # forge_bucket.update([r for r, _ in updated_resources if not r._synchronized])

