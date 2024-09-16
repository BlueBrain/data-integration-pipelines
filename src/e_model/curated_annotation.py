import getpass

from kgforge.core import Resource

from src.e_model.querying import curated_e_models
from src.helpers import allocate, _as_list


CURATED_ANNOTATION = {
    "@type": [
        "QualityAnnotation",
        "Annotation"
    ],
    "hasBody": {
        "@id": "https://neuroshapes.org/Curated",
        "@type": [
            "AnnotationBody",
            "DataMaturity"
        ],
        "label": "Curated"
    },
    "motivatedBy": {
        "@id": "https://neuroshapes.org/qualityAssessment",
        "@type": "Motivation"
    },
    "name": "Data maturity annotation",
}


def _add_annotation(resource: Resource, annotation: Resource) -> Resource:

    if "annotation" in resource.__dict__:

        resource.annotation = _as_list(resource.annotation)

        exists = next(
            (
                i for i in _as_list(resource.annotation)
                if i.hasBody.get_identifier() == CURATED_ANNOTATION["hasBody"]["@id"]
            ),
            None
        )

        if exists is None:
            resource.annotation.append(ann)
    else:
        resource.annotation = [annotation]

    return resource


if __name__ == "__main__":

    token = getpass.getpass()
    forge_bucket = allocate("bbp", "mmb-point-neuron-framework-model", is_prod=True, token=token)
    ann = forge_bucket.from_json(CURATED_ANNOTATION)
    resources = curated_e_models(forge_bucket)
    updated_resources = [_add_annotation(r, ann) for r in resources]
    updated_resources = [i for i in updated_resources if not i._synchronized]
    # forge_bucket.update(resources)

