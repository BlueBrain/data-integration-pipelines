"""
Helper functions to create curation-related annotations,
and checking the current curation annotation of a Resource
"""
import copy
from enum import Enum
from typing import Optional, Dict, Tuple, List

from kgforge.core import Resource, KnowledgeGraphForge

from src.helpers import _as_list


class CurationStatus(Enum):
    CURATED = "Curated"
    UNASSESSED = "Unassessed"
    NOTHING = "Nothing"


ontology_id = {
    CurationStatus.CURATED:  "https://neuroshapes.org/Curated",
    CurationStatus.UNASSESSED: "https://neuroshapes.org/Unassessed"
}


def _make_curation_annotation(curation_status: CurationStatus) -> Dict:
    if curation_status == CurationStatus.NOTHING:
        raise Exception(f"Invalid CurationStatus: {curation_status}")

    return {
        "@type": [
            "QualityAnnotation",
            "Annotation"
        ],
        "hasBody": {
            "@id": ontology_id[curation_status],
            "@type": [
                "AnnotationBody",
                "DataMaturity"
            ],
            "label": curation_status.value
        },
        "motivatedBy": {
            "@id": "https://neuroshapes.org/qualityAssessment",
            "@type": "Motivation"
        },
        "name": "Data maturity annotation"
    }


def _make_curation_annotation_with_note(curation_status: CurationStatus, note: Optional[str] = None) -> Dict:
    base_annotation = _make_curation_annotation(curation_status)

    temp = copy.deepcopy(base_annotation)
    if note:
        temp["note"] = note
    return temp


def _check_curation_status(resource: Resource) -> Tuple[CurationStatus, Optional[int], List[Resource]]:
    existing_annotations_copy = _as_list(resource.annotation) if "annotation" in resource.__dict__ else []

    existing_curation_idx_fc = lambda curation_status: next(
        (idx for idx, ann in enumerate(existing_annotations_copy)
         if ann.hasBody.label == curation_status.value), None
    )

    existing_curated_idx: Optional[int] = existing_curation_idx_fc(CurationStatus.CURATED)
    existing_unassessed_idx: Optional[int] = existing_curation_idx_fc(CurationStatus.UNASSESSED)

    if existing_curated_idx is not None and existing_unassessed_idx is not None:
        raise Exception(f"Weirdo labelled as both curated and unassessed {resource.get_identifier()}")

    if existing_curated_idx is not None:
        current_curation_status = CurationStatus.CURATED
        existing_curation_idx_value = existing_curated_idx
    elif existing_unassessed_idx is not None:
        current_curation_status = CurationStatus.UNASSESSED
        existing_curation_idx_value = existing_unassessed_idx
    else:
        current_curation_status = CurationStatus.NOTHING
        existing_curation_idx_value = None

    return current_curation_status, existing_curation_idx_value, existing_annotations_copy


def create_update_curated_annotation(
        resource: Resource, forge: KnowledgeGraphForge,
        new_curation_status: CurationStatus, note: Optional[str],
) -> Tuple[Resource, CurationStatus]:

    previous_curation_status, existing_curation_annotation_idx, existing_annotations_copy = _check_curation_status(resource)

    curation_annotation = forge.from_json(
        _make_curation_annotation_with_note(curation_status=new_curation_status, note=note)
    )

    if existing_curation_annotation_idx is not None:
        del existing_annotations_copy[existing_curation_annotation_idx]

    existing_annotations_copy.append(curation_annotation)

    resource.annotation = existing_annotations_copy

    return resource, previous_curation_status


def bool_to_curation_status(curated: bool) -> CurationStatus:
    return CurationStatus.CURATED if curated else CurationStatus.UNASSESSED
