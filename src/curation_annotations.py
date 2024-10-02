import copy
from typing import Optional, Dict

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
    "name": "Data maturity annotation"
}

UNASSESSED_ANNOTATION = {
    "@type": [
        "QualityAnnotation",
        "Annotation"
    ],
    "hasBody": {
        "@id": "https://neuroshapes.org/Unassessed",
        "@type": [
            "AnnotationBody",
            "DataMaturity"
        ],
        "label": "Unassessed"
    },
    "motivatedBy": {
        "@id": "https://neuroshapes.org/qualityAssessment",
        "@type": "Motivation"
    },
    "name": "Data maturity annotation"
}


def _annotation_plus_note(base_annotation: Dict, note: Optional[str] = None):
    temp = copy.deepcopy(base_annotation)
    if note:
        temp["note"] = note
    return temp


def make_curated(note: Optional[str] = None) -> Dict:
    return _annotation_plus_note(CURATED_ANNOTATION, note)


def make_unassessed(note: Optional[str] = None) -> Dict:
    return _annotation_plus_note(UNASSESSED_ANNOTATION, note)
