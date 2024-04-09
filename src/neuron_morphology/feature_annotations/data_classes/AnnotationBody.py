from typing import List, Dict
from kgforge.core import KnowledgeGraphForge


class AnnotationBody:
    isMeasurementOf: Dict
    type: List[str]
    series: List

    def __init__(self, is_measurement_of, series):
        self.isMeasurementOf = is_measurement_of
        self.series = series

    @staticmethod
    def dict_to_obj(item: Dict):
        return AnnotationBody(
            is_measurement_of=item["isMeasurementOf"]["label"],
            series=item["value"]["series"]
        )

    @staticmethod
    def obj_to_dict(item: 'AnnotationBody'):
        return {
            "type": "AnnotationBody",
            "isMeasurementOf": {
                "label": item.isMeasurementOf
            },
            "value": {
                "series": item.series
            }
        }

    @staticmethod
    def obj_to_resource(item: 'AnnotationBody', forge: KnowledgeGraphForge):
        return forge.from_json(AnnotationBody.obj_to_dict(item))
