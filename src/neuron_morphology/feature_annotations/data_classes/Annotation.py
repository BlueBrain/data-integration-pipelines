from typing import List, Dict, Optional
from kgforge.core import KnowledgeGraphForge, Resource

from src.neuron_morphology.feature_annotations.data_classes.AnnotationBody import AnnotationBody

from src.neuron_morphology.feature_annotations.data_classes.AnnotationTarget import AnnotationTarget
from src.neuron_morphology.feature_annotations.data_classes.AtlasRelease import AtlasRelease
from src.neuron_morphology.feature_annotations.data_classes.Contribution import Contribution


class Annotation:
    type: List[str]
    compartment: str
    contribution: Optional[Contribution]
    hasBody: List[AnnotationBody]
    hasTarget: Optional[AnnotationTarget]
    atlasRelease: Optional[AtlasRelease]
    name: str

    def __init__(
            self, compartment: str, has_body: List[AnnotationBody], type_: List[str], name: str,
            has_target: Optional[AnnotationTarget] = None,
            atlas_release: Optional[AtlasRelease] = None,
            contribution: Optional[Contribution] = None
    ):

        self.type = type_
        self.compartment = compartment
        self.hasBody = has_body
        self.hasTarget = has_target
        self.atlasRelease = atlas_release
        self.name = name
        self.contribution = contribution

    @staticmethod
    def dict_to_obj(item: Dict) -> 'Annotation':
        return Annotation(
            name=item["name"],
            type_=item["type"],
            compartment=item["compartment"],
            has_body=[AnnotationBody.dict_to_obj(e) for e in item["hasBody"]],
            has_target=AnnotationTarget.dic_to_obj(item["hasTarget"])
            if item.get("hasTarget") is not None else None,
            atlas_release=AtlasRelease.dic_to_obj(item["atlasRelease"])
            if item.get("atlasRelease") is not None else None,
            contribution=Contribution.dic_to_obj(item["contribution"])
            if item.get("contribution") is not None else None
        )

    @staticmethod
    def obj_to_dict(item: 'Annotation') -> Dict:
        temp = {
            "type": item.type,
            "compartment": item.compartment,
            "hasBody": [AnnotationBody.obj_to_dict(e) for e in item.hasBody],
            "name": item.name
        }

        if item.hasTarget is not None:
            temp["hasTarget"] = AnnotationTarget.obj_to_dict(item.hasTarget)
        if item.atlasRelease is not None:
            temp["atlasRelease"] = AtlasRelease.obj_to_dict(item.atlasRelease)
        if item.contribution is not None:
            temp["contribution"] = Contribution.obj_to_dict(item.contribution)

        return temp

    @staticmethod
    def obj_to_resource(item: 'Annotation', forge: KnowledgeGraphForge,
                        target: AnnotationTarget,
                        atlas_release: AtlasRelease,
                        contribution: Contribution) -> Resource:

        item.set_contribution(contribution)
        item.set_annotation_target(target)
        item.set_atlas_release(atlas_release)
        return forge.from_json(Annotation.obj_to_dict(item))

    def set_contribution(self, c: Contribution):
        self.contribution = c

    def add_annotation_body(self, ab: AnnotationBody):
        self.hasBody.append(ab)

    def set_atlas_release(self, ar: AtlasRelease):
        self.atlasRelease = ar

    def set_annotation_target(self, at: AnnotationTarget):
        self.hasTarget = at
