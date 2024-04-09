from typing import Dict


class AnnotationTarget:
    hasSource: Dict
    hasSelector: Dict
    type_: str

    def __init__(self, id_, type_, rev):
        self.type_ = "AnnotationTarget"
        self.hasSource = {
            "id": id_,
            "type": type_,
            "_rev": rev
        }

    @staticmethod
    def obj_to_dict(item: 'AnnotationTarget'):
        return {
            "hasSource": item.hasSource,
            "type": item.type_
        }

    @staticmethod
    def dic_to_obj(item: Dict):
        return AnnotationTarget(
            id_=item["hasSource"]["id"],
            type_=item["hasSource"]["type"],
            rev=item["hasSource"]["_rev"]
        )
