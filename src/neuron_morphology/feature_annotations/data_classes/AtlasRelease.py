from typing import Dict, List


class AtlasRelease:
    id_: str
    type_: List[str]
    rev: int

    def __init__(self, id_: str, rev: int):
        self.type_ = [
            "BrainAtlasRelease",
            "AtlasRelease"
        ]
        self.id_ = id_
        self.rev = rev

    @staticmethod
    def obj_to_dict(item: 'AtlasRelease'):
        return {
            "id": item.id_,
            "type": item.type_,
            "_rev": item.rev
        }

    @staticmethod
    def dic_to_obj(item: Dict):
        return AtlasRelease(id_=item["id"], rev=item["_rev"])
