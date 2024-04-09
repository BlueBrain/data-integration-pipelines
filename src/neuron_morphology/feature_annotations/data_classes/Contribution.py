from typing import Dict, List


class Contribution:
    agent: Dict
    type: List[str]

    def __init__(self, id_, first_name, last_name, email):
        self.type_ = "Contribution"
        self.agent = {
            "id": id_,
            "type": [
                "Agent",
                "Person"
            ],
            "email": email,
            "familyName": last_name,
            "givenName": first_name,
            "name":  f"{first_name} {last_name}",
        }

    @staticmethod
    def obj_to_dict(item: 'Contribution'):
        return {
            "agent": item.agent,
            "type": item.type_
        }

    @staticmethod
    def dic_to_obj(item: Dict):
        return Contribution(
            id_=item["agent"]["id"],
            first_name=item["agent"]["givenName"],
            last_name=item["agent"]["familyName"],
            email=item["agent"]["email"]
        )
