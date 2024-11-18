from datetime import datetime
from typing import Optional, Dict
from kgforge.core import Resource, KnowledgeGraphForge


def create_brain_region(forge: KnowledgeGraphForge, region_label: str) -> Optional[Dict]:
    """Create BrainLocation field from a brain region label"""
    region = forge.resolve(region_label, scope='ontology', target='BrainRegion', strategy='EXACT_CASE_INSENSITIVE_MATCH')
    if region:
        return {'id': region.id, 'label': region.label}
    else:
        raise ValueError(f"Brain location {region_label} was not found in the ontology, try with a different name")


def create_bbp_person_contribution(given_name: str, last_name: str, user_name: str) -> Dict:
    contribution = {"type": "Contribution",
                      "agent": {
                          "type": [
                              "Agent",
                              "Person"
                          ],
                          "id": f"https://bbp.epfl.ch/nexus/v1/realms/bbp/users/{user_name}",
                          "givenName": given_name,
                          "lastName": last_name,
                          "username": user_name
                          }}
    return contribution


def create_existing_agent_contribution(forge: KnowledgeGraphForge, name: str) -> Optional[Dict]:
    resolved_agent = forge.resolve(name, scope='agent', target="agents", strategy='EXACT_CASE_INSENSITIVE_MATCH')
    if resolved_agent:
        return {"type": "Contribution",
                "agent": resolved_agent
                }
    else:
        raise ValueError(f"The name of the agent was not found in the knowledge graph, make sure it was registered previously.")


def create_organization_contribution(organization_id: str, organization_name: str) -> Dict:
    contribution = {"type": "Contribution",
                      "agent": {
                          'id': organization_id,
                          "type": [
                              "Agent",
                              "Organization"
                          ],
                          "name": organization_name
                          }}
    return contribution


def create_generation(activity_id: str, activity_type: str) -> Dict:
    return {
        "type": "Generation",
        "activity": {
            "id": activity_id,
            "type": activity_type
        }
    }


def create_derivation(derivation_entity_id: str, derivation_entity_type: str) -> Dict:
    return {
        "type": "Derivation",
        "entity": {
            "id": derivation_entity_id,
            "type": derivation_entity_type
        }
    }


def create_date(date, begin=True) -> Dict:
    if begin:
        date = date + 'T00:00:00'
    else:
        date = date + 'T23:59:00'
    return {
        'type': 'xsd:dateTime',
        "@value": datetime.strptime(date,'%m/%d/%YT%H:%M:%S').strftime('%Y-%m-%dT%H:%M:%S')
    }


def create_subject_dictionary(forge: KnowledgeGraphForge, subject_dict: Dict) -> Optional[Dict]:
    """The minimum requirement is the species label"""
    species_info = forge.resolve(subject_dict['species'], scope="ontology", target="Species", strategy='EXACT_CASE_INSENSITIVE_MATCH')
    if not species_info:
        raise ValueError(f"Species label provided {subject_dict['species']} was not found in our ontologies")
    else:
        species = {'id': species_info.id,
                   'label': species_info.label}
    if 'strain' in subject_dict and subject_dict['strain']:
        strain = forge.resolve(subject_dict['strain'], scope="ontology", target="Species", strategy='EXACT_CASE_INSENSITIVE_MATCH')
        if not strain:
            raise ValueError(f"Strain label provided {subject_dict['strain']} was not found in our ontologies")
    else:
        strain = None
    if 'sex' in subject_dict and subject_dict['sex']:
        sex_label = subject_dict['sex'].lower()
        if sex_label == 'male':
            sex_id = "http://purl.obolibrary.org/obo/PATO_0000384"
        elif sex_label == 'female':
            sex_id = "http://purl.obolibrary.org/obo/PATO_0000383"
        else:
            raise ValueError(f"Sex label provided {subject_dict['sex']} is incorrect, it should be male or female")
        sex = {
                "id": sex_id,
                "label": sex_label
        }
    else:
        sex = None
    if 'age' in subject_dict and subject_dict['age']:
        age = subject_dict['age']
        if "PN" in age:
            days = age.split('PN')[-1]
        elif "P" in age:
            days = age.split('P')[-1]
        else:
            raise ValueError(f"Only post-natal age is supported/expected, value should start with PN or P.")
        age = {
            "period": "Post-natal",
            "unitCode": "days",
            "value": int(days)
        }
    else:
        age = None
    
    return {'type': 'Subject',
            'species': species,
            'strain': strain,
            'sex': sex,
            'age': age,
            'name': subject_dict['id'],
            'comment': subject_dict['comment']}