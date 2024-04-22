from typing import Dict

from importlib_metadata import version
import platform
import jwt
import time


def get_contribution(token, production=True) -> Dict:
    decoded = jwt.decode(token, "secret", audience="https://slack.com",
                         options={"verify_signature": False})

    contribution_id_base = (
        "https://staging.nise.bbp.epfl.ch/nexus/v1/realms/bbp/users/{}"
        if not production else "https://bbp.epfl.ch/nexus/v1/realms/bbp/users/{}"
    )

    return {
        "type": "Contribution",
        "agent": {
            "id": contribution_id_base.format(decoded['preferred_username']),
            "type": ["Agent", "Person"],
            "givenName": decoded["given_name"],
            "familyName": decoded["family_name"],
            "name": decoded["name"],
            "email": decoded["email"]
        }
    }


def get_generation() -> Dict:
    started_at_time = time.strftime("%Y-%m-%dT%H:%M:%S")

    software_agent = {
        "type": ["Agent", "SoftwareAgent"],
        "softwareSourceCode": {
            "type": "SoftwareSourceCode",
            "codeRepository": {
                "id": "https://github.com/BlueBrain/NeuroM"
            },
            "programmingLanguage": "Python",
            "runtimePlatform": platform.python_version(),
            "version": version("neurom")

        },
        "name": "NeuroM",
        "description":
            "NeuroM is a Python toolkit for the analysis and processing of neuron morphologies."
    }

    generation = {
        "type": "Generation",
        "activity": {
            "type": "Activity",
            "startedAtTime": {
                "@value": started_at_time,
                "@type": "xsd:dateTime"
            },
            "endedAtTime": {
                "@value": time.strftime("%Y-%m-%dT%H:%M:%S"),
                "@type": "xsd:dateTime"
            },
            "wasAssociatedWith": software_agent
        }
    }

    return generation
