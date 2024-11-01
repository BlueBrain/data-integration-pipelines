from typing import Dict

from importlib_metadata import version
import platform
import jwt
import time

from src.helpers import Deployment


def get_contribution(token, deployment: Deployment) -> Dict:
    decoded = jwt.decode(
        token, "secret", audience="https://slack.com",
        options={"verify_signature": False}
    )

    base = deployment.value if deployment != Deployment.AWS else Deployment.PRODUCTION.value

    agent_id = f"{base}/realms/bbp/users/{decoded['preferred_username']}"

    # TODO contribution from service account?

    return {
        "type": "Contribution",
        "agent": {
            "id": agent_id,
            "type": ["Agent", "Person"],
            # "givenName": decoded["given_name"],
            # "familyName": decoded["family_name"],
            # "name": decoded["name"],
            # "email": decoded["email"]
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
