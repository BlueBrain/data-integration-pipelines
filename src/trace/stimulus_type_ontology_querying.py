"""
Queries the stimulus type ontology.
Returns two dictionaries:
1. Id to Label dictionary of all stimulus types that are a subclass of ElectricalStimulus
2. Id to Label dictionary of all stimulus types that are a subclass of SingleCellProtocolStimulus
"""
from typing import Tuple, Dict

from urllib.parse import quote_plus
import requests

from src.helpers import DEFAULT_SPARQL_VIEW, authenticate_from_parser_arguments
from src.trace.arguments import trace_command_line_args


def stimulus_type_ontology(deployment_str: str, token: str) -> Tuple[Dict, Dict]:

    org, project = "neurosciencegraph", "datamodels"
    endpoint = f"{deployment_str}/views/{org}/{project}/{quote_plus(DEFAULT_SPARQL_VIEW)}/sparql"

    def subclass_of(type_):
        query = """
        SELECT ?id ?label WHERE {
            ?id <http://www.w3.org/2000/01/rdf-schema#subClassOf>+ <%s> ;
                <http://www.w3.org/2000/01/rdf-schema#label> ?label
        }
        """ % type_

        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/sparql-query"
        }

        response_sparql = requests.post(
            url=endpoint,
            headers=headers,
            data=query
        )

        response_sparql.raise_for_status()
        response_body = response_sparql.json()
        response_list = response_body["results"]["bindings"]
        return dict((i["id"]["value"], i["label"]["value"]) for i in response_list)

    return (
        subclass_of("http://bbp.epfl.ch/neurosciencegraph/ontologies/stimulustypes/SingleCellProtocolStimulus"),
        subclass_of("https://bbp.epfl.ch/ontologies/core/bmo/ElectricalStimulus")
    )


if __name__ == "__main__":
    parser = trace_command_line_args()

    received_args, leftovers = parser.parse_known_args()

    deployment, auth_token = authenticate_from_parser_arguments(received_args)

    b = stimulus_type_ontology(deployment_str=deployment.value, token=auth_token)

    # from src.trace.stimulus_type_ontology_loading import single_cell_stimulus_type_id_to_label
    # a = single_cell_stimulus_type_id_to_label
    # print(set(b.keys()).difference(set(a.keys())))
    # print(set(b.values()).difference(set(a.values())))
