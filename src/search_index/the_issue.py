import getpass
from urllib.parse import quote_plus

import requests

from src.helpers import Deployment
from src.search_index.search_index_discrepancy import _make_sp_query


def compare_sp_weirdness(token: str, endpoint: str):
    org, project = "public", "hippocampus"
    type_ = "https://bbp.epfl.ch/ontologies/core/bmo/ExperimentalTrace"

    def _make_sp_query4():
        q = """
                    SELECT ?id
                    WHERE {
                        ?id a <%s> .
                        ?id <https://neuroshapes.org/annotation>/<https://neuroshapes.org/hasBody>/<http://www.w3.org/2000/01/rdf-schema#label>  "Curated" .


                        OPTIONAL {  
                            ?id a <https://bbp.epfl.ch/ontologies/core/bmo/ExperimentalTrace> ;
                            <http://schema.org/distribution>/<http://schema.org/encodingFormat> ?encodingFormat .
                        } 

                        FILTER(!bound(?encodingFormat) || ?encodingFormat = "application/nwb")  
                    }

                    LIMIT 10000
                """

        return q % type_

    def _make_sp_query3():
        q = """
            SELECT ?id
            WHERE {
              ?id a <%s> .
              ?id <https://neuroshapes.org/annotation>/<https://neuroshapes.org/hasBody>/<http://www.w3.org/2000/01/rdf-schema#label>  "Curated" .
                FILTER NOT EXISTS {
                ?id   a <https://neuroshapes.org/Trace>       ;
                      <http://schema.org/distribution>/<http://schema.org/encodingFormat> ?encodingFormat .
                FILTER (?encodingFormat != "application/nwb")
              }
            }
            LIMIT 10000
        """

        return q % type_

    def _make_sp_query2():
        q = """
            SELECT ?id
            WHERE {
              ?id a <%s> .
              ?id <https://neuroshapes.org/annotation>/<https://neuroshapes.org/hasBody>/<http://www.w3.org/2000/01/rdf-schema#label>  "Curated" .
              ?id <http://schema.org/distribution>/<http://schema.org/encodingFormat> "application/nwb"
            }
            LIMIT 10000
        """

        return q % type_

    h = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/sparql-query"
    }
    default_sp_view = quote_plus("https://bluebrain.github.io/nexus/vocabulary/defaultSparqlIndex")
    endpoint_sp = f"{endpoint}/views/{org}/{project}/{default_sp_view}/sparql"

    queries = [
        _make_sp_query(brain_regions=None, type_=type_),  # No filter on encoding format
        _make_sp_query2(),  # What should be - but cannot be expressed like that
        _make_sp_query3(),  # The current state
        _make_sp_query4()   # The fix
    ]

    responses = [
        requests.post(endpoint_sp, headers=h, data=q_v).json()
        for q_v in queries
    ]

    res = [
        set(i["id"]['value'] for i in resp['results']['bindings'])
        for resp in responses
    ]

    print([len(res_i) for res_i in res])


if __name__ == "__main__":
    t = getpass.getpass()
    compare_sp_weirdness(token=t, endpoint=Deployment.AWS.value)
