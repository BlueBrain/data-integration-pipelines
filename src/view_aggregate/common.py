import requests
import json
from typing import Dict


class DeltaException(Exception):
    body: Dict
    status_code: int

    def __init__(self, body: Dict, status_code: int):
        self.body = body
        self.status_code = status_code


class DeltaUtils:

    @staticmethod
    def make_header(token) -> Dict:
        """
        Makes request headers for delta API calls
        @param token: the authentication token to put inside the headers
        @type token:  str
        @return: the headers
        @rtype: Dict
        """
        return {
            "mode": "cors",
            "Content-Type": "application/json",
            "Accept": "application/ld+json, application/json",
            "Authorization": "Bearer " + token
        }

    @staticmethod
    def check_response(response: requests.Response) -> Dict:
        """
        Checks the status code of a response and returns
        @param response: the response to check
        @type response: requests.Response
        @return: the response body parsed as json
        @rtype: Dict
        """
        if response.status_code not in range(200, 229):
            raise DeltaException(body=json.loads(response.text), status_code=response.status_code)
        return json.loads(response.text)
