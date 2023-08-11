import requests
from urllib.parse import urlparse
import json


class HypothesisClient(object):

    def __init__(self, client_id, client_secret, jwt_client_id,
                 jwt_client_secret, authority, service):
        self.authority = authority
        self.client_id = client_id
        self.client_secret = client_secret
        self.jwt_client_id = jwt_client_id
        self.jwt_client_secret = jwt_client_secret
        self.service = service

    def create_account(self, username, email, display_name=None):
        """
        Create an account on the Hypothesis service.

        This creates an account on the Hypothesis service, in the publisher's
        namespace, with the given `username` and `email`.
        """
        data = {'authority': self.authority,
                'username': username,
                'email': email,
                'display_name': display_name,
                }
        return self._make_request('POST', 'users', data)

    def add_to_organization_group(self, username: str, group_id: str):
        return self._make_request("POST", f"groups/{group_id}/members/{username}", {})

    def get_group(self, group_id: str):
        return self._make_request("GET", f"groups/{group_id}", {})

    def get_group_members(self, group_id: str):
        return self._make_request("GET", f"groups/{group_id}/members", {})

    def get_groups(self):
        return self._make_request("GET", f"groups", {})

    def create_group(self, name: str, description: str):
        return self._make_request("POST", f"groups", {"name": name, "description": description})

    def update_account(self, username, email=None, display_name=None):
        data = {}
        if email is not None:
            data['email'] = email
        if display_name is not None:
            data['display_name'] = display_name
        return self._make_request('PATCH', 'users/{}'.format(username), data)

    def _make_request(self, method, path, data):
        auth = (self.client_id, self.client_secret)
        rsp = requests.request(
            method,
            '{}/api/{}'.format(self.service, path),
            data=json.dumps(data),
            auth=auth,
            # Don't verify SSL certificates if posting to localhost.
            verify=urlparse(self.service).hostname != 'localhost')
        print(f"rsp is {rsp}")
        rsp.raise_for_status()
        return rsp.json()

# THESE ARE NOT TO BE CHECKED IN!!!
user_client_id = ""
user_client_secret = ""
jwt_client_id = ""
jwt_client_secret = ""

authority = "isample.xyz"
service = "https://hypothes.is"
client = HypothesisClient(user_client_id, user_client_secret, jwt_client_id, jwt_client_secret, authority, service)

# example usage for creating an account -- ORCID must have '-' characters removed:
#client.create_account("0000000000000000", "email@server", "User full name")

# example of how to query for group members
# members = client.get_group_members()

# example of how to add a user to a group -- must be executed after account is created
# add_rsp = client.add_to_organization_group("acct:0000000000000000@isample.xyz", <group id>)

# response = client.get_groups()
